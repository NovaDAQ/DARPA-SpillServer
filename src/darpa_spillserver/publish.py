"""!
@file src/darpa_spillserver/publish.py
@brief Cron-driven dump -> shift -> upload -> status pipeline
       (darpa-spill-publish) for the NOvA gateway.

Cron-driven publication of spill dumps (``darpa-spill-publish``).

On the gateway, cron runs this tool to turn the archive behind a running
``darpa-spill-server`` into published files, one stage feeding the next:

    dump  ->  shift  ->  upload  ->  status

* **dump** runs ``darpa-spill-dump --split hour|day`` against the server for
  each configured stream (``numi``, ``all``, ``booster``), writing CSV and
  JSON from one fetch into ``{data_root}/raw/{mon}/{year}-{mon}-{day}/``.
* **shift** blurs every file with ``darpa-spill-shift apply`` into the
  matching ``shifted/`` directory, with the ``dts1`` token of the month the
  *data* belongs to, so the hourly and daily copies of an event are shifted
  identically and cannot be averaged against each other (plan D7).
* **upload** copies raw files to ``fdp.raw_dest`` and shifted files to
  ``fdp.shifted_dest`` with ``fdp-put --skip-existing``, run inside the FDP
  environment script.  A file counts as uploaded only once ``fdp-stat``
  shows its size and adler32 on FDP; an incomplete copy left by a failed
  PUT (``size -1``) is removed with ``fdp-rm -f`` and sent again.
* **status** writes ``index.html`` and ``status.json`` for the server's
  ``/status/`` mount.

    darpa-spill-publish hourly                # cron, HH:10
    darpa-spill-publish daily                 # cron, 00:30
    darpa-spill-publish backfill --start 2026-09-17 --end 2026-10-21
    darpa-spill-publish -n hourly --hour 2026-10-22T13   # print, do nothing

The design record is ``docs/deployment/GATEWAY_DEPLOYMENT_PLAN.md``; the
decisions it numbers (D1-D18) are cited below.

**State.**  Every file's progress is a row in a SQLite database,
``{state_dir}/publish.db``, so any stage can fail and be finished by a later
run: ``hourly`` catches up recent hours that are not complete and ``upload``
retries anything not yet recorded as uploaded.  A raw file that is recorded,
or that is found complete on disk, is never dumped again.  Hourly files are
never replaced (D10), and a file once uploaded has to stay identical to its
copy on FDP, where dCache forbids overwriting (D9).

**Labels and paths come from darpa-spill-dump.**  Pieces, labels and file
names are computed with :func:`~darpa_spillserver.dump.split_range` and
:func:`~darpa_spillserver.dump.segment_path`, the code the dump itself uses,
so the publisher's idea of where a file is cannot drift from where it was
written.  That matters on daylight-saving days, which are 23 or 25 hours
long and whose repeated autumn hour is its own file, told apart by its UTC
offset (D2, D4).

**Ingest gate.**  Before an hour or day is dumped, every enabled source must
have been read past its end: its newest event, or the start of its last
complete poll (so a quiet beam still passes).  Both come from
``/api/sources``, which answers in seconds; ``/api/status`` takes minutes on
the gateway and is avoided (D18).  A source that is behind is waited for,
then the run gives up with exit 75 and the next one tries again.

**Locks.**  Each job holds ``{state_dir}/JOB.lock``; hourly, daily and
backfill also share ``dump.lock`` around dump and shift, and uploads hold
``upload.lock``.  Every subprocess has a timeout and runs in its own process
group, which is killed whole when it expires.

**Exit status.**  0 success; 1 failure; 2 usage or configuration error;
3 partial (some files failed); 75 temporary (ingest not ready, the same
job is already running, or the dump lock is busy).

@par Timestamp blur (shift stage) in brief
The transform itself (dts1, keyed HMAC-SHA256 offsets per
``(source, event_number)``) lives in :mod:`darpa_spillserver.shift` and is
specified in ``docs/TIMESTAMP_SHIFT.md``.  This module only decides *which
token* blurs *which file* and keeps the blurred and unblurred copies apart:
- One token per **data month** (local time, D2), named
  ``shift.token_name`` in ``shift.token_dir`` (D7, D8); see
  Publisher.token_path(), Publisher.ensure_token() and Publisher.shift().
- Every copy of an event (hourly file, daily file, a re-run) is blurred
  with the same token, hence with the same offset, so comparing or
  averaging copies reveals nothing (D7).
- Raw files live under ``raw/`` and go only to ``fdp.raw_dest`` (private);
  shifted files live under ``shifted/`` with a ``.shifted.`` infix and go
  only to ``fdp.shifted_dest`` (public); see Publisher.templates() and
  Publisher.remote_dir().
- A file is shifted exactly when its row has ``dumped_at`` and no
  ``shifted_at``; the shift is deterministic, so redoing it after a crash
  rewrites a byte-identical file.
"""

from __future__ import annotations

import argparse
import base64
import contextlib
import copy
import html
import json
import logging
import os
import re
import shlex
import shutil
import socket
import sqlite3
import ssl
import subprocess
import sys
import time
import zlib
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows; the gateway is Linux
    fcntl = None  # type: ignore[assignment]

from . import __version__
from .config import ENV_PREFIX
from .dump import MONTH_ABBREVIATIONS, label_fields, segment_path, split_range
from .novatime import (
    TICKS_PER_SECOND,
    NovaTimeError,
    datetime_to_nova,
    get_timezone,
    nova_to_datetime,
    parse_instant,
    parse_range,
)

__all__ = [
    "main",
    "build_parser",
    "load_settings",
    "Publisher",
    "State",
    "PublishConfigError",
    "DEFAULTS",
    "count_rows",
    "parse_klist",
    "jwt_expiry",
    "render_html",
    "adler32_file",
    "parse_fdp_stat",
]

## Module logger; :func:`_setup_logging` gives it its stderr handler.
log = logging.getLogger("darpa_spillserver.publish")

## Program name: mail subjects, log lines and the HTTP User-Agent.
PROG = "darpa-spill-publish"

## Environment variables read by this tool: ``DARPA_SPILL_PUBLISH_*``.  They
## sit under the server's prefix but name none of its sections, so the
## server's loader ignores them.
ENV_PUBLISH = ENV_PREFIX + "PUBLISH_"
## ``DARPA_SPILL_PUBLISH_CONFIG``: the YAML file when ``--config`` is not
## given.
ENV_CONFIG = ENV_PUBLISH + "CONFIG"

## Used when neither ``--config`` nor ``DARPA_SPILL_PUBLISH_CONFIG`` is given
## and the file exists.
DEFAULT_CONFIG_FILE = "~/DARPA/publish/publish.yaml"

## Exit status: success.
EXIT_OK = 0
## Exit status: failure (errors and nothing done, or a failed check).
EXIT_FAILURE = 1
## Exit status: usage or configuration error.
EXIT_USAGE = 2
## Exit status: partial; some files failed, the rest went through.
EXIT_PARTIAL = 3
## Exit status: temporary (sysexits EX_TEMPFAIL): ingest not ready, the
## job already running, or the dump lock busy.  The next run retries.
EXIT_TEMPFAIL = 75

## One hour of the NOvA clock.  Hours are stepped in real time, as
## :func:`split_range` does, so this is exact across a changeover.
HOUR = 3600 * TICKS_PER_SECOND

## The split unit each kind of file is dumped with.
UNITS = {"hourly": "hour", "daily": "day"}

## Files per ``fdp-put`` call, which bounds the length of its command line.
UPLOAD_BATCH = 200

## Seconds a job waits for another job's upload stage before leaving its
## files to the next ``upload`` run.
UPLOAD_LOCK_WAIT = 600.0

## Default seconds hourly, daily and backfill wait for one another's dump
## stage (``locks.dump_wait``).  They share the ``dump`` lock because
## darpa-spill-dump writes each piece to a fixed ``FILE.part``: two writers of
## one label would interleave there.  Make it longer than one backfill day
## takes, or cron runs give up while a history backfill is running.
DUMP_LOCK_WAIT = 1800.0

## Seconds between SIGTERM and SIGKILL for a subprocess past its timeout.
KILL_GRACE = 10.0

## Exit status of :data:`ENV_WRAPPER` when the environment script is missing
## or unreadable.
EXIT_ENV_SCRIPT = 125

## ``fdp-put`` exit codes (Fermi-Data-Platform-Tools), plus the wrapper's.
FDP_EXIT = {0: "ok", 4: "authentication", 6: "exists", 7: "network/TLS",
            9: "checksum mismatch", 12: "partial",
            EXIT_ENV_SCRIPT: "env script unreadable", 124: "timed out"}

## ``fdp-stat`` exit status for a path that does not exist.
FDP_STAT_NOT_FOUND = 5

## Bytes per read when checksumming a local file.
ADLER32_CHUNK = 1 << 20

## Values in the credentials ``.env`` that mean "not filled in yet".
PLACEHOLDERS = ("", "CHANGEME")

## Runs as ``bash -c`` so the FDP tools see the environment their install
## needs; ``"$@"`` is the real command, so nothing is re-parsed by a shell.
## The script's own exit status is ignored (a last line such as
## ``[ -d X ] && ...`` may be false); a script that cannot be read exits
## :data:`EXIT_ENV_SCRIPT` with a message.  Its stderr is kept.
ENV_WRAPPER = ('[ -r "$ENV_SCRIPT" ] || { echo "cannot read env script '
               '$ENV_SCRIPT" >&2; exit 125; }; '
               'source "$ENV_SCRIPT" >/dev/null; exec "$@"')

## Subcommands that take a job lock (``check`` and ``status`` included).
JOBS = ("hourly", "daily", "backfill", "upload", "token", "creds", "status",
        "check")

## How many runs per job the state database keeps.
RUNS_KEPT = 500


class PublishConfigError(ValueError):
    """!@brief Configuration is missing, malformed, or contradictory.

    Raised by the settings loader; :func:`main` turns it into exit 2.
    """


# ------------------------------------------------------------- configuration


## Every setting and its default.  The YAML file, ``.env``, the environment
## and ``--set`` may only name keys that appear here (``streams`` excepted,
## whose keys are the stream names), and values are coerced to the type of
## the default, as in :mod:`darpa_spillserver.config`.
DEFAULTS: Dict[str, Any] = {
    "server": {
        "url": "https://novadaq-near-gateway-01.fnal.gov:8443",
        "ca_bundle": "/home/novadaq/DARPA/DARPA-SpillServer/tls/cert.pem",
        "insecure": False,
        "timeout": 120.0,
    },
    "timezone": "America/Chicago",
    "data_root": "/data/DARPA-spilldata",
    "formats": ["csv", "json"],
    "prefix": "nd",
    "streams": {
        "numi": {"signals": ["$74", "$A9"]},
        "all": {},
        "booster": {"types": ["BNB", "BNB_TCLK"]},
    },
    "hourly": {"catchup_hours": 24},
    "daily": {"catchup_days": 3},
    "ingest_check": {"enabled": True, "wait": 300.0, "poll": 30.0,
                     "settle": 60.0},
    "shift": {
        "token_dir": "/home/novadaq/anorman-fdp/tokens",
        "token_name": "dts1-{year}-{month}.token",
        "dist": "gauss",
        "sigma": "50us",
        "cut": "3sigma",
    },
    "fdp": {
        "enabled": True,
        "env_script": "/home/novadaq/anorman-fdp/env.sh",
        "ca_path": "/home/novadaq/anorman-fdp/certificates",
        "token_file": "/home/novadaq/anorman-fdp/tokens/"
                      "fdp_bt_u46550_amsc_novaendwrite",
        "raw_dest": "/amsc/novaend/raw",
        "shifted_dest": "/amsc/public/novaend/shifted",
        "parallel": 4,
        "timeout": 3600.0,
        "extra_args": [],
    },
    "credentials": {
        "enabled": True,
        "krb5ccname": "FILE:/home/novadaq/anorman-fdp/tokens/krb5cc_anorman",
        "principal": "anorman@FNAL.GOV",
        "renew_below_hours": 12.0,
        "warn_renew_until_hours": 24.0,
        "env_file": "/home/novadaq/anorman-fdp/.env",
        "htgettoken_args": ["-a", "htvaultprod.fnal.gov", "-i", "amsc",
                            "-r", "novaendwrite"],
        "sso_cookie_file": "/home/novadaq/anorman-fdp/tokens/fermi_sso.cookies",
        "min_token_seconds": 3600,
        "timeout": 300.0,
        "sso_backoff_hours": 12.0,
    },
    "paths": {
        "state_dir": "/data/DARPA-spilldata/publish/state",
        "status_dir": "/data/DARPA-spilldata/publish/status",
        "log_dir": "/data/DARPA-spilldata/publish/logs",
        "dotenv": "",
    },
    "alerts": {
        "enabled": True,
        "email": ["anorman@fnal.gov"],
        "mail_command": "mailx",
        "min_interval": 3600.0,
        "max_interval": 86400.0,
        "disk_warn_percent": 85.0,
        "stale_job_hours": 3.0,
    },
    "tools": {"dump": "", "shift": "", "dump_timeout": 7200.0,
              "shift_timeout": 3600.0},
    "locks": {"dump_wait": DUMP_LOCK_WAIT},
}

## Short environment names, besides ``DARPA_SPILL_PUBLISH_<SECTION>_<KEY>``.
ENV_ALIASES = {
    "SERVER": ("server", "url"),
    "CA_BUNDLE": ("server", "ca_bundle"),
    "INSECURE": ("server", "insecure"),
    "STATE_DIR": ("paths", "state_dir"),
    "STATUS_DIR": ("paths", "status_dir"),
    "LOG_DIR": ("paths", "log_dir"),
    "DOTENV": ("paths", "dotenv"),
}

## What a stream may select by, each passed to darpa-spill-dump repeatedly.
STREAM_KEYS = {"signals": "--signal", "types": "--type", "sources": "--source"}


def _env_names() -> Dict[str, Tuple[str, ...]]:
    """!@brief Map every recognised environment variable onto its setting.

    Long form ``DARPA_SPILL_PUBLISH_<SECTION>_<KEY>`` for every key of
    :data:`DEFAULTS` (``streams`` excepted: stream names are free-form), plus
    the short aliases of :data:`ENV_ALIASES`.
    @return ``{variable name: (section, key)}`` or ``{name: (key,)}`` for a
            top-level setting.
    """
    names: Dict[str, Tuple[str, ...]] = {}
    for key, default in DEFAULTS.items():
        if key == "streams":
            continue
        if isinstance(default, dict):
            for sub in default:
                names[ENV_PUBLISH + (key + "_" + sub).upper()] = (key, sub)
        else:
            names[ENV_PUBLISH + key.upper()] = (key,)
    for short, path in ENV_ALIASES.items():
        names[ENV_PUBLISH + short] = path
    return names


def _coerce(value: Any, default: Any, where: str) -> Any:
    """!@brief Coerce a YAML, environment or command-line value to
           *default*'s type.

    Booleans accept 1/0, true/false, yes/no, on/off; lists accept a YAML flow
    list (``[a, b]``) or a comma-separated string.
    @param value   the value as read (string from env/CLI, any YAML type).
    @param default the default from :data:`DEFAULTS`, which fixes the type.
    @param where   dotted setting name, for error messages.
    @return the value converted to the type of *default*.
    @throws PublishConfigError if the value cannot be converted.
    """
    # bool before int: bool is a subclass of int.
    if isinstance(default, bool):
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
        raise PublishConfigError("{} must be a boolean, got {!r}".format(
            where, value))
    if isinstance(default, int):
        try:
            return int(value)
        except (TypeError, ValueError):
            raise PublishConfigError("{} must be an integer, got {!r}".format(
                where, value)) from None
    if isinstance(default, float):
        try:
            return float(value)
        except (TypeError, ValueError):
            raise PublishConfigError("{} must be a number, got {!r}".format(
                where, value)) from None
    if isinstance(default, list):
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            if text.startswith("["):
                return _coerce(_yaml_value(text, where), default, where)
            return [part.strip() for part in text.split(",") if part.strip()]
        if isinstance(value, (list, tuple)):
            return [str(item) for item in value]
        raise PublishConfigError("{} must be a list, got {!r}".format(
            where, value))
    return "" if value is None else str(value)


def _yaml_value(text: str, where: str) -> Any:
    """!@brief Parse one YAML value given as text (e.g. a ``--set`` list).
    @param text  YAML source.
    @param where where it came from, for error messages.
    @return the parsed value.
    @throws PublishConfigError if the text is not valid YAML.
    """
    import yaml
    try:
        return yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise PublishConfigError("{}: cannot parse {!r}: {}".format(
            where, text, exc)) from None


def _stream_spec(name: str, spec: Any, origin: str) -> Dict[str, List[str]]:
    """!@brief Validate and normalise one stream definition.
    @param name   stream name (becomes part of file names).
    @param spec   mapping of ``signals``/``types``/``sources`` lists, or None
                  for an unfiltered stream.
    @param origin where it came from, for error messages.
    @return ``{key: [values]}`` with only :data:`STREAM_KEYS` keys.
    @throws PublishConfigError on a non-mapping or an unknown key.
    """
    if spec is None:
        return {}
    if not isinstance(spec, Mapping):
        raise PublishConfigError("stream {!r} in {} must be a mapping of "
                                 "signals/types/sources".format(name, origin))
    out: Dict[str, List[str]] = {}
    for key, value in spec.items():
        if key not in STREAM_KEYS:
            raise PublishConfigError(
                "unknown key {!r} in stream {!r} of {} (expected one of: "
                "{})".format(key, name, origin, ", ".join(STREAM_KEYS)))
        out[key] = _coerce(value, [], "streams.{}.{}".format(name, key))
    return out


def _merge(cfg: Dict[str, Any], data: Mapping[str, Any], origin: str) -> None:
    """!@brief Merge a nested mapping into *cfg*, refusing unknown keys.
    @param cfg    settings being built (modified in place).
    @param data   mapping from one source (YAML, ``.env``, environment, CLI).
    @param origin name of that source, for error messages.
    @throws PublishConfigError on an unknown section/key or a bad value.
    """
    for key, value in data.items():
        if key not in DEFAULTS:
            raise PublishConfigError(
                "unknown setting {!r} in {} (expected one of: {})".format(
                    key, origin, ", ".join(sorted(DEFAULTS))))
        default = DEFAULTS[key]
        if key == "streams":
            if not isinstance(value, Mapping):
                raise PublishConfigError("streams in {} must be a mapping"
                                         .format(origin))
            # A stream list replaces the defaults, so a stream can be dropped.
            cfg["streams"] = {str(name): _stream_spec(str(name), spec, origin)
                              for name, spec in value.items()}
        elif isinstance(default, dict):
            if value is None:
                continue
            if not isinstance(value, Mapping):
                raise PublishConfigError("section {!r} in {} must be a "
                                         "mapping".format(key, origin))
            for sub, subvalue in value.items():
                if sub not in default:
                    raise PublishConfigError(
                        "unknown setting {!r} in section {!r} of {} (expected "
                        "one of: {})".format(sub, key, origin,
                                             ", ".join(sorted(default))))
                cfg[key][sub] = _coerce(subvalue, default[sub],
                                        "{}.{}".format(key, sub))
        else:
            cfg[key] = _coerce(value, default, key)


def _nest(path: Sequence[str], value: Any) -> Dict[str, Any]:
    """!@brief Build ``{a: {b: value}}`` from the path ``(a, b)``.
    @param path  setting path, outermost first.
    @param value the leaf value.
    @return the nested mapping, ready for :func:`_merge`.
    """
    out: Dict[str, Any] = {path[-1]: value}
    for part in reversed(path[:-1]):
        out = {part: out}
    return out


def _parse_set(text: str) -> Tuple[Tuple[str, ...], Any]:
    """!@brief Parse one ``--set section.key=value``.
    @param text the option argument.
    @return ``(path tuple, value string)``.
    @throws PublishConfigError if there is no ``=`` or no key.
    """
    name, sep, value = text.partition("=")
    path = tuple(p for p in name.strip().split(".") if p)
    if not sep or not path:
        raise PublishConfigError("--set wants SECTION.KEY=VALUE, got {!r}"
                                 .format(text))
    return path, value


def _apply_set(cfg: Dict[str, Any], path: Tuple[str, ...], value: Any,
               origin: str) -> None:
    """!@brief Apply one parsed ``--set`` to *cfg*.

    ``streams.NAME`` takes a whole stream spec as YAML and
    ``streams.NAME.KEY`` one selector list; anything else goes through
    :func:`_merge` like the other sources.
    @param cfg    settings being built (modified in place).
    @param path   dotted path from :func:`_parse_set`.
    @param value  the value text.
    @param origin name of the source, for error messages.
    @throws PublishConfigError on an unknown key or a bad value.
    """
    if path[0] == "streams" and len(path) >= 2:
        # streams.NAME (whole spec, as YAML) or streams.NAME.KEY (a list).
        streams = dict(cfg["streams"])
        if len(path) == 2:
            parsed = _yaml_value(value, "--set") if isinstance(value, str) \
                else value
            streams[path[1]] = _stream_spec(path[1], parsed, origin)
        else:
            spec = dict(streams.get(path[1], {}))
            spec.update(_stream_spec(path[1], {path[2]: value}, origin))
            streams[path[1]] = spec
        cfg["streams"] = streams
        return
    _merge(cfg, _nest(path, value), origin)


def read_dotenv(path: str) -> Dict[str, str]:
    """!@brief Read ``KEY=VALUE`` lines; ``#`` comments, ``export`` and quotes
           allowed.

    One level of matching single or double quotes is stripped; no variable
    expansion is done.  Lines without ``=`` are ignored.
    @param path file name (``~`` expanded).
    @return ``{key: value}``.
    @throws OSError if the file cannot be read.
    """
    values: Dict[str, str] = {}
    with open(os.path.expanduser(path), encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export "):].strip()
            key, sep, value = line.partition("=")
            if not sep:
                continue
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]
            values[key.strip()] = value
    return values


def load_settings(args: argparse.Namespace,
                  environ: Mapping[str, str]) -> Dict[str, Any]:
    """!@brief Merge defaults, YAML, ``.env``, environment and command line.

    Precedence is command line > environment > ``.env`` > YAML > defaults.
    ``paths.dotenv`` itself is resolved from the command line, the
    environment and the YAML file before ``.env`` is read.
    @param args    parsed command line (``config``, ``set``, ``set_after``).
    @param environ the process environment.
    @return the merged, validated settings; ``_source`` names the YAML file
            used, or ``(defaults)``.
    @throws PublishConfigError on any unreadable or invalid source.
    """
    cfg = copy.deepcopy(DEFAULTS)

    path = getattr(args, "config", None) or environ.get(ENV_CONFIG)
    if path:
        path = os.path.expanduser(path)
        if not os.path.isfile(path):
            raise PublishConfigError("config file not found: {}".format(path))
    elif os.path.isfile(os.path.expanduser(DEFAULT_CONFIG_FILE)):
        path = os.path.expanduser(DEFAULT_CONFIG_FILE)
    if path:
        import yaml
        try:
            with open(path, encoding="utf-8") as handle:
                data = yaml.safe_load(handle)
        except (OSError, yaml.YAMLError) as exc:
            raise PublishConfigError("cannot read {}: {}".format(path, exc))
        if data is not None:
            if not isinstance(data, Mapping):
                raise PublishConfigError("{} must hold a mapping".format(path))
            _merge(cfg, data, path)

    cli = [_parse_set(text) for text in
           list(getattr(args, "set", None) or [])
           + list(getattr(args, "set_after", None) or [])]
    names = _env_names()
    env = [(names[name], environ[name]) for name in sorted(names)
           if name in environ]

    # paths.dotenv may itself be set in the environment or by --set,
    # which are merged only after .env; look for it there first.
    dotenv = cfg["paths"]["dotenv"]
    for setting, value in env + cli:
        if setting == ("paths", "dotenv"):
            dotenv = value
    if dotenv:
        try:
            values = read_dotenv(dotenv)
        except OSError as exc:
            raise PublishConfigError("cannot read paths.dotenv {}: {}".format(
                dotenv, exc))
        # Only this tool's own names are taken from .env; the file may
        # hold others (it can be shared), which are ignored.
        for name in sorted(values):
            if name in names:
                _merge(cfg, _nest(names[name], values[name]), dotenv)

    for setting, value in env:
        _merge(cfg, _nest(setting, value), "the environment")
    for setting, value in cli:
        _apply_set(cfg, setting, value, "--set")

    _validate(cfg)
    cfg["_source"] = path or "(defaults)"
    return cfg


def _validate(cfg: Dict[str, Any]) -> None:
    """!@brief Cross-check merged settings and normalise ``formats``.
    @param cfg merged settings (``formats`` de-duplicated in place).
    @throws PublishConfigError on an unknown timezone, bad formats, no
            streams, a name unusable in file names, negative catch-up
            windows, ``fdp.parallel`` < 1 or a non-http(s) server URL.
    """
    try:
        get_timezone(cfg["timezone"])
    except NovaTimeError as exc:
        raise PublishConfigError("timezone: {}".format(exc))
    formats = cfg["formats"]
    if not formats or any(f not in ("csv", "json") for f in formats):
        raise PublishConfigError("formats must be csv and/or json, got {}"
                                 .format(formats))
    cfg["formats"] = list(dict.fromkeys(formats))
    if not cfg["streams"]:
        raise PublishConfigError("no streams configured")
    for name in list(cfg["streams"]) + [cfg["prefix"]]:
        if not re.match(r"^[A-Za-z0-9][A-Za-z0-9._-]*$", name):
            raise PublishConfigError(
                "{!r} cannot be part of a file name; use letters, digits, "
                "'.', '_' and '-'".format(name))
    if cfg["hourly"]["catchup_hours"] < 0 or cfg["daily"]["catchup_days"] < 0:
        raise PublishConfigError("catch-up windows cannot be negative")
    if cfg["fdp"]["parallel"] < 1:
        raise PublishConfigError("fdp.parallel must be at least 1")
    if not cfg["server"]["url"].startswith(("http://", "https://")):
        raise PublishConfigError("server.url must be an http(s) URL")


# --------------------------------------------------------------- utilities


def _utc_iso(moment: datetime) -> str:
    """!@brief An aware datetime as UTC ISO 8601 to the second.

    All stamps in the state database use this form, so they sort and compare
    correctly as strings.
    @param moment aware datetime.
    @return e.g. ``2026-10-22T18:10:00+00:00``.
    """
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds")


def _from_iso(text: Optional[str]) -> Optional[datetime]:
    """!@brief Parse an ISO 8601 stamp written by :func:`_utc_iso`.
    @param text the stamp, or None/empty.
    @return the datetime, or None if absent or unparsable.
    """
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def count_rows(path: str, fmt: str) -> Optional[int]:
    """!@brief Events in a darpa-spill-dump file.

    CSV: lines that are neither ``#`` comments nor the header.  JSON: the
    ``summary.returned`` count the dump writes last, read from the tail so a
    multi-gigabyte file is not parsed; failing that, ``meta.expected`` or the
    length of ``events``.
    @param path the dump file.
    @param fmt  ``csv`` or ``json``.
    @return the event count, or None if the file cannot be read or parsed.
    """
    try:
        if fmt == "csv":
            lines = 0
            with open(path, "rb") as handle:
                for line in handle:
                    if not line.startswith(b"#") and line.strip():
                        lines += 1
            return max(0, lines - 1)        # less the header line
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            # The summary object is the last thing the dump writes;
            # 64 KiB from the end is ample for it.
            handle.seek(max(0, size - 65536))
            tail = handle.read().decode("utf-8", "replace")
        at = tail.rfind('"summary":')
        if at >= 0:
            try:
                summary, _ = json.JSONDecoder().raw_decode(
                    tail[at + len('"summary":'):].lstrip())
                if isinstance(summary, dict) and "returned" in summary:
                    return int(summary["returned"])
            except ValueError:
                pass
        with open(path, encoding="utf-8") as handle:
            doc = json.load(handle)
        summary = doc.get("summary") or {}
        if "returned" in summary:
            return int(summary["returned"])
        if "expected" in (doc.get("meta") or {}):
            return int(doc["meta"]["expected"])
        return len(doc.get("events") or [])
    except (OSError, ValueError, AttributeError):
        return None


## Formats ``klist`` uses for times: MIT in the C locale (2- or 4-digit
## year), Heimdal, and ISO.
_KLIST_FORMATS = ("%m/%d/%Y %H:%M:%S", "%m/%d/%y %H:%M:%S",
                  "%b %d %H:%M:%S %Y", "%d %b %Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                  "%Y-%m-%d %H:%M:%S")


def _klist_time(text: str) -> Optional[float]:
    """!@brief A ``klist`` time (host local time) as a UNIX timestamp.
    @param text the time as klist prints it, in any of :data:`_KLIST_FORMATS`.
    @return seconds since the epoch, or None if no format matches.
    """
    text = " ".join(text.split())
    for fmt in _KLIST_FORMATS:
        try:
            # Naive: astimezone() takes it as host local time, which
            # is what klist prints.
            return datetime.strptime(text, fmt).astimezone().timestamp()
        except ValueError:
            continue
    return None


def parse_klist(text: str) -> Dict[str, Any]:
    """!@brief The principal, TGT expiry and renew-until from ``klist`` output.

    Times are UNIX seconds, or ``None`` where the line was missing or not
    understood.
    @param text ``klist`` stdout (run with ``LC_ALL=C``).
    @return ``{"principal", "expires", "renew_until"}``.
    """
    info: Dict[str, Any] = {"principal": None, "expires": None,
                            "renew_until": None}
    lines = text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped.lower().startswith(("default principal:", "principal:")):
            info["principal"] = stripped.split(":", 1)[1].strip()
        if "krbtgt/" in stripped and info["expires"] is None:
            # Columns are separated by two or more spaces; single spaces
            # occur inside the times.  The renew line follows the TGT.
            parts = re.split(r"\s{2,}", stripped)
            if len(parts) >= 3:
                info["expires"] = _klist_time(parts[1])
            for follow in lines[index + 1:index + 3]:
                match = re.search(r"renew until\s+(.+)$", follow.strip(),
                                  re.IGNORECASE)
                if match:
                    info["renew_until"] = _klist_time(match.group(1))
                    break
    return info


def jwt_expiry(text: str) -> Optional[int]:
    """!@brief The ``exp`` claim of a JWT, decoded without verifying it.

    Only for reporting how long the FDP bearer token has left; nothing is
    trusted on the strength of it.
    @param text the token file's contents.
    @return ``exp`` in UNIX seconds, or None if it is not a decodable JWT.
    """
    parts = text.strip().split(".")
    if len(parts) < 2:
        return None
    # Restore the base64url padding that JWTs strip.
    payload = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
        return int(claims["exp"])
    except (ValueError, KeyError, TypeError):
        return None


## Shapes of secrets that must never reach a log: JWTs and Vault tokens.
_SECRET_SHAPES = re.compile(r"eyJ[\w-]+\.[\w-]+\.[\w-]*|hv[sb]\.[\w-]{8,}")


def _redact(text: str, secrets: Sequence[str] = ()) -> str:
    """!@brief Mask secrets in text bound for logs, mail or the status page.
    @param text    text that may contain secrets.
    @param secrets literal secrets (e.g. the SSO password) to mask as well as
                   anything shaped like a JWT or Vault token.
    @return the text with each secret replaced by ``***``.
    """
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return _SECRET_SHAPES.sub("***", text)


def _signal_group(proc: subprocess.Popen, signum: int) -> None:
    """!@brief Send *signum* to the process group led by *proc*.

    The child was started with ``start_new_session``, so its PID is its
    process-group ID.  A group that is already gone is ignored.
    @param proc   the child process.
    @param signum signal number.
    """
    try:
        if hasattr(os, "killpg"):
            os.killpg(proc.pid, signum)
        else:  # pragma: no cover - Windows
            proc.kill()
    except (ProcessLookupError, PermissionError):
        pass


def _kill_group(proc: subprocess.Popen) -> Tuple[str, str]:
    """!@brief Stop a timed-out child and its process group; return its output.

    SIGTERM, then SIGKILL after :data:`KILL_GRACE` seconds.
    @param proc the timed-out child.
    @return ``(stdout, stderr)`` collected so far (empty if the pipes had
            to be abandoned).
    """
    import signal
    _signal_group(proc, signal.SIGTERM)
    try:
        out, err = proc.communicate(timeout=KILL_GRACE)
    except subprocess.TimeoutExpired:
        _signal_group(proc, getattr(signal, "SIGKILL", signal.SIGTERM))
        try:
            out, err = proc.communicate(timeout=KILL_GRACE)
        except subprocess.TimeoutExpired:
            # Something outside the group holds the pipes; give up on them.
            proc.kill()
            for pipe in (proc.stdout, proc.stderr):
                if pipe:
                    pipe.close()
            proc.wait()
            out = err = ""
    return out or "", err or ""


def _tail(text: str, lines: int = 5) -> str:
    """!@brief The last non-blank lines of *text*, joined on one line.
    @param text  subprocess output.
    @param lines how many lines to keep.
    @return the lines joined with ``" | "``, for one-line log and DB errors.
    """
    kept = [l for l in (text or "").strip().splitlines() if l.strip()]
    return " | ".join(kept[-lines:])


def http_get_json(url: str, ca_bundle: str, insecure: bool,
                  timeout: float) -> Any:
    """!@brief GET *url* and parse the JSON body.  Tests replace this function.
    @param url       full URL.
    @param ca_bundle CA file to verify the server with; ignored if missing.
    @param insecure  skip TLS verification.
    @param timeout   seconds.
    @return the decoded JSON body.
    @throws httpx.HTTPError on a transport error or a non-2xx status.
    """
    import httpx
    if insecure:
        verify: Any = False
    elif ca_bundle and os.path.isfile(ca_bundle):
        verify = ssl.create_default_context(cafile=ca_bundle)
    else:
        verify = True
    headers = {"User-Agent": "{}/{}".format(PROG, __version__)}
    with httpx.Client(verify=verify, timeout=timeout, headers=headers) as client:
        response = client.get(url)
        response.raise_for_status()
        return response.json()


## Replaced by tests so the ingest wait does not sleep.
_sleep = time.sleep


def adler32_file(path: str) -> str:
    """!
    @brief The adler32 of a file as dCache reports it: 8 lowercase hex digits.

    Read in :data:`ADLER32_CHUNK` pieces, so multi-gigabyte daily files are
    not loaded whole.
    @param path local file.
    @return e.g. ``00620062``.
    """
    # adler32 starts from 1; the mask keeps the result unsigned.
    value = 1
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(ADLER32_CHUNK)
            if not chunk:
                break
            value = zlib.adler32(chunk, value)
    return "{:08x}".format(value & 0xFFFFFFFF)


def _norm_adler32(value: Any) -> Optional[str]:
    """!
    @brief A remote adler32 in the form of :func:`adler32_file`, or ``None``.

    Accepts an optional ``0x`` prefix, upper case and missing leading zeros.
    @param value checksum as found in ``fdp-stat`` output.
    @return 8 lowercase hex digits, or None if absent or malformed.
    """
    if value is None:
        return None
    text = str(value).strip().lower()
    if text.startswith("0x"):
        text = text[2:]
    if not text or not re.match(r"^[0-9a-f]{1,8}$", text):
        return None
    return text.zfill(8)


def parse_fdp_stat(text: str) -> List[Dict[str, Any]]:
    """!
    @brief The entries printed by ``fdp-stat --json``; ValueError if not JSON.

    The tool prints a list of objects; a single object is accepted too.
    @param text ``fdp-stat --json`` stdout.
    @return list of entry dicts (non-dict items dropped).
    @throws ValueError if the text is not JSON or not a list/object.
    """
    data = json.loads(text or "")
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise ValueError("fdp-stat --json printed {}".format(type(data).__name__))
    return [e for e in data if isinstance(e, dict)]


def _stat_size(entry: Mapping[str, Any]) -> Optional[int]:
    """!@brief The size of an ``fdp-stat`` entry.

    ``-1`` is the namespace entry a failed PUT leaves behind (no data).
    @param entry one entry from :func:`parse_fdp_stat`.
    @return size in bytes, or None if missing or not an integer.
    """
    try:
        return int(entry.get("size"))
    except (TypeError, ValueError):
        return None


def _stat_adler32(entry: Mapping[str, Any]) -> Optional[str]:
    """!@brief The adler32 of an ``fdp-stat`` entry, normalised.

    Accepts both the ``{"adler32": v}`` and the
    ``[{"type": "adler32", "value": v}]`` shapes of ``checksums``.
    @param entry one entry from :func:`parse_fdp_stat`.
    @return 8 lowercase hex digits, or None if the entry has no adler32.
    """
    sums = entry.get("checksums")
    if isinstance(sums, dict):
        return _norm_adler32(sums.get("adler32") or sums.get("ADLER32"))
    if isinstance(sums, list):          # [{"type": "adler32", "value": ...}]
        for item in sums:
            if isinstance(item, dict) and str(
                    item.get("type", "")).lower() == "adler32":
                return _norm_adler32(item.get("value"))
    return None


def _contiguous(pieces: Sequence[Tuple[int, int, str]]
                ) -> List[List[Tuple[int, int, str]]]:
    """!@brief Split time-ordered pieces into runs that abut.

    Each run can be fetched with one darpa-spill-dump call; a gap (a piece
    already complete) starts a new run.
    @param pieces ``(start, end, label)`` pieces, ordered by start.
    @return list of runs, each a list of pieces with
            ``prev.end == next.start``.
    """
    runs: List[List[Tuple[int, int, str]]] = []
    for piece in pieces:
        if runs and runs[-1][-1][1] == piece[0]:
            runs[-1].append(piece)
        else:
            runs.append([piece])
    return runs


def _human_bytes(n: Optional[float]) -> str:
    """!@brief Format a byte count with a binary-prefixed unit (B, KB, ... TB).
    @param n bytes, or None.
    @return e.g. ``4.6 GB``; ``-`` for None.
    """
    if n is None:
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024 or unit == "TB":
            return "{:.0f} {}".format(n, unit) if unit == "B" \
                else "{:.1f} {}".format(n, unit)
        n /= 1024.0
    return str(n)  # pragma: no cover


def _human_seconds(s: Optional[float]) -> str:
    """!@brief Format a duration as s, min, h or d, keeping the sign.
    @param s seconds, or None.
    @return e.g. ``12 min``; ``-`` for None.
    """
    if s is None:
        return "-"
    sign = "-" if s < 0 else ""
    s = abs(s)
    if s < 120:
        return "{}{:.0f} s".format(sign, s)
    if s < 7200:
        return "{}{:.0f} min".format(sign, s / 60)
    if s < 172800:
        return "{}{:.1f} h".format(sign, s / 3600)
    return "{}{:.1f} d".format(sign, s / 86400)


# ------------------------------------------------------------------- state


## State database schema.  ``files`` has one row per raw file, keyed
## by its path, and carries its shifted twin (``shifted_path``): a stage
## is done when its stamp is set -- ``dumped_at``, ``shifted_at``,
## ``raw_uploaded_at`` / ``shifted_uploaded_at`` (set only after
## ``fdp-stat`` verification).  ``raw_adler32`` / ``shifted_adler32``
## cache local checksums.  ``runs`` is the job history, ``alerts`` the
## per-condition mail rate limit, ``kv`` small persistent values.
## Columns added later are migrated in :class:`State`.
SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    raw_path TEXT PRIMARY KEY,
    stream TEXT NOT NULL,
    kind TEXT NOT NULL,
    label TEXT NOT NULL,
    fmt TEXT NOT NULL,
    rows INTEGER,
    bytes INTEGER,
    dumped_at TEXT,
    shifted_path TEXT,
    shifted_at TEXT,
    raw_uploaded_at TEXT,
    shifted_uploaded_at TEXT,
    last_error TEXT,
    updated_at TEXT,
    raw_adler32 TEXT,
    shifted_adler32 TEXT
);
CREATE INDEX IF NOT EXISTS files_kind_label ON files (kind, label);
CREATE TABLE IF NOT EXISTS runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    result TEXT,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS runs_job ON runs (job, id);
CREATE TABLE IF NOT EXISTS alerts (
    key TEXT PRIMARY KEY,
    last_sent TEXT,
    count INTEGER NOT NULL DEFAULT 0,
    sent INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class State:
    """!@brief The publisher's SQLite database.

    In a dry run the database is copied into memory (or created there), so
    every decision is made from the real state but nothing is written.

    Tables (see :data:`SCHEMA`): ``files`` (one row per raw file, carrying
    its shifted twin), ``runs``, ``alerts`` and ``kv``.  Every write commits
    at once, so a run killed at any point leaves a consistent state for the
    next one.
    """

    def __init__(self, path: str, dry_run: bool = False):
        """!@brief Open (or create) the database and migrate older schemas.
        @param path    database file, ``{state_dir}/publish.db``.
        @param dry_run work on an in-memory copy; the file is opened read-only.
        """
        self.path = path
        if dry_run:
            self.db = sqlite3.connect(":memory:")
            if os.path.isfile(path):
                # Read-only URI: a dry run must not create a journal or
                # otherwise touch the real database.
                source = sqlite3.connect("file:{}?mode=ro".format(quote(path)),
                                         uri=True)
                try:
                    source.backup(self.db)
                finally:
                    source.close()
        else:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            self.db = sqlite3.connect(path, timeout=60)
            # Local disk; a rollback journal is enough and leaves no -wal.
            self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.row_factory = sqlite3.Row
        self.db.executescript(SCHEMA)
        columns = [r[1] for r in self.db.execute("PRAGMA table_info(alerts)")]
        if "sent" not in columns:       # a database from before 'sent'
            self.db.execute("ALTER TABLE alerts ADD COLUMN sent INTEGER "
                            "NOT NULL DEFAULT 0")
        columns = [r[1] for r in self.db.execute("PRAGMA table_info(files)")]
        for column in ("raw_adler32", "shifted_adler32"):
            if column not in columns:   # a database from before the cache
                self.db.execute("ALTER TABLE files ADD COLUMN {} TEXT"
                                .format(column))
        self.db.commit()

    def close(self) -> None:
        """!@brief Close the database connection."""
        self.db.close()

    # -- files

    def get(self, raw_path: str) -> Optional[Dict[str, Any]]:
        """!@brief The ``files`` row of one raw file.
        @param raw_path absolute path of the raw file (the primary key).
        @return the row as a dict, or None.
        """
        row = self.db.execute("SELECT * FROM files WHERE raw_path = ?",
                              (raw_path,)).fetchone()
        return dict(row) if row else None

    def record_dump(self, raw_path: str, stream: str, kind: str, label: str,
                    fmt: str, rows: Optional[int], size: Optional[int],
                    shifted_path: str, when: str) -> None:
        """!
        @brief Record a raw file as dumped (insert, or complete a failed row).

        Also records where its shifted copy goes, clears any error and drops
        the cached checksums, which belonged to an earlier file of that name.
        Upload and shift stamps of an existing row are left alone.
        @param raw_path     raw file path (key).
        @param stream       stream name.
        @param kind         ``hourly`` or ``daily``.
        @param label        piece label (``2026-10-22T13-0500`` or
                            ``2026-10-22``).
        @param fmt          ``csv`` or ``json``.
        @param rows         event count, or None if unknown.
        @param size         bytes, or None if unknown.
        @param shifted_path where the shifted copy is (to be) written.
        @param when         UTC ISO stamp.
        """
        self.db.execute(
            "INSERT INTO files (raw_path, stream, kind, label, fmt, rows, "
            "bytes, dumped_at, shifted_path, last_error, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?) "
            "ON CONFLICT (raw_path) DO UPDATE SET rows = excluded.rows, "
            "bytes = excluded.bytes, dumped_at = excluded.dumped_at, "
            "shifted_path = excluded.shifted_path, last_error = NULL, "
            "raw_adler32 = NULL, shifted_adler32 = NULL, "
            "updated_at = excluded.updated_at",
            (raw_path, stream, kind, label, fmt, rows, size, when,
             shifted_path, when))
        self.db.commit()

    def record_missing(self, raw_path: str, stream: str, kind: str, label: str,
                       fmt: str, shifted_path: str, error: str,
                       when: str) -> None:
        """!@brief Note an error for a file that was never written.

        The row has no ``dumped_at``, so the piece stays incomplete and the
        next run in the catch-up window dumps it again.
        @param raw_path     raw file path (key).
        @param stream       stream name.
        @param kind         ``hourly`` or ``daily``.
        @param label        piece label.
        @param fmt          ``csv`` or ``json``.
        @param shifted_path where the shifted copy would go.
        @param error        reason, shown on the status page.
        @param when         UTC ISO stamp.
        """
        self.db.execute(
            "INSERT INTO files (raw_path, stream, kind, label, fmt, "
            "shifted_path, last_error, updated_at) VALUES "
            "(?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (raw_path) DO UPDATE SET "
            "last_error = excluded.last_error, updated_at = excluded.updated_at",
            (raw_path, stream, kind, label, fmt, shifted_path, error, when))
        self.db.commit()

    def update(self, raw_path: str, when: str, **values: Any) -> None:
        """!@brief Set columns of one ``files`` row (and ``updated_at``).
        @param raw_path raw file path (key).
        @param when     UTC ISO stamp for ``updated_at``.
        @param values   column=value pairs; column names come from this module
                        only, never from input.
        """
        values["updated_at"] = when
        columns = ", ".join("{} = ?".format(k) for k in values)
        self.db.execute("UPDATE files SET {} WHERE raw_path = ?".format(columns),
                        list(values.values()) + [raw_path])
        self.db.commit()

    def for_labels(self, kind: str,
                   labels: Sequence[str]) -> List[Dict[str, Any]]:
        """!@brief All ``files`` rows of the given kind and labels.
        @param kind   ``hourly`` or ``daily``.
        @param labels piece labels.
        @return list of row dicts (every stream and format).
        """
        if not labels:
            return []
        marks = ", ".join("?" for _ in labels)
        rows = self.db.execute(
            "SELECT * FROM files WHERE kind = ? AND label IN ({})".format(marks),
            [kind] + list(labels)).fetchall()
        return [dict(r) for r in rows]

    def pending_uploads(self) -> List[Dict[str, Any]]:
        """!@brief Rows with a raw or shifted copy present locally but not yet
        uploaded.

        A shifted copy counts only once ``shifted_at`` is set, so an unshifted
        or half-shifted file is never offered to FDP.
        @return list of row dicts, ordered by raw path.
        """
        rows = self.db.execute(
            "SELECT * FROM files WHERE (dumped_at IS NOT NULL AND "
            "raw_uploaded_at IS NULL) OR (shifted_at IS NOT NULL AND "
            "shifted_uploaded_at IS NULL) ORDER BY raw_path").fetchall()
        return [dict(r) for r in rows]

    def uploaded(self, since: Optional[str] = None) -> List[Dict[str, Any]]:
        """!@brief Rows with a copy recorded uploaded (at or after *since*).
        @param since UTC ISO stamp; None or empty for all.
        @return list of row dicts, ordered by raw path.
        """
        rows = self.db.execute(
            "SELECT * FROM files WHERE (raw_uploaded_at IS NOT NULL AND "
            "raw_uploaded_at >= ?) OR (shifted_uploaded_at IS NOT NULL AND "
            "shifted_uploaded_at >= ?) ORDER BY raw_path",
            (since or "", since or "")).fetchall()
        return [dict(r) for r in rows]

    def recent_errors(self, limit: int = 20) -> List[Dict[str, Any]]:
        """!@brief The newest file errors, for the status page.
        @param limit maximum number of rows.
        @return list of dicts, newest first.
        """
        rows = self.db.execute(
            "SELECT raw_path, stream, kind, label, fmt, last_error, updated_at "
            "FROM files WHERE last_error IS NOT NULL "
            "ORDER BY updated_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def totals(self) -> Dict[str, Any]:
        """!
        @brief Counts over all files: dumped, rows, bytes, shifted, uploaded,
        errors.
        @return one dict of totals.
        """
        row = self.db.execute(
            "SELECT COUNT(dumped_at) AS files, COALESCE(SUM(rows), 0) AS rows, "
            "COALESCE(SUM(bytes), 0) AS bytes, COUNT(shifted_at) AS shifted, "
            "COUNT(raw_uploaded_at) AS raw_uploaded, "
            "COUNT(shifted_uploaded_at) AS shifted_uploaded, "
            "COUNT(last_error) AS errors FROM files").fetchone()
        return dict(row)

    # -- runs

    def run_start(self, job: str, when: str) -> int:
        """!
        @brief Record the start of a job run and prune old runs of that job.

        Keeps the newest :data:`RUNS_KEPT` runs per job.
        @param job  subcommand name.
        @param when UTC ISO stamp.
        @return the run id.
        """
        cursor = self.db.execute(
            "INSERT INTO runs (job, started_at) VALUES (?, ?)", (job, when))
        self.db.execute(
            "DELETE FROM runs WHERE job = ? AND id NOT IN (SELECT id FROM runs "
            "WHERE job = ? ORDER BY id DESC LIMIT ?)", (job, job, RUNS_KEPT))
        self.db.commit()
        return int(cursor.lastrowid)

    def run_finish(self, run_id: int, when: str, result: str,
                   detail: Dict[str, Any]) -> None:
        """!@brief Record the end of a run.
        @param run_id id from :meth:`run_start`.
        @param when   UTC ISO stamp.
        @param result ``ok``, ``partial``, ``failed``, ``busy``,
                      ``ingest-not-ready`` or ``usage``.
        @param detail JSON-serialisable summary (errors, warnings, counts).
        """
        self.db.execute(
            "UPDATE runs SET finished_at = ?, result = ?, detail = ? "
            "WHERE id = ?", (when, result, json.dumps(detail, default=str),
                             run_id))
        self.db.commit()

    def last_runs(self) -> Dict[str, Dict[str, Any]]:
        """!@brief The latest finished run of every job.
        @return ``{job: row}``, ``detail`` decoded from JSON.
        """
        rows = self.db.execute(
            "SELECT * FROM runs WHERE id IN (SELECT MAX(id) FROM runs "
            "WHERE finished_at IS NOT NULL GROUP BY job)").fetchall()
        out = {}
        for row in rows:
            data = dict(row)
            try:
                data["detail"] = json.loads(data["detail"] or "{}")
            except ValueError:
                data["detail"] = {}
            out[data["job"]] = data
        return out

    def first_run_started(self, job: str) -> Optional[str]:
        """!@brief When *job* first ran (as far as the pruned history goes).
        @param job subcommand name.
        @return UTC ISO stamp, or None if it never ran.
        """
        row = self.db.execute("SELECT MIN(started_at) AS first FROM runs "
                              "WHERE job = ?", (job,)).fetchone()
        return row["first"] if row else None

    def failed_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        """!@brief The newest finished runs whose result was not ``ok``.
        @param limit maximum number of rows.
        @return list of row dicts (``detail`` still JSON text), newest first.
        """
        rows = self.db.execute(
            "SELECT * FROM runs WHERE result NOT IN ('ok') AND finished_at "
            "IS NOT NULL ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    # -- alerts

    def alert_due(self, key: str, min_interval: float, now: datetime,
                  max_interval: Optional[float] = None) -> bool:
        """!@brief Count an occurrence of *key*; whether it is time to mail it.

        The wait doubles with every mail sent for a condition that has not
        cleared (:meth:`alert_clear`): *min_interval*, twice that, four
        times, ... up to *max_interval*.
        @param key          condition name, e.g. ``upload-exit-4``.
        @param min_interval seconds before the first repeat.
        @param now          the current time (aware).
        @param max_interval cap on the doubled wait, seconds; None for no cap.
        @return True if a mail should go now.
        """
        row = self.db.execute("SELECT last_sent, sent FROM alerts "
                              "WHERE key = ?", (key,)).fetchone()
        self.db.execute(
            "INSERT INTO alerts (key, count) VALUES (?, 1) ON CONFLICT (key) "
            "DO UPDATE SET count = count + 1", (key,))
        self.db.commit()
        last = _from_iso(row["last_sent"]) if row else None
        if last is None:
            return True
        # After n mails: min_interval * 2**(n-1); the exponent is capped so
        # the float stays finite however long a condition lasts.
        wait = min_interval * 2 ** min(max(0, (row["sent"] or 1) - 1), 30)
        if max_interval:
            wait = min(wait, max(max_interval, min_interval))
        return (now - last).total_seconds() >= wait

    def alert_sent(self, key: str, now: datetime) -> None:
        """!@brief Record that the alert *key* was mailed.
        @param key condition name.
        @param now the current time (aware).
        """
        self.db.execute("UPDATE alerts SET last_sent = ?, sent = sent + 1 "
                        "WHERE key = ?", (_utc_iso(now), key))
        self.db.commit()

    def alert_active(self, key: str) -> bool:
        """!@brief Whether *key* has been mailed and its condition not cleared.
        @param key condition name.
        @return True if mailed and not cleared.
        """
        row = self.db.execute("SELECT sent FROM alerts WHERE key = ?",
                              (key,)).fetchone()
        return bool(row and row["sent"])

    def alert_clear(self, pattern: str) -> List[str]:
        """!@brief Forget conditions matching *pattern* (SQL ``LIKE``) that
        cleared,
        so the next occurrence is mailed at once.  Returns the keys that had
        been mailed.
        @param pattern SQL ``LIKE`` pattern, e.g. ``upload-exit-%``.
        @return keys that had been mailed (for a "cleared" log line).
        """
        keys = [r["key"] for r in self.db.execute(
            "SELECT key FROM alerts WHERE key LIKE ? AND sent > 0",
            (pattern,))]
        self.db.execute("DELETE FROM alerts WHERE key LIKE ?", (pattern,))
        self.db.commit()
        return keys

    # -- small persistent values

    def kv_get(self, key: str) -> Optional[str]:
        """!
        @brief Read a small persistent value (SSO back-off, partial counts).
        @param key name.
        @return the value, or None if unset.
        """
        row = self.db.execute("SELECT value FROM kv WHERE key = ?",
                              (key,)).fetchone()
        return row["value"] if row else None

    def kv_set(self, key: str, value: Optional[str]) -> None:
        """!@brief Write a small persistent value; None deletes it.
        @param key   name.
        @param value text, or None to delete.
        """
        if value is None:
            self.db.execute("DELETE FROM kv WHERE key = ?", (key,))
        else:
            self.db.execute("INSERT INTO kv (key, value) VALUES (?, ?) ON "
                            "CONFLICT (key) DO UPDATE SET value = "
                            "excluded.value", (key, value))
        self.db.commit()

    def alerts(self) -> List[Dict[str, Any]]:
        """!@brief Every alert row, for tests and diagnostics.
        @return list of row dicts ordered by key.
        """
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM alerts ORDER BY key").fetchall()]


# --------------------------------------------------------------- publisher


## A split piece ``(start, end, label)``: half-open ``[start, end)`` in
## NOvA ticks and the label darpa-spill-dump names its file with.
Piece = Tuple[int, int, str]


class Publisher:
    """!
    @brief One invocation of ``darpa-spill-publish``: configuration, state and
    the bookkeeping that decides the exit status.

    @param cfg merged settings from :func:`load_settings`.
    @param environ the environment subprocesses inherit.
    @param dry_run print commands instead of running them; write nothing.
    @param now the current time (aware); tests pin it.

    Run bookkeeping read by :meth:`execute`: ``errors`` and ``warnings``
    (messages), ``done`` (steps completed), ``not_ready`` (ingest gate
    held pieces back), ``busy`` (the dump lock was not obtained) and
    ``detail`` (JSON stored with the run).
    """

    def __init__(self, cfg: Dict[str, Any], environ: Mapping[str, str],
                 dry_run: bool = False, now: Optional[datetime] = None):
        """!@brief Set up one run; opens the state database.
        @param cfg     merged settings from :func:`load_settings`.
        @param environ the environment subprocesses inherit.
        @param dry_run print commands instead of running them; write nothing.
        @param now     the current time (aware); tests pin it.
        @throws OSError if the state directory or database cannot be opened.
        @throws sqlite3.Error if the database cannot be opened or migrated.
        """
        self.cfg = cfg
        self.environ = dict(environ)
        self.dry_run = dry_run
        self.zone = get_timezone(cfg["timezone"])
        self._now = now
        self.state = State(os.path.join(self.dir("state_dir"), "publish.db"),
                           dry_run)
        self.errors: List[str] = []
        self.warnings: List[str] = []
        self.done = 0
        self.not_ready = False
        self.busy = False
        self.detail: Dict[str, Any] = {}
        self.holding_upload_lock = False
        # Token paths created by this run.  In a dry run the file never
        # appears, so without this every file of a month would "create"
        # (print, and announce) its token again.
        self._tokens_made: set = set()

    # -- small helpers

    def now(self) -> datetime:
        """!@brief The current time (aware UTC), or the pinned test time.
        @return aware datetime.
        """
        return self._now or datetime.now(timezone.utc)

    def stamp(self) -> str:
        """!@brief :meth:`now` as a UTC ISO stamp for the state database.
        @return e.g. ``2026-10-22T18:10:00+00:00``.
        """
        return _utc_iso(self.now())

    def dir(self, key: str) -> str:
        """!@brief A ``paths`` setting with ``~`` expanded.
        @param key ``state_dir``, ``status_dir`` or ``log_dir``.
        @return the directory path.
        """
        return os.path.expanduser(self.cfg["paths"][key])

    @property
    def data_root(self) -> str:
        """!@brief ``data_root`` with ``~`` expanded: parent of ``raw/`` and
        ``shifted/`` (D1, D3).
        @return the directory path.
        """
        return os.path.expanduser(self.cfg["data_root"])

    def error(self, message: str) -> None:
        """!@brief Log an error and count it against the run (partial/failed).
        @param message the error text.
        """
        log.error("%s", message)
        self.errors.append(message)

    def warn(self, message: str) -> None:
        """!
        @brief Log a warning and keep it in the run detail (does not fail it).
        @param message the warning text.
        """
        log.warning("%s", message)
        self.warnings.append(message)

    def local(self, nova: int) -> datetime:
        """!@brief A NOvA time as an aware datetime in the configured zone.
        @param nova 64 MHz ticks of the NOvA clock.
        @return aware local datetime.
        """
        return nova_to_datetime(nova).astimezone(self.zone)

    def iso(self, nova: int) -> str:
        """!@brief Local ISO 8601 with offset, unambiguous on the changeover.
        @param nova 64 MHz ticks of the NOvA clock.
        @return e.g. ``2026-11-01T01:00:00-06:00``.
        """
        return self.local(nova).isoformat()

    def floor_hour(self, nova: int) -> int:
        """!@brief The start of the local hour containing *nova*.
        @param nova 64 MHz ticks.
        @return NOvA ticks of the hour's start.
        """
        return datetime_to_nova(self.local(nova).replace(
            minute=0, second=0, microsecond=0))

    def midnight(self, day: date) -> int:
        """!@brief Local midnight starting *day*, as NOvA ticks.
        @param day a calendar date in the configured zone.
        @return NOvA ticks.
        """
        return datetime_to_nova(datetime.combine(day, dtime(0),
                                                 tzinfo=self.zone))

    def tool(self, name: str, key: str) -> str:
        """!
        @brief A package tool: configured, beside this interpreter, or on PATH.
        @param name executable name, e.g. ``darpa-spill-dump``.
        @param key  ``tools`` setting that overrides it (``dump``, ``shift``).
        @return the path (or bare name, left to ``Popen`` to fail with 127).
        """
        configured = self.cfg["tools"].get(key)
        if configured:
            return os.path.expanduser(configured)
        beside = os.path.join(os.path.dirname(sys.executable), name)
        if os.access(beside, os.X_OK):
            return beside
        return shutil.which(name, path=self.environ.get("PATH")) or name

    # -- subprocesses

    def run(self, cmd: Sequence[str], env: Optional[Mapping[str, str]] = None,
            timeout: Optional[float] = None, input_text: Optional[str] = None,
            mutating: bool = True, secrets: Sequence[str] = ()
            ) -> subprocess.CompletedProcess:
        """!@brief Run *cmd*, capturing its output.

        In a dry run a *mutating* command is printed and not run; read-only
        ones (``klist``, version probes) still run.  A missing program or a
        timeout come back as exit 127 and 124, like a shell's.

        The child leads its own process group, and a timeout signals the
        whole group (SIGTERM, then SIGKILL after :data:`KILL_GRACE`), so the
        workers of ``fdp-put -j`` or a helper of ``htgettoken`` cannot outlive
        it and race the next run.
        @param cmd        argv.
        @param env        variables added to :attr:`environ` for this child.
        @param timeout    seconds; None for no limit.
        @param input_text text for stdin; stdin is /dev/null otherwise.
        @param mutating   whether a dry run must skip it.
        @param secrets    literal strings to mask in the logged command line.
        @return the completed process (126 for a permission error).
        """
        shown = _redact(" ".join(shlex.quote(str(c)) for c in cmd), secrets)
        if self.dry_run:
            print("+ " + shown, flush=True)
            if mutating:
                return subprocess.CompletedProcess(list(cmd), 0, "", "")
        else:
            log.debug("run: %s", shown)
        child = dict(self.environ)
        child.update(env or {})
        # start_new_session makes the child a process-group leader
        # (pgid == pid), so _kill_group can signal it and its children.
        try:
            proc = subprocess.Popen(
                list(cmd), env=child, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, text=True,
                stdin=subprocess.PIPE if input_text is not None
                else subprocess.DEVNULL,
                start_new_session=hasattr(os, "killpg"))
        except FileNotFoundError as exc:
            return subprocess.CompletedProcess(list(cmd), 127, "", str(exc))
        except PermissionError as exc:
            return subprocess.CompletedProcess(list(cmd), 126, "", str(exc))
        try:
            out, err = proc.communicate(input_text, timeout=timeout)
        except subprocess.TimeoutExpired:
            out, err = _kill_group(proc)
            log.warning("timed out after %s s; killed: %s", timeout, shown)
            return subprocess.CompletedProcess(
                list(cmd), 124, out, (err + "\n" if err else "")
                + "timed out after {} s".format(timeout))
        return subprocess.CompletedProcess(list(cmd), proc.returncode,
                                           out or "", err or "")

    def via_env_script(self, argv: Sequence[str]
                       ) -> Tuple[List[str], Dict[str, str]]:
        """!@brief Wrap *argv* so it runs after sourcing ``fdp.env_script``.
        @param argv the real command.
        @return ``(argv to run, extra environment)``; unwrapped if no script is
                configured.
        """
        script = os.path.expanduser(self.cfg["fdp"]["env_script"])
        if not script:
            return list(argv), {}
        # The "bash" after the script is its $0; argv becomes "$@".
        return ["bash", "-c", ENV_WRAPPER, "bash"] + list(argv), \
            {"ENV_SCRIPT": script}

    # -- alerts

    def alert(self, key: str, subject: str, body: str) -> None:
        """!@brief Mail an alert about the condition *key*, rate-limited.

        The first occurrence is mailed at once; while the condition lasts the
        wait doubles from ``alerts.min_interval`` up to
        ``alerts.max_interval`` (:meth:`State.alert_due`), until
        :meth:`resolve` clears it.  When the state database cannot be written
        (a full disk) the mail still goes, rate-limited by a stamp file in
        the temporary directory instead.

        Never raises: a broken mailer is logged, and the alert is not marked
        sent so the next run tries again.
        @param key     condition name (the rate-limit key).
        @param subject mail subject, prefixed with ``[darpa-spill-publish]``.
        @param body    mail text; host, time and config file are appended.
        """
        log.warning("alert: %s", subject)
        settings = self.cfg["alerts"]
        if not settings["enabled"] or not settings["email"]:
            return
        try:
            try:
                due = self.state.alert_due(key, settings["min_interval"],
                                           self.now(), settings["max_interval"])
                fallback = False
            except (sqlite3.Error, OSError) as exc:
                log.error("state database unusable for alert %r (%s); "
                          "rate-limiting by stamp file", key, exc)
                due, fallback = self._stamp_due(key), True
            if not due:
                log.info("alert %r already sent; not repeated yet", key)
                return
            cmd = shlex.split(settings["mail_command"]) + [
                "-s", "[{}] {}".format(PROG, subject)] + list(settings["email"])
            text = "{}\n\n{}\n\nhost: {}\ntime: {}\nconfig: {}\n".format(
                subject, body.rstrip(), socket.gethostname(), self.stamp(),
                self.cfg.get("_source"))
            proc = self.run(cmd, input_text=text, timeout=60)
            if proc.returncode != 0:
                log.error("cannot send alert %r: %s exited %d: %s", key,
                          cmd[0], proc.returncode, _tail(proc.stderr))
                return
            if fallback:
                self._stamp_sent(key)
            else:
                try:
                    self.state.alert_sent(key, self.now())
                except (sqlite3.Error, OSError) as exc:
                    log.error("cannot record alert %r as sent: %s", key, exc)
                    self._stamp_sent(key)
        except Exception as exc:  # an alert must never end the run
            log.error("cannot send alert %r: %s", key, exc)

    def _stamp_path(self, key: str) -> str:
        """!@brief The fallback rate-limit stamp file for alert *key*.
        @param key condition name (sanitised into the file name).
        @return path in the system temporary directory, per user.
        """
        import tempfile
        uid = os.getuid() if hasattr(os, "getuid") else 0
        return os.path.join(tempfile.gettempdir(), "{}-{}-alert-{}".format(
            PROG, uid, re.sub(r"[^A-Za-z0-9_.-]", "_", key)))

    def _stamp_due(self, key: str) -> bool:
        """!
        @brief Whether alert *key* may be mailed by the stamp-file fallback.

        A fixed ``alerts.min_interval``; no back-off without the database.
        @param key condition name.
        @return True if no stamp exists or it is older than the interval.
        """
        try:
            age = time.time() - os.path.getmtime(self._stamp_path(key))
        except OSError:
            return True
        return age >= self.cfg["alerts"]["min_interval"]

    def _stamp_sent(self, key: str) -> None:
        """!@brief Touch the fallback stamp of alert *key* (not in a dry run).
        @param key condition name.
        """
        if self.dry_run:
            return
        try:
            with open(self._stamp_path(key), "a"):
                pass
            os.utime(self._stamp_path(key))
        except OSError as exc:
            log.error("cannot write the alert stamp for %r: %s", key, exc)

    def resolve(self, pattern: str) -> None:
        """!@brief A condition matching *pattern* has cleared; never raises.
        @param pattern SQL ``LIKE`` pattern of alert keys.
        """
        try:
            for key in self.state.alert_clear(pattern):
                log.info("condition %r cleared", key)
        except (sqlite3.Error, OSError) as exc:
            log.error("cannot clear alert %r: %s", pattern, exc)

    # -- layout

    def file_paths(self, stream: str, label: str) -> Dict[str, Tuple[str, str]]:
        """!@brief ``{fmt: (raw, shifted)}`` for one piece of one stream.

        Computed with :func:`segment_path`, exactly as darpa-spill-dump names
        the file, so the two can never disagree.
        @param stream stream name.
        @param label  piece label.
        @return ``{fmt: (raw path, shifted path)}``.
        """
        return {fmt: (segment_path(raw, label), segment_path(shifted, label))
                for fmt, (raw, shifted) in self.templates(stream).items()}

    def templates(self, stream: str) -> Dict[str, Tuple[str, str]]:
        """!@brief darpa-spill-dump path templates (D3, D4),
               ``{fmt: (raw, shifted)}``.
        @param stream stream name.
        @return ``{fmt: (raw template, shifted template)}`` with ``{mon}``,
                ``{year}``, ``{day}`` and ``{start}`` placeholders.
        """
        day_dir = os.path.join("{mon}", "{year}-{mon}-{day}")
        name = "{}-{}-{{start}}".format(self.cfg["prefix"], stream)
        # Raw and shifted copies live in parallel trees, raw/ and
        # shifted/, with the same day directories, and the shifted name
        # carries ".shifted." before the extension (darpa-spill-shift's
        # own default output name).  The tree decides where a copy is
        # uploaded (remote_dir: raw/ -> fdp.raw_dest, private; shifted/
        # -> fdp.shifted_dest, public); the infix keeps a blurred file
        # recognisable by name wherever it is copied.
        return {fmt: (os.path.join(self.data_root, "raw", day_dir,
                                   name + "." + fmt),
                      os.path.join(self.data_root, "shifted", day_dir,
                                   name + ".shifted." + fmt))
                for fmt in self.cfg["formats"]}

    def token_path(self, year: str, month: str) -> str:
        """!@brief Path of the dts1 shift token of one data month (D7, D8).

        ``shift.token_name`` in ``shift.token_dir``; the name may use
        ``{year}``, ``{month}`` (``01``-``12``) and ``{mon}``
        (``jan``-``dec``).
        @param year  four-digit year of the data, as text.
        @param month two-digit month of the data, as text.
        @return the token file path (which may not exist yet).
        """
        name = self.cfg["shift"]["token_name"].format(
            year=year, month=month, mon=MONTH_ABBREVIATIONS[int(month) - 1])
        # Default name dts1-YYYY-MM.token; {mon} is the same
        # locale-independent abbreviation darpa-spill-dump uses.
        return os.path.join(os.path.expanduser(self.cfg["shift"]["token_dir"]),
                            name)

    def complete(self, kind: str, label: str) -> bool:
        """!@brief Every stream and format of a piece is dumped and shifted.

        Upload is not part of it: uploads are retried independently of the
        catch-up window by every ``upload`` run.
        @param kind  ``hourly`` or ``daily``.
        @param label piece label.
        @return True if nothing of the piece is left to dump or shift.
        """
        rows = {r["raw_path"]: r for r in self.state.for_labels(kind, [label])}
        for stream in self.cfg["streams"]:
            for raw, _ in self.file_paths(stream, label).values():
                row = rows.get(raw)
                # shifted_at belongs to completeness: a piece whose shift
                # failed stays in the catch-up window and is retried,
                # without its raw file being dumped again (_have_raw).
                if not row or not row["dumped_at"] or not row["shifted_at"]:
                    return False
        return True

    # -- ingest gate

    def ingest_watermark(self) -> Tuple[Optional[int], str]:
        """!
        @brief How far every enabled source is ingested, or ``None`` and why.

        A source counts as read up to the later of its ``watermark_nova``
        (the newest event stored) and its ``polled_through_nova`` (the start
        of its last complete poll) less ``ingest_check.settle`` seconds for
        the TDU to record late events.  The second is what lets a quiet beam
        -- a shutdown, a maintenance day, a silent TDU -- through the gate:
        its newest event stops moving, but its polls still cover the hour.
        @return ``(mark, "")`` with *mark* the minimum over enabled sources in
                NOvA ticks, or ``(None, reason)``.
        """
        server = self.cfg["server"]
        # Seconds -> 64 MHz NOvA ticks, the unit of every watermark.
        settle = int(self.cfg["ingest_check"]["settle"] * TICKS_PER_SECOND)
        try:
            body = http_get_json(server["url"].rstrip("/") + "/api/sources",
                                 os.path.expanduser(server["ca_bundle"]),
                                 server["insecure"], server["timeout"])
        except Exception as exc:
            return None, "cannot read {}/api/sources: {}".format(
                server["url"], exc)
        marks = []
        for source in body.get("sources", []):
            if not source.get("enabled", True):
                continue
            ingest = source.get("ingest") or {}
            candidates = []
            if ingest.get("watermark_nova") is not None:
                candidates.append(int(ingest["watermark_nova"]))
            if ingest.get("polled_through_nova") is not None:
                candidates.append(int(ingest["polled_through_nova"]) - settle)
            if not candidates:
                return None, "source {} {}".format(
                    source.get("name"),
                    "has not completed an ingest poll since the server "
                    "started" if ingest.get("running")
                    else "is enabled but its ingest is not running")
            marks.append(max(candidates))
        if not marks:
            return None, "the server has no enabled sources"
        # The slowest enabled source bounds what may be dumped.
        return min(marks), ""

    def wait_for_ingest(self, needed: int) -> Tuple[Optional[int], str]:
        """!@brief Wait until every enabled source has ingested up to *needed*.

        Returns ``(None, "")`` when the check is disabled, ``(mark, "")``
        when ready, or ``(mark, reason)`` after ``ingest_check.wait`` seconds,
        where *mark* (possibly 0) is how far ingest has got.
        @param needed NOvA ticks the data must reach (end of the last piece).
        @return see above.
        """
        check = self.cfg["ingest_check"]
        if not check["enabled"]:
            return None, ""
        # A dry run reports where ingest stands; it does not wait for it.
        deadline = time.monotonic() + (0.0 if self.dry_run else check["wait"])
        while True:
            mark, reason = self.ingest_watermark()
            if mark is not None and mark >= needed:
                log.debug("ingest watermark %s covers %s", self.iso(mark),
                          self.iso(needed))
                return mark, ""
            if not reason:
                reason = ("ingest watermark {} is {:.0f} s short of {}".format(
                    self.iso(mark), (needed - mark) / TICKS_PER_SECOND,
                    self.iso(needed)))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return (mark or 0), reason
            log.info("waiting for ingest: %s", reason)
            _sleep(max(0.0, min(check["poll"], remaining)))

    # -- dump and shift

    def process(self, kind: str, pieces: Sequence[Piece]) -> None:
        """!@brief Dump whatever of *pieces* is missing, then shift it.

        Two passes: first every stream's missing raw files are dumped (abutting
        pieces in one call), then every dumped but unshifted file of these
        pieces is blurred.  Shifting after all dumps keeps a dump failure of
        one stream from holding back the shift of the others.
        @param kind   ``hourly`` or ``daily``.
        @param pieces ``(start, end, label)`` pieces not yet complete.
        """
        for stream in self.cfg["streams"]:
            # Formats missing the same pieces share a dump call; normally
            # that is both, but a recorded CSV must not be overwritten
            # because its JSON is missing.
            groups: Dict[Tuple[str, ...], List[str]] = {}
            for fmt in self.cfg["formats"]:
                missing = tuple(p[2] for p in pieces
                                if not self._have_raw(stream, kind, p[2], fmt))
                if missing:
                    groups.setdefault(missing, []).append(fmt)
            for labels, fmts in groups.items():
                wanted = [p for p in pieces if p[2] in labels]
                for run in _contiguous(wanted):
                    self.dump(stream, kind, run, fmts)
        # Shift stage.  Whether a file is already shifted is decided by
        # the state database alone: a row with dumped_at and no
        # shifted_at is shifted, every other row is left alone.  The
        # shifted file on disk is not consulted: one left by a run killed
        # after the rename but before shifted_at was recorded is simply
        # written again, and since the raw file and the token are the
        # same and dts1 is deterministic, the result is byte-identical.
        # A shifted copy is uploaded only after shifted_at is set, so no
        # published copy can ever disagree with the local one.
        for stream in self.cfg["streams"]:
            for piece in pieces:
                for fmt, (raw, _) in self.file_paths(stream, piece[2]).items():
                    row = self.state.get(raw)
                    if row and row["dumped_at"] and not row["shifted_at"]:
                        self.shift(row)

    def _have_raw(self, stream: str, kind: str, label: str, fmt: str) -> bool:
        """!@brief Whether a raw file is recorded; adopt one complete on disk.

        darpa-spill-dump renames a file into place only once it is complete,
        so a file on disk but not in the database (a run killed between the
        two) is whole and is recorded rather than dumped again.
        @param stream stream name.
        @param kind   ``hourly`` or ``daily``.
        @param label  piece label.
        @param fmt    ``csv`` or ``json``.
        @return True if the raw file exists and is (now) recorded.
        """
        raw, shifted = self.file_paths(stream, label)[fmt]
        row = self.state.get(raw)
        if row and row["dumped_at"]:
            return True
        if os.path.isfile(raw):
            log.info("adopting %s, found on disk but not recorded", raw)
            self._record(raw, stream, kind, label, fmt, shifted)
            return True
        return False

    def _record(self, raw: str, stream: str, kind: str, label: str, fmt: str,
                shifted: str) -> None:
        """!@brief Record a raw file as dumped, with its row count and size.
        @param raw     raw path.
        @param stream  stream name.
        @param kind    ``hourly`` or ``daily``.
        @param label   piece label.
        @param fmt     ``csv`` or ``json``.
        @param shifted where its shifted copy goes.
        """
        rows = size = None
        if os.path.isfile(raw):
            rows = count_rows(raw, fmt)
            size = os.path.getsize(raw)
        self.state.record_dump(raw, stream, kind, label, fmt, rows, size,
                               shifted, self.stamp())

    def dump_command(self, stream: str, kind: str, start: int, end: int,
                     fmts: Sequence[str]) -> List[str]:
        """!
        @brief The darpa-spill-dump argv for one stream over ``[start, end)``.

        ``--split`` makes the dump write one file per hour or day piece, named
        from the raw templates; the stream's selectors are passed repeatedly.
        @param stream stream name.
        @param kind   ``hourly`` or ``daily`` (selects ``--split hour|day``).
        @param start  NOvA ticks, start of the first piece.
        @param end    NOvA ticks, end of the last piece.
        @param fmts   formats to write from the one fetch.
        @return argv.
        """
        server = self.cfg["server"]
        cmd = [self.tool("darpa-spill-dump", "dump"), "-s", server["url"]]
        if server["insecure"]:
            cmd.append("-k")
        elif server["ca_bundle"]:
            cmd += ["--ca-bundle", os.path.expanduser(server["ca_bundle"])]
        cmd += ["--timeout", "{:g}".format(server["timeout"]), "-q",
                "--timezone", self.cfg["timezone"],
                "--start=" + self.iso(start), "--end=" + self.iso(end),
                "--split", UNITS[kind]]
        templates = self.templates(stream)
        for fmt in fmts:
            cmd += ["--" + fmt, templates[fmt][0]]
        spec = self.cfg["streams"][stream] or {}
        for key, flag in STREAM_KEYS.items():
            for value in spec.get(key, []):
                cmd += [flag, value]
        return cmd

    def dump(self, stream: str, kind: str, run: Sequence[Piece],
             fmts: Sequence[str]) -> None:
        """!@brief One darpa-spill-dump call for abutting pieces of one stream.

        Every piece written is recorded; a piece missing afterwards is recorded
        with an error, which keeps it incomplete for the next run.
        @param stream stream name.
        @param kind   ``hourly`` or ``daily``.
        @param run    abutting pieces (see :func:`_contiguous`).
        @param fmts   formats missing for all of these pieces.
        """
        start, end = run[0][0], run[-1][1]
        what = "{} {} {}".format(kind, stream, run[0][2] if len(run) == 1
                                 else "{}..{}".format(run[0][2], run[-1][2]))
        cmd = self.dump_command(stream, kind, start, end, fmts)
        # tools.dump_timeout is per day of data, so a month-long backfill
        # call gets a month's worth; a hung dump still ends (exit 124).
        days = max(1.0, (end - start) / (86400.0 * TICKS_PER_SECOND))
        # A dump_timeout of 0 means no limit (0 * days or None).
        timeout = self.cfg["tools"]["dump_timeout"] * days or None
        log.info("dump %s (%s)", what, ", ".join(fmts))
        proc = self.run(cmd, timeout=timeout)
        if proc.returncode == 3:
            # The archive changed under the dump (a backfill or late data):
            # the files are self-consistent, but take a second, stable look.
            log.warning("dump %s: archive changed during the dump; retrying",
                        what)
            proc = self.run(cmd, timeout=timeout)
            if proc.returncode == 3:
                self.warn("dump {}: archive changed during the dump twice; "
                          "kept the second copy: {}".format(
                              what, _tail(proc.stderr)))
        failed = proc.returncode not in (0, 3)
        if failed:
            message = "dump {} failed (exit {}): {}".format(
                what, proc.returncode, _tail(proc.stderr))
            self.error(message)
            self.alert("dump-failed", "darpa-spill-dump failed", message)
        else:
            self.resolve("dump-failed")
        # Every piece that reached disk is complete (darpa-spill-dump
        # renames a file into place only when done), even after a failure.
        for _, _, label in run:
            for fmt in fmts:
                raw, shifted = self.file_paths(stream, label)[fmt]
                if self.dry_run or os.path.isfile(raw):
                    self._record(raw, stream, kind, label, fmt, shifted)
                    self.done += 1
                    log.debug("dumped %s", raw)
                else:
                    reason = "not written: dump exit {}".format(proc.returncode)
                    if not failed:
                        reason = ("darpa-spill-dump wrote no file for this "
                                  "piece (outside the archive?)")
                        self.error("dump {}: no file {}".format(what, raw))
                    self.state.record_missing(raw, stream, kind, label, fmt,
                                              shifted, reason, self.stamp())

    def ensure_token(self, year: str, month: str) -> Tuple[Optional[str], bool]:
        """!
        @brief The shift token for a data month, created if missing (D7, D8).

        Returns ``(path, created)``; *path* is ``None`` if creation failed.
        An existing token file is never touched.

        @details
        A token is the only key to undo the blur of a whole month of
        published data, so it is created once and never replaced:
        - An existing file is used as is; nothing here ever writes over it.
        - ``darpa-spill-shift token -o PATH`` writes the token mode 0600 and
          links it into place atomically, refusing if the name exists, so two
          creators cannot both "win" (``shift._write_token_file``).
        - If creation fails but the file now exists, another run created it
          in between, and that token is used.
        - A newly created token is mailed about at once so it is backed up
          before data blurred with it is published (D8): a lost token makes
          the month irreversible.

        @param year  four-digit year of the data month, as text.
        @param month two-digit month of the data month, as text.
        @return ``(path, created)``; ``(None, False)`` after an error, which is
                recorded and alerted.
        """
        path = self.token_path(year, month)
        # An existing token is used as is, whatever it holds: replacing
        # it would blur part of a month with a different key (D7) and
        # orphan the data already shifted with the old one.
        if os.path.isfile(path) or path in self._tokens_made:
            return path, False
        shift = self.cfg["shift"]
        directory = os.path.dirname(path)
        if not self.dry_run:
            # 0700 applies only to a directory created here (and through
            # the umask); an existing one keeps its mode, which "check"
            # fails if group or other have any access.
            os.makedirs(directory, mode=0o700, exist_ok=True)
        cmd = [self.tool("darpa-spill-shift", "shift"), "token",
               "--dist", shift["dist"]]
        if shift["dist"] == "flat":
            # A flat token takes its half-width from shift.sigma; there
            # is no separate width setting.
            cmd += ["--width", shift["sigma"]]
        else:
            # Gaussian (D7): sigma 50us = 3200 ticks, cut 3sigma = 150 us.
            cmd += ["--sigma", shift["sigma"], "--cut", shift["cut"]]
        # darpa-spill-shift writes the token mode 0600 under a temporary
        # name and hard-links it into place: it appears whole or not at
        # all, and if PATH exists the tool refuses (non-zero exit).  The
        # parameters live in the token, so later config changes do not
        # affect months whose token already exists.
        cmd += ["-o", path]
        proc = self.run(cmd, timeout=self.cfg["tools"]["shift_timeout"] or None)
        if proc.returncode != 0 and not self.dry_run and os.path.isfile(path):
            # Another job (hourly, daily and backfill hold different locks)
            # created it between the check above and this call; that token
            # is the one to use, and 'token' refused to overwrite it.
            # (The "token" job holds only token.lock, so cron's token
            # --next can race any dumping job here.)
            log.info("shift token %s was created by another run", path)
            return path, False
        if proc.returncode != 0:
            message = "cannot create shift token {} (exit {}): {}".format(
                path, proc.returncode, _tail(proc.stderr))
            self.error(message)
            self.alert("token-failed", "cannot create shift token", message)
            return None, False
        self._tokens_made.add(path)
        log.info("created shift token %s for %s-%s", path, year, month)
        # Mailed once per month (the key names it): the only copy is on
        # the gateway until a person backs it up (D8), and a lost token
        # makes the month's published data irreversible.
        self.alert(
            "token-created-{}-{}".format(year, month),
            "new shift token created - back it up",
            "A new dts1 shift token for data month {}-{} was created:\n\n"
            "  {}\n\nWithout it the shifted files of that month cannot be "
            "restored.  Copy it to ~/Credentials/DARPA-shift-tokens/ on the "
            "Mac now, and check its fingerprint with\n\n"
            "  darpa-spill-shift info --token-file {}\n".format(
                year, month, path, path))
        return path, True

    def shift(self, row: Dict[str, Any]) -> None:
        """!@brief Shift one raw file with the token of its data month.

        @details
        The token is chosen from the file's *label*, i.e. the local date the
        data starts on (D2, D7), never from the time the job runs: the 00:10
        run on the 1st that publishes 23:00-24:00 of the 31st uses the old
        month's token.  Every event therefore has exactly one token, whichever
        copy (hourly file, daily file, a re-dump) it is in, and dts1 derives
        its offset from that token and the event's ``(source, event_number)``
        alone; so all copies of an event carry the identical offset.  With a
        different token per copy the offsets would be independent, and
        averaging k copies of an event would narrow its blur by a factor
        sqrt(k): the averaging attack D7 rules out.

        The shifted file goes to ``row["shifted_path"]`` under ``shifted/``,
        never beside the raw file.  ``darpa-spill-shift apply`` writes it as
        ``OUT.part`` and renames it on success, so a shifted name never holds
        a partial file.  On success ``shifted_at`` is recorded; on failure the
        row keeps ``shifted_at`` NULL and an error, and the next run retries.

        @param row the ``files`` row of a dumped, not yet shifted raw file.
        """
        # Data month = the local year and month of the label (D2, D7).
        # An hourly piece lies within one local day and a day within one
        # month, so an hour and the day containing it always resolve to
        # the same token, and each event is blurred by one offset only.
        fields = label_fields(row["label"])
        token, _ = self.ensure_token(fields["year"], fields["month"])
        if token is None:
            # No token, no shift: shifted_at stays NULL, so nothing of
            # this file reaches the public area, and the next run retries.
            self.state.update(row["raw_path"], self.stamp(),
                              last_error="no shift token for {}-{}".format(
                                  fields["year"], fields["month"]))
            return
        out = row["shifted_path"]
        if not self.dry_run:
            os.makedirs(os.path.dirname(out), exist_ok=True)
        # The token goes by file name, never as text on the command line,
        # so the secret is not visible in ps or in the logged command.
        # The input is always the raw file, which carries no shift
        # marker; darpa-spill-shift would refuse an already-shifted input
        # (double blur) and refuses -o equal to the input.
        cmd = [self.tool("darpa-spill-shift", "shift"), "apply", "-q",
               "--token-file", token, "-o", out, row["raw_path"]]
        proc = self.run(cmd, timeout=self.cfg["tools"]["shift_timeout"] or None)
        if proc.returncode == 0:
            # A rewritten shifted file invalidates its cached adler32.
            self.state.update(row["raw_path"], self.stamp(),
                              shifted_at=self.stamp(), last_error=None,
                              shifted_adler32=None)
            self.done += 1
            log.debug("shifted %s", out)
        else:
            message = "shift {} failed (exit {}): {}".format(
                row["raw_path"], proc.returncode, _tail(proc.stderr))
            self.error(message)
            self.state.update(row["raw_path"], self.stamp(),
                              last_error=message)
            self.alert("shift-failed", "darpa-spill-shift failed", message)

    # -- upload

    @contextlib.contextmanager
    def lock(self, name: str, wait: Optional[float] = None) -> Iterator[bool]:
        """!@brief ``flock`` ``{state_dir}/NAME.lock``; yields whether it was
               taken.

        *wait* ``None`` tries once; otherwise retries for that many seconds.
        A dry run takes no lock, since it writes nothing.
        @param name lock name: a job name, ``dump`` or ``upload``.
        @param wait seconds to keep retrying; None for a single attempt.
        @return a context manager yielding True if the lock is held.
        """
        if self.dry_run or fcntl is None:
            yield True
            return
        os.makedirs(self.dir("state_dir"), exist_ok=True)
        path = os.path.join(self.dir("state_dir"), name + ".lock")
        # flock is advisory and belongs to this open file description:
        # the kernel drops it when the process dies, however it dies, so
        # a leftover lock file never blocks a later run.  The file is
        # never deleted; unlinking it while another process waits would
        # let two holders lock different inodes.
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        got = False
        try:
            deadline = time.monotonic() + (wait or 0.0)
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    got = True
                    break
                except OSError:
                    if wait is None or time.monotonic() >= deadline:
                        break
                    time.sleep(1.0)
            if got:
                try:
                    # Who holds it, and since when (the mtime), for the
                    # status page and the stale-job alert; on a full disk
                    # the lock still holds without it.
                    os.ftruncate(fd, 0)
                    os.write(fd, "{}\n".format(os.getpid()).encode())
                except OSError as exc:
                    log.warning("cannot write the PID to %s: %s", path, exc)
            yield got
        finally:
            if got:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def remote_dir(self, local: str, area: str) -> Optional[str]:
        """!
        @brief The FDP directory for a local file: same layout under the dest.
        @param local local file path.
        @param area  ``raw`` or ``shifted``; selects both the local tree
                     (``data_root/area``) and the destination.
        @return e.g. ``/amsc/novaend/raw/oct/2026-oct-22``, or None if *local*
                is not under ``data_root/area``.
        """
        dest = self.cfg["fdp"]["raw_dest" if area == "raw" else "shifted_dest"]
        # The area picks both the local tree and the destination, so a
        # copy can only go where its own tree maps.  A file outside
        # data_root/area (relpath "..") is refused rather than sent to
        # the wrong area; in particular nothing outside shifted/ can
        # reach the public shifted_dest.
        rel = os.path.relpath(os.path.dirname(local),
                              os.path.join(self.data_root, area))
        if rel.startswith(".."):
            return None
        return dest.rstrip("/") + ("" if rel == "." else "/" + rel.replace(
            os.sep, "/"))

    def upload(self) -> None:
        """!@brief Upload every file not yet recorded as uploaded (D9).

        Takes ``upload.lock`` (waiting up to :data:`UPLOAD_LOCK_WAIT`) unless
        this is the ``upload`` job, whose job lock already is ``upload.lock``.
        """
        fdp = self.cfg["fdp"]
        if not fdp["enabled"]:
            log.info("upload skipped: fdp.enabled is false")
            return
        # The upload job's own job lock is upload.lock; flocking it again
        # on a new descriptor would conflict with this very process.
        if self.holding_upload_lock:
            self._upload_pending()
            return
        with self.lock("upload", wait=UPLOAD_LOCK_WAIT) as got:
            if not got:
                self.warn("another upload is still running; left for the "
                          "next 'upload' run")
                return
            self._upload_pending()

    def _upload_pending(self) -> None:
        """!
        @brief Send every pending copy in batches per destination directory.

        Each batch is one ``fdp-put --skip-existing`` followed by an
        ``fdp-stat`` verification (:meth:`_verify_batch`); only verified copies
        are marked uploaded.  Stops early on errors that every further call
        would repeat (credentials, network, timeout, environment script).
        """
        groups: Dict[str, List[Tuple[str, str, str]]] = {}
        for row in self.state.pending_uploads():
            # Each copy is tagged with its area: raw from raw_path,
            # shifted from shifted_path.  The area alone chooses the
            # destination (remote_dir).  A shifted copy is offered only
            # once shifted_at is set, i.e. after darpa-spill-shift has
            # renamed the complete file into place.
            items = []
            if row["dumped_at"] and not row["raw_uploaded_at"]:
                items.append(("raw", row["raw_path"]))
            if row["shifted_at"] and not row["shifted_uploaded_at"]:
                items.append(("shifted", row["shifted_path"]))
            for area, local in items:
                if not self.dry_run and not os.path.isfile(local):
                    message = "cannot upload {}: file is missing".format(local)
                    self.error(message)
                    self.state.update(row["raw_path"], self.stamp(),
                                      last_error=message)
                    continue
                dest = self.remote_dir(local, area)
                if dest is None:
                    message = "cannot upload {}: not under {}".format(
                        local, os.path.join(self.data_root, area))
                    self.error(message)
                    self.state.update(row["raw_path"], self.stamp(),
                                      last_error=message)
                    continue
                groups.setdefault(dest, []).append(
                    (row["raw_path"], area, local))
        if not groups:
            log.debug("nothing to upload")
            return
        log.info("uploading %d file(s) to %d director(y/ies)",
                 sum(len(v) for v in groups.values()), len(groups))
        failed = False
        for dest in sorted(groups):
            items = groups[dest]
            for first in range(0, len(items), UPLOAD_BATCH):
                batch = items[first:first + UPLOAD_BATCH]
                code = self._fdp_put(dest, batch)
                ok = code == 0
                if code not in (4, EXIT_ENV_SCRIPT):
                    # fdp-put's exit status is not trusted on its own: a
                    # failed PUT can leave an empty namespace entry that
                    # --skip-existing then skips.  Only a copy whose size
                    # and adler32 match on FDP counts as uploaded.
                    ok = self._verify_batch(dest, batch, code) and ok
                failed = failed or not ok
                if code in (4, 7, 124, EXIT_ENV_SCRIPT):
                    # Credentials, the network or the environment: every
                    # further call would fail the same way.  The next
                    # 'upload' run retries.
                    return
        if not failed and not self.dry_run:
            self.resolve("upload-exit-%")

    # -- FDP commands

    def _fdp_options(self) -> List[str]:
        """!
        @brief ``--token-file`` and ``--ca-path``, shared by every FDP tool.
        @return argv fragment.
        """
        fdp = self.cfg["fdp"]
        argv = ["--token-file", os.path.expanduser(fdp["token_file"])]
        if fdp["ca_path"]:
            argv += ["--ca-path", os.path.expanduser(fdp["ca_path"])]
        return argv

    def _fdp_run(self, argv: Sequence[str]) -> subprocess.CompletedProcess:
        """!@brief Run an FDP tool inside ``fdp.env_script``.

        Always *mutating* for :meth:`run`: a dry run prints even the
        read-only ``fdp-stat`` and contacts nothing.
        @param argv the FDP command.
        @return the completed process (``fdp.timeout`` applies).
        """
        fdp = self.cfg["fdp"]
        cmd, env = self.via_env_script(argv)
        # libfdp finds the bearer token by directory; FDP_TRANSFER_CA_PATH
        # supplies the AMSC CA directory (D12).
        env["FDP_TOKEN_DIR"] = os.path.dirname(
            os.path.expanduser(fdp["token_file"]))
        if fdp["ca_path"]:
            env["FDP_TRANSFER_CA_PATH"] = os.path.expanduser(fdp["ca_path"])
        return self.run(cmd, env=env, timeout=fdp["timeout"])

    def _fdp_put(self, dest: str, items: Sequence[Tuple[str, str, str]]) -> int:
        """!
        @brief ``fdp-put`` one batch; records failures, marks nothing uploaded
        (that is :meth:`_verify_batch`'s job).
        @param dest  remote directory.
        @param items ``(raw_path, area, local)`` per file, all for *dest*.
        @return fdp-put's exit status (see :data:`FDP_EXIT`).
        """
        fdp = self.cfg["fdp"]
        # --skip-existing, never --replace (D9): dCache forbids
        # overwriting, and a published file must not change under its
        # readers.  A copy already there is judged by the stat instead.
        argv = ["fdp-put", "--skip-existing"] + self._fdp_options()
        argv += ["-j", str(fdp["parallel"])] + list(fdp["extra_args"])
        argv += [local for _, _, local in items] + [dest + "/"]
        proc = self._fdp_run(argv)
        code = proc.returncode
        if code == 0:
            # A clean batch resets the directory's consecutive-partial
            # count.
            self.state.kv_set("partial:" + dest, None)
            log.info("%s %d file(s) to %s", "would upload" if self.dry_run
                     else "fdp-put sent", len(items), dest)
            return code
        meaning = FDP_EXIT.get(code, "error")
        message = "fdp-put to {} exited {} ({}): {}".format(
            dest, code, meaning, _redact(_tail(proc.stderr or proc.stdout)))
        if code == 12:
            # Some files went, but fdp-put does not say which; the stat that
            # follows does.  Each row notes the partial until it is
            # verified, so the status grid shows it; a directory partial
            # twice running is mailed.
            note = "partial upload (fdp-put exit 12)"
            key = "partial:" + dest
            repeated = int(self.state.kv_get(key) or 0) + 1
            self.state.kv_set(key, str(repeated))
            self.error(message + "; the next upload run retries it")
            for raw_path, area, _ in items:
                self.state.update(raw_path, self.stamp(),
                                  last_error="{} {}".format(area, note))
            if repeated >= 2:
                self.alert("upload-exit-12",
                           "upload failed: fdp-put exit 12 (partial), again",
                           message + "\n\nUploads to this directory were "
                           "partial on {} runs in a row; some file fails every "
                           "time.  Run 'darpa-spill-publish upload -v'."
                           .format(repeated))
            return code
        self.error(message)
        for raw_path, area, _ in items:
            self.state.update(raw_path, self.stamp(),
                              last_error="{} upload: fdp-put exit {} ({})".format(
                                  area, code, meaning))
        hint = {4: "Run 'darpa-spill-publish creds -v' and check the bearer "
                   "token and Kerberos ticket.",
                7: "Check the network and the CA directory fdp.ca_path.",
                EXIT_ENV_SCRIPT: "Check fdp.env_script: {}".format(
                    fdp["env_script"])}
        if code == 4 and any(self.state.alert_active(key) for key in
                             ("kinit-needed", "htgettoken-failed")):
            # The credentials alert already says why; one mail is enough.
            log.warning("upload-exit-4 not mailed: a credentials alert is "
                        "active")
            return code
        self.alert("upload-exit-{}".format(code),
                   "upload failed: fdp-put exit {} ({})".format(code, meaning),
                   message + "\n\n" + hint.get(code, ""))
        return code

    def remote_stat(self, paths: Sequence[str]
                    ) -> Tuple[Dict[str, Optional[Dict[str, Any]]],
                               Dict[str, str]]:
        """!
        @brief ``fdp-stat --json`` *paths*: ``({path: entry or None}, {path:
        error})``.  ``None`` means not on FDP; a path in the second map
        could not be checked.

        One call stats the lot; if it fails (``fdp-stat`` exits 5 when any
        path is missing) each path is asked alone.  A dry run prints the
        command and returns nothing.
        @param paths remote paths.
        @return ``(found, errors)`` as above.
        """
        found: Dict[str, Optional[Dict[str, Any]]] = {}
        errors: Dict[str, str] = {}
        if not paths:
            return found, errors
        base = ["fdp-stat", "--json"] + self._fdp_options() + list(
            self.cfg["fdp"]["extra_args"])
        proc = self._fdp_run(base + list(paths))
        if self.dry_run:
            return found, errors
        entries = None
        if proc.returncode == 0:
            try:
                entries = parse_fdp_stat(proc.stdout)
            except ValueError as exc:
                problem = "fdp-stat printed no JSON list: {}".format(exc)
            else:
                problem = ""
        else:
            problem = "fdp-stat exit {}: {}".format(
                proc.returncode, _redact(_tail(proc.stderr or proc.stdout, 2)))
        if entries is not None:
            self._match_stat(paths, entries, found)
            return found, errors
        if len(paths) == 1:
            if proc.returncode == FDP_STAT_NOT_FOUND:
                found[paths[0]] = None
            else:
                errors[paths[0]] = problem
            return found, errors
        if proc.returncode in (4, 124, 126, 127, EXIT_ENV_SCRIPT):
            # Credentials, a timeout or no fdp-stat at all: asking path by
            # path would only fail the same way, many times over.
            errors.update((path, problem) for path in paths)
            return found, errors
        log.debug("fdp-stat of %d paths failed (%s); one at a time",
                  len(paths), problem)
        for path in paths:
            one, bad = self.remote_stat([path])
            found.update(one)
            errors.update(bad)
        return found, errors

    @staticmethod
    def _match_stat(paths: Sequence[str], entries: Sequence[Dict[str, Any]],
                    found: Dict[str, Optional[Dict[str, Any]]]) -> None:
        """!
        @brief Pair stat entries with the requested paths, by path, else name.

        A name match is used only when it is unique among the entries.
        @param paths   requested remote paths.
        @param entries entries printed by ``fdp-stat --json``.
        @param found   ``{path: entry or None}``, filled in place.
        """
        by_path = {str(e.get("path", "")).rstrip("/"): e for e in entries
                   if e.get("path")}
        names: Dict[str, List[Dict[str, Any]]] = {}
        for entry in entries:
            name = entry.get("name") or os.path.basename(
                str(entry.get("path", "")).rstrip("/"))
            names.setdefault(str(name), []).append(entry)
        for path in paths:
            entry = by_path.get(path.rstrip("/"))
            if entry is None:
                same = names.get(path.rstrip("/").rsplit("/", 1)[-1], [])
                entry = same[0] if len(same) == 1 else None
            found[path] = entry

    def local_adler32(self, raw_path: str, area: str, local: str) -> str:
        """!@brief The adler32 of a local copy, cached in the state database.

        The cache (``raw_adler32`` / ``shifted_adler32``) is cleared whenever
        the file is rewritten (:meth:`State.record_dump`, :meth:`shift`).
        @param raw_path row key.
        @param area     ``raw`` or ``shifted``: which copy.
        @param local    path of that copy.
        @return 8 lowercase hex digits.
        """
        column = area + "_adler32"
        cached = (self.state.get(raw_path) or {}).get(column)
        if cached:
            return cached
        value = adler32_file(local)
        self.state.update(raw_path, self.stamp(), **{column: value})
        return value

    def remove_remote(self, path: str) -> subprocess.CompletedProcess:
        """!@brief ``fdp-rm -f`` one remote path.

        Only ever called for an incomplete (``size -1``) entry; see
        :meth:`_remove_bad_copy`.
        @param path remote path.
        @return the completed process.
        """
        argv = ["fdp-rm", "-f"] + self._fdp_options() + list(
            self.cfg["fdp"]["extra_args"]) + [path]
        return self._fdp_run(argv)

    def _mark_uploaded(self, raw_path: str, area: str) -> None:
        """!@brief Record one copy (raw or shifted) of a row as uploaded.
        @param raw_path row key.
        @param area     ``raw`` or ``shifted``.
        """
        # A row holds both copies: an upload error of the other copy stays
        # until that copy goes too.
        other = "shifted" if area == "raw" else "raw"
        error = (self.state.get(raw_path) or {}).get("last_error")
        keep = error if error and error.startswith(other + " ") else None
        self.state.update(raw_path, self.stamp(),
                          **{area + "_uploaded_at": self.stamp(),
                             "last_error": keep})

    def _verify_batch(self, dest: str, items: Sequence[Tuple[str, str, str]],
                      put_code: int) -> bool:
        """!@brief Stat one fdp-put batch on FDP and settle each copy.

        Size and adler32 match: uploaded.  Not there: still pending.  There
        but incomplete (``size -1``, the trace of a failed PUT): removed with
        ``fdp-rm -f`` so the next run can send it again, unless it was
        already recorded as uploaded, which is only reported.  Complete but
        different: never removed, reported and left pending.
        Returns whether every copy was verified.
        @param dest     remote directory of the batch.
        @param items    ``(raw_path, area, local)`` per file.
        @param put_code fdp-put's exit status for the batch.
        @return True if every copy was verified.
        """
        # fdp-put keeps each file's base name in the destination.
        remote = {local: dest + "/" + os.path.basename(local)
                  for _, _, local in items}
        found, errors = self.remote_stat([remote[local]
                                          for _, _, local in items])
        if self.dry_run:
            self.done += len(items)
            log.info("would verify %d file(s) on %s", len(items), dest)
            return True
        verified = 0
        for raw_path, area, local in items:
            path = remote[local]
            if path in errors:
                message = "{} upload: cannot verify {}: {}".format(
                    area, path, errors[path])
                self.error(message)
                self.state.update(raw_path, self.stamp(), last_error=message)
                continue
            entry = found.get(path)
            if entry is None:
                if put_code == 0:
                    message = ("{} upload: fdp-put exited 0 but {} is not on "
                               "FDP; will retry".format(area, path))
                    self.error(message)
                    self.state.update(raw_path, self.stamp(),
                                      last_error=message)
                # Otherwise fdp-put's failure, already recorded, says why.
                continue
            size, adler = _stat_size(entry), _stat_adler32(entry)
            local_size = os.path.getsize(local)
            local_adler = self.local_adler32(raw_path, area, local)
            # Size and adler32 must both match.  A shifted copy that
            # differs can only come from a different raw file (a re-dump):
            # the same raw file and token always shift identically.
            if size == local_size and adler == local_adler:
                self._mark_uploaded(raw_path, area)
                verified += 1
                continue
            what = "size {}, adler32 {}".format(
                "?" if size is None else size, adler or "none")
            if size != -1:
                # A complete file that differs is never removed (D9, D10):
                # it may be a published copy from an earlier dump.
                message = ("{} upload: {} on FDP ({}) differs from {} (size "
                           "{}, adler32 {}); not replaced".format(
                               area, path, what, local, local_size,
                               local_adler))
                self.error(message)
                self.state.update(raw_path, self.stamp(), last_error=message)
                self.alert("upload-mismatch", "uploaded file differs on FDP",
                           message + "\n\ndCache forbids overwriting (D9); "
                           "compare the copies and remove the remote one by "
                           "hand if it is wrong: fdp-rm -f " + path)
                continue
            # size -1: the empty namespace entry of a failed PUT.  It is
            # removed only if this copy was never recorded uploaded; a
            # recorded one (by another run, between the pending query and
            # this stat) is reported, never removed.
            if (self.state.get(raw_path) or {}).get(area + "_uploaded_at"):
                message = ("{} upload: {} on FDP ({}) differs from {} (size "
                           "{}, adler32 {}) but is recorded uploaded; not "
                           "removed".format(area, path, what, local,
                                            local_size, local_adler))
                self.error(message)
                self.state.update(raw_path, self.stamp(), last_error=message)
                self.alert("upload-mismatch", "uploaded file differs on FDP",
                           message)
                continue
            self._remove_bad_copy(raw_path, area, path, what)
        self.done += verified
        log.info("verified %d of %d file(s) on %s", verified, len(items), dest)
        return verified == len(items)

    def _remove_bad_copy(self, raw_path: str, area: str, path: str,
                         what: str, repair: bool = False) -> bool:
        """!
        @brief ``fdp-rm -f`` an incomplete remote copy; the row stays pending.

        In an upload that copy is still missing, an error of the run; for
        ``upload --verify`` (*repair*) it is a repair, and the upload that
        follows decides.
        @param raw_path row key.
        @param area     ``raw`` or ``shifted``.
        @param path     remote path.
        @param what     ``size S, adler32 A`` as found, for messages.
        @param repair   called from ``upload --verify``: log a warning, not an
                        error.
        @return True if the remote copy was removed.
        """
        proc = self.remove_remote(path)
        if proc.returncode == 0:
            message = "{} upload: removed {} remote copy ({}); will retry" \
                .format(area, "incomplete" if what.startswith("size -1")
                        else "differing", what)
            (self.warn if repair else self.error)(
                "{}: {}".format(message, path))
            self.state.update(raw_path, self.stamp(), last_error=message)
            return True
        message = ("{} upload: incomplete remote copy {} ({}) and fdp-rm -f "
                   "exited {}: {}".format(area, path, what, proc.returncode,
                                          _redact(_tail(proc.stderr
                                                        or proc.stdout, 2))))
        self.error(message)
        self.state.update(raw_path, self.stamp(), last_error=message)
        self.alert("upload-rm-failed", "cannot remove an incomplete upload "
                   "on FDP", message + "\n\nUntil it is removed, fdp-put "
                   "--skip-existing skips the file.  Remove it by hand: "
                   "fdp-rm -f " + path)
        return False

    def verify_uploaded(self, since: Optional[str] = None) -> None:
        """!@brief Re-stat every copy recorded uploaded (``upload --verify``).

        A copy that is gone, or is an incomplete ``size -1`` entry (which is
        removed), goes back to pending for the upload that follows; one
        that differs otherwise cannot be replaced (D9) and is reported.
        @param since UTC ISO stamp: only copies uploaded at or after it; None
                     for all.
        """
        groups: Dict[str, List[Tuple[str, str, str]]] = {}
        for row in self.state.uploaded(since):
            for area, local in (("raw", row["raw_path"]),
                                ("shifted", row["shifted_path"])):
                stamp = row[area + "_uploaded_at"]
                # The query matched the row on either copy; filter each.
                if not stamp or (since and stamp < since) or not local:
                    continue
                dest = self.remote_dir(local, area)
                if dest is not None:
                    groups.setdefault(dest, []).append(
                        (row["raw_path"], area, local))
        counts = {"checked": 0, "ok": 0, "reset": 0, "mismatch": 0,
                  "unchecked": 0}
        self.detail["verify"] = counts
        log.info("verifying %d uploaded file(s) in %d director(y/ies)",
                 sum(len(v) for v in groups.values()), len(groups))
        for dest in sorted(groups):
            items = groups[dest]
            for first in range(0, len(items), UPLOAD_BATCH):
                self._reverify(dest, items[first:first + UPLOAD_BATCH], counts)
        log.info("verify: %s", ", ".join("{} {}".format(v, k)
                                          for k, v in counts.items()))

    def _reverify(self, dest: str, items: Sequence[Tuple[str, str, str]],
                  counts: Dict[str, int]) -> None:
        """!@brief Re-verify one batch of copies recorded uploaded.
        @param dest   remote directory.
        @param items  ``(raw_path, area, local)`` per copy.
        @param counts ``checked``/``ok``/``reset``/``mismatch``/``unchecked``
                      tallies, updated in place.
        """
        remote = {local: dest + "/" + os.path.basename(local)
                  for _, _, local in items}
        found, errors = self.remote_stat([remote[local]
                                          for _, _, local in items])
        if self.dry_run:
            return
        column = {"raw": "raw_uploaded_at", "shifted": "shifted_uploaded_at"}
        for raw_path, area, local in items:
            path = remote[local]
            counts["checked"] += 1
            if path in errors:
                counts["unchecked"] += 1
                self.error("{} verify: cannot stat {}: {}".format(
                    area, path, errors[path]))
                continue
            entry = found.get(path)
            if entry is None:
                counts["reset"] += 1
                message = ("{} upload: recorded uploaded but {} is not on "
                           "FDP; will retry".format(area, path))
                self.warn(message)
                self.state.update(raw_path, self.stamp(), last_error=message,
                                  **{column[area]: None})
                continue
            size, adler = _stat_size(entry), _stat_adler32(entry)
            what = "size {}, adler32 {}".format(
                "?" if size is None else size, adler or "none")
            if size == -1:
                # Back to pending: the upload that job_upload runs next
                # sends it again once the entry is gone.
                counts["reset"] += 1
                self._remove_bad_copy(raw_path, area, path, what, repair=True)
                self.state.update(raw_path, self.stamp(),
                                  **{column[area]: None})
                continue
            if not os.path.isfile(local):
                counts["unchecked"] += 1
                self.warn("{} verify: {} is on FDP ({}) but {} is gone; not "
                          "compared".format(area, path, what, local))
                continue
            local_size = os.path.getsize(local)
            local_adler = self.local_adler32(raw_path, area, local)
            if size == local_size and adler == local_adler:
                counts["ok"] += 1
                continue
            counts["mismatch"] += 1
            message = ("{} upload: {} on FDP ({}) differs from {} (size {}, "
                       "adler32 {}); not replaced".format(
                           area, path, what, local, local_size, local_adler))
            self.error(message)
            self.state.update(raw_path, self.stamp(), last_error=message)
            self.alert("upload-mismatch", "uploaded file differs on FDP",
                       message + "\n\ndCache forbids overwriting (D9); "
                       "compare the copies and remove the remote one by hand "
                       "if it is wrong: fdp-rm -f " + path)

    # -- jobs

    def _run_pieces(self, kind: str, pieces: Sequence[Piece]) -> None:
        """!@brief Gate *pieces* on ingest, then dump and shift those that are
               ready.

        Runs under the ``dump`` lock shared by hourly, daily and backfill, so
        two jobs never dump the same piece at once (see
        ``locks.dump_wait``).
        @param kind   ``hourly`` or ``daily``.
        @param pieces ``(start, end, label)`` pieces.
        """
        wait = float(self.cfg["locks"]["dump_wait"])
        with self.lock("dump", wait=wait) as got:
            if not got:
                self.busy = True
                self.warn("another job held the dump lock for {:.0f} s; {} "
                          "left for the next run".format(wait, kind))
                return
            self._run_pieces_locked(kind, pieces)

    def _run_pieces_locked(self, kind: str, pieces: Sequence[Piece]) -> None:
        """!@brief :meth:`_run_pieces` with the dump lock held.

        Skips complete pieces, holds back those past the ingest watermark (the
        run then ends 75) and processes the rest.
        @param kind   ``hourly`` or ``daily``.
        @param pieces ``(start, end, label)`` pieces.
        """
        todo = [p for p in pieces if not self.complete(kind, p[2])]
        self.detail.setdefault("labels", {})[kind] = [p[2] for p in todo]
        if not todo:
            log.info("%s: every piece is already complete", kind)
            return
        ready, reason = self.wait_for_ingest(max(p[1] for p in todo))
        # Pieces ending past the watermark wait for a later run; the
        # earlier ones go ahead.  ready None: the gate is disabled.
        late = [] if ready is None or not reason else \
            [p for p in todo if p[1] > ready]
        todo = [p for p in todo if p not in late]
        if late:
            self.not_ready = True
            labels = [p[2] for p in late]
            self.detail.setdefault("not_ready", []).extend(labels)
            message = "ingest not ready for {} {}: {}".format(
                kind, ", ".join(labels), reason)
            log.warning("%s", message)
            self.alert("ingest-not-ready", "ingest not ready", message)
        elif not self.dry_run:
            self.resolve("ingest-not-ready")
        if todo:
            log.info("%s: %d piece(s): %s", kind, len(todo),
                     ", ".join(p[2] for p in todo))
            self.process(kind, todo)

    def check_expired(self, kind: str, piece: Piece) -> None:
        """!@brief Alert if *piece*, just past the catch-up window, never
               completed.

        Only once the publisher has been running for that long (its first
        recorded run of *kind* began before the piece ended), so a fresh
        install does not report the hours before it.  The piece is left to
        a backfill; one alert per piece.
        @param kind  ``hourly`` or ``daily``.
        @param piece the piece that has just left the window.
        """
        if self.dry_run or self.complete(kind, piece[2]):
            return
        first = _from_iso(self.state.first_run_started(kind))
        if first is None or datetime_to_nova(first) >= piece[1]:
            return
        command = "darpa-spill-publish backfill --start={} --end={} {}".format(
            self.iso(piece[0]), self.iso(piece[1]),
            "--no-daily" if kind == "hourly" else "--no-hourly")
        message = ("The {} piece {} left the catch-up window without being "
                   "completed for every stream (see the status page), and "
                   "later runs will not retry it.  Once its cause is fixed, "
                   "run:\n\n  {}\n".format(kind, piece[2], command))
        self.warn("{} {} left the catch-up window incomplete".format(
            kind, piece[2]))
        self.alert("expired-{}-{}".format(kind, piece[2]),
                   "{} {} not published; backfill needed".format(
                       kind, piece[2]), message)

    def parse_hour(self, text: str) -> int:
        """!@brief The start of the local hour named by *text*.

        Accepts a dump label (``2026-11-01T01-0600``), or any time
        :func:`parse_instant` understands; a naive time is local.
        @param text the ``--hour`` argument.
        @return NOvA ticks of the hour's start.
        @throws NovaTimeError if *text* is not a time.
        """
        match = re.match(r"^(\d{4}-\d{2}-\d{2}T\d{2})(?:([+-]\d{2})(\d{2}))?$",
                         text.strip())
        if match:
            hour, sign_hours, minutes = match.groups()
            text = hour + ":00:00" + ("{}:{}".format(sign_hours, minutes)
                                      if sign_hours else "")
        return self.floor_hour(parse_instant(text, now=self.now(),
                                             tz=self.zone))

    def job_hourly(self, args) -> Optional[int]:
        """!@brief The preceding local hour, plus any recent hour not complete.

        ``--hour H`` processes the hour beginning at H as though run in the
        hour after it, catch-up included.
        @param args parsed command line (``hour``).
        @return None, or :data:`EXIT_USAGE` for a bad ``--hour``.
        """
        if args.hour:
            try:
                end = self.parse_hour(args.hour) + HOUR
            except NovaTimeError as exc:
                log.error("--hour: %s", exc)
                return EXIT_USAGE
        else:
            end = self.floor_hour(datetime_to_nova(self.now()))
        # end is the start of the current local hour; the window is the
        # catchup_hours hours before it (always the one just finished).
        hours = max(1, self.cfg["hourly"]["catchup_hours"])
        self._run_pieces("hourly", split_range(end - hours * HOUR, end,
                                               "hour", self.zone))
        if not args.hour and not self.busy:
            # The hour just before the window has now left it for good.
            for piece in split_range(end - (hours + 1) * HOUR,
                                     end - hours * HOUR, "hour", self.zone):
                self.check_expired("hourly", piece)
        self.upload()
        return None

    def job_daily(self, args) -> Optional[int]:
        """!@brief The preceding local day (23, 24 or 25 hours), plus catch-up.
        @param args parsed command line (``date``).
        @return None, or :data:`EXIT_USAGE` for a bad ``--date``.
        """
        if args.date:
            try:
                day = date.fromisoformat(args.date)
            except ValueError:
                log.error("--date wants YYYY-MM-DD, got %r", args.date)
                return EXIT_USAGE
        else:
            day = self.now().astimezone(self.zone).date() - timedelta(days=1)
        # Days first..day inclusive; first - 1 has just left the window.
        first = day - timedelta(days=self.cfg["daily"]["catchup_days"])
        self._run_pieces("daily", split_range(
            self.midnight(first), self.midnight(day + timedelta(days=1)),
            "day", self.zone))
        if not args.date and not self.busy:
            for piece in split_range(self.midnight(first - timedelta(days=1)),
                                     self.midnight(first), "day", self.zone):
                self.check_expired("daily", piece)
        self.upload()
        return None

    def _full(self, piece: Piece, unit: str) -> bool:
        """!@brief Whether a split piece covers its whole hour or day.
        @param piece ``(start, end, label)`` from :func:`split_range`.
        @param unit  ``hour`` or ``day``.
        @return False for the clipped first or last piece of a range.
        """
        start, end, _ = piece
        local = self.local(start)
        if unit == "hour":
            floor = local.replace(minute=0, second=0, microsecond=0)
            # One real hour later (UTC arithmetic), right across a
            # changeover; a day ends at the next local midnight, 23 or 25
            # hours away on DST days.
            following = (floor.astimezone(timezone.utc)
                         + timedelta(hours=1)).astimezone(self.zone)
        else:
            floor = datetime.combine(local.date(), dtime(0), tzinfo=self.zone)
            following = datetime.combine(local.date() + timedelta(days=1),
                                         dtime(0), tzinfo=self.zone)
        return datetime_to_nova(floor) == start and \
            datetime_to_nova(following) == end

    def job_backfill(self, args) -> Optional[int]:
        """!@brief Whole hours and days of a historical range; partial ones
               skipped.

        Clipped pieces at the ends of the range are skipped: a partial file
        under a full piece's name would later block the complete one.
        @param args parsed command line (``start``, ``end``, ``hourly``,
                    ``daily``, ``upload``).
        @return None, or :data:`EXIT_USAGE`.
        """
        try:
            start, end = parse_range(args.start, args.end, now=self.now(),
                                     tz=self.zone)
        except NovaTimeError as exc:
            log.error("%s", exc)
            return EXIT_USAGE
        # Never ask for the future; the still-running piece at the end
        # is then not full and is dropped.
        end = min(end, datetime_to_nova(self.now()))
        kinds = [k for k, on in (("hourly", args.hourly),
                                 ("daily", args.daily)) if on]
        if not kinds:
            log.error("nothing to do: both --no-hourly and --no-daily")
            return EXIT_USAGE
        for kind in kinds:
            unit = UNITS[kind]
            pieces = [p for p in split_range(start, end, unit, self.zone)
                      if p[1] > p[0] and self._full(p, unit)]
            log.info("backfill %s: %d whole %s(s) from %s to %s", kind,
                     len(pieces), unit, self.iso(start), self.iso(end))
            self._run_pieces(kind, pieces)
        if args.upload:
            self.upload()
        return None

    def job_upload(self, args) -> Optional[int]:
        """!
        @brief Retry every pending upload; with ``--verify`` re-check uploaded
        copies first.
        @param args parsed command line (``verify``, ``since``).
        @return None, or :data:`EXIT_USAGE` for a bad ``--since``.
        """
        since = None
        if getattr(args, "since", None):
            if not getattr(args, "verify", False):
                log.error("--since needs --verify")
                return EXIT_USAGE
            try:
                day = date.fromisoformat(args.since)
            except ValueError:
                log.error("--since wants YYYY-MM-DD, got %r", args.since)
                return EXIT_USAGE
            since = _utc_iso(datetime.combine(day, dtime(0), tzinfo=self.zone))
        if getattr(args, "verify", False):
            if not self.cfg["fdp"]["enabled"]:
                log.info("verify skipped: fdp.enabled is false")
                return None
            # Under the upload lock: this job's own lock is upload.lock.
            self.verify_uploaded(since)
        self.upload()
        return None

    def job_token(self, args) -> Optional[int]:
        """!@brief Create the shift token of a data month if it is missing.

        Cron runs ``token --next`` on the 25th (D8), so next month's token
        exists, and has been mailed about and backed up, days before the first
        data of that month is blurred with it; :meth:`shift` would otherwise
        create it lazily.
        @param args parsed command line (``month``, ``next``).
        @return None, or :data:`EXIT_USAGE` for a bad ``--month``.
        """
        # Months are local (D2), as is the data month in shift().
        local = self.now().astimezone(self.zone)
        if args.month:
            match = re.match(r"^(\d{4})-(\d{2})$", args.month.strip())
            if not match or not 1 <= int(match.group(2)) <= 12:
                log.error("--month wants YYYY-MM, got %r", args.month)
                return EXIT_USAGE
            year, month = int(match.group(1)), int(match.group(2))
        elif args.next:
            year, month = (local.year + 1, 1) if local.month == 12 \
                else (local.year, local.month + 1)
        else:
            year, month = local.year, local.month
        path, created = self.ensure_token(str(year), "{:02d}".format(month))
        if path is None:
            return None
        if not created:
            log.info("shift token %s already exists; left untouched", path)
        self.detail["token"] = {"path": path, "created": created}
        self.done += 1
        return None

    # -- credentials

    def _klist(self, env: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """!@brief Run ``klist`` on the configured cache, in the C locale.
        @param env environment with ``KRB5CCNAME``.
        @return :func:`parse_klist` output, or None (no ticket, or a dry run).
        """
        proc = self.run(["klist"], env=dict(env, LC_ALL="C", LANG="C"),
                        timeout=30, mutating=False)
        if self.dry_run or proc.returncode != 0:
            return None
        return parse_klist(proc.stdout)

    def _sso_credentials(self) -> Optional[Tuple[str, str]]:
        """!@brief The non-MFA SSO account from ``credentials.env_file`` (D11).
        @return ``(user, password)``, or None if the file is unreadable or
                still holds placeholders.
        """
        path = os.path.expanduser(self.cfg["credentials"]["env_file"])
        try:
            values = read_dotenv(path)
        except OSError:
            return None
        user = values.get("FERMI_USER", "").strip()
        password = values.get("FERMI_PASSWORD", "")
        if user in PLACEHOLDERS or password.strip() in PLACEHOLDERS:
            return None
        return user, password

    def job_creds(self, args) -> Optional[int]:
        """!@brief Renew the Kerberos ticket and refresh the FDP bearer token
               (D11).
        @param args parsed command line (unused).
        @return None.
        """
        creds = self.cfg["credentials"]
        if not creds["enabled"]:
            log.info("credentials refresh disabled")
            return None
        env = {"KRB5CCNAME": creds["krb5ccname"]} if creds["krb5ccname"] else {}
        report = {"kerberos": self._kerberos(env)}
        report["bearer"] = self._htgettoken(env)
        report["vault"] = self.vault_token()
        self.detail["credentials"] = report
        return None

    def _kerberos(self, env: Dict[str, str]) -> Dict[str, Any]:
        """!
        @brief Check the Kerberos TGT; ``kinit -R`` it when it nears expiry.

        Alerts when there is no valid ticket, when renewal fails, and a day
        before the renewable lifetime ends (a person must run ``kinit -r 7d``).
        @param env environment with ``KRB5CCNAME``.
        @return report for the run detail and the status page.
        """
        creds = self.cfg["credentials"]
        cache = creds["krb5ccname"]
        kinit = "kinit -r 7d -c {} {}".format(cache, creds["principal"])
        now = self.now().timestamp()
        info = self._klist(env)
        report: Dict[str, Any] = {"cache": cache, "kinit_command": kinit}
        if self.dry_run:
            print("+ kinit -R -c {}   (if the ticket expires within {} h)"
                  .format(cache, creds["renew_below_hours"]))
            return report
        if info is None or (info["expires"] is not None
                            and info["expires"] <= now):
            report["valid"] = False
            message = ("No valid Kerberos ticket in {}.  As novadaq on the "
                       "gateway, run:\n\n  {}\n".format(cache, kinit))
            self.error("no valid Kerberos ticket; run: " + kinit)
            self.alert("kinit-needed", "kinit needed", message)
            return report
        report.update(valid=True, principal=info["principal"])
        self.resolve("kinit-needed")
        expires, renew_until = info["expires"], info["renew_until"]
        # kinit -R extends the ticket up to renew_until, never past it;
        # beyond that a person must run kinit -r 7d (D11).
        if expires is not None and expires - now < creds["renew_below_hours"] * 3600:
            if renew_until is not None and renew_until > now:
                proc = self.run(["kinit", "-R", "-c", cache], env=env,
                                timeout=60)
                if proc.returncode == 0:
                    log.info("renewed the Kerberos ticket in %s", cache)
                    report["renewed"] = True
                    info = self._klist(env) or info
                    expires, renew_until = info["expires"], info["renew_until"]
                else:
                    message = "kinit -R failed (exit {}): {}".format(
                        proc.returncode, _tail(proc.stderr))
                    self.error(message)
                    self.alert("kinit-renew-failed", "Kerberos renewal failed",
                               message + "\n\nRun:\n\n  " + kinit + "\n")
            else:
                self.warn("Kerberos ticket expires soon and cannot be renewed "
                          "further; run: " + kinit)
                self.alert("kinit-needed", "kinit needed",
                           "The Kerberos ticket in {} expires at {} and is past "
                           "its renewable lifetime.  Run:\n\n  {}\n".format(
                               cache, _utc_iso(datetime.fromtimestamp(
                                   expires, timezone.utc)), kinit))
        if renew_until is not None and \
                renew_until - now < creds["warn_renew_until_hours"] * 3600:
            ends = _utc_iso(datetime.fromtimestamp(renew_until, timezone.utc))
            self.warn("Kerberos renewable lifetime ends {}".format(ends))
            self.alert("kerberos-renew-until",
                       "Kerberos ticket must be re-created by {}".format(ends),
                       "The Kerberos ticket in {} can be renewed only until "
                       "{}.  Before then, as novadaq on the gateway, run:\n\n"
                       "  {}\n".format(cache, ends, kinit))
        for key, value in (("expires", expires), ("renew_until", renew_until)):
            report[key] = None if value is None else _utc_iso(
                datetime.fromtimestamp(value, timezone.utc))
        return report

    def _htgettoken(self, env: Dict[str, str]) -> Dict[str, Any]:
        """!@brief Refresh the FDP bearer token with htgettoken (D11).

        Kerberos first; SSO password approval as the fallback only when real
        credentials are configured and not held off (:meth:`_sso_backoff`).
        The password is passed in the child's environment only and is masked
        in every message.
        @param env environment with ``KRB5CCNAME``.
        @return report: mode, exit status, and :meth:`bearer_token`.
        """
        creds = self.cfg["credentials"]
        token_file = os.path.expanduser(self.cfg["fdp"]["token_file"])
        argv = ["htgettoken"] + list(creds["htgettoken_args"]) + [
            "--minsecs={}".format(creds["min_token_seconds"])]
        child = dict(env)
        secrets: List[str] = []
        sso = self._sso_credentials()
        held = self._sso_backoff() if sso else ""
        if held:
            log.warning("%s", held)
            sso = None
        if sso:
            # Only this process sees the account; it is never logged.
            child.update(FERMI_USER=sso[0], FERMI_PASSWORD=sso[1])
            secrets.append(sso[1])
            argv += ["--ssoapprove=password", "--ssocookiefile=" +
                     os.path.expanduser(creds["sso_cookie_file"])]
        else:
            # No browser or device flow from cron: Kerberos (or a still
            # valid Vault token) only.
            argv.append("--nooidc")
        argv += ["-o", token_file]
        if not self.dry_run:
            os.makedirs(os.path.dirname(token_file), mode=0o700, exist_ok=True)
        cmd, wrap = self.via_env_script(argv)
        child.update(wrap)
        proc = self.run(cmd, env=child, timeout=creds["timeout"],
                        secrets=secrets)
        report = {"mode": "kerberos, then sso password" if sso
                  else "kerberos only (--nooidc)", "exit": proc.returncode}
        if held:
            report["sso_held"] = held
        if proc.returncode != 0:
            message = "htgettoken failed (exit {}): {}".format(
                proc.returncode, _redact(_tail(proc.stderr or proc.stdout),
                                         secrets))
            if proc.returncode == EXIT_ENV_SCRIPT:
                message += " (fdp.env_script {} unreadable)".format(
                    self.cfg["fdp"]["env_script"])
            if sso and not self.dry_run:
                self._sso_failed()
                message += ("\n\nSSO password approval was tried and is now "
                            "held off for {:g} h, or until {} changes, so a "
                            "wrong or expired password cannot lock the "
                            "account.".format(creds["sso_backoff_hours"],
                                              creds["env_file"]))
            elif held:
                message += "\n\n" + held
            self.error(message)
            self.alert("htgettoken-failed", "bearer token refresh failed",
                       message + "\n\nCheck the Kerberos ticket "
                       "(darpa-spill-publish creds -v) and the SSO account "
                       "in " + creds["env_file"])
        else:
            if sso and not self.dry_run:
                self.state.kv_set("sso_failed", None)
            if not self.dry_run:
                self.resolve("htgettoken-failed")
        report.update(self.bearer_token())
        return report

    def _env_file_mtime(self) -> Optional[float]:
        """!@brief Modification time of ``credentials.env_file``.
        @return seconds since the epoch, or None if missing.
        """
        try:
            return os.path.getmtime(os.path.expanduser(
                self.cfg["credentials"]["env_file"]))
        except OSError:
            return None

    def _sso_failed(self) -> None:
        """!
        @brief Remember an SSO password failure, with the env file's mtime.
        """
        self.state.kv_set("sso_failed", json.dumps(
            {"at": self.stamp(), "env_mtime": self._env_file_mtime()}))

    def _sso_backoff(self) -> str:
        """!@brief Why SSO password mode is held off, or "" if it may be tried.

        After a failure in that mode it is not tried again for
        ``credentials.sso_backoff_hours``, unless the env file has changed
        (a new password), so a stale password is not replayed every 30 min.
        @return the reason, or "".
        """
        try:
            record = json.loads(self.state.kv_get("sso_failed") or "null")
        except ValueError:
            record = None
        if not record:
            return ""
        if record.get("env_mtime") != self._env_file_mtime():
            return ""
        failed = _from_iso(record.get("at"))
        hours = self.cfg["credentials"]["sso_backoff_hours"]
        if failed is None or \
                (self.now() - failed).total_seconds() >= hours * 3600:
            return ""
        return ("SSO password approval failed at {} and is held off for "
                "{:g} h (until {} changes); using --nooidc".format(
                    record.get("at"), hours,
                    self.cfg["credentials"]["env_file"]))

    def bearer_token(self) -> Dict[str, Any]:
        """!@brief Expiry of the FDP bearer token, from its JWT payload.
        @return ``{"file", "present"}`` and, if present, ``exp``,
                ``seconds_left`` and ``age_seconds``.
        """
        path = os.path.expanduser(self.cfg["fdp"]["token_file"])
        try:
            with open(path, encoding="utf-8") as handle:
                exp = jwt_expiry(handle.read())
            mtime = os.path.getmtime(path)
        except OSError:
            return {"file": path, "present": False}
        return {"file": path, "present": True,
                "exp": None if exp is None else _utc_iso(
                    datetime.fromtimestamp(exp, timezone.utc)),
                "seconds_left": None if exp is None
                else round(exp - self.now().timestamp()),
                "age_seconds": round(self.now().timestamp() - mtime)}

    def vault_token(self) -> Dict[str, Any]:
        """!
        @brief Presence and age of htgettoken's Vault token ``/tmp/vt_u<uid>``.
        @return ``{"file", "present"}`` and, if present, ``age_seconds``.
        """
        # htgettoken's default Vault token location.
        path = "/tmp/vt_u{}".format(os.getuid() if hasattr(os, "getuid")
                                    else 0)
        try:
            age = self.now().timestamp() - os.path.getmtime(path)
        except OSError:
            return {"file": path, "present": False}
        return {"file": path, "present": True, "age_seconds": round(age)}

    # -- check

    def job_check(self, args) -> Optional[int]:
        """!@brief Preflight: print a table of checks; exit 1 if any fails.
        @param args parsed command line (unused).
        @return None if every check passed, else :data:`EXIT_FAILURE`.
        """
        results: List[Tuple[str, bool, str]] = []

        def add(name: str, ok: bool, detail: str) -> None:
            """!@brief Append one row to the preflight table.
            @param name   check name (first column).
            @param ok     whether it passed.
            @param detail what was checked.
            """
            results.append((name, ok, detail))

        server = self.cfg["server"]
        try:
            body = http_get_json(server["url"].rstrip("/") + "/api/health",
                                 os.path.expanduser(server["ca_bundle"]),
                                 server["insecure"], min(30, server["timeout"]))
            add("server", body.get("status") == "ok",
                "{}/api/health: {}".format(server["url"], body.get("status")))
        except Exception as exc:
            add("server", False, "{}: {}".format(server["url"], exc))
        ca = os.path.expanduser(server["ca_bundle"])
        if server["insecure"]:
            add("ca bundle", True, "not used (server.insecure)")
        else:
            add("ca bundle", not ca or os.access(ca, os.R_OK),
                ca or "system store")
        root = self.data_root
        add("data root", os.path.isdir(root) and os.access(root, os.W_OK),
            root + ("" if os.path.isdir(root) else " (missing)"))
        for key in ("state_dir", "status_dir"):
            path = self.dir(key)
            probe = path
            while probe and not os.path.exists(probe):
                probe = os.path.dirname(probe)
            add(key.replace("_", " "), bool(probe) and os.access(probe, os.W_OK),
                path)
        # Shift tokens are secrets (D8): their directory must give group
        # and other no access at all.
        tokens = os.path.expanduser(self.cfg["shift"]["token_dir"])
        try:
            mode = os.stat(tokens).st_mode & 0o777
            add("token dir", mode & 0o077 == 0,
                "{} mode {:04o}".format(tokens, mode))
        except OSError:
            add("token dir", False, tokens + " (missing)")
        for name, key in (("darpa-spill-dump", "dump"),
                          ("darpa-spill-shift", "shift")):
            path = self.tool(name, key)
            add(name, os.access(path, os.X_OK), path)
        script = os.path.expanduser(self.cfg["fdp"]["env_script"])
        add("env script", os.path.isfile(script), script)
        for name, argv in (("fdp-put", ["fdp-put", "--version"]),
                           ("fdp-stat", ["bash", "-c", "command -v fdp-stat"]),
                           ("fdp-rm", ["bash", "-c", "command -v fdp-rm"]),
                           ("htgettoken",
                            ["bash", "-c", "command -v htgettoken"])):
            cmd, env = self.via_env_script(argv)
            proc = self.run(cmd, env=env, timeout=60, mutating=False)
            add(name, proc.returncode == 0,
                _tail(proc.stdout or proc.stderr, 1) or
                "exit {}".format(proc.returncode))
        mail = shlex.split(self.cfg["alerts"]["mail_command"] or "mailx")[0]
        found = shutil.which(mail, path=self.environ.get("PATH"))
        add("mail command", bool(found), found or mail + " not found")
        disk = self.disk_usage()
        if disk:
            add("disk", disk["percent"] < self.cfg["alerts"]["disk_warn_percent"],
                "{} {:.1f}% used, {} free".format(
                    disk["path"], disk["percent"], _human_bytes(disk["free"])))
        else:
            add("disk", False, "cannot stat " + root)

        width = max(len(r[0]) for r in results)
        for name, ok, detail in results:
            print("{:<{w}}  {:<4}  {}".format(name, "ok" if ok else "FAIL",
                                              detail, w=width))
        self.detail["check"] = [{"name": n, "ok": ok, "detail": d}
                                for n, ok, d in results]
        if all(ok for _, ok, _ in results):
            self.done += 1
            return None
        self.errors.append("preflight: {} check(s) failed".format(
            sum(1 for _, ok, _ in results if not ok)))
        return EXIT_FAILURE

    # -- status

    def disk_usage(self) -> Optional[Dict[str, Any]]:
        """!
        @brief Usage of the file system holding ``data_root`` (or its nearest
        existing parent).
        @return ``{path, total, used, free, warn_percent, percent}``, or None.
        """
        path = self.data_root
        while path and not os.path.exists(path):
            path = os.path.dirname(path)
        try:
            usage = shutil.disk_usage(path or "/")
        except OSError:
            return None
        return {"path": path, "total": usage.total, "used": usage.used,
                "free": usage.free,
                "warn_percent": self.cfg["alerts"]["disk_warn_percent"],
                "percent": 100.0 * usage.used / usage.total if usage.total
                else 0.0}

    def _ingest_status(self) -> Dict[str, Any]:
        """!@brief Per-source ingest watermark and lag for the status page.
        @return ``{"ok", "sources"}``, or ``{"ok": False, "error"}`` if
                ``/api/sources`` cannot be read.
        """
        server = self.cfg["server"]
        try:
            body = http_get_json(server["url"].rstrip("/") + "/api/sources",
                                 os.path.expanduser(server["ca_bundle"]),
                                 server["insecure"], min(30, server["timeout"]))
        except Exception as exc:
            return {"ok": False, "error": str(exc), "sources": []}
        now = self.now()
        sources = []
        for source in body.get("sources", []):
            ingest = source.get("ingest") or {}
            mark = ingest.get("watermark_nova")
            moment = nova_to_datetime(int(mark)) if mark is not None else None
            sources.append({
                "name": source.get("name"),
                "enabled": source.get("enabled", True),
                "running": ingest.get("running"),
                "watermark": _utc_iso(moment) if moment else None,
                "lag_seconds": round((now - moment).total_seconds())
                if moment else None,
                "last_error": ingest.get("last_error") or "",
            })
        return {"ok": True, "sources": sources}

    def _grid(self, kind: str, pieces: Sequence[Piece]) -> Dict[str, Any]:
        """!
        @brief The status grid of one kind: pieces (newest first) by stream.
        @param kind   ``hourly`` or ``daily``.
        @param pieces pieces to show.
        @return ``{"labels", "streams", "grid": {label: {stream: state}}}``.
        """
        labels = [p[2] for p in reversed(pieces)]
        rows = {r["raw_path"]: r for r in self.state.for_labels(kind, labels)}
        grid: Dict[str, Dict[str, str]] = {}
        for label in labels:
            grid[label] = {}
            for stream in self.cfg["streams"]:
                found = [rows.get(raw) for raw, _ in
                         self.file_paths(stream, label).values()]
                grid[label][stream] = _cell_state(found)
        return {"labels": labels, "streams": list(self.cfg["streams"]),
                "grid": grid}

    def build_status(self) -> Dict[str, Any]:
        """!@brief Everything the status page shows, as JSON-serialisable data.

        Last 48 hours and 14 days of the grid, the latest run of every job,
        credentials, ingest, disk, running jobs, totals and recent errors.
        @return the dict written to ``status.json``.
        """
        now = self.now()
        end = self.floor_hour(datetime_to_nova(now))
        today = now.astimezone(self.zone).date()
        hours = split_range(end - 48 * HOUR, end, "hour", self.zone)
        days = split_range(self.midnight(today - timedelta(days=14)),
                           self.midnight(today), "day", self.zone)
        runs = self.state.last_runs()
        jobs = {}
        for job, run in sorted(runs.items()):
            started, finished = _from_iso(run["started_at"]), \
                _from_iso(run["finished_at"])
            jobs[job] = {"started": run["started_at"],
                         "finished": run["finished_at"],
                         "result": run["result"],
                         "duration": (finished - started).total_seconds()
                         if started and finished else None,
                         "errors": (run["detail"] or {}).get("errors", [])[:5]}
        creds = (runs.get("creds") or {}).get("detail", {}).get(
            "credentials", {})
        errors = [{"time": e["updated_at"], "what": "{} {} {} {}".format(
            e["kind"], e["stream"], e["label"], e["fmt"]),
            "error": e["last_error"]} for e in self.state.recent_errors(20)]
        for run in self.state.failed_runs(20):
            try:
                detail = json.loads(run["detail"] or "{}")
            except ValueError:
                detail = {}
            for message in (detail.get("errors") or [run["result"]])[:3]:
                errors.append({"time": run["finished_at"],
                               "what": "{} run".format(run["job"]),
                               "error": message})
        errors.sort(key=lambda e: e["time"] or "", reverse=True)
        return {
            "generated": _utc_iso(now),
            "generated_local": now.astimezone(self.zone).isoformat(
                timespec="seconds"),
            "version": __version__,
            "host": socket.gethostname(),
            "config": {"source": self.cfg.get("_source"),
                       "server": self.cfg["server"]["url"],
                       "data_root": self.data_root,
                       "timezone": self.cfg["timezone"],
                       "formats": self.cfg["formats"],
                       "streams": self.cfg["streams"],
                       "raw_dest": self.cfg["fdp"]["raw_dest"],
                       "shifted_dest": self.cfg["fdp"]["shifted_dest"],
                       "upload_enabled": self.cfg["fdp"]["enabled"]},
            "jobs": jobs,
            "credentials": {"kerberos": creds.get("kerberos"),
                            "htgettoken": {k: v for k, v in
                                           (creds.get("bearer") or {}).items()
                                           if k in ("mode", "exit")},
                            "checked": (runs.get("creds") or {}).get(
                                "finished_at"),
                            "bearer": self.bearer_token(),
                            "vault": self.vault_token()},
            "ingest": self._ingest_status(),
            "disk": self.disk_usage(),
            "running": self.running_jobs(),
            "hourly": self._grid("hourly", hours),
            "daily": self._grid("daily", days),
            "totals": self.state.totals(),
            "pending_uploads": len(self.state.pending_uploads())
            if self.cfg["fdp"]["enabled"] else 0,
            "errors": errors[:20],
        }

    def write_status(self) -> None:
        """!@brief Render ``index.html`` and ``status.json`` atomically.

        Also raises or clears the disk alert, first, since a full disk is what
        makes the writes fail.
        """
        directory = self.dir("status_dir")
        if self.dry_run:
            print("+ (would write {} and {})".format(
                os.path.join(directory, "status.json"),
                os.path.join(directory, "index.html")))
            return
        # The disk check comes first: on a full disk the writes below fail.
        disk = self.disk_usage()
        if disk and disk["percent"] >= self.cfg["alerts"]["disk_warn_percent"]:
            self.alert("disk", "disk {:.0f}% full".format(disk["percent"]),
                       "{} is {:.1f}% used ({} free); the warning level is "
                       "{}%.".format(disk["path"], disk["percent"],
                                     _human_bytes(disk["free"]),
                                     self.cfg["alerts"]["disk_warn_percent"]))
        elif disk:
            self.resolve("disk")
        status = self.build_status()
        os.makedirs(directory, exist_ok=True)
        for name, text in (("status.json", json.dumps(status, indent=1,
                                                      default=str) + "\n"),
                           ("index.html", render_html(status))):
            path = os.path.join(directory, name)
            # Written beside the target and renamed, so the server never
            # serves a half-written file; 0644 so it can serve it at all.
            tmp = "{}.tmp.{}".format(path, os.getpid())
            try:
                with open(tmp, "w", encoding="utf-8") as handle:
                    handle.write(text)
                os.chmod(tmp, 0o644)
                os.replace(tmp, path)
            finally:
                if os.path.exists(tmp):
                    os.unlink(tmp)

    def _disk_line(self) -> str:
        """!@brief A disk-usage sentence to append to state-unwritable alerts.
        @return the sentence, or "" if usage is unknown.
        """
        disk = self.disk_usage()
        if not disk:
            return ""
        return "\n\n{} is {:.1f}% used ({} free).".format(
            disk["path"], disk["percent"], _human_bytes(disk["free"]))

    def lock_holder(self, name: str) -> Optional[Dict[str, Any]]:
        """!@brief The live process that last took ``NAME.lock``, if any.

        Read from the PID the lock file holds and its mtime (when it was
        taken), without touching the lock: probing it would make a job that
        starts at that moment find it busy.
        @param name lock name.
        @return ``{"pid", "since", "seconds"}``, or None if no live holder.
        """
        path = os.path.join(self.dir("state_dir"), name + ".lock")
        try:
            with open(path, encoding="utf-8") as handle:
                pid = int(handle.read().strip() or 0)
            since = os.path.getmtime(path)
        except (OSError, ValueError):
            return None
        if pid <= 0 or pid == os.getpid():
            return None
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return None
        except PermissionError:
            pass                       # alive, another user's
        return {"pid": pid, "since": _utc_iso(datetime.fromtimestamp(
            since, timezone.utc)), "seconds": round(time.time() - since)}

    def running_jobs(self) -> Dict[str, Dict[str, Any]]:
        """!@brief Jobs whose lock is held by a live process, flagged ``stale``
        when held past ``alerts.stale_job_hours`` (backfill exempt).
        @return ``{job: holder}``.
        """
        out = {}
        for job in JOBS:
            holder = self.lock_holder(job)
            if holder:
                # A history backfill legitimately runs for days.
                holder["stale"] = job != "backfill" and holder["seconds"] > \
                    self.cfg["alerts"]["stale_job_hours"] * 3600
                out[job] = holder
        return out

    def check_stale(self, job: str) -> None:
        """!
        @brief Alert when *job* finds its lock held for too long (a hung run).
        @param job the job that found its lock busy.
        """
        holder = self.lock_holder(job)
        limit = self.cfg["alerts"]["stale_job_hours"] * 3600
        if job == "backfill" or not holder or holder["seconds"] <= limit:
            return
        self.alert("stale-{}".format(job),
                   "{} has been running for {}".format(
                       job, _human_seconds(holder["seconds"])),
                   "PID {} has held {} since {} ({}), longer than "
                   "alerts.stale_job_hours = {}; every later {} run exits 75 "
                   "until it ends.  Check it with\n\n  ps -fp {}\n\nand "
                   "kill it if it is hung.".format(
                       holder["pid"], os.path.join(self.dir("state_dir"),
                                                   job + ".lock"),
                       holder["since"], _human_seconds(holder["seconds"]),
                       self.cfg["alerts"]["stale_job_hours"], job,
                       holder["pid"]))

    # -- driver

    def execute(self, args: argparse.Namespace) -> int:
        """!@brief Run one subcommand under its lock and record the run.

        The result is ``usage`` for a handler's own exit code, else
        ``partial``/``failed`` if there were errors (partial when some steps
        succeeded), else ``ingest-not-ready``, ``busy`` or ``ok``.  The status
        page is re-rendered after every job.
        @param args parsed command line.
        @return the exit status.
        """
        job = args.command
        handler = getattr(self, "job_" + job, None)
        with self.lock(job) as got:
            if not got:
                log.info("%s is already running; nothing to do", job)
                self.check_stale(job)
                return EXIT_TEMPFAIL
            # The upload job's lock is upload.lock itself; see upload().
            self.holding_upload_lock = job == "upload"
            try:
                run_id = self.state.run_start(job, self.stamp())
            except (sqlite3.Error, OSError) as exc:
                # Most likely a full disk: say so by mail, which needs none.
                message = "cannot record the {} run in {}: {}".format(
                    job, self.state.path, exc)
                log.error("%s", message)
                self.alert("state-unwritable", "state database unwritable "
                           "(disk full?)", message + self._disk_line())
                return EXIT_FAILURE
            started = time.monotonic()
            code: Optional[int] = None
            try:
                if handler is not None:   # 'status' only re-renders
                    code = handler(args)
            except Exception as exc:
                log.exception("%s failed", job)
                self.errors.append("{}: {}".format(type(exc).__name__, exc))
            if code is not None and code != EXIT_FAILURE:
                result = "usage"
            elif self.errors:
                result = "partial" if self.done and code is None else "failed"
            elif self.not_ready:
                result = "ingest-not-ready"
            elif self.busy:
                result = "busy"
            else:
                result = "ok"
            if code is None:
                code = {"ok": EXIT_OK, "partial": EXIT_PARTIAL,
                        "failed": EXIT_FAILURE, "busy": EXIT_TEMPFAIL,
                        "ingest-not-ready": EXIT_TEMPFAIL}[result]
            self.detail.update(errors=self.errors[:50],
                               warnings=self.warnings[:50],
                               steps=self.done,
                               seconds=round(time.monotonic() - started, 1))
            try:
                self.state.run_finish(run_id, self.stamp(), result,
                                      self.detail)
            except (sqlite3.Error, OSError) as exc:
                log.error("cannot record the end of the run: %s", exc)
                self.alert("state-unwritable", "state database unwritable "
                           "(disk full?)", "cannot record the end of the {} "
                           "run in {}: {}{}".format(job, self.state.path, exc,
                                                    self._disk_line()))
                # A run that cannot be recorded is not "ok".
                code = code or EXIT_FAILURE
            try:
                self.write_status()
            except Exception as exc:
                log.error("cannot write the status page: %s", exc)
            log.info("%s: %s (%d step(s), %d error(s)) in %.1f s", job, result,
                     self.done, len(self.errors), time.monotonic() - started)
            return code


def _cell_state(rows: Sequence[Optional[Dict[str, Any]]]) -> str:
    """!@brief One stream of one piece, summarised across its formats.

    The tests run from worst to best: any error wins, then an incomplete
    dump; ``public`` needs both copies of every format verified on FDP.
    @param rows the ``files`` rows of each format (None where absent).
    @return ``missing``, ``error``, ``partial``, ``public``, ``raw-up``,
            ``shifted`` or ``dumped`` (also the CSS class of the cell).
    """
    if not any(rows):
        return "missing"
    if any(r and r["last_error"] for r in rows):
        return "error"
    if not all(r and r["dumped_at"] for r in rows):
        return "partial"
    if all(r["shifted_uploaded_at"] and r["raw_uploaded_at"] for r in rows):
        return "public"
    if all(r["raw_uploaded_at"] for r in rows):
        return "raw-up"
    if all(r["shifted_at"] for r in rows):
        return "shifted"
    return "dumped"


# -------------------------------------------------------------- status page


## Inline stylesheet of the status page: light and dark themes, and one
## class per grid state of :func:`_cell_state`.
_CSS = """
:root { --bg:#fff; --fg:#1d232a; --muted:#5b6672; --line:#d8dee4;
  --card:#f6f8fa; --ok:#1a7f37; --warn:#9a6700; --bad:#cf222e;
  --c-missing:#eaeef2; --c-dumped:#ddf4ff; --c-shifted:#b6e3ff;
  --c-raw-up:#aceebb; --c-public:#4ac26b; --c-error:#ff8182;
  --c-partial:#fff8c5; }
@media (prefers-color-scheme: dark) {
  :root { --bg:#0d1117; --fg:#e6edf3; --muted:#9198a1; --line:#30363d;
    --card:#161b22; --ok:#3fb950; --warn:#d29922; --bad:#f85149;
    --c-missing:#21262d; --c-dumped:#0c2d6b; --c-shifted:#1158c7;
    --c-raw-up:#116329; --c-public:#2ea043; --c-error:#a40e26;
    --c-partial:#6c5d00; } }
body { background:var(--bg); color:var(--fg); margin:1.5rem;
  font:14px/1.45 -apple-system, "Segoe UI", Helvetica, Arial, sans-serif; }
h1 { font-size:1.4rem; margin:0 0 .2rem; } h2 { font-size:1.1rem;
  margin:1.6rem 0 .5rem; border-bottom:1px solid var(--line); }
.muted { color:var(--muted); } .ok { color:var(--ok); }
.warn { color:var(--warn); } .bad { color:var(--bad); }
table { border-collapse:collapse; margin:.3rem 0; }
th, td { border:1px solid var(--line); padding:.25rem .5rem; text-align:left;
  vertical-align:top; } th { background:var(--card); }
td.num { text-align:right; font-variant-numeric:tabular-nums; }
table.grid td { width:4.5rem; text-align:center; font-size:12px; }
.missing { background:var(--c-missing); } .dumped { background:var(--c-dumped); }
.shifted { background:var(--c-shifted); } .raw-up { background:var(--c-raw-up); }
.public { background:var(--c-public); } .error { background:var(--c-error); }
.partial { background:var(--c-partial); }
code { font-family:ui-monospace, Menlo, Consolas, monospace; font-size:12px; }
.cards { display:flex; flex-wrap:wrap; gap:1rem; }
.card { background:var(--card); border:1px solid var(--line);
  border-radius:6px; padding:.6rem .9rem; min-width:14rem; }
"""


def _esc(value: Any) -> str:
    """!@brief HTML-escape a value for the status page; None becomes empty.
    @param value anything printable.
    @return escaped text.
    """
    return html.escape("" if value is None else str(value))


def _result_class(result: Optional[str]) -> str:
    """!@brief CSS class for a run result: ``ok``, ``warn`` or ``bad``.
    @param result run result string, or None.
    @return the class name; unknown results are ``bad``.
    """
    return {"ok": "ok", "ingest-not-ready": "warn", "partial": "warn",
            "busy": "warn"}.get(result or "", "bad")


def render_html(status: Dict[str, Any]) -> str:
    """!
    @brief The self-contained status page; *status* is what status.json holds.

    No external resources: inline CSS (light and dark), a 60 s meta refresh,
    and every value from *status* passed through :func:`_esc`, since error
    texts come from subprocess output.
    @param status dict from :meth:`Publisher.build_status`.
    @return the HTML document.
    """
    out: List[str] = []
    w = out.append
    w("<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">")
    w("<meta http-equiv=\"refresh\" content=\"60\">")
    w("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")
    w("<title>DARPA spill publishing - {}</title>".format(_esc(status["host"])))
    w("<style>{}</style></head><body>".format(_CSS))
    w("<h1>DARPA spill data publishing</h1>")
    w("<div class=\"muted\">{} &middot; generated {} ({}) &middot; "
      "darpa-spill-publish {} &middot; <a href=\"status.json\">status.json</a>"
      "</div>".format(_esc(status["host"]), _esc(status["generated_local"]),
                      _esc(status["generated"]), _esc(status["version"])))

    totals = status["totals"]
    disk = status.get("disk") or {}
    w("<div class=\"cards\" style=\"margin-top:1rem\">")
    w("<div class=\"card\"><b>Files</b><br>{} dumped, {} shifted<br>{} rows, "
      "{}</div>".format(totals["files"], totals["shifted"],
                        "{:,}".format(totals["rows"] or 0),
                        _human_bytes(totals["bytes"])))
    w("<div class=\"card\"><b>Uploaded</b><br>raw {}, public {}<br>"
      "<span class=\"{}\">{} pending</span></div>".format(
          totals["raw_uploaded"], totals["shifted_uploaded"],
          "warn" if status["pending_uploads"] else "ok",
          status["pending_uploads"]))
    if disk:
        w("<div class=\"card\"><b>Disk</b> <code>{}</code><br>"
          "<span class=\"{}\">{:.1f}% used</span>, {} free</div>".format(
              _esc(disk["path"]),
              "bad" if disk["percent"] >= disk.get("warn_percent", 85) else "ok",
              disk["percent"], _human_bytes(disk["free"])))
    w("</div>")

    w("<h2>Jobs</h2><table><tr><th>job</th><th>last finished</th>"
      "<th>result</th><th>duration</th><th>errors</th></tr>")
    for job, run in status["jobs"].items():
        w("<tr><td>{}</td><td>{}</td><td class=\"{}\">{}</td>"
          "<td class=\"num\">{}</td><td>{}</td></tr>".format(
              _esc(job), _esc(run["finished"]), _result_class(run["result"]),
              _esc(run["result"]), _human_seconds(run["duration"]),
              "<br>".join(_esc(e) for e in run["errors"])))
    w("</table>")
    running = status.get("running") or {}
    if running:
        w("<table><tr><th>running now</th><th>PID</th><th>since</th>"
          "<th>for</th></tr>")
        for job, holder in running.items():
            w("<tr><td>{}</td><td class=\"num\">{}</td><td>{}</td>"
              "<td class=\"num {}\">{}{}</td></tr>".format(
                  _esc(job), _esc(holder["pid"]), _esc(holder["since"]),
                  "bad" if holder.get("stale") else "ok",
                  _human_seconds(holder["seconds"]),
                  " (stale?)" if holder.get("stale") else ""))
        w("</table>")

    creds = status["credentials"]
    krb = creds.get("kerberos") or {}
    bearer = creds.get("bearer") or {}
    vault = creds.get("vault") or {}
    w("<h2>Credentials</h2><table>")
    w("<tr><th>Kerberos</th><td class=\"{}\">{}</td><td>expires {}<br>"
      "renew until {}</td></tr>".format(
          "ok" if krb.get("valid") else "bad",
          "valid" if krb.get("valid") else "not valid / unknown",
          _esc(krb.get("expires")), _esc(krb.get("renew_until"))))
    left = bearer.get("seconds_left")
    w("<tr><th>Bearer token</th><td class=\"{}\">{}</td><td>exp {} ({} left)"
      "<br><code>{}</code></td></tr>".format(
          "ok" if left and left > 0 else "bad",
          "present" if bearer.get("present") else "missing",
          _esc(bearer.get("exp")), _human_seconds(left),
          _esc(bearer.get("file"))))
    w("<tr><th>Vault token</th><td>{}</td><td>age {}<br><code>{}</code>"
      "</td></tr>".format("present" if vault.get("present") else "missing",
                          _human_seconds(vault.get("age_seconds")),
                          _esc(vault.get("file"))))
    w("<tr><th>Last refresh</th><td colspan=\"2\">{} {}</td></tr>".format(
        _esc(creds.get("checked")),
        _esc((creds.get("htgettoken") or {}).get("mode"))))
    w("</table>")

    ingest = status["ingest"]
    w("<h2>Ingest</h2>")
    if not ingest.get("ok"):
        w("<p class=\"bad\">cannot read /api/sources: {}</p>".format(
            _esc(ingest.get("error"))))
    else:
        w("<table><tr><th>source</th><th>enabled</th><th>watermark (UTC)</th>"
          "<th>lag</th><th>last error</th></tr>")
        for source in ingest["sources"]:
            lag = source["lag_seconds"]
            w("<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"num {}\">{}"
              "</td><td>{}</td></tr>".format(
                  _esc(source["name"]), "yes" if source["enabled"] else "no",
                  _esc(source["watermark"]),
                  "ok" if lag is not None and lag < 900 else "warn",
                  _human_seconds(lag), _esc(source["last_error"])))
        w("</table>")

    legend = " ".join("<span class=\"{0}\" style=\"padding:0 .4rem\">{0}"
                      "</span>".format(s) for s in
                      ("missing", "dumped", "shifted", "raw-up", "public",
                       "partial", "error"))
    for kind, title in (("hourly", "Hourly files (last 48 h)"),
                        ("daily", "Daily files (last 14 days)")):
        # Pieces newest first, one column per stream; the cell text is
        # its state and also its CSS class (_cell_state).
        grid = status[kind]
        w("<h2>{}</h2><div class=\"muted\">{}</div>".format(title, legend))
        w("<table class=\"grid\"><tr><th>label</th>")
        for stream in grid["streams"]:
            w("<th>{}</th>".format(_esc(stream)))
        w("</tr>")
        for label in grid["labels"]:
            w("<tr><th><code>{}</code></th>".format(_esc(label)))
            for stream in grid["streams"]:
                state = grid["grid"][label][stream]
                w("<td class=\"{0}\">{0}</td>".format(_esc(state)))
            w("</tr>")
        w("</table>")

    w("<h2>Recent errors</h2>")
    if not status["errors"]:
        w("<p class=\"ok\">none</p>")
    else:
        w("<table><tr><th>time</th><th>what</th><th>error</th></tr>")
        for entry in status["errors"]:
            w("<tr><td>{}</td><td>{}</td><td><code>{}</code></td></tr>".format(
                _esc(entry["time"]), _esc(entry["what"]),
                _esc(entry["error"])))
        w("</table>")

    config = status["config"]
    w("<h2>Configuration</h2><table>")
    for key in ("source", "server", "data_root", "timezone", "raw_dest",
                "shifted_dest", "upload_enabled"):
        w("<tr><th>{}</th><td><code>{}</code></td></tr>".format(
            _esc(key), _esc(config[key])))
    w("<tr><th>streams</th><td><code>{}</code></td></tr>".format(
        _esc(json.dumps(config["streams"]))))
    w("</table></body></html>\n")
    return "\n".join(out)


# --------------------------------------------------------------------- CLI


def _common_options(sub: bool) -> argparse.ArgumentParser:
    """!@brief The options every subcommand takes.

    They are accepted before or after the subcommand.  The copy attached to
    the subcommands defaults to "absent", or its defaults would overwrite
    what was given before the subcommand; ``--set`` collects separately and
    is joined in :func:`load_settings`.
    @param sub True for the copy attached to the subcommands.
    @return a parent parser (``add_help=False``).
    """
    quiet = {"default": argparse.SUPPRESS} if sub else {}
    common = argparse.ArgumentParser(add_help=False)
    group = common.add_argument_group("common options")
    group.add_argument("-c", "--config", metavar="FILE",
                       help="YAML configuration (default: ${} or {} if it "
                            "exists)".format(ENV_CONFIG, DEFAULT_CONFIG_FILE),
                       **quiet)
    group.add_argument("--set", action="append",
                       dest="set_after" if sub else "set",
                       default=[], metavar="SECTION.KEY=VALUE",
                       help="override one setting; repeatable")
    group.add_argument("-n", "--dry-run", action="store_true",
                       help="print every command instead of running it; "
                            "write nothing", **quiet)
    group.add_argument("-v", "--verbose", action="store_true",
                       help="log every step", **quiet)
    group.add_argument("-q", "--quiet", action="store_true",
                       help="log only warnings and errors", **quiet)
    return common


def build_parser() -> argparse.ArgumentParser:
    """!@brief The ``darpa-spill-publish`` argument parser and its subcommands.
    @return the parser.
    """
    top = _common_options(sub=False)
    common = _common_options(sub=True)
    parser = argparse.ArgumentParser(
        prog=PROG, parents=[top],
        description="Dump, shift, upload and report NOvA spill data on the "
                    "gateway; run from cron.",
        epilog=(
            "precedence: command line > environment ({0}*) > .env "
            "(paths.dotenv) > YAML > defaults.\n"
            "exit status: 0 ok, 1 failure, 2 usage, 3 partial, 75 temporary "
            "(ingest not ready or job already running).\n"
            "Write relative times with '=': --start=-2d.".format(ENV_PUBLISH)),
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("-V", "--version", action="store_true",
                        help="show version information and exit")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    p = sub.add_parser("hourly", parents=[common],
                       help="the preceding local hour, plus catch-up")
    p.add_argument("--hour", metavar="TIME",
                   help="process the hour beginning at TIME (local, or a "
                        "label such as 2026-11-01T01-0600) instead")
    p = sub.add_parser("daily", parents=[common],
                       help="the preceding local day, plus catch-up")
    p.add_argument("--date", metavar="YYYY-MM-DD",
                   help="process this local day instead")
    p = sub.add_parser("backfill", parents=[common],
                       help="whole hours and days of a historical range")
    p.add_argument("--start", required=True, metavar="TIME")
    p.add_argument("--end", required=True, metavar="TIME",
                   help="exclusive; a date includes that whole day")
    p.add_argument("--hourly", action=argparse.BooleanOptionalAction,
                   default=True, help="hourly files")
    p.add_argument("--daily", action=argparse.BooleanOptionalAction,
                   default=True, help="daily files")
    p.add_argument("--no-upload", dest="upload", action="store_false",
                   help="dump and shift only")
    p = sub.add_parser("upload", parents=[common],
                       help="retry every upload not yet done")
    p.add_argument("--verify", action="store_true",
                   help="first re-check every file recorded uploaded "
                        "against FDP (size and adler32); missing or "
                        "incomplete copies go back to pending")
    p.add_argument("--since", metavar="YYYY-MM-DD",
                   help="with --verify: only files uploaded on or after "
                        "this local date")
    p = sub.add_parser("token", parents=[common],
                       help="create the shift token for a data month if "
                            "missing")
    which = p.add_mutually_exclusive_group()
    which.add_argument("--month", metavar="YYYY-MM",
                       help="that month (default: the current month)")
    which.add_argument("--next", action="store_true",
                       help="next month")
    sub.add_parser("creds", parents=[common],
                   help="renew Kerberos, refresh the FDP bearer token")
    sub.add_parser("status", parents=[common],
                   help="re-render the status page only")
    sub.add_parser("check", parents=[common],
                   help="preflight checks; exit 1 if any fails")
    return parser


def _setup_logging(verbose: bool, quiet: bool) -> None:
    """!@brief Log to stderr (cron mails or redirects it) with ISO timestamps.
    @param verbose DEBUG level.
    @param quiet   WARNING level (ignored if *verbose*).
    """
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s " + PROG + " %(levelname)s %(message)s",
        "%Y-%m-%dT%H:%M:%S%z"))
    log.handlers[:] = [handler]
    log.propagate = False
    log.setLevel(logging.DEBUG if verbose
                 else logging.WARNING if quiet else logging.INFO)


def main(argv: Optional[Sequence[str]] = None,
         environ: Optional[Mapping[str, str]] = None,
         now: Optional[datetime] = None) -> int:
    """!@brief Command-line entry point of ``darpa-spill-publish``.
    @param argv    arguments (default ``sys.argv[1:]``).
    @param environ environment (default ``os.environ``); tests pass their own.
    @param now     pinned current time (aware), for tests.
    @return the exit status: 0, 1, 2, 3 or 75 (see the module docstring).
    """
    environ = dict(os.environ if environ is None else environ)
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        print("{} {}".format(PROG, __version__))
        return EXIT_OK
    if not args.command:
        parser.print_help(sys.stderr)
        return EXIT_USAGE
    _setup_logging(args.verbose, args.quiet)
    try:
        cfg = load_settings(args, environ)
    except PublishConfigError as exc:
        print("{}: error: {}".format(PROG, exc), file=sys.stderr)
        return EXIT_USAGE
    try:
        publisher = Publisher(cfg, environ, dry_run=args.dry_run, now=now)
    except (OSError, sqlite3.Error) as exc:
        print("{}: error: cannot open the state database: {}".format(PROG, exc),
              file=sys.stderr)
        return EXIT_FAILURE
    try:
        return publisher.execute(args)
    except OSError as exc:
        # The lock file itself cannot be opened: a full or read-only disk.
        log.error("%s: %s", args.command, exc)
        publisher.alert("state-unwritable", "state directory unwritable "
                        "(disk full?)", "{} {}: {}{}".format(
                            PROG, args.command, exc, publisher._disk_line()))
        return EXIT_FAILURE
    finally:
        publisher.state.close()


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
