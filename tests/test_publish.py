"""Tests for the gateway publishing pipeline, ``darpa-spill-publish``.

Nothing here touches the network or the gateway.  darpa-spill-dump is the
real tool reading a local test archive through a thin wrapper that drops the
server options; fdp-put, htgettoken, klist, kinit, mailx and the FDP
environment script are small fakes in ``tmp_path`` that record how they
were called.  darpa-spill-shift is a fake that copies, except in the
end-to-end inversion test, which uses the real one.
"""

import base64
import json
import os
import sqlite3
import stat
import subprocess
import sys
import zlib
from datetime import datetime, timedelta, timezone

import pytest
import yaml

from darpa_spillserver import publish
from darpa_spillserver.dump import split_range
from darpa_spillserver.novatime import (TICKS_PER_SECOND, datetime_to_nova,
                                        get_timezone)
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import SpillEvent, SpillStore

CHICAGO = get_timezone("America/Chicago")
UTC = timezone.utc
PASSWORD = "s3cr3t-Pa55word!"


def utc(*args):
    return datetime(*args, tzinfo=UTC)


def nova(moment):
    return datetime_to_nova(moment)


# ----------------------------------------------------------------- fixtures


FAKE_DUMP = r'''#!{python}
"""darpa-spill-dump against a local archive: drop the server options."""
import json, os, sys
LOG = {log!r}
CTL = {ctl!r}
argv = sys.argv[1:]
with open(os.path.join(LOG, "dump.log"), "a") as h:
    h.write(json.dumps(argv) + "\n")
code = None
codes = os.path.join(CTL, "dump.codes")
if os.path.exists(codes):
    lines = open(codes).read().split()
    if lines:
        code = int(lines[0])
        open(codes, "w").write("\n".join(lines[1:]))
if code not in (None, 0, 3):
    sys.stderr.write("fake dump failure\n")
    sys.exit(code)
out, skip = [], 0
for i, a in enumerate(argv):
    if skip:
        skip -= 1
        continue
    if a in ("-s", "--ca-bundle"):
        skip = 1
        continue
    if a == "-k":
        continue
    out.append(a)
from darpa_spillserver.dump import main
rc = main(["-d", {db!r}] + out, environ={{}})
sys.exit(code if code is not None else rc)
'''

FAKE_SHIFT = r'''#!/bin/sh
printf '%s\n' "$*" >> {log}/shift.log
cmd=$1; shift
out=""; last=""
while [ $# -gt 0 ]; do
  case "$1" in
    -o) out=$2; shift 2;;
    --token-file|--dist|--sigma|--cut|--width) shift 2;;
    -q) shift;;
    *) last=$1; shift;;
  esac
done
if [ "$cmd" = token ]; then
  [ -e "$out" ] && exit 2
  umask 077; echo "fake-token" > "$out"; exit 0
fi
cp "$last" "$out"
'''

#: The fake FDP tools share a tiny remote store under REMOTE: the entry for
#: remote path P is the JSON file REMOTE + P holding its size and adler32
#: ({"size": -1, "adler32": null} for the empty entry a failed PUT leaves).
FAKE_FDP_COMMON = r'''
import json, os, sys, zlib
LOG = {log!r}
CTL = {ctl!r}
REMOTE = {remote!r}
VALUED = ("--token-file", "--ca-path", "-j")

def next_code(tool):
    path = os.path.join(CTL, tool + ".codes")
    if os.path.exists(path):
        lines = open(path).read().split()
        if lines:
            open(path, "w").write("\n".join(lines[1:]))
            return int(lines[0])
    return 0

def positional(argv):
    out, skip = [], False
    for a in argv:
        if skip:
            skip = False
        elif a in VALUED:
            skip = True
        elif not a.startswith("-"):
            out.append(a)
    return out

def entry_file(path):
    return REMOTE + path

def read_entry(path):
    try:
        return json.load(open(entry_file(path)))
    except OSError:
        return None

def write_entry(path, size, adler):
    os.makedirs(os.path.dirname(entry_file(path)), exist_ok=True)
    with open(entry_file(path), "w") as h:
        json.dump({{"size": size, "adler32": adler}}, h)

def adler32(local):
    value = 1
    with open(local, "rb") as h:
        for chunk in iter(lambda: h.read(65536), b""):
            value = zlib.adler32(chunk, value)
    return "%08x" % (value & 0xffffffff)

def record(tool, **extra):
    data = {{"argv": sys.argv[1:], "token_dir": os.environ.get("FDP_TOKEN_DIR"),
             "ca_path": os.environ.get("FDP_TRANSFER_CA_PATH"),
             "sourced": os.environ.get("FAKE_ENV_SOURCED")}}
    data.update(extra)
    with open(os.path.join(LOG, tool + ".log"), "a") as h:
        h.write(json.dumps(data) + "\n")
'''

FAKE_FDP_PUT = r'''#!{python}
''' + FAKE_FDP_COMMON + r'''
# exit 0 sends every file; exit 12 leaves an empty entry (size -1) for the
# files named in fdp-put.fail (default: the first one sent) and sends the
# rest; any other exit sends nothing.  --skip-existing skips any entry.
code = next_code("fdp-put")
if "--version" in sys.argv:
    print("fdp-put 0.0-fake")
    sys.exit(0)
args = positional(sys.argv[1:])
sources, dest = args[:-1], args[-1].rstrip("/")
failing = os.path.join(CTL, "fdp-put.fail")
fail = open(failing).read().split() if os.path.exists(failing) else None
dropping = os.path.join(CTL, "fdp-put.drop")
dropped = open(dropping).read().split() if os.path.exists(dropping) else []
sent, skipped = [], []
if code in (0, 12):
    for local in sources:
        target = dest + "/" + os.path.basename(local)
        if "--skip-existing" in sys.argv and read_entry(target) is not None:
            skipped.append(target)
            continue
        broken = code == 12 and (os.path.basename(local) in fail
                                 if fail is not None else not sent)
        if os.path.basename(local) in dropped:
            sent.append((target, True))     # "sent", but nothing arrives
        elif broken:
            write_entry(target, -1, None)
            sys.stderr.write("PUT {{}}: SSL connect error\n".format(target))
        else:
            write_entry(target, os.path.getsize(local), adler32(local))
        sent.append((target, broken))
record("fdp-put", code=code, skipped=skipped)
if code:
    sys.stderr.write("fake fdp-put exit {{}}\n".format(code))
sys.exit(code)
'''

FAKE_FDP_STAT = r'''#!{python}
''' + FAKE_FDP_COMMON + r'''
# Prints the entries it finds; exits 5 if any path is missing (after
# printing the rest), like a tool that treats not-found as an error.
code = next_code("fdp-stat")
paths = positional(sys.argv[1:])
record("fdp-stat", code=code, paths=paths)
if code:
    sys.stderr.write("fake fdp-stat exit {{}}\n".format(code))
    sys.exit(code)
out, missing = [], False
for path in paths:
    e = read_entry(path)
    if e is None:
        missing = True
        continue
    out.append({{"path": path, "name": os.path.basename(path), "type": "file",
                "size": e["size"], "mtime": 1792000000,
                "checksums": {{"adler32": e["adler32"]}} if e["adler32"]
                else {{}}}})
print(json.dumps(out))
if missing:
    sys.stderr.write("not found\n")
    sys.exit(5)
'''

FAKE_FDP_RM = r'''#!{python}
''' + FAKE_FDP_COMMON + r'''
code = next_code("fdp-rm")
paths = positional(sys.argv[1:])
record("fdp-rm", code=code, paths=paths)
if not code:
    for path in paths:
        if os.path.exists(entry_file(path)):
            os.remove(entry_file(path))
        elif "-f" not in sys.argv:
            sys.exit(5)
sys.exit(code)
'''

FAKE_HTGETTOKEN = r'''#!{python}
import base64, json, os, sys
LOG = {log!r}
CTL = {ctl!r}
argv = sys.argv[1:]
with open(os.path.join(LOG, "htgettoken.log"), "a") as h:
    h.write(json.dumps({{"argv": argv,
                        "user": os.environ.get("FERMI_USER"),
                        "has_password": "FERMI_PASSWORD" in os.environ,
                        "password_ok": os.environ.get("FERMI_PASSWORD") == {password!r},
                        "krb5ccname": os.environ.get("KRB5CCNAME"),
                        "sourced": os.environ.get("FAKE_ENV_SOURCED"),
                        "stdin_tty": sys.stdin.isatty()}}) + "\n")
exp = int(open(os.path.join(CTL, "jwt.exp")).read())
payload = base64.urlsafe_b64encode(json.dumps({{"exp": exp, "sub": "x"}}).encode()).decode().rstrip("=")
out = argv[argv.index("-o") + 1]
with open(out, "w") as h:
    h.write("eyJhbGciOiJub25lIn0." + payload + ".sig\n")
sys.exit(int(open(os.path.join(CTL, "htgettoken.code")).read() or 0)
         if os.path.exists(os.path.join(CTL, "htgettoken.code")) else 0)
'''

FAKE_KLIST = r'''#!/bin/sh
echo "KRB5CCNAME=$KRB5CCNAME" >> {log}/klist.log
if [ -f {ctl}/klist.out ]; then cat {ctl}/klist.out; exit 0; fi
echo "klist: No credentials cache found (filename: /tmp/x)" >&2
exit 1
'''

FAKE_KINIT = r'''#!/bin/sh
printf '%s\n' "$*" >> {log}/kinit.log
if [ -f {ctl}/klist.renewed ]; then cp {ctl}/klist.renewed {ctl}/klist.out; fi
exit 0
'''

FAKE_MAILX = r'''#!{python}
import json, os, sys
LOG = {log!r}
with open(os.path.join(LOG, "mail.log"), "a") as h:
    h.write(json.dumps({{"argv": sys.argv[1:], "body": sys.stdin.read()}}) + "\n")
'''

ENV_SCRIPT = "export FAKE_ENV_SOURCED=yes\n"


class Rig:
    """Paths, fakes and helpers for one test."""

    def __init__(self, tmp_path, archive_path):
        self.root = tmp_path
        self.bin = tmp_path / "bin"
        self.logs = tmp_path / "logs"
        self.ctl = tmp_path / "ctl"
        self.data = tmp_path / "data"
        self.state_dir = tmp_path / "state"
        self.status_dir = tmp_path / "status"
        self.tokens = tmp_path / "tokens"
        self.remote = tmp_path / "remote"
        for d in (self.bin, self.logs, self.ctl, self.remote):
            d.mkdir()
        self.tokens.mkdir(mode=0o700)
        fmt = dict(python=sys.executable, log=str(self.logs), ctl=str(self.ctl),
                   db=str(archive_path), password=PASSWORD,
                   remote=str(self.remote))
        for name, text in (("darpa-spill-dump", FAKE_DUMP),
                           ("fake-shift", FAKE_SHIFT),
                           ("fdp-put", FAKE_FDP_PUT),
                           ("fdp-stat", FAKE_FDP_STAT),
                           ("fdp-rm", FAKE_FDP_RM),
                           ("htgettoken", FAKE_HTGETTOKEN),
                           ("klist", FAKE_KLIST), ("kinit", FAKE_KINIT),
                           ("mailx", FAKE_MAILX)):
            path = self.bin / name
            path.write_text(text.format(**fmt))
            path.chmod(0o755)
        self.env_script = tmp_path / "env.sh"
        self.env_script.write_text(ENV_SCRIPT)
        (self.ctl / "jwt.exp").write_text("0")
        self.env_file = tmp_path / "fdp.env"
        self.env_file.write_text("FERMI_USER=CHANGEME\nFERMI_PASSWORD=CHANGEME\n")
        self.real_shift = os.path.join(os.path.dirname(sys.executable),
                                       "darpa-spill-shift")
        self.config = {
            "server": {"url": "https://spills.test.invalid:8443",
                       "insecure": True, "timeout": 30},
            "timezone": "America/Chicago",
            "data_root": str(self.data),
            "formats": ["csv", "json"],
            "prefix": "nd",
            "streams": {"all": {}},
            "hourly": {"catchup_hours": 1},
            "daily": {"catchup_days": 0},
            "ingest_check": {"enabled": True, "wait": 0, "poll": 1},
            "shift": {"token_dir": str(self.tokens)},
            "fdp": {"env_script": str(self.env_script),
                    "ca_path": str(tmp_path / "certs"),
                    "token_file": str(self.tokens / "fdp_bt_test"),
                    "raw_dest": "/amsc/novaend/raw",
                    "shifted_dest": "/amsc/public/novaend/shifted"},
            "credentials": {
                "krb5ccname": "FILE:" + str(self.tokens / "krb5cc_test"),
                "principal": "anorman@FNAL.GOV",
                "env_file": str(self.env_file),
                "sso_cookie_file": str(self.tokens / "sso.cookies")},
            "paths": {"state_dir": str(self.state_dir),
                      "status_dir": str(self.status_dir),
                      "log_dir": str(tmp_path / "publog")},
            "alerts": {"email": ["ops@example.invalid"],
                       "mail_command": str(self.bin / "mailx"),
                       "disk_warn_percent": 101},
            "tools": {"dump": str(self.bin / "darpa-spill-dump"),
                      "shift": str(self.bin / "fake-shift")},
        }
        self.environ = {"PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
                        "HOME": str(tmp_path)}
        self.watermark = 10 ** 18          # far future: ingest always ready
        self.polled_through = None         # older servers do not report it
        self.running = True

    def configure(self, **sections):
        for key, value in sections.items():
            if isinstance(value, dict) and isinstance(self.config.get(key), dict) \
                    and key != "streams":
                self.config[key].update(value)
            else:
                self.config[key] = value

    def write_config(self):
        path = self.root / "publish.yaml"
        path.write_text(yaml.safe_dump(self.config, sort_keys=False))
        return path

    def main(self, *argv, now, environ=None):
        env = dict(self.environ)
        env.update(environ or {})
        return publish.main(["-c", str(self.write_config())] + list(argv),
                            environ=env, now=now)

    def log(self, name):
        path = self.logs / name
        if not path.exists():
            return []
        lines = path.read_text().splitlines()
        if name.endswith(".log") and name not in ("shift.log", "kinit.log",
                                                   "klist.log"):
            return [json.loads(l) for l in lines]
        return lines

    def mails(self):
        return self.log("mail.log")

    def db(self):
        conn = sqlite3.connect(str(self.state_dir / "publish.db"))
        conn.row_factory = sqlite3.Row
        return conn

    def files(self):
        with self.db() as conn:
            return {r["raw_path"]: dict(r) for r in
                    conn.execute("SELECT * FROM files")}

    def runs(self):
        with self.db() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM runs ORDER BY id")]

    def remote_entry(self, path):
        """The fake FDP store's entry for remote *path*, or None."""
        entry = self.remote / path.lstrip("/")
        return json.loads(entry.read_text()) if entry.exists() else None

    def set_remote(self, path, size, adler32):
        entry = self.remote / path.lstrip("/")
        entry.parent.mkdir(parents=True, exist_ok=True)
        entry.write_text(json.dumps({"size": size, "adler32": adler32}))

    def set_codes(self, tool, *codes):
        (self.ctl / "{}.codes".format(tool)).write_text(
            "\n".join(str(c) for c in codes))

    def sources(self, url, ca_bundle, insecure, timeout):
        if url.endswith("/api/health"):
            return {"status": "ok"}
        ingest = {"watermark_nova": self.watermark, "running": self.running}
        if self.polled_through is not None:
            ingest["polled_through_nova"] = self.polled_through
        return {"sources": [
            {"name": "tdu-a", "enabled": True, "ingest": ingest},
            {"name": "tdu-off", "enabled": False,
             "ingest": {"watermark_nova": 0}},
        ]}


@pytest.fixture(scope="module")
def archive(tmp_path_factory):
    """Events every 5 min from 2026-10-21 to 2026-11-03 UTC, cycling types."""
    path = tmp_path_factory.mktemp("archive") / "spills.db"
    store = SpillStore(str(path))
    kinds = [SpillType.ACCEL_ONE_HZ_TCLK, SpillType.NUMI, SpillType.BNB]
    start = nova(utc(2026, 10, 21))
    step = 300 * TICKS_PER_SECOND
    events = [SpillEvent(nova_time=start + i * step + 12345,
                         spill_type=kinds[i % 3], event_number=10000 + i,
                         source="tdu-a", route="spill_history")
              for i in range(13 * 288)]
    store.insert_events(events)
    store.close()
    return path


@pytest.fixture
def rig(tmp_path, archive, monkeypatch):
    r = Rig(tmp_path, archive)
    monkeypatch.setattr(publish, "http_get_json", r.sources)
    monkeypatch.setattr(publish, "_sleep", lambda seconds: None)
    return r


def publisher(rig, now=None, dry_run=False):
    args = publish.build_parser().parse_args(["-c", str(rig.write_config()),
                                              "status"])
    cfg = publish.load_settings(args, rig.environ)
    return publish.Publisher(cfg, rig.environ, dry_run=dry_run, now=now)


# ------------------------------------------------------------ labels/paths


def test_paths_for_a_normal_hour(rig):
    pub = publisher(rig)
    paths = pub.file_paths("numi", "2026-10-22T13-0500")
    raw, shifted = paths["csv"]
    assert raw == str(rig.data / "raw/oct/2026-oct-22/nd-numi-2026-10-22T13-0500.csv")
    assert shifted == str(rig.data / "shifted/oct/2026-oct-22/"
                          "nd-numi-2026-10-22T13-0500.shifted.csv")
    assert paths["json"][0].endswith("nd-numi-2026-10-22T13-0500.json")
    assert pub.remote_dir(raw, "raw") == "/amsc/novaend/raw/oct/2026-oct-22"
    assert pub.remote_dir(shifted, "shifted") == \
        "/amsc/public/novaend/shifted/oct/2026-oct-22"


def test_daily_label_and_path(rig):
    pub = publisher(rig)
    raw, shifted = pub.file_paths("booster", "2026-10-22")["json"]
    assert raw.endswith("raw/oct/2026-oct-22/nd-booster-2026-10-22.json")
    assert shifted.endswith("shifted/oct/2026-oct-22/"
                            "nd-booster-2026-10-22.shifted.json")


def test_spring_forward_day_has_23_hours(rig):
    pub = publisher(rig)
    from datetime import date
    start = pub.midnight(date(2027, 3, 14))
    end = pub.midnight(date(2027, 3, 15))
    assert (end - start) == 23 * publish.HOUR
    days = split_range(start, end, "day", CHICAGO)
    assert [d[2] for d in days] == ["2027-03-14"]
    hours = split_range(start, end, "hour", CHICAGO)
    labels = [h[2] for h in hours]
    assert len(labels) == 23
    assert "2027-03-14T01-0600" in labels and "2027-03-14T03-0500" in labels
    assert not any(l.startswith("2027-03-14T02") for l in labels)
    assert pub.file_paths("all", labels[0])["csv"][0].endswith(
        "raw/mar/2027-mar-14/nd-all-2027-03-14T00-0600.csv")


def test_fall_back_day_has_25_hours_and_two_one_oclocks(rig):
    pub = publisher(rig)
    from datetime import date
    start = pub.midnight(date(2026, 11, 1))
    end = pub.midnight(date(2026, 11, 2))
    assert end - start == 25 * publish.HOUR
    labels = [h[2] for h in split_range(start, end, "hour", CHICAGO)]
    assert len(labels) == 25
    assert labels[1:3] == ["2026-11-01T01-0500", "2026-11-01T01-0600"]
    first = pub.file_paths("all", labels[1])["csv"][0]
    second = pub.file_paths("all", labels[2])["csv"][0]
    assert first != second
    assert os.path.dirname(first) == os.path.dirname(second)


def test_month_and_year_rollover(rig):
    pub = publisher(rig)
    start = nova(datetime(2026, 12, 31, 23, tzinfo=CHICAGO))
    labels = [p[2] for p in split_range(start, start + 2 * publish.HOUR,
                                        "hour", CHICAGO)]
    assert labels == ["2026-12-31T23-0600", "2027-01-01T00-0600"]
    assert pub.file_paths("all", labels[0])["csv"][0].endswith(
        "raw/dec/2026-dec-31/nd-all-2026-12-31T23-0600.csv")
    assert pub.file_paths("all", labels[1])["csv"][0].endswith(
        "raw/jan/2027-jan-01/nd-all-2027-01-01T00-0600.csv")
    assert pub.token_path("2027", "01").endswith("dts1-2027-01.token")
    # Oct -> Nov at local midnight, not UTC midnight.
    late = nova(datetime(2026, 10, 31, 23, tzinfo=CHICAGO))
    labels = [p[2] for p in split_range(late, late + 2 * publish.HOUR,
                                        "hour", CHICAGO)]
    assert pub.file_paths("all", labels[1])["csv"][0].endswith(
        "raw/nov/2026-nov-01/nd-all-2026-11-01T00-0500.csv")


def test_parse_hour_accepts_labels_and_local_times(rig):
    pub = publisher(rig)
    assert pub.parse_hour("2026-11-01T01-0600") == nova(utc(2026, 11, 1, 7))
    assert pub.parse_hour("2026-11-01T01-0500") == nova(utc(2026, 11, 1, 6))
    assert pub.parse_hour("2026-10-22T13") == nova(utc(2026, 10, 22, 18))
    assert pub.parse_hour("2026-10-22T13:42:10") == nova(utc(2026, 10, 22, 18))


# ---------------------------------------------------------------- hourly


def test_hourly_dumps_shifts_uploads_and_reports(rig):
    rig.configure(streams={"numi": {"signals": ["$74", "$A9"]}, "all": {},
                           "booster": {"types": ["BNB", "BNB_TCLK"]}})
    # 13:10 CDT: the preceding hour is 12:00-13:00.
    code = rig.main("hourly", now=utc(2026, 10, 22, 18, 10))
    assert code == 0

    dumps = rig.log("dump.log")
    assert len(dumps) == 3                      # one call per stream
    numi = dumps[0]
    assert "--start=2026-10-22T12:00:00-05:00" in numi
    assert "--end=2026-10-22T13:00:00-05:00" in numi
    assert numi[numi.index("--split") + 1] == "hour"
    assert numi[numi.index("--timezone") + 1] == "America/Chicago"
    assert "-k" in numi and "-q" in numi
    assert numi[numi.index("-s") + 1] == "https://spills.test.invalid:8443"
    assert "--skip-empty" not in numi
    assert numi[numi.index("--csv") + 1].endswith(
        "raw/{mon}/{year}-{mon}-{day}/nd-numi-{start}.csv")
    assert [numi[i + 1] for i, a in enumerate(numi) if a == "--signal"] == \
        ["$74", "$A9"]
    booster = dumps[2]
    assert [booster[i + 1] for i, a in enumerate(booster) if a == "--type"] == \
        ["BNB", "BNB_TCLK"]

    files = rig.files()
    assert len(files) == 6
    for stream in ("numi", "all", "booster"):
        for ext in ("csv", "json"):
            raw = rig.data / "raw/oct/2026-oct-22/nd-{}-2026-10-22T12-0500.{}".format(
                stream, ext)
            shifted = rig.data / ("shifted/oct/2026-oct-22/nd-{}-2026-10-22T12"
                                  "-0500.shifted.{}".format(stream, ext))
            assert raw.is_file() and shifted.is_file()
            row = files[str(raw)]
            assert row["kind"] == "hourly" and row["label"] == "2026-10-22T12-0500"
            assert row["dumped_at"] and row["shifted_at"]
            assert row["raw_uploaded_at"] and row["shifted_uploaded_at"]
            assert row["bytes"] == raw.stat().st_size
    # 12 events an hour, cycling three types; numi selects NUMI only.
    assert files[str(rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.csv")]["rows"] == 12
    assert files[str(rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.json")]["rows"] == 12
    assert files[str(rig.data / "raw/oct/2026-oct-22/nd-numi-2026-10-22T12-0500.csv")]["rows"] == 4

    # Raw and shifted go in separate calls, each to its own directory.
    puts = rig.log("fdp-put.log")
    assert len(puts) == 2
    for put in puts:
        argv = put["argv"]
        assert argv[0] == "--skip-existing"
        assert argv[argv.index("--token-file") + 1] == str(rig.tokens / "fdp_bt_test")
        assert argv[argv.index("--ca-path") + 1] == str(rig.root / "certs")
        assert argv[argv.index("-j") + 1] == "4"
        assert put["token_dir"] == str(rig.tokens)
        assert put["sourced"] == "yes"
        assert "--replace" not in argv
    dests = sorted(p["argv"][-1] for p in puts)
    assert dests == ["/amsc/novaend/raw/oct/2026-oct-22/",
                     "/amsc/public/novaend/shifted/oct/2026-oct-22/"]
    raw_put = [p for p in puts if p["argv"][-1].startswith("/amsc/novaend/raw")][0]
    assert len([a for a in raw_put["argv"] if a.endswith((".csv", ".json"))]) == 6

    # The token of October was created (and announced) for October data.
    assert (rig.tokens / "dts1-2026-10.token").is_file()
    subjects = [m["argv"][1] for m in rig.mails()]
    assert "[darpa-spill-publish] new shift token created - back it up" in subjects

    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["jobs"]["hourly"]["result"] == "ok"
    assert status["hourly"]["grid"]["2026-10-22T12-0500"] == {
        "numi": "public", "all": "public", "booster": "public"}
    assert status["hourly"]["labels"][0] == "2026-10-22T12-0500"
    assert len(status["hourly"]["labels"]) == 48
    assert status["hourly"]["grid"]["2026-10-22T11-0500"]["all"] == "missing"
    assert status["pending_uploads"] == 0
    assert status["totals"]["files"] == 6
    assert status["ingest"]["sources"][0]["name"] == "tdu-a"
    page = (rig.status_dir / "index.html").read_text()
    assert '<meta http-equiv="refresh" content="60">' in page
    assert "prefers-color-scheme: dark" in page
    assert "2026-10-22T12-0500" in page and "tdu-a" in page

    # A second run in the same hour has nothing left to do.
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 40)) == 0
    assert len(rig.log("dump.log")) == 3
    assert len(rig.log("fdp-put.log")) == 2


def test_hourly_catches_up_missing_hours_in_one_call(rig):
    rig.configure(hourly={"catchup_hours": 3})
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    first = rig.log("dump.log")
    assert len(first) == 1
    assert "--start=2026-10-22T10:00:00-05:00" in first[0]
    assert "--end=2026-10-22T13:00:00-05:00" in first[0]

    # Three hourly runs missed; the next covers 13, 14 and 15 in one call.
    rig.configure(hourly={"catchup_hours": 6})
    assert rig.main("hourly", now=utc(2026, 10, 22, 21, 10)) == 0
    second = rig.log("dump.log")[1:]
    assert len(second) == 1
    assert "--start=2026-10-22T13:00:00-05:00" in second[0]
    assert "--end=2026-10-22T16:00:00-05:00" in second[0]
    labels = sorted({r["label"] for r in rig.files().values()})
    assert labels == ["2026-10-22T{:02d}-0500".format(h) for h in range(10, 16)]


def test_a_hole_in_the_catchup_window_is_refilled_alone(rig):
    rig.configure(hourly={"catchup_hours": 3})
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    # Lose the 11:00 hour's JSON from both disk and the database.
    lost = rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T11-0500.json"
    lost.unlink()
    with rig.db() as conn:
        conn.execute("DELETE FROM files WHERE raw_path = ?", (str(lost),))
    # The new dump differs from the complete copy already on FDP.  That copy
    # may be the published one, so it is never removed automatically (D9,
    # D10): the file stays pending with an error for a person to resolve.
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 20)) == 3
    assert rig.log("fdp-rm.log") == []
    error = rig.files()[str(lost)]["last_error"]
    assert "differs from" in error and "not replaced" in error
    # Every pending copy conflicts, so the retry fails outright (exit 1).
    assert rig.main("upload", now=utc(2026, 10, 22, 18, 25)) == 1
    assert rig.files()[str(lost)]["raw_uploaded_at"] is None
    call = rig.log("dump.log")[-1]
    assert "--start=2026-10-22T11:00:00-05:00" in call
    assert "--end=2026-10-22T12:00:00-05:00" in call
    # Only the missing format is dumped; the recorded CSV is not rewritten.
    assert "--json" in call and "--csv" not in call
    assert lost.is_file()


def test_file_on_disk_but_not_recorded_is_adopted_not_redumped(rig):
    pub = publisher(rig)
    label = "2026-10-22T12-0500"
    for fmt, (raw, _) in pub.file_paths("all", label).items():
        os.makedirs(os.path.dirname(raw), exist_ok=True)
        with open(raw, "w") as handle:
            handle.write("# comment\nnova_time,source\n1,a\n2,a\n" if fmt == "csv"
                         else '{"meta": {}, "events": [{}, {}],\n'
                              '"summary": {"returned": 2}\n}\n')
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    assert rig.log("dump.log") == []
    rows = {r["fmt"]: r for r in rig.files().values()}
    assert rows["csv"]["rows"] == 2 and rows["json"]["rows"] == 2
    assert rows["csv"]["shifted_at"]


def test_fall_back_hours_are_two_distinct_files(rig):
    rig.configure(hourly={"catchup_hours": 3})
    # The second 01:00 (CST), as a label; catch-up adds 00:00 and 01:00 CDT.
    assert rig.main("hourly", "--hour", "2026-11-01T01-0600",
                    now=utc(2026, 11, 3)) == 0
    day = rig.data / "raw/nov/2026-nov-01"
    cdt = day / "nd-all-2026-11-01T01-0500.csv"
    cst = day / "nd-all-2026-11-01T01-0600.csv"
    assert cdt.is_file() and cst.is_file()
    assert (day / "nd-all-2026-11-01T00-0500.csv").is_file()
    rows_cdt = [l for l in cdt.read_text().splitlines()[5:]]
    rows_cst = [l for l in cst.read_text().splitlines()[5:]]
    assert len(rows_cdt) == len(rows_cst) == 12
    assert set(rows_cdt).isdisjoint(rows_cst)


def test_fall_back_day_file_holds_all_25_hours(rig):
    rig.configure(hourly={"catchup_hours": 25})
    assert rig.main("hourly", "--hour", "2026-11-01T23",
                    now=utc(2026, 11, 3)) == 0
    assert rig.main("daily", "--date", "2026-11-01",
                    now=utc(2026, 11, 3)) == 0
    files = rig.files().values()
    hourly = [r for r in files if r["kind"] == "hourly" and r["fmt"] == "csv"]
    daily = [r for r in files if r["kind"] == "daily" and r["fmt"] == "csv"]
    assert len(hourly) == 25
    assert {r["label"][:10] for r in hourly} == {"2026-11-01"}
    assert len(daily) == 1 and daily[0]["label"] == "2026-11-01"
    assert daily[0]["rows"] == sum(r["rows"] for r in hourly) == 25 * 12
    call = rig.log("dump.log")[-1]
    assert "--start=2026-11-01T00:00:00-05:00" in call
    assert "--end=2026-11-02T00:00:00-06:00" in call
    assert call[call.index("--split") + 1] == "day"


def test_token_is_chosen_by_data_month_not_run_month(rig):
    # 00:10 CDT on 1 November: the preceding hour is 23:00 on 31 October.
    assert rig.main("hourly", now=utc(2026, 11, 1, 5, 10)) == 0
    shifts = [l for l in rig.log("shift.log") if l.startswith("apply")]
    assert shifts and all("dts1-2026-10.token" in l for l in shifts)
    assert all("raw/oct/2026-oct-31/nd-all-2026-10-31T23-0500" in l
               for l in shifts)
    assert (rig.tokens / "dts1-2026-10.token").is_file()
    assert not (rig.tokens / "dts1-2026-11.token").exists()


def test_existing_token_is_never_overwritten(rig):
    token = rig.tokens / "dts1-2026-10.token"
    token.write_text("keep-me\n")
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    assert token.read_text() == "keep-me\n"
    assert not [l for l in rig.log("shift.log") if l.startswith("token")]
    assert not rig.mails()


def test_real_shift_inverts_byte_identically(rig):
    rig.configure(tools={"shift": rig.real_shift})
    if not os.access(rig.real_shift, os.X_OK):
        pytest.skip("darpa-spill-shift is not installed beside this python")
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    token = rig.tokens / "dts1-2026-10.token"
    assert token.read_text().startswith("dts1:gauss:sigma=3200:cut=9600:")
    assert stat.S_IMODE(token.stat().st_mode) == 0o600
    for ext in ("csv", "json"):
        raw = rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.{}".format(ext)
        shifted = rig.data / ("shifted/oct/2026-oct-22/"
                              "nd-all-2026-10-22T12-0500.shifted.{}".format(ext))
        restored = rig.root / ("restored." + ext)
        assert shifted.read_bytes() != raw.read_bytes()
        subprocess.run([rig.real_shift, "invert", "-q", "--token-file",
                        str(token), "-o", str(restored), str(shifted)],
                       check=True)
        assert restored.read_bytes() == raw.read_bytes()


# ------------------------------------------------------------ ingest gate


def test_stalled_ingest_blocks_the_dump(rig):
    rig.watermark = nova(utc(2026, 10, 22, 17, 30))      # 12:30 CDT
    code = rig.main("hourly", now=utc(2026, 10, 22, 18, 10))
    assert code == publish.EXIT_TEMPFAIL
    assert rig.log("dump.log") == []
    assert rig.files() == {}
    assert rig.runs()[-1]["result"] == "ingest-not-ready"
    mails = rig.mails()
    assert len(mails) == 1
    assert mails[0]["argv"][1] == "[darpa-spill-publish] ingest not ready"
    assert "2026-10-22T12-0500" in mails[0]["body"]
    # Rate-limited: the next run does not mail again.
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 20)) == 75
    assert len(rig.mails()) == 1


def test_ingest_gate_lets_ready_catchup_hours_through(rig):
    rig.configure(hourly={"catchup_hours": 3})
    rig.watermark = nova(utc(2026, 10, 22, 17, 30))      # 12:30 CDT
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 75
    labels = sorted({r["label"] for r in rig.files().values()})
    assert labels == ["2026-10-22T10-0500", "2026-10-22T11-0500"]
    # Ingest catches up; the next run does the remaining hour only.
    rig.watermark = 10 ** 18
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 30)) == 0
    assert "--start=2026-10-22T12:00:00-05:00" in rig.log("dump.log")[-1]


def test_unreadable_sources_count_as_not_ready(rig, monkeypatch):
    def broken(*args):
        raise OSError("connection refused")
    monkeypatch.setattr(publish, "http_get_json", broken)
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 75
    assert "connection refused" in rig.mails()[0]["body"]


def test_ingest_check_can_be_disabled(rig):
    rig.configure(ingest_check={"enabled": False})
    rig.watermark = 0
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0


# ---------------------------------------------------------------- dump exits


def test_dump_exit_3_is_retried_once(rig):
    rig.set_codes("dump", 3, 3)
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    assert len(rig.log("dump.log")) == 2
    detail = json.loads(rig.runs()[-1]["detail"])
    assert any("changed during the dump twice" in w for w in detail["warnings"])
    assert len(rig.files()) == 2


def test_dump_failure_is_recorded_and_alerted(rig):
    rig.set_codes("dump", 1)
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 1
    rows = list(rig.files().values())
    assert len(rows) == 2 and all(r["last_error"] and not r["dumped_at"]
                                  for r in rows)
    assert rig.mails()[0]["argv"][1] == "[darpa-spill-publish] darpa-spill-dump failed"
    assert rig.runs()[-1]["result"] == "failed"
    # The next run retries the hour.
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 20)) == 0
    assert all(r["dumped_at"] and not r["last_error"]
               for r in rig.files().values())


# ------------------------------------------------------------------ upload


@pytest.mark.parametrize("code", [4, 7])
def test_upload_auth_or_network_failure_then_retry(rig, code):
    rig.configure(hourly={"catchup_hours": 2})
    rig.set_codes("fdp-put", code)
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == \
        publish.EXIT_PARTIAL
    # The first failure stops the stage: no point hammering the server.
    assert len(rig.log("fdp-put.log")) == 1
    rows = rig.files().values()
    assert not any(r["raw_uploaded_at"] or r["shifted_uploaded_at"] for r in rows)
    assert any("fdp-put exit {}".format(code) in (r["last_error"] or "")
               for r in rows)
    subject = rig.mails()[-1]["argv"][1]
    assert "upload failed: fdp-put exit {}".format(code) in subject

    assert rig.main("upload", now=utc(2026, 10, 22, 18, 25)) == 0
    puts = rig.log("fdp-put.log")[1:]
    assert len(puts) == 2 and all("--skip-existing" in p["argv"] for p in puts)
    rows = rig.files().values()
    assert all(r["raw_uploaded_at"] and r["shifted_uploaded_at"]
               and not r["last_error"] for r in rows)
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["pending_uploads"] == 0
    assert status["jobs"]["upload"]["result"] == "ok"


RAW_DIR = "/amsc/novaend/raw/oct/2026-oct-22/"
SHIFTED_DIR = "/amsc/public/novaend/shifted/oct/2026-oct-22/"
ZOMBIE_ERROR = ("raw upload: removed incomplete remote copy (size -1, "
                "adler32 none); will retry")


def test_partial_upload_zombie_is_removed_and_retried(rig):
    rig.set_codes("fdp-put", 12, 0)
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 3
    zombie = RAW_DIR + "nd-all-2026-10-22T12-0500.csv"
    files = rig.files()
    row = files[str(rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.csv")]
    # The failed PUT left an empty entry: removed, the copy still pending.
    assert row["raw_uploaded_at"] is None and row["last_error"] == ZOMBIE_ERROR
    assert rig.log("fdp-rm.log")[0]["paths"] == [zombie]
    assert rig.log("fdp-rm.log")[0]["argv"][0] == "-f"
    assert rig.remote_entry(zombie) is None
    # The rest of the partial batch is on FDP and verified, so uploaded.
    others = [r for p, r in files.items() if p != row["raw_path"]]
    assert len(others) == 1 and others[0]["raw_uploaded_at"]
    assert all(r["shifted_uploaded_at"] for r in files.values())
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["pending_uploads"] == 1
    # One partial is not mailed.
    assert not [m for m in rig.mails() if "upload" in m["argv"][1]]

    assert rig.main("upload", now=utc(2026, 10, 22, 18, 25)) == 0
    last = rig.log("fdp-put.log")[-1]
    assert last["argv"][-1] == RAW_DIR and last["skipped"] == []
    assert [a for a in last["argv"] if a.endswith(".csv")] == [row["raw_path"]]
    assert all(r["raw_uploaded_at"] and not r["last_error"]
               for r in rig.files().values())
    assert rig.remote_entry(zombie)["size"] == os.path.getsize(row["raw_path"])
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["pending_uploads"] == 0


def test_skip_existing_over_a_zombie_is_not_marked_uploaded(rig):
    """The gateway bug: the retry's --skip-existing skipped the empty entry
    a failed PUT left, exited 0, and every file was recorded uploaded."""
    rig.set_codes("fdp-put", 12, 0)
    rig.set_codes("fdp-rm", 1)                 # the zombie survives run 1
    raw = str(rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.csv")
    zombie = RAW_DIR + "nd-all-2026-10-22T12-0500.csv"
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 3
    assert rig.remote_entry(zombie) == {"size": -1, "adler32": None}
    assert "fdp-rm -f exited 1" in rig.files()[raw]["last_error"]
    assert "cannot remove an incomplete upload" in rig.mails()[-1]["argv"][1]

    # Retry: fdp-put skips the zombie and exits 0; the stat catches it.
    assert rig.main("upload", now=utc(2026, 10, 22, 18, 25)) == 1
    put = rig.log("fdp-put.log")[-1]
    assert put["code"] == 0 and put["skipped"] == [zombie]
    row = rig.files()[raw]
    assert row["raw_uploaded_at"] is None and row["last_error"] == ZOMBIE_ERROR
    assert rig.remote_entry(zombie) is None
    assert "--replace" not in [a for c in rig.log("fdp-put.log")
                               for a in c["argv"]]

    assert rig.main("upload", now=utc(2026, 10, 22, 18, 40)) == 0
    row = rig.files()[raw]
    assert row["raw_uploaded_at"] and not row["last_error"]


def test_a_missing_remote_file_stays_pending(rig):
    (rig.ctl / "fdp-put.drop").write_text("nd-all-2026-10-22T12-0500.json\n")
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 3
    raw = str(rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.json")
    row = rig.files()[raw]
    assert row["raw_uploaded_at"] is None and row["shifted_uploaded_at"]
    assert "fdp-put exited 0 but" in row["last_error"]
    assert "is not on FDP" in row["last_error"]
    assert rig.log("fdp-rm.log") == []
    # fdp-stat exits 5 for the batch, so each path is asked alone.
    stats = [c["paths"] for c in rig.log("fdp-stat.log")]
    assert len(stats[0]) == 2 and stats[1:3] == [[p] for p in stats[0]]
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["pending_uploads"] == 1
    assert status["hourly"]["grid"]["2026-10-22T12-0500"]["all"] == "error"
    (rig.ctl / "fdp-put.drop").unlink()
    assert rig.main("upload", now=utc(2026, 10, 22, 18, 25)) == 0
    row = rig.files()[raw]
    assert row["raw_uploaded_at"] and not row["last_error"]


def test_a_good_remote_file_is_uploaded_only_after_the_stat_matches(rig):
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    stats = rig.log("fdp-stat.log")
    assert len(stats) == 2                       # one per batch, both found
    for call in stats:
        argv = call["argv"]
        assert argv[0] == "--json" and call["sourced"] == "yes"
        assert argv[argv.index("--token-file") + 1] == str(rig.tokens / "fdp_bt_test")
        assert argv[argv.index("--ca-path") + 1] == str(rig.root / "certs")
        assert call["token_dir"] == str(rig.tokens)
    assert sorted(len(c["paths"]) for c in stats) == [2, 2]
    for row in rig.files().values():
        assert row["raw_adler32"] == publish.adler32_file(row["raw_path"])
        assert row["shifted_adler32"] == publish.adler32_file(row["shifted_path"])

    # A complete copy with a wrong checksum keeps the file pending and is
    # left on FDP: only size -1 leftovers of failed PUTs are removed.
    rig.configure(fdp={"enabled": False})
    assert rig.main("hourly", now=utc(2026, 10, 22, 19, 10)) == 0
    rig.configure(fdp={"enabled": True})
    name = "nd-all-2026-10-22T13-0500.csv"
    raw = str(rig.data / "raw/oct/2026-oct-22" / name)
    rig.set_remote(RAW_DIR + name, os.path.getsize(raw), "0000beef")
    assert rig.main("upload", now=utc(2026, 10, 22, 19, 20)) == 3
    row = rig.files()[raw]
    assert row["raw_uploaded_at"] is None
    assert "adler32 0000beef) differs from" in row["last_error"]
    assert "not replaced" in row["last_error"]
    assert rig.log("fdp-rm.log") == []
    assert rig.remote_entry(RAW_DIR + name)["adler32"] == "0000beef"


def test_upload_verify_repairs_zombies_and_reports_mismatches(rig):
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    rows = rig.files()
    zombie = RAW_DIR + "nd-all-2026-10-22T12-0500.csv"
    gone = SHIFTED_DIR + "nd-all-2026-10-22T12-0500.shifted.csv"
    differs = RAW_DIR + "nd-all-2026-10-22T12-0500.json"
    differs_raw = str(rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.json")
    rig.set_remote(zombie, -1, None)
    (rig.remote / gone.lstrip("/")).unlink()
    rig.set_remote(differs, rows[differs_raw]["bytes"], "12345678")
    stats_before = len(rig.log("fdp-stat.log"))
    # A plain upload does not look at uploaded files.
    assert rig.main("upload", now=utc(2026, 10, 22, 18, 20)) == 0
    assert len(rig.log("fdp-stat.log")) == stats_before

    assert rig.main("upload", "--verify", now=utc(2026, 10, 22, 18, 30)) == 3
    # Only the size -1 entry is removed; a real mismatch is never touched.
    assert [c["paths"] for c in rig.log("fdp-rm.log")] == [[zombie]]
    assert rig.remote_entry(differs)["adler32"] == "12345678"
    row = rig.files()[differs_raw]
    assert row["raw_uploaded_at"] == rows[differs_raw]["raw_uploaded_at"]
    assert "differs from" in row["last_error"]
    assert "not replaced" in row["last_error"]
    mail = rig.mails()[-1]
    assert mail["argv"][1] == "[darpa-spill-publish] uploaded file differs on FDP"
    assert "fdp-rm -f " + differs in mail["body"]
    # The zombie and the missing copy were reset and sent again.
    csv = rig.files()[str(rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.csv")]
    assert csv["raw_uploaded_at"] > rows[csv["raw_path"]]["raw_uploaded_at"]
    assert csv["shifted_uploaded_at"] > rows[csv["raw_path"]]["shifted_uploaded_at"]
    assert not csv["last_error"]
    assert rig.remote_entry(zombie)["size"] == csv["bytes"]
    assert rig.remote_entry(gone)["size"] > 0
    run = rig.runs()[-1]
    assert json.loads(run["detail"])["verify"] == {
        "checked": 4, "ok": 1, "reset": 2, "mismatch": 1, "unchecked": 0}
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["pending_uploads"] == 0


def test_a_mismatch_on_a_copy_already_recorded_uploaded_is_never_removed(rig):
    rig.configure(fdp={"enabled": False})
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    raw = str(rig.data / "raw/oct/2026-oct-22/nd-all-2026-10-22T12-0500.csv")
    remote = RAW_DIR + "nd-all-2026-10-22T12-0500.csv"
    rig.set_remote(remote, -1, None)
    pub = publisher(rig, now=utc(2026, 10, 22, 18, 20))
    # Recorded by another run between the pending query and the stat.
    pub.state.update(raw, pub.stamp(), raw_uploaded_at=pub.stamp())
    assert pub._verify_batch(RAW_DIR.rstrip("/"), [(raw, "raw", raw)], 0) is False
    pub.state.close()
    assert rig.log("fdp-rm.log") == []
    assert rig.remote_entry(remote) == {"size": -1, "adler32": None}
    assert "recorded uploaded; not removed" in rig.files()[raw]["last_error"]
    assert rig.mails()[-1]["argv"][1] == \
        "[darpa-spill-publish] uploaded file differs on FDP"


def test_upload_verify_since_and_dry_run(rig, capsys):
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    stats = len(rig.log("fdp-stat.log"))
    assert rig.main("upload", "--verify", "--since", "2026-10-23",
                    now=utc(2026, 10, 23, 18)) == 0
    assert len(rig.log("fdp-stat.log")) == stats
    assert rig.main("upload", "--since", "2026-10-23",
                    now=utc(2026, 10, 23, 18)) == publish.EXIT_USAGE
    assert rig.main("upload", "--verify", "--since", "10/22",
                    now=utc(2026, 10, 23, 18)) == publish.EXIT_USAGE

    rig.set_remote(RAW_DIR + "nd-all-2026-10-22T12-0500.csv", -1, None)
    before = (rig.state_dir / "publish.db").read_bytes()
    capsys.readouterr()
    assert rig.main("-n", "upload", "--verify", "--since", "2026-10-22",
                    now=utc(2026, 10, 23, 18)) == 0
    out = capsys.readouterr().out
    assert out.count("fdp-stat --json") == 2
    assert len(rig.log("fdp-stat.log")) == stats and rig.log("fdp-rm.log") == []
    assert (rig.state_dir / "publish.db").read_bytes() == before

    assert rig.main("upload", "--verify", "--since", "2026-10-22",
                    now=utc(2026, 10, 23, 18, 10)) == 0
    assert len(rig.log("fdp-stat.log")) > stats
    assert len(rig.log("fdp-rm.log")) == 1


def test_adler32_formatting_and_stat_parsing(tmp_path):
    empty = tmp_path / "empty"
    empty.write_bytes(b"")
    assert publish.adler32_file(str(empty)) == "00000001"
    small = tmp_path / "a"
    small.write_bytes(b"a")
    assert publish.adler32_file(str(small)) == "00620062"
    big = tmp_path / "big"
    data = bytes(range(256)) * 9000             # > 2 MiB: several chunks
    big.write_bytes(data)
    assert publish.adler32_file(str(big)) == "{:08x}".format(zlib.adler32(data))
    assert publish._norm_adler32("620062") == "00620062"
    assert publish._norm_adler32("0xC4E71A00") == "c4e71a00"
    assert publish._norm_adler32("") is None and publish._norm_adler32(None) is None
    entry = {"path": "/x/f", "name": "f", "type": "file", "size": 5509453,
             "checksums": {"adler32": "c4e71a00"}}
    assert publish.parse_fdp_stat(json.dumps([entry])) == [entry]
    assert publish.parse_fdp_stat(json.dumps(entry)) == [entry]
    with pytest.raises(ValueError):
        publish.parse_fdp_stat("not json")
    assert publish._stat_adler32({"checksums": {}}) is None
    assert publish._stat_adler32({"checksums": [
        {"type": "ADLER32", "value": "1a00"}]}) == "00001a00"
    assert publish._stat_size({"size": -1}) == -1


def test_old_state_database_gains_the_checksum_columns(tmp_path):
    path = tmp_path / "publish.db"
    conn = sqlite3.connect(str(path))
    conn.executescript(
        "CREATE TABLE files (raw_path TEXT PRIMARY KEY, stream TEXT NOT NULL, "
        "kind TEXT NOT NULL, label TEXT NOT NULL, fmt TEXT NOT NULL, rows "
        "INTEGER, bytes INTEGER, dumped_at TEXT, shifted_path TEXT, "
        "shifted_at TEXT, raw_uploaded_at TEXT, shifted_uploaded_at TEXT, "
        "last_error TEXT, updated_at TEXT);"
        "INSERT INTO files (raw_path, stream, kind, label, fmt) VALUES "
        "('/r', 'all', 'hourly', 'L', 'csv');")
    conn.commit()
    conn.close()
    state = publish.State(str(path))
    assert state.get("/r")["raw_adler32"] is None
    state.update("/r", "now", raw_adler32="00000001")
    assert state.get("/r")["raw_adler32"] == "00000001"
    state.close()


def test_upload_groups_by_destination_directory(rig):
    rig.configure(hourly={"catchup_hours": 3}, fdp={"enabled": False})
    # 23:00, 00:00 and 01:00 CDT across midnight: two day directories.
    assert rig.main("hourly", now=utc(2026, 10, 23, 6, 10)) == 0
    assert rig.log("fdp-put.log") == []
    rig.configure(fdp={"enabled": True, "parallel": 2,
                       "extra_args": ["--verbose"]})
    assert rig.main("upload", now=utc(2026, 10, 23, 6, 20)) == 0
    puts = rig.log("fdp-put.log")
    dests = sorted(p["argv"][-1] for p in puts)
    assert dests == ["/amsc/novaend/raw/oct/2026-oct-22/",
                     "/amsc/novaend/raw/oct/2026-oct-23/",
                     "/amsc/public/novaend/shifted/oct/2026-oct-22/",
                     "/amsc/public/novaend/shifted/oct/2026-oct-23/"]
    for put in puts:
        argv = put["argv"]
        assert argv[argv.index("-j") + 1] == "2" and "--verbose" in argv
        dest = argv[-1]
        sources = [a for a in argv if a.endswith((".csv", ".json"))]
        day = dest.rstrip("/").rsplit("/", 1)[1]
        assert sources and all("/{}/".format(day) in s for s in sources)
        assert all(".shifted." in s for s in sources) == ("shifted" in dest)
    assert len([a for p in puts for a in p["argv"]
                if a.endswith((".csv", ".json"))]) == 12


def test_upload_disabled_leaves_files_pending(rig):
    rig.configure(fdp={"enabled": False})
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    assert rig.log("fdp-put.log") == []
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["hourly"]["grid"]["2026-10-22T12-0500"]["all"] == "shifted"


# ---------------------------------------------------------------- backfill


def test_backfill_whole_pieces_only(rig):
    code = rig.main("backfill", "--start", "2026-10-22T09:30",
                    "--end", "2026-10-22T13:00", "--no-upload",
                    now=utc(2026, 10, 25))
    assert code == 0
    calls = rig.log("dump.log")
    assert len(calls) == 1                   # no whole day in the range
    assert "--start=2026-10-22T10:00:00-05:00" in calls[0]
    assert "--end=2026-10-22T13:00:00-05:00" in calls[0]
    assert rig.log("fdp-put.log") == []
    labels = sorted({r["label"] for r in rig.files().values()})
    assert labels == ["2026-10-22T10-0500", "2026-10-22T11-0500",
                      "2026-10-22T12-0500"]


def test_backfill_days_and_hours_one_call_per_kind(rig):
    code = rig.main("backfill", "--start", "2026-10-21", "--end", "2026-10-22",
                    now=utc(2026, 10, 25))
    assert code == 0
    calls = rig.log("dump.log")
    assert [c[c.index("--split") + 1] for c in calls] == ["hour", "day"]
    files = list(rig.files().values())
    assert len([r for r in files if r["kind"] == "hourly"]) == 48 * 2
    assert sorted({r["label"] for r in files if r["kind"] == "daily"}) == \
        ["2026-10-21", "2026-10-22"]
    # Re-running is a no-op.
    assert rig.main("backfill", "--start", "2026-10-21", "--end", "2026-10-22",
                    "--no-hourly", now=utc(2026, 10, 25)) == 0
    assert len(rig.log("dump.log")) == 2


# ------------------------------------------------------------- credentials


def klist_text(expires, renew_until):
    fmt = "%m/%d/%Y %H:%M:%S"
    local = lambda m: datetime.fromtimestamp(m.timestamp()).strftime(fmt)
    return ("Ticket cache: FILE:/x\nDefault principal: anorman@FNAL.GOV\n\n"
            "Valid starting       Expires              Service principal\n"
            "{}  {}  krbtgt/FNAL.GOV@FNAL.GOV\n\trenew until {}\n".format(
                local(expires - timedelta(hours=24)), local(expires),
                local(renew_until)))


def test_creds_without_a_ticket_asks_for_kinit(rig):
    now = utc(2026, 10, 22, 18)
    (rig.ctl / "jwt.exp").write_text(str(int(now.timestamp()) + 7200))
    assert rig.main("creds", now=now) == 1
    mail = [m for m in rig.mails() if "kinit needed" in m["argv"][1]][0]
    cache = "FILE:" + str(rig.tokens / "krb5cc_test")
    assert "kinit -r 7d -c {} anorman@FNAL.GOV".format(cache) in mail["body"]
    assert rig.log("klist.log") == ["KRB5CCNAME=" + cache]
    # htgettoken still ran (a Vault token may carry it), with --nooidc.
    call = rig.log("htgettoken.log")[0]
    assert "--nooidc" in call["argv"] and not call["has_password"]
    assert call["krb5ccname"] == cache and call["sourced"] == "yes"
    assert not call["stdin_tty"]


def test_creds_renews_a_ticket_close_to_expiry(rig):
    now = utc(2026, 10, 22, 18)
    (rig.ctl / "klist.out").write_text(klist_text(now + timedelta(hours=2),
                                                  now + timedelta(days=5)))
    (rig.ctl / "klist.renewed").write_text(klist_text(now + timedelta(hours=26),
                                                      now + timedelta(days=5)))
    (rig.ctl / "jwt.exp").write_text(str(int(now.timestamp()) + 10800))
    assert rig.main("creds", now=now) == 0
    cache = "FILE:" + str(rig.tokens / "krb5cc_test")
    assert rig.log("kinit.log") == ["-R -c " + cache]
    detail = json.loads(rig.runs()[-1]["detail"])["credentials"]
    assert detail["kerberos"]["renewed"] is True
    assert detail["kerberos"]["expires"] == publish._utc_iso(now + timedelta(hours=26))
    assert detail["bearer"]["seconds_left"] == 10800
    assert not rig.mails()
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["credentials"]["kerberos"]["valid"] is True
    assert status["credentials"]["bearer"]["exp"] == publish._utc_iso(
        now + timedelta(hours=3))


def test_creds_leaves_a_fresh_ticket_alone_and_warns_on_renew_until(rig):
    now = utc(2026, 10, 22, 18)
    (rig.ctl / "klist.out").write_text(klist_text(now + timedelta(hours=20),
                                                  now + timedelta(hours=10)))
    assert rig.main("creds", now=now) == 0
    assert rig.log("kinit.log") == []
    subjects = [m["argv"][1] for m in rig.mails()]
    assert any("Kerberos ticket must be re-created" in s for s in subjects)
    body = rig.mails()[0]["body"]
    assert "kinit -r 7d -c FILE:" in body


def test_creds_uses_sso_only_with_real_values_and_never_leaks_them(rig, capfd):
    now = utc(2026, 10, 22, 18)
    (rig.ctl / "klist.out").write_text(klist_text(now + timedelta(hours=20),
                                                  now + timedelta(days=5)))
    rig.env_file.write_text("# account\nFERMI_USER=nova-svc\n"
                            "FERMI_PASSWORD='{}'\n".format(PASSWORD))
    (rig.ctl / "htgettoken.code").write_text("1")
    assert rig.main("-v", "creds", now=now) == 1
    call = rig.log("htgettoken.log")[0]
    assert "--ssoapprove=password" in call["argv"]
    assert "--ssocookiefile=" + str(rig.tokens / "sso.cookies") in call["argv"]
    assert "--nooidc" not in call["argv"]
    assert call["user"] == "nova-svc" and call["password_ok"]
    assert "--minsecs=3600" in call["argv"]
    assert call["argv"][-2:] == ["-o", str(rig.tokens / "fdp_bt_test")]
    out, err = capfd.readouterr()
    everything = out + err + json.dumps(rig.runs()) + json.dumps(rig.mails()) \
        + (rig.status_dir / "status.json").read_text() \
        + (rig.status_dir / "index.html").read_text() \
        + json.dumps(call["argv"])
    assert PASSWORD not in everything
    # The fdp-put and dump environment never see the account.
    assert "FERMI_PASSWORD" not in rig.environ


def test_creds_with_placeholder_values_uses_nooidc(rig):
    now = utc(2026, 10, 22, 18)
    (rig.ctl / "klist.out").write_text(klist_text(now + timedelta(hours=20),
                                                  now + timedelta(days=5)))
    rig.env_file.write_text("FERMI_USER=nova-svc\nFERMI_PASSWORD=CHANGEME\n")
    assert rig.main("creds", now=now) == 0
    call = rig.log("htgettoken.log")[0]
    assert "--nooidc" in call["argv"]
    assert not any(a.startswith("--sso") for a in call["argv"])
    assert call["user"] is None and not call["has_password"]


def test_parse_klist_formats():
    mit = ("Ticket cache: FILE:/tmp/krb5cc_1\nDefault principal: a@FNAL.GOV\n\n"
           "Valid starting       Expires              Service principal\n"
           "10/22/26 08:00:00  10/23/26 10:00:00  krbtgt/FNAL.GOV@FNAL.GOV\n"
           "\trenew until 10/29/26 08:00:00\n")
    info = publish.parse_klist(mit)
    assert info["principal"] == "a@FNAL.GOV"
    assert info["expires"] == datetime(2026, 10, 23, 10).timestamp()
    assert info["renew_until"] == datetime(2026, 10, 29, 8).timestamp()
    heimdal = ("Credentials cache: FILE:/x\n        Principal: a@FNAL.GOV\n\n"
               "  Issued                Expires               Principal\n"
               "Oct 22 08:00:00 2026  Oct 23 10:00:00 2026  "
               "krbtgt/FNAL.GOV@FNAL.GOV\n")
    info = publish.parse_klist(heimdal)
    assert info["expires"] == datetime(2026, 10, 23, 10).timestamp()
    assert info["renew_until"] is None


def test_jwt_expiry():
    body = base64.urlsafe_b64encode(b'{"exp": 1790000000}').decode().rstrip("=")
    assert publish.jwt_expiry("eyJhbGciOiJub25lIn0.{}.sig\n".format(body)) == \
        1790000000
    assert publish.jwt_expiry("not a token") is None


def test_redaction_hides_tokens_and_passwords():
    text = "token eyJabc.def.ghi and hvs.ABCDEFGHIJKL and pw hunter2"
    out = publish._redact(text, ["hunter2"])
    assert "eyJabc" not in out and "hvs.ABC" not in out and "hunter2" not in out


# -------------------------------------------------------------- token job


def test_token_job_month_next_and_no_overwrite(rig):
    rig.configure(tools={"shift": rig.real_shift}
                  if os.access(rig.real_shift, os.X_OK) else {})
    now = utc(2026, 10, 25, 14)
    assert rig.main("token", "--next", now=now) == 0
    nov = rig.tokens / "dts1-2026-11.token"
    assert nov.is_file()
    content = nov.read_text()
    assert rig.main("token", "--month", "2026-11", now=now) == 0
    assert nov.read_text() == content
    assert len([m for m in rig.mails() if "back it up" in m["argv"][1]]) == 1
    assert rig.main("token", "--month", "2026-13", now=now) == 2
    assert rig.main("token", now=utc(2026, 12, 31, 23)) == 0   # Dec in CST
    assert (rig.tokens / "dts1-2026-12.token").is_file()


# ----------------------------------------------------------- alerts, locks


def test_alert_rate_limit_per_key(rig):
    now = utc(2026, 10, 22, 18)
    pub = publisher(rig, now=now)
    pub.alert("a", "first", "body")
    pub.alert("a", "first again", "body")
    pub.alert("b", "other", "body")
    assert [m["argv"][1] for m in rig.mails()] == [
        "[darpa-spill-publish] first", "[darpa-spill-publish] other"]
    assert rig.mails()[0]["argv"][2:] == ["ops@example.invalid"]
    later = publisher(rig, now=now + timedelta(seconds=3600))
    later.alert("a", "after the interval", "body")
    assert len(rig.mails()) == 3
    counts = {a["key"]: a["count"] for a in later.state.alerts()}
    assert counts == {"a": 3, "b": 1}


def test_a_broken_mailer_never_raises(rig):
    rig.configure(alerts={"mail_command": str(rig.root / "no-such-mailer")})
    pub = publisher(rig, now=utc(2026, 10, 22, 18))
    pub.alert("x", "subject", "body")          # logged, not raised
    assert pub.state.alerts()[0]["last_sent"] is None


def test_a_busy_job_exits_75(rig):
    import fcntl
    rig.state_dir.mkdir()
    with open(rig.state_dir / "hourly.lock", "w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 75
    assert rig.log("dump.log") == []


# ----------------------------------------------------------------- dry run


def test_dry_run_prints_commands_and_writes_nothing(rig, capsys):
    rig.configure(hourly={"catchup_hours": 2})
    assert rig.main("-n", "hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    out = capsys.readouterr().out
    assert "darpa-spill-dump -s https://spills.test.invalid:8443" in out
    assert "--start=2026-10-22T11:00:00-05:00" in out
    assert "fake-shift token --dist gauss --sigma 50us --cut 3sigma" in out
    assert "fake-shift apply -q --token-file" in out
    assert "fdp-put --skip-existing" in out
    assert "fdp-stat --json" in out
    assert "mailx -s" in out
    for path in (rig.data, rig.state_dir, rig.status_dir):
        assert not path.exists()
    assert list(rig.tokens.iterdir()) == []
    assert rig.log("dump.log") == [] and rig.log("fdp-put.log") == []
    assert rig.log("fdp-stat.log") == [] and rig.log("fdp-rm.log") == []
    assert rig.mails() == []


def test_dry_run_starts_from_the_real_state(rig, capsys):
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    before = (rig.state_dir / "publish.db").read_bytes()
    capsys.readouterr()
    rig.configure(hourly={"catchup_hours": 2})
    assert rig.main("-n", "hourly", now=utc(2026, 10, 22, 18, 20)) == 0
    out = capsys.readouterr().out
    assert "--start=2026-10-22T11:00:00-05:00" in out
    assert "--end=2026-10-22T12:00:00-05:00" in out
    assert (rig.state_dir / "publish.db").read_bytes() == before


# ------------------------------------------------------ review fixes (1.5.0)


def test_quiet_beam_passes_the_gate_on_polled_through(rig):
    # Beam off since 09:00 CDT: the newest event stops, the polls go on.
    rig.watermark = nova(utc(2026, 10, 22, 14))
    rig.polled_through = nova(utc(2026, 10, 22, 18, 9))
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    assert len(rig.files()) == 2
    # Within ingest_check.settle of the hour's end it is not yet enough.
    rig.polled_through = nova(utc(2026, 10, 22, 19, 0, 30))
    assert rig.main("hourly", now=utc(2026, 10, 22, 19, 10)) == 75
    assert "s short of 2026-10-22T14:00:00-05:00" in rig.mails()[-1]["body"]


def test_a_source_that_never_polled_or_is_stopped_blocks(rig):
    rig.watermark = None
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 75
    assert "has not completed an ingest poll" in rig.mails()[-1]["body"]
    rig.running = False
    pub = publisher(rig)
    assert "its ingest is not running" in pub.ingest_watermark()[1]


def test_ingest_ready_again_clears_the_alert(rig):
    rig.watermark = nova(utc(2026, 10, 22, 17, 30))
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 75
    rig.watermark = 10 ** 18
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 20)) == 0
    # Cleared: the next stall is mailed at once, not an hour later.
    rig.watermark = nova(utc(2026, 10, 22, 18, 30))
    assert rig.main("hourly", now=utc(2026, 10, 22, 19, 10)) == 75
    assert len([m for m in rig.mails() if "ingest not ready" in
                m["argv"][1]]) == 2


def test_dry_run_reads_ingest_once_and_does_not_wait(rig, monkeypatch, capsys):
    slept = []
    monkeypatch.setattr(publish, "_sleep", slept.append)
    rig.configure(ingest_check={"wait": 300})
    rig.watermark = nova(utc(2026, 10, 22, 17, 30))
    assert rig.main("-n", "hourly", now=utc(2026, 10, 22, 18, 10)) == 75
    assert slept == []


def test_an_hour_leaving_the_catchup_window_incomplete_is_alerted(rig):
    rig.configure(hourly={"catchup_hours": 2})
    rig.watermark = nova(utc(2026, 10, 22, 15, 30))          # stalled
    for hour in range(16, 20):
        rig.main("hourly", now=utc(2026, 10, 22, hour, 10))
    subjects = [m["argv"][1] for m in rig.mails()]
    # The first run was at 11:10 CDT.  The 14:10 CDT run's window is 12:00-
    # 14:00, so 11:00 CDT has just left it; the hours that left it earlier
    # ended before the first run and are not reported.
    expired = [s for s in subjects if "backfill needed" in s]
    assert expired == ["[darpa-spill-publish] hourly 2026-10-22T11-0500 not "
                       "published; backfill needed"]
    body = [m for m in rig.mails() if "backfill needed" in m["argv"][1]][0]
    assert "backfill --start=2026-10-22T11:00:00-05:00" in body["body"]


def test_a_fresh_install_does_not_report_hours_before_it(rig):
    rig.configure(hourly={"catchup_hours": 1})
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    assert not [m for m in rig.mails() if "backfill" in m["argv"][1]]


def test_dumping_jobs_share_the_dump_lock(rig, monkeypatch):
    import fcntl
    monkeypatch.setattr(publish, "DUMP_LOCK_WAIT", 0.0)
    rig.state_dir.mkdir()
    with open(rig.state_dir / "dump.lock", "w") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        assert rig.main("backfill", "--start", "2026-10-22T10:00",
                        "--end", "2026-10-22T12:00", "--no-upload",
                        now=utc(2026, 10, 25)) == 75
        assert rig.main("daily", now=utc(2026, 10, 23, 5, 30)) == 75
    assert rig.log("dump.log") == []
    assert rig.runs()[-1]["result"] == "busy"
    assert rig.main("daily", now=utc(2026, 10, 23, 5, 30)) == 0


def test_a_token_created_by_a_concurrent_run_is_used(rig):
    # The other job wins the race: the file appears, 'token' refuses (2).
    racer = rig.bin / "racing-shift"
    racer.write_text("#!/bin/sh\nif [ \"$1\" = token ]; then\n"
                     "  for a; do last=$a; done\n"
                     "  echo other-run > \"$last\"; exit 2\nfi\n"
                     "exec {} \"$@\"\n".format(rig.bin / "fake-shift"))
    racer.chmod(0o755)
    rig.configure(tools={"shift": str(racer)})
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    assert (rig.tokens / "dts1-2026-10.token").read_text() == "other-run\n"
    assert all(r["shifted_at"] for r in rig.files().values())
    assert not [m for m in rig.mails() if "token" in m["argv"][1]]


def test_a_timeout_kills_the_whole_process_group(rig, tmp_path):
    pub = publisher(rig)
    marker = tmp_path / "grandchild-alive"
    script = ("(sleep 3; touch {}) & sleep 30".format(marker))
    proc = pub.run(["sh", "-c", script], timeout=0.5)
    assert proc.returncode == 124 and "timed out" in proc.stderr
    import time as _time
    _time.sleep(3.5)
    assert not marker.exists()


def test_dump_and_shift_run_with_timeouts(rig, monkeypatch):
    rig.configure(tools={"dump_timeout": 100, "shift_timeout": 7})
    pub = publisher(rig, now=utc(2026, 10, 22, 18, 10))
    seen = []
    real = pub.run

    def spy(cmd, **kwargs):
        seen.append((os.path.basename(cmd[0]), cmd[1] if len(cmd) > 1
                     else "", kwargs.get("timeout")))
        return real(cmd, **kwargs)
    monkeypatch.setattr(pub, "run", spy)
    day = split_range(pub.midnight(datetime(2026, 10, 21).date()),
                      pub.midnight(datetime(2026, 10, 23).date()), "day",
                      CHICAGO)
    pub._run_pieces("daily", day)
    dumps = [t for name, _, t in seen if name == "darpa-spill-dump"]
    assert dumps == [200.0]                     # per day of data
    assert {t for name, sub, t in seen if name == "fake-shift"} == {7}


def test_a_hung_dump_is_alerted_and_the_job_reported_stale(rig):
    import fcntl
    import time as _time
    rig.state_dir.mkdir()
    sleeper = subprocess.Popen(["sleep", "30"])
    try:
        lock = rig.state_dir / "hourly.lock"
        with open(lock, "w") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            handle.write("{}\n".format(sleeper.pid))
            handle.flush()
            old = _time.time() - 4 * 3600
            os.utime(lock, (old, old))
            assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 75
            mail = rig.mails()[-1]
            assert "hourly has been running for" in mail["argv"][1]
            assert "ps -fp {}".format(sleeper.pid) in mail["body"]
            assert rig.main("status", now=utc(2026, 10, 22, 18, 12)) == 0
            status = json.loads((rig.status_dir / "status.json").read_text())
            assert status["running"]["hourly"]["pid"] == sleeper.pid
            assert status["running"]["hourly"]["stale"] is True
            assert "stale?" in (rig.status_dir / "index.html").read_text()
    finally:
        sleeper.kill()
        sleeper.wait()


def test_alerts_still_go_when_the_state_database_fails(rig, monkeypatch):
    pub = publisher(rig, now=utc(2026, 10, 22, 18))

    def full(*args, **kwargs):
        raise sqlite3.OperationalError("database or disk is full")
    monkeypatch.setattr(pub.state, "alert_due", full)
    stamp = pub._stamp_path("disk-test")
    if os.path.exists(stamp):
        os.unlink(stamp)
    try:
        pub.alert("disk-test", "disk full", "body")
        pub.alert("disk-test", "disk full", "body")     # stamp rate-limits
        assert [m["argv"][1] for m in rig.mails()] == [
            "[darpa-spill-publish] disk full"]
    finally:
        if os.path.exists(stamp):
            os.unlink(stamp)


def test_an_unwritable_state_database_is_mailed(rig, monkeypatch):
    def full(self, job, when):
        raise sqlite3.OperationalError("database or disk is full")
    monkeypatch.setattr(publish.State, "run_start", full)
    monkeypatch.setattr(publish.State, "alert_due",
                        lambda *a, **k: (_ for _ in ()).throw(
                            sqlite3.OperationalError("disk is full")))
    stamp = publisher(rig)._stamp_path("state-unwritable")
    if os.path.exists(stamp):
        os.unlink(stamp)
    try:
        assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 1
        assert "state database unwritable" in rig.mails()[-1]["argv"][1]
        assert rig.log("dump.log") == []
    finally:
        if os.path.exists(stamp):
            os.unlink(stamp)


def test_a_failed_status_write_leaves_no_temporary_file(rig, monkeypatch):
    monkeypatch.setattr(publish, "render_html", lambda status: 12345)
    pub = publisher(rig, now=utc(2026, 10, 22, 18))
    with pytest.raises(TypeError):
        pub.write_status()
    assert not list(rig.status_dir.glob("*.tmp.*"))
    assert (rig.status_dir / "status.json").is_file()


def test_disk_alert_is_sent_before_the_status_is_written(rig, monkeypatch):
    rig.configure(alerts={"disk_warn_percent": 0})
    monkeypatch.setattr(publish, "render_html", lambda status: 12345)
    pub = publisher(rig, now=utc(2026, 10, 22, 18))
    with pytest.raises(TypeError):
        pub.write_status()
    assert "full" in rig.mails()[-1]["argv"][1]


@pytest.mark.parametrize("code,meaning", [(6, "exists"),
                                          (9, "checksum mismatch")])
def test_upload_exists_or_checksum_failure_then_retry(rig, code, meaning):
    rig.configure(hourly={"catchup_hours": 2})
    rig.set_codes("fdp-put", code)
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == \
        publish.EXIT_PARTIAL
    # Unlike 4 and 7 the stage goes on to the next destination group.
    puts = rig.log("fdp-put.log")
    assert len(puts) == 2
    rows = rig.files().values()
    raw_group = [r for r in rows if r["raw_path"] in puts[0]["argv"]]
    assert raw_group and all(
        not r["raw_uploaded_at"] and "fdp-put exit {}".format(code)
        in r["last_error"] for r in raw_group)
    assert rig.mails()[-1]["argv"][1] == \
        "[darpa-spill-publish] upload failed: fdp-put exit {} ({})".format(
            code, meaning)
    assert rig.main("upload", now=utc(2026, 10, 22, 18, 25)) == 0
    assert all(r["raw_uploaded_at"] and r["shifted_uploaded_at"]
               and not r["last_error"] for r in rig.files().values())


def test_a_batch_partial_twice_is_alerted(rig):
    rig.configure(fdp={"enabled": False})
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    rig.configure(fdp={"enabled": True})
    rig.set_codes("fdp-put", 12, 12, 12, 12)
    # Each batch leaves its first file behind; the rest are verified.
    assert rig.main("upload", now=utc(2026, 10, 22, 18, 25)) == 3
    assert not [m for m in rig.mails() if "upload" in m["argv"][1]]
    errors = [r for r in rig.files().values() if r["last_error"]]
    assert len(errors) == 1
    assert "removed incomplete remote copy" in errors[0]["last_error"]
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["hourly"]["grid"]["2026-10-22T12-0500"]["all"] == "error"
    assert status["pending_uploads"] == 1
    assert rig.main("upload", now=utc(2026, 10, 22, 18, 40)) == 1
    assert "exit 12 (partial), again" in rig.mails()[-1]["argv"][1]


def test_upload_auth_failure_is_not_mailed_twice_for_one_cause(rig):
    pub = publisher(rig, now=utc(2026, 10, 22, 18))
    pub.alert("kinit-needed", "kinit needed", "body")
    rig.set_codes("fdp-put", 4, 4)
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 3
    assert any("fdp-put exit 4" in (r["last_error"] or "")
               for r in rig.files().values())
    assert not [m for m in rig.mails() if "upload" in m["argv"][1]]


def test_alert_backoff_doubles_and_clears(rig):
    t0 = utc(2026, 10, 22, 18)
    sends = []
    for minutes in (0, 60, 110, 180, 300, 420):
        pub = publisher(rig, now=t0 + timedelta(minutes=minutes))
        before = len(rig.mails())
        pub.alert("k", "subject", "body")
        if len(rig.mails()) > before:
            sends.append(minutes)
        pub.state.close()
    # 1 h, then 2 h, then 4 h between mails.
    assert sends == [0, 60, 180, 420]
    pub = publisher(rig, now=t0 + timedelta(minutes=430))
    pub.resolve("k")
    pub.alert("k", "subject", "body")
    assert len(rig.mails()) == 5


def test_sso_password_mode_backs_off_after_a_failure(rig):
    now = utc(2026, 10, 22, 18)
    (rig.ctl / "klist.out").write_text(klist_text(now + timedelta(hours=20),
                                                  now + timedelta(days=5)))
    rig.env_file.write_text("FERMI_USER=nova-svc\nFERMI_PASSWORD='{}'\n"
                            .format(PASSWORD))
    (rig.ctl / "htgettoken.code").write_text("1")
    assert rig.main("creds", now=now) == 1
    assert rig.main("creds", now=now + timedelta(minutes=30)) == 1
    calls = rig.log("htgettoken.log")
    assert "--ssoapprove=password" in calls[0]["argv"]
    assert "--nooidc" in calls[1]["argv"] and not calls[1]["has_password"]
    assert "held off" in rig.mails()[-1]["body"]
    # A new password (the file changes) is tried at once.
    rig.env_file.write_text("FERMI_USER=nova-svc\nFERMI_PASSWORD='{}'\n# new\n"
                            .format(PASSWORD))
    os.utime(rig.env_file, (1, 1))
    (rig.ctl / "htgettoken.code").write_text("0")
    assert rig.main("creds", now=now + timedelta(minutes=60)) == 0
    assert "--ssoapprove=password" in rig.log("htgettoken.log")[2]["argv"]
    # ... and after the back-off the old one would be too.
    pub = publisher(rig, now=now + timedelta(hours=13))
    assert pub._sso_backoff() == ""


def test_env_script_last_status_is_ignored_but_unreadable_is_reported(rig):
    rig.env_script.write_text(ENV_SCRIPT + "[ -d /no/such/dir ] && echo x\n")
    assert rig.main("hourly", now=utc(2026, 10, 22, 18, 10)) == 0
    assert rig.log("fdp-put.log")[0]["sourced"] == "yes"
    rig.configure(fdp={"env_script": str(rig.root / "missing-env.sh")})
    assert rig.main("hourly", now=utc(2026, 10, 22, 19, 10)) == 3
    mail = rig.mails()[-1]
    assert "fdp-put exit 125 (env script unreadable)" in mail["argv"][1]
    assert "cannot read env script" in mail["body"]
    assert "missing-env.sh" in mail["body"]


def test_backfill_help_shows_no_doubled_default(capsys):
    with pytest.raises(SystemExit):
        publish.build_parser().parse_args(["backfill", "-h"])
    out = capsys.readouterr().out
    assert "(default: yes)" not in out


# ------------------------------------------------------------ configuration


def test_config_precedence_cli_env_dotenv_yaml(rig, tmp_path):
    rig.configure(data_root="/from/yaml", prefix="yaml")
    dotenv = tmp_path / "publish.env"
    dotenv.write_text("DARPA_SPILL_PUBLISH_DATA_ROOT=/from/dotenv\n"
                      "DARPA_SPILL_PUBLISH_PREFIX=dotenv\n"
                      "DARPA_SPILL_PUBLISH_TIMEZONE=UTC\n"
                      "OTHER_SETTING=ignored\n")
    rig.configure(paths={"dotenv": str(dotenv)})
    path = rig.write_config()

    def settings(argv, environ):
        args = publish.build_parser().parse_args(["-c", str(path)] + argv)
        return publish.load_settings(args, environ)

    cfg = settings(["status"], {})
    assert (cfg["data_root"], cfg["prefix"], cfg["timezone"]) == \
        ("/from/dotenv", "dotenv", "UTC")
    cfg = settings(["status"], {"DARPA_SPILL_PUBLISH_DATA_ROOT": "/from/env",
                                "DARPA_SPILL_PUBLISH_FDP_PARALLEL": "9",
                                "DARPA_SPILL_PUBLISH_SERVER": "https://env:1",
                                "DARPA_SPILL_PUBLISH_INSECURE": "no"})
    assert (cfg["data_root"], cfg["prefix"]) == ("/from/env", "dotenv")
    assert cfg["fdp"]["parallel"] == 9
    assert cfg["server"]["url"] == "https://env:1"
    assert cfg["server"]["insecure"] is False
    cfg = settings(["--set", "data_root=/from/cli", "status",
                    "--set", "alerts.email=a@x,b@y",
                    "--set", "streams.numi.signals=[$74]"],
                   {"DARPA_SPILL_PUBLISH_DATA_ROOT": "/from/env"})
    assert cfg["data_root"] == "/from/cli"
    assert cfg["alerts"]["email"] == ["a@x", "b@y"]
    assert cfg["streams"]["numi"] == {"signals": ["$74"]}
    # The config file itself can come from the environment.
    args = publish.build_parser().parse_args(["status"])
    cfg = publish.load_settings(args, {"DARPA_SPILL_PUBLISH_CONFIG": str(path)})
    assert cfg["prefix"] == "dotenv"


def test_bad_configuration_is_a_usage_error(rig, capsys):
    rig.configure(fdp={"enabled": True})
    rig.config["fdp"]["paralel"] = 3
    assert rig.main("status", now=utc(2026, 10, 22)) == 2
    assert "paralel" in capsys.readouterr().err
    del rig.config["fdp"]["paralel"]
    rig.config["streams"] = {"numi": {"signal": ["$74"]}}
    assert rig.main("status", now=utc(2026, 10, 22)) == 2
    rig.config["streams"] = {"all": {}}
    assert rig.main("status", "--set", "timezone=Mars/Olympus",
                    now=utc(2026, 10, 22)) == 2


def test_example_config_matches_the_defaults():
    path = os.path.join(os.path.dirname(__file__), "..", "config",
                        "publish.yaml")
    args = publish.build_parser().parse_args(["-c", path, "status"])
    cfg = publish.load_settings(args, {})
    cfg.pop("_source")
    assert cfg == publish.DEFAULTS


# ------------------------------------------------------- status, check, misc


def test_status_job_renders_errors_and_jobs(rig):
    rig.set_codes("dump", 1)
    rig.main("hourly", now=utc(2026, 10, 22, 18, 10))
    assert rig.main("status", now=utc(2026, 10, 22, 18, 15)) == 0
    status = json.loads((rig.status_dir / "status.json").read_text())
    assert status["jobs"]["hourly"]["result"] == "failed"
    assert status["jobs"]["status"]["result"] == "ok"
    assert status["hourly"]["grid"]["2026-10-22T12-0500"]["all"] == "error"
    assert any("fake dump failure" in e["error"] for e in status["errors"])
    assert status["disk"]["percent"] >= 0
    page = (rig.status_dir / "index.html").read_text()
    assert "fake dump failure" in page
    assert not list(rig.status_dir.glob("*.tmp.*"))


def test_count_rows(tmp_path):
    csv_path = tmp_path / "a.csv"
    csv_path.write_text("# c1\n# c2\nnova_time,source\n1,a\n2,a\n3,a\n")
    assert publish.count_rows(str(csv_path), "csv") == 3
    empty = tmp_path / "e.csv"
    empty.write_text("# c1\nnova_time,source\n")
    assert publish.count_rows(str(empty), "csv") == 0
    doc = tmp_path / "a.json"
    doc.write_text('{\n"meta": {"expected": 9},\n"events": [\n{}],\n'
                   '"summary": {"returned": 1, "complete": true}\n}\n')
    assert publish.count_rows(str(doc), "json") == 1
    bare = tmp_path / "b.json"
    bare.write_text('{"events": [{}, {}]}')
    assert publish.count_rows(str(bare), "json") == 2


def test_check_reports_a_table(rig, capsys):
    (rig.root / "certs").mkdir()
    rig.data.mkdir()
    code = rig.main("check", now=utc(2026, 10, 22))
    out = capsys.readouterr().out
    assert code == (0 if "FAIL" not in out else 1)
    lines = {l.split()[0]: l for l in out.splitlines() if l.strip()}
    assert "ok" in lines["server"].split()
    assert "ok" in lines["token"].split()                  # token dir 0700
    assert "ok" in lines["fdp-put"].split()
    assert "ok" in lines["fdp-stat"].split()
    assert "ok" in lines["fdp-rm"].split()
    assert "ok" in lines["htgettoken"].split()
    assert "ok" in lines["mail"].split()
    assert "ok" in lines["darpa-spill-dump"].split()
    rig.tokens.chmod(0o755)
    assert rig.main("check", now=utc(2026, 10, 22)) == 1
    out = capsys.readouterr().out
    token_line = [l for l in out.splitlines() if l.startswith("token dir")][0]
    assert "FAIL" in token_line and "0755" in token_line


def test_version_and_usage(capsys):
    assert publish.main(["-V"], environ={}) == 0
    assert "darpa-spill-publish" in capsys.readouterr().out
    assert publish.main([], environ={}) == 2
