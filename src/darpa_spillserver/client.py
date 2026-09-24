"""A client for the server's HTTP API.

The contract this module implements --- configuration keys and precedence,
the parameters each call sends, and the errors it raises --- is written down
in ``docs/CLIENT.md`` and shared with the C++ library in ``src/cpp``.  The
two are kept in step by a test that runs both command-line programs against
one live server and compares their output byte for byte.

Only the standard library is used for HTTP, so the client runs anywhere the
package's Python does without pulling in the server's dependencies; PyYAML is
needed only to read a configuration file.

Typical use::

    from darpa_spillserver.client import SpillClient, Selection

    client = SpillClient("http://novadaq-near-gateway-01.fnal.gov:8080")
    page = client.events(Selection(start="today", signals=["$74"]), limit=100)
    for event in page["events"]:
        print(event["utc_string"], event["signal"])
"""

from __future__ import annotations

import json
import os
import socket
import ssl
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import IO, Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote

from . import __version__
from .config import ENV_PREFIX, ConfigError, resolve_dotenv

__all__ = [
    "ADMIN_HEADER",
    "CLIENT_ENV_PREFIX",
    "DEFAULT_CLIENT_CONFIG_PATHS",
    "ClientConfig",
    "Response",
    "Selection",
    "SpillClient",
    "SpillClientError",
    "SpillConnectionError",
    "SpillHTTPError",
    "encode_query",
    "load_client_config",
]

#: Header that carries the admin token on requests that change sources.
ADMIN_HEADER = "X-Admin-Token"

#: Prefix of the environment variables that set client options.
CLIENT_ENV_PREFIX = ENV_PREFIX + "CLIENT_"

#: Chunk size for streamed exports.
_CHUNK = 64 * 1024


def _system_config_dir() -> str:
    if sys.platform == "win32":
        return os.path.join(os.environ.get("PROGRAMDATA", r"C:\ProgramData"),
                            "darpa-spillserver")
    return "/etc/darpa-spillserver"


#: Searched in order when neither ``--config`` nor
#: ``$DARPA_SPILL_CLIENT_CONFIG`` names a file.
DEFAULT_CLIENT_CONFIG_PATHS = (
    "./config/spillclient.yaml",
    "~/.config/darpa-spillserver/spillclient.yaml",
    os.path.join(_system_config_dir(), "spillclient.yaml"),
)


# ------------------------------------------------------------------ errors


class SpillClientError(Exception):
    """Base class for every error this client raises."""


class SpillConnectionError(SpillClientError):
    """The server could not be reached: DNS, refused, TLS, or timeout."""


class SpillHTTPError(SpillClientError):
    """The server answered with an HTTP error status.

    :ivar status: the HTTP status code.
    :ivar message: the server's explanation.
    :ivar hint: how to fix it, when the server offered one, else ``None``.
    """

    def __init__(self, status: int, message: str, hint: Optional[str] = None):
        self.status = status
        self.message = message
        self.hint = hint
        super().__init__("HTTP {}: {}".format(status, message))


def _parse_error(status: int, reason: str, body: bytes) -> SpillHTTPError:
    """Read the message and hint out of an error body, whatever its shape."""
    message, hint = reason or "error", None
    try:
        data = json.loads(body.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        data = None
    if isinstance(data, dict):
        detail = data.get("detail", data)
        if isinstance(detail, dict):
            message = str(detail.get("error", message))
            if detail.get("hint") is not None:
                hint = str(detail["hint"])
        elif isinstance(detail, str):
            message = detail
        elif isinstance(detail, list) and detail and isinstance(detail[0], dict):
            # FastAPI's validation errors: [{"loc": [...], "msg": ...}, ...]
            first = detail[0]
            where = ".".join(str(p) for p in first.get("loc", []))
            message = "{}: {}".format(where, first.get("msg", "invalid"))
    return SpillHTTPError(status, message, hint)


# ----------------------------------------------------------- configuration


@dataclass
class ClientConfig:
    """The client's settings; see ``docs/CLIENT.md`` for each key."""

    url: str = "http://localhost:8080"
    timeout: float = 30.0
    admin_token: str = ""
    admin_token_file: str = ""
    ca_file: str = ""
    verify_tls: bool = True
    format: str = "table"
    timezone: str = ""

    #: Where the YAML layer came from, or ``None``; not a setting.
    source: Optional[str] = field(default=None, compare=False, repr=False)

    def resolved_token(self) -> str:
        """The admin token, read from ``admin_token_file`` if that is set."""
        if self.admin_token:
            return self.admin_token
        if self.admin_token_file:
            try:
                text = Path(self.admin_token_file).expanduser().read_text(
                    encoding="utf-8")
            except OSError as exc:
                raise ConfigError("admin_token_file: cannot read {}: {}".format(
                    self.admin_token_file, exc.strerror)) from None
            lines = text.splitlines()
            return lines[0].strip() if lines else ""
        return ""

    def to_yaml(self) -> str:
        """The merged settings in the ``--print-config`` layout.

        Written by hand rather than with ``yaml.dump`` because the C++ client
        prints the same text and both have to agree byte for byte.  The token
        itself is never printed.
        """
        lines = ["client:"]
        for name in _KEYS:
            value = getattr(self, name)
            if name == "admin_token" and value:
                # Quoted: a bare * would start a YAML alias.
                lines.append("  admin_token: '***'")
                continue
            if isinstance(value, bool):
                text = "true" if value else "false"
            elif isinstance(value, float):
                text = repr(value)
            elif value == "":
                text = "''"
            else:
                text = str(value)
            lines.append("  {}: {}".format(name, text))
        return "\n".join(lines) + "\n"

    def validate(self) -> None:
        if not (self.url.startswith("http://") or self.url.startswith("https://")):
            raise ConfigError("url must start with http:// or https://, got {!r}"
                              .format(self.url))
        if self.timeout <= 0:
            raise ConfigError("timeout must be positive, got {}".format(self.timeout))
        if self.format not in ("table", "json", "csv"):
            raise ConfigError("format must be table, json or csv, got {!r}"
                              .format(self.format))


_KEYS = [f.name for f in fields(ClientConfig) if f.name != "source"]


def _coerce(name: str, value: Any, origin: str) -> Any:
    """Convert *value* to the type of the setting *name*."""
    default = getattr(ClientConfig, name)
    if isinstance(default, bool):
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
        raise ConfigError("{}: {} must be a boolean, got {!r}".format(origin, name, value))
    if isinstance(default, float):
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ConfigError("{}: {} must be a number, got {!r}".format(
                origin, name, value)) from None
    if value is None:
        return ""
    return str(value)


def _apply(config: ClientConfig, values: Mapping[str, Any], origin: str) -> None:
    for name, value in values.items():
        if name not in _KEYS:
            raise ConfigError("{}: unknown client setting {!r}; known: {}".format(
                origin, name, ", ".join(_KEYS)))
        setattr(config, name, _coerce(name, value, origin))


def _env_values(environ: Mapping[str, str]) -> Dict[str, str]:
    values = {}
    for name in _KEYS:
        variable = CLIENT_ENV_PREFIX + name.upper()
        if variable in environ:
            values[name] = environ[variable]
    return values


def load_client_config(
    config_file: Optional[str] = None,
    env_file: Optional[str] = None,
    overrides: Optional[Mapping[str, Any]] = None,
    environ: Optional[Mapping[str, str]] = None,
    search_paths: Optional[Sequence[str]] = None,
    dotenv_path: Optional[str] = "./.env",
) -> ClientConfig:
    """Merge defaults, YAML file, ``.env``, environment and *overrides*.

    :param config_file: YAML file; else ``$DARPA_SPILL_CLIENT_CONFIG``, else
        the first of *search_paths* that exists.
    :param env_file: ``.env`` file; else ``$DARPA_SPILL_ENV_FILE``, else
        *dotenv_path* if it exists.
    :param overrides: settings from the command line, which win over all
        else.  ``None`` values are ignored.
    :raises ConfigError: on an unreadable file, unknown key or bad value.
    """
    environ = os.environ if environ is None else environ
    dotenv = resolve_dotenv(env_file, environ, dotenv_path)
    config = ClientConfig()

    chosen = (config_file or environ.get(CLIENT_ENV_PREFIX + "CONFIG")
              or dotenv.get(CLIENT_ENV_PREFIX + "CONFIG"))
    path: Optional[Path] = None
    if chosen:
        path = Path(chosen).expanduser()
        if not path.is_file():
            raise ConfigError("config file not found: {}".format(path))
    else:
        for candidate in (DEFAULT_CLIENT_CONFIG_PATHS if search_paths is None
                          else search_paths):
            if Path(candidate).expanduser().is_file():
                path = Path(candidate).expanduser()
                break

    if path is not None:
        try:
            import yaml
        except ImportError:
            raise ConfigError("reading {} needs PyYAML: pip install pyyaml"
                              .format(path)) from None
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except (OSError, yaml.YAMLError) as exc:
            raise ConfigError("cannot read {}: {}".format(path, exc)) from None
        if not isinstance(data, dict) or set(data) - {"client"}:
            raise ConfigError("{}: expected a single 'client:' section".format(path))
        section = data.get("client") or {}
        if not isinstance(section, dict):
            raise ConfigError("{}: 'client' must be a mapping".format(path))
        _apply(config, section, str(path))
        config.source = str(path)

    _apply(config, _env_values(dotenv), "the .env file")
    _apply(config, _env_values(environ), "the environment")
    _apply(config, {k: v for k, v in (overrides or {}).items() if v is not None},
           "the command line")
    config.validate()
    return config


# ------------------------------------------------------------------ queries


@dataclass
class Selection:
    """Which events a query asks for; every field is optional.

    Times are any expression the server accepts (``today``, ``-2h``,
    ``2026-07-01T09:15:00Z``, ``nova:...``); see ``/api/time/help``.
    """

    start: Optional[str] = None
    end: Optional[str] = None
    signals: List[str] = field(default_factory=list)
    types: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)

    def params(self) -> List[Tuple[str, str]]:
        items: List[Tuple[str, str]] = []
        if self.start is not None:
            items.append(("start", self.start))
        if self.end is not None:
            items.append(("end", self.end))
        items += [("signal", s) for s in self.signals]
        items += [("type", t) for t in self.types]
        items += [("source", s) for s in self.sources]
        return items


def encode_query(params: Sequence[Tuple[str, str]]) -> str:
    """Percent-encode *params* per RFC 3986, keeping their order.

    Only unreserved characters pass through, so ``$74`` becomes ``%2474``;
    the C++ client encodes identically.
    """
    return "&".join("{}={}".format(quote(k, safe=""), quote(str(v), safe=""))
                    for k, v in params)


@dataclass
class Response:
    """A successful response: status, lower-cased headers, and body."""

    status: int
    headers: Dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))

    @property
    def text(self) -> str:
        return self.body.decode("utf-8")


class SpillClient:
    """Talk to one DARPA Spill Information Server.

    :param url: base URL, e.g. ``http://novadaq-near-gateway-01.fnal.gov:8080``.
    :param timeout: seconds allowed for each request.
    :param admin_token: sent as ``X-Admin-Token`` when not empty; needed only
        for :meth:`admin_check`, :meth:`update_source` and :meth:`reset_source`.
    :param ca_file: PEM bundle to verify an ``https`` server against.
    :param verify_tls: ``False`` accepts any certificate.
    :param timezone: sent as ``tz=`` on time-bearing queries when not empty.
    """

    def __init__(self, url: str, timeout: float = 30.0, admin_token: str = "",
                 ca_file: str = "", verify_tls: bool = True, timezone: str = ""):
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.admin_token = admin_token
        self.timezone = timezone
        self._context: Optional[ssl.SSLContext] = None
        if self.url.startswith("https://"):
            self._context = ssl.create_default_context(cafile=ca_file or None)
            if not verify_tls:
                self._context.check_hostname = False
                self._context.verify_mode = ssl.CERT_NONE

    @classmethod
    def from_config(cls, config: ClientConfig) -> "SpillClient":
        return cls(config.url, timeout=config.timeout,
                   admin_token=config.resolved_token(), ca_file=config.ca_file,
                   verify_tls=config.verify_tls, timezone=config.timezone)

    # -- transport ------------------------------------------------------

    def _open(self, method: str, path: str,
              params: Sequence[Tuple[str, str]] = (),
              body: Optional[Mapping[str, Any]] = None):
        url = self.url + path
        if params:
            url += "?" + encode_query(params)
        headers = {"Accept": "application/json, text/csv",
                   "User-Agent": "darpa-spill-client/{}".format(__version__)}
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if self.admin_token:
            headers[ADMIN_HEADER] = self.admin_token
        request = urllib.request.Request(url, data=data, headers=headers,
                                         method=method)
        try:
            return urllib.request.urlopen(request, timeout=self.timeout,
                                          context=self._context)
        except urllib.error.HTTPError as exc:
            with exc:
                raise _parse_error(exc.code, exc.reason, exc.read()) from None
        except (urllib.error.URLError, socket.timeout, ssl.SSLError,
                ConnectionError) as exc:
            reason = getattr(exc, "reason", exc)
            raise SpillConnectionError("cannot reach {}: {}".format(
                self.url, reason)) from None

    def request(self, method: str, path: str,
                params: Sequence[Tuple[str, str]] = (),
                body: Optional[Mapping[str, Any]] = None) -> Response:
        """Send one request and return the whole response.

        :raises SpillHTTPError: on a 4xx or 5xx answer.
        :raises SpillConnectionError: if the server cannot be reached.
        """
        with self._open(method, path, params, body) as answer:
            try:
                payload = answer.read()
            except (socket.timeout, ConnectionError) as exc:
                raise SpillConnectionError("reading from {}: {}".format(
                    self.url, exc)) from None
            return Response(answer.status,
                            {k.lower(): v for k, v in answer.headers.items()},
                            payload)

    def stream(self, method: str, path: str,
               params: Sequence[Tuple[str, str]] = ()) -> Iterator[bytes]:
        """Yield the response body in chunks, for bodies too big to hold."""
        with self._open(method, path, params) as answer:
            while True:
                try:
                    chunk = answer.read(_CHUNK)
                except (socket.timeout, ConnectionError) as exc:
                    raise SpillConnectionError("reading from {}: {}".format(
                        self.url, exc)) from None
                if not chunk:
                    return
                yield chunk

    def _tz(self, tz: Optional[str]) -> List[Tuple[str, str]]:
        zone = tz if tz is not None else self.timezone
        return [("tz", zone)] if zone else []

    # -- status and reference -------------------------------------------

    def health(self) -> Dict[str, Any]:
        return self.request("GET", "/api/health").json()

    def status(self) -> Dict[str, Any]:
        return self.request("GET", "/api/status").json()

    def sources(self) -> Dict[str, Any]:
        return self.request("GET", "/api/sources").json()

    def signals(self) -> Dict[str, Any]:
        return self.request("GET", "/api/signals").json()

    def types(self) -> Dict[str, Any]:
        return self.request("GET", "/api/types").json()

    def time_convert(self, t: str, tz: Optional[str] = None) -> Dict[str, Any]:
        """Render one instant as NOvA ticks, UNIX, UTC and GPS."""
        return self.request("GET", "/api/time/convert",
                            [("t", t)] + self._tz(tz)).json()

    def time_help(self) -> Dict[str, Any]:
        return self.request("GET", "/api/time/help").json()

    # -- events ---------------------------------------------------------

    def latest(self, signal: Optional[str] = None,
               sources: Sequence[str] = ()) -> Dict[str, Any]:
        """The newest archived event, as ``{"event": {...}}``."""
        params = ([("signal", signal)] if signal else []) + [
            ("source", s) for s in sources]
        return self.request("GET", "/api/latest", params).json()

    @staticmethod
    def events_params(selection: Selection, fmt: str, limit: Optional[int],
                      offset: int, descending: bool, tz: List[Tuple[str, str]],
                      columns: Optional[str]) -> List[Tuple[str, str]]:
        params = selection.params() + [("format", fmt)]
        if limit is not None:
            params.append(("limit", str(limit)))
        if offset:
            params.append(("offset", str(offset)))
        if descending:
            params.append(("order", "desc"))
        params += tz
        if columns:
            params.append(("columns", columns))
        return params

    def events(self, selection: Optional[Selection] = None, limit: Optional[int] = None,
               offset: int = 0, descending: bool = False,
               tz: Optional[str] = None) -> Dict[str, Any]:
        """One page of matching events, as ``{"meta": ..., "events": [...]}``.

        ``meta.total`` is the number matching overall; page through with
        *offset*, or use :meth:`export` for a whole range.
        """
        return self.request("GET", "/api/events", self.events_params(
            selection or Selection(), "json", limit, offset, descending,
            self._tz(tz), None)).json()

    def events_csv(self, selection: Optional[Selection] = None,
                   limit: Optional[int] = None, offset: int = 0,
                   descending: bool = False, tz: Optional[str] = None,
                   columns: Optional[str] = None) -> str:
        """One page of matching events as CSV text, with its ``#`` header."""
        return self.request("GET", "/api/events", self.events_params(
            selection or Selection(), "csv", limit, offset, descending, self._tz(tz),
            columns)).text

    def export_params(self, selection: Selection, fmt: str,
                      tz: Optional[str], columns: Optional[str]) -> List[Tuple[str, str]]:
        params = selection.params() + [("format", fmt)] + self._tz(tz)
        if columns:
            params.append(("columns", columns))
        return params

    def export(self, selection: Selection, out: IO[bytes], fmt: str = "csv",
               tz: Optional[str] = None, columns: Optional[str] = None) -> int:
        """Stream every matching event to the binary file *out*.

        :returns: the number of bytes written.
        """
        written = 0
        for chunk in self.stream("GET", "/api/export",
                                 self.export_params(selection, fmt, tz, columns)):
            out.write(chunk)
            written += len(chunk)
        return written

    # -- administration -------------------------------------------------

    def admin_check(self) -> Dict[str, Any]:
        return self.request("GET", "/api/admin/check").json()

    def update_source(self, name: str, enabled: Optional[bool] = None,
                      base_url: Optional[str] = None) -> Dict[str, Any]:
        """Enable, disable or re-point a source; needs the admin token."""
        body: Dict[str, Any] = {}
        if enabled is not None:
            body["enabled"] = enabled
        if base_url is not None:
            body["base_url"] = base_url
        return self.request("PATCH", "/api/sources/" + quote(name, safe=""),
                            body=body).json()

    def reset_source(self, name: str) -> Dict[str, Any]:
        """Return a source to the configuration file; needs the admin token."""
        return self.request("POST", "/api/sources/{}/reset".format(
            quote(name, safe=""))).json()
