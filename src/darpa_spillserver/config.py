"""Configuration: defaults, YAML file, environment, command line.

Design.md requires that everything be settable from either the command line or
a YAML file.  Four layers are merged, each overriding the one before:

1. the defaults in this module,
2. a YAML file (``--config``, or the first of the search paths that exists),
3. environment variables prefixed ``DARPA_SPILL_``,
4. explicit command-line options.

An environment variable names its setting by section and key joined with an
underscore --- ``DARPA_SPILL_TDU_BASE_URL``, ``DARPA_SPILL_STORAGE_PATH`` ---
which lets a systemd unit or container set anything without a file.

Unknown keys in the YAML file are an error, not a warning.  A silently ignored
``retention_days`` misspelling is exactly the kind of thing that is noticed
only once the archive has already been pruned.
"""

from __future__ import annotations

import argparse
import copy
import logging
import os
import re
import sys
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from urllib.parse import urlsplit
from typing import (
    Any, Dict, List, Mapping, Optional, Sequence,
    get_origin, get_type_hints,
)

__all__ = [
    "Config",
    "Source",
    "parse_source",
    "DEFAULT_TDU_URL",
    "ServerConfig",
    "TDUConfig",
    "IngestConfig",
    "StorageConfig",
    "QueryConfig",
    "AuthConfig",
    "AdminConfig",
    "LoggingConfig",
    "ConfigError",
    "DEFAULT_CONFIG_PATHS",
    "ENV_PREFIX",
    "build_parser",
    "load_config",
    "configure_logging",
]

log = logging.getLogger(__name__)

ENV_PREFIX = "DARPA_SPILL_"

#: Searched in order when no ``--config`` is given.
DEFAULT_CONFIG_PATHS = (
    "./spillserver.yaml",
    "~/.config/darpa-spillserver/spillserver.yaml",
    "/etc/darpa-spillserver/spillserver.yaml",
)


class ConfigError(ValueError):
    """Raised when configuration is missing, malformed, or contradictory."""


@dataclass
class ServerConfig:
    """Where and how the HTTP server listens."""

    host: str = "0.0.0.0"
    port: int = 8080
    root_path: str = ""
    """Mount prefix when running behind a reverse proxy, e.g. ``/spills``."""

    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    """Origins allowed to call the API from a browser."""

    ssl_certfile: str = ""
    """PEM certificate (with any intermediates) to serve HTTPS directly.
    Empty serves plain HTTP, which is right when a TLS-terminating proxy
    fronts the server."""

    ssl_keyfile: str = ""
    """PEM private key matching ``ssl_certfile``."""

    @property
    def scheme(self) -> str:
        return "https" if self.ssl_certfile else "http"


#: Polled when neither ``tdu.sources`` nor ``tdu.base_url`` is configured.
DEFAULT_TDU_URL = "http://tdu-near-master-ppc-01:8080"

#: What a source name may look like. It appears in URLs (``?source=``), in CSV
#: cells and in the archive, so it is kept to characters none of them escape.
_SOURCE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


@dataclass
class Source:
    """One TDU the server records from.

    *name* tags every event ingested from it, and is what a query selects
    with ``source=``.  It stays fixed while *base_url* may change: pointing a
    source at a replacement TDU keeps its history under the same name.
    """

    name: str
    base_url: str
    enabled: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "base_url": self.base_url,
                "enabled": self.enabled}


def check_source_url(url: str, where: str = "source URL") -> str:
    """Return *url* normalised, or raise :class:`ConfigError`."""
    url = str(url).strip().rstrip("/")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise ConfigError(
            "{} must be an http:// or https:// URL with a host, got {!r}".format(
                where, url
            )
        )
    return url


def check_source_name(name: str) -> str:
    if not _SOURCE_NAME.match(name):
        raise ConfigError(
            "source name {!r} must be 1-64 letters, digits, '.', '_' or '-', "
            "starting with a letter or digit".format(name)
        )
    return name


def parse_source(spec: str) -> Source:
    """Parse one ``tdu.sources`` entry: ``URL`` or ``NAME=URL``.

    A bare URL is named after its host, so
    ``http://tdu-near-master-ppc-02:8080`` becomes ``tdu-near-master-ppc-02``.
    """
    spec = str(spec).strip()
    name, sep, url = spec.partition("=")
    if not sep or "://" in name:
        # No name given, or the "=" belongs to the URL's query string.
        name, url = "", spec
    url = check_source_url(url, "tdu.sources entry {!r}".format(spec))
    if not name:
        name = urlsplit(url).hostname or ""
    return Source(name=check_source_name(name.strip()), base_url=url)


@dataclass
class TDUConfig:
    """How to reach the TDUs' embedded bottle servers."""

    sources: List[str] = field(default_factory=list)
    """TDUs to record from, each ``URL`` or ``NAME=URL``."""

    base_url: str = ""
    """A single TDU, as configured before ``sources`` existed.  Kept so older
    configuration files keep working; it cannot be combined with
    ``sources``."""

    timeout: float = 10.0
    retries: int = 2
    retry_backoff: float = 0.5

    def resolved_sources(self) -> List[Source]:
        """The configured sources, in order, as :class:`Source` objects."""
        if self.sources:
            return [parse_source(spec) for spec in self.sources]
        return [parse_source(self.base_url or DEFAULT_TDU_URL)]


@dataclass
class IngestConfig:
    """The background poller that copies events out of the TDU."""

    enabled: bool = True

    interval: float = 1.0
    """Seconds between polls.  With the history route this only bounds
    latency; without it, it bounds how many events are lost."""

    batch_limit: int = 5000
    """Maximum records requested per poll."""

    overlap: float = 5.0
    """Seconds of already-ingested time re-read on each poll.  Inserts are
    idempotent, so overlap costs nothing but closes the gap that clock skew
    or a slow poll would otherwise open."""

    backfill: float = 3600.0
    """Seconds of history to request on a cold start, when the archive is
    empty and there is no watermark to resume from."""

    max_consecutive_errors: int = 0
    """Stop the poller after this many consecutive failures; 0 means never
    stop, which is what an unattended deployment wants."""


@dataclass
class StorageConfig:
    """The local SQLite archive."""

    path: str = "./spills.db"
    timeout: float = 15.0

    retention_days: float = 0.0
    """Delete events older than this many days.  0 keeps everything, which is
    the default: an event archive is small and losing it is unrecoverable."""

    prune_interval: float = 3600.0
    """Seconds between retention sweeps."""

    ingest_log_keep: int = 1000
    """Ingest-log rows to retain."""


@dataclass
class QueryConfig:
    """Defaults applied to client queries."""

    timezone: str = "UTC"
    """Zone assumed for inputs that carry none, such as ``09:15``.  UTC by
    default so an unconfigured server never shifts a result silently; set
    ``America/Chicago`` for a deployment whose operators speak local time."""

    default_limit: int = 10000
    max_limit: int = 1000000
    max_export_rows: int = 5000000
    """Ceiling on a streamed CSV or JSON export."""


@dataclass
class AuthConfig:
    """OIDC single sign-on.

    Disabled by default, as Design.md requires, but every setting the
    Fermilab SSO integration needs is present so that enabling it is a
    configuration change rather than a code change.  See
    :mod:`darpa_spillserver.auth`.
    """

    enabled: bool = False
    provider: str = "oidc"
    issuer: str = ""
    """OIDC issuer URL; discovery is read from ``{issuer}/.well-known/openid-configuration``."""

    client_id: str = ""
    client_secret: str = ""
    """Prefer ``client_secret_file`` or the environment over writing this into YAML."""

    client_secret_file: str = ""
    redirect_url: str = ""
    scopes: List[str] = field(default_factory=lambda: ["openid", "profile", "email"])
    allowed_groups: List[str] = field(default_factory=list)
    """If non-empty, a caller must hold one of these groups.  Empty means any
    successfully authenticated caller is allowed."""

    session_secret: str = ""
    session_cookie: str = "darpa_spill_session"
    session_max_age: int = 28800

    def resolved_client_secret(self) -> str:
        """The client secret, read from ``client_secret_file`` if set."""
        if self.client_secret_file:
            return Path(self.client_secret_file).expanduser().read_text().strip()
        return self.client_secret


@dataclass
class AdminConfig:
    """Who may change sources at runtime, from the /config page or the API.

    Nobody, until one of these is set.  The server binds every interface by
    default, and a source URL decides what ends up in the archive, so editing
    is opt-in.
    """

    token: str = ""
    """Shared secret sent as ``X-Admin-Token``.  Prefer ``token_file``."""

    token_file: str = ""

    allowed_groups: List[str] = field(default_factory=list)
    """With ``auth.enabled``, signed-in members of these groups may edit
    without the token.  Empty means the token is the only way in."""

    def resolved_token(self) -> str:
        if self.token_file:
            return Path(self.token_file).expanduser().read_text().strip()
        return self.token


@dataclass
class LoggingConfig:
    level: str = "INFO"
    file: str = ""
    format: str = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


@dataclass
class Config:
    """The full server configuration."""

    server: ServerConfig = field(default_factory=ServerConfig)
    tdu: TDUConfig = field(default_factory=TDUConfig)
    ingest: IngestConfig = field(default_factory=IngestConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    query: QueryConfig = field(default_factory=QueryConfig)
    auth: AuthConfig = field(default_factory=AuthConfig)
    admin: AdminConfig = field(default_factory=AdminConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)

    #: Path the configuration was loaded from, for the /status endpoint.
    source: str = ""

    def to_dict(self, redact: bool = True) -> Dict[str, Any]:
        """Render as plain dictionaries, with secrets masked by default."""
        data = asdict(self)
        if redact:
            for key in ("client_secret", "session_secret"):
                if data["auth"].get(key):
                    data["auth"][key] = "***redacted***"
            if data["admin"].get("token"):
                data["admin"]["token"] = "***redacted***"
        return data

    def validate(self) -> None:
        """Check the configuration is internally consistent.

        :raises ConfigError: describing the first problem found.
        """
        if not 1 <= self.server.port <= 65535:
            raise ConfigError(
                "server.port must be 1..65535, got {}".format(self.server.port)
            )
        if bool(self.server.ssl_certfile) != bool(self.server.ssl_keyfile):
            raise ConfigError(
                "server.ssl_certfile and server.ssl_keyfile must be set "
                "together; HTTPS needs both the certificate and its key"
            )
        if self.tdu.sources and self.tdu.base_url:
            raise ConfigError(
                "tdu.base_url and tdu.sources are both set; list every TDU "
                "under tdu.sources and remove tdu.base_url"
            )
        if self.tdu.base_url:
            check_source_url(self.tdu.base_url, "tdu.base_url")
        sources = self.tdu.resolved_sources()
        for attribute in ("name", "base_url"):
            seen = set()
            for source in sources:
                value = getattr(source, attribute)
                if value in seen:
                    raise ConfigError(
                        "tdu.sources lists {} {!r} twice{}".format(
                            "the name" if attribute == "name" else "the URL",
                            value,
                            "; give one of them an explicit NAME=URL"
                            if attribute == "name" else "",
                        )
                    )
                seen.add(value)
        if self.ingest.interval <= 0:
            raise ConfigError("ingest.interval must be positive")
        if self.ingest.batch_limit < 1:
            raise ConfigError("ingest.batch_limit must be at least 1")
        if self.ingest.overlap < 0:
            raise ConfigError("ingest.overlap cannot be negative")
        if self.storage.retention_days < 0:
            raise ConfigError("storage.retention_days cannot be negative")
        if self.query.max_limit < self.query.default_limit:
            raise ConfigError(
                "query.max_limit ({}) is below query.default_limit ({})".format(
                    self.query.max_limit, self.query.default_limit
                )
            )

        # Fail at startup rather than when the first client sends a bare time.
        from .novatime import NovaTimeError, get_timezone
        try:
            get_timezone(self.query.timezone)
        except NovaTimeError as exc:
            raise ConfigError(str(exc)) from None

        if self.auth.enabled:
            missing = [
                name
                for name in ("issuer", "client_id", "redirect_url")
                if not getattr(self.auth, name)
            ]
            if not self.auth.client_secret and not self.auth.client_secret_file:
                missing.append("client_secret or client_secret_file")
            if missing:
                raise ConfigError(
                    "auth.enabled is true but these settings are missing: "
                    + ", ".join(missing)
                )
            if not self.auth.session_secret:
                raise ConfigError(
                    "auth.enabled is true but auth.session_secret is unset; "
                    "generate one with: openssl rand -hex 32"
                )


_SECTIONS = {
    "server": ServerConfig,
    "tdu": TDUConfig,
    "ingest": IngestConfig,
    "storage": StorageConfig,
    "query": QueryConfig,
    "auth": AuthConfig,
    "admin": AdminConfig,
    "logging": LoggingConfig,
}


def _field_types(section: Any) -> Dict[str, Any]:
    """Resolve a dataclass's field types to real objects.

    ``dataclasses.fields()`` reports ``.type`` as a *string* under
    ``from __future__ import annotations`` (PEP 563), which this module uses.
    Comparing those strings against ``bool`` or ``int`` silently never
    matches, so every setting would be coerced to ``str`` and a port would
    arrive as ``"9999"``.  ``get_type_hints`` resolves them properly.
    """
    section_type = section if isinstance(section, type) else type(section)
    try:
        hints = get_type_hints(section_type)
    except Exception:  # pragma: no cover - only on an unresolvable annotation
        hints = {}
    return {f.name: hints.get(f.name, f.type) for f in fields(section_type)}


def _coerce(value: Any, target_type: Any, where: str) -> Any:
    """Coerce a YAML/env scalar to the type the dataclass field declares."""
    origin = get_origin(target_type)
    if origin is list or target_type is list or target_type == List[str]:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        if isinstance(value, (list, tuple)):
            return [str(item) for item in value]
        raise ConfigError("{} must be a list, got {!r}".format(where, value))

    if target_type is bool:
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
        raise ConfigError("{} must be a boolean, got {!r}".format(where, value))

    if target_type is int:
        try:
            return int(value)
        except (TypeError, ValueError):
            raise ConfigError(
                "{} must be an integer, got {!r}".format(where, value)
            ) from None

    if target_type is float:
        try:
            return float(value)
        except (TypeError, ValueError):
            raise ConfigError(
                "{} must be a number, got {!r}".format(where, value)
            ) from None

    return str(value) if value is not None else ""


def _apply_mapping(config: Config, data: Mapping[str, Any], origin: str) -> None:
    """Merge a nested ``{section: {key: value}}`` mapping into *config*."""
    for section_name, section_data in data.items():
        if section_name in ("source",):
            continue
        if section_name not in _SECTIONS:
            raise ConfigError(
                "unknown configuration section {!r} in {} "
                "(expected one of: {})".format(
                    section_name, origin, ", ".join(sorted(_SECTIONS))
                )
            )
        if section_data is None:
            continue
        if not isinstance(section_data, Mapping):
            raise ConfigError(
                "section {!r} in {} must be a mapping, got {}".format(
                    section_name, origin, type(section_data).__name__
                )
            )

        section = getattr(config, section_name)
        known = _field_types(section)
        for key, value in section_data.items():
            if key not in known:
                raise ConfigError(
                    "unknown setting {!r} in section {!r} of {} "
                    "(expected one of: {})".format(
                        key, section_name, origin, ", ".join(sorted(known))
                    )
                )
            where = "{}.{}".format(section_name, key)
            setattr(section, key, _coerce(value, known[key], where))


def _load_yaml(path: Path) -> Dict[str, Any]:
    try:
        import yaml
    except ImportError:  # pragma: no cover - depends on install extras
        raise ConfigError(
            "reading a YAML config needs PyYAML; install it with "
            "'pip install pyyaml' or run ./bootstrap.sh"
        ) from None

    try:
        text = path.read_text()
    except OSError as exc:
        raise ConfigError("cannot read config file {}: {}".format(path, exc)) from None

    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError("{} is not valid YAML: {}".format(path, exc)) from None

    if data is None:
        return {}
    if not isinstance(data, Mapping):
        raise ConfigError(
            "{} must contain a mapping at the top level, got {}".format(
                path, type(data).__name__
            )
        )
    return dict(data)


def _env_overrides(environ: Mapping[str, str]) -> Dict[str, Dict[str, Any]]:
    """Collect ``DARPA_SPILL_<SECTION>_<KEY>`` variables into nested form."""
    overrides: Dict[str, Dict[str, Any]] = {}
    for name, value in environ.items():
        if not name.startswith(ENV_PREFIX):
            continue
        remainder = name[len(ENV_PREFIX):].lower()
        for section_name, section_type in _SECTIONS.items():
            prefix = section_name + "_"
            if remainder.startswith(prefix):
                key = remainder[len(prefix):]
                if key in {f.name for f in fields(section_type)}:
                    overrides.setdefault(section_name, {})[key] = value
                break
    return overrides


def build_parser(prog: str = "darpa-spill-server") -> argparse.ArgumentParser:
    """Build the command-line parser.

    Every option defaults to ``None`` so that "not given" is distinguishable
    from "given the same value as the default" --- otherwise a flag left off
    the command line would override the YAML file.
    """
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Serve NOvA accelerator event timestamps collected from a TDU.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("-c", "--config", metavar="FILE",
                        help="YAML configuration file")
    parser.add_argument("-V", "--version", action="store_true",
                        help="show version information and exit")
    parser.add_argument("--print-config", action="store_true",
                        help="print the merged configuration and exit")

    group = parser.add_argument_group("server")
    group.add_argument("--host", help="address to bind")
    group.add_argument("--port", type=int, help="port to bind")
    group.add_argument("--root-path", help="mount prefix behind a reverse proxy")
    group.add_argument("--cors-origin", action="append", dest="cors_origins",
                       metavar="ORIGIN",
                       help="allowed CORS origin; repeatable")
    group.add_argument("--ssl-certfile", metavar="FILE",
                       help="PEM certificate; serves HTTPS when given")
    group.add_argument("--ssl-keyfile", metavar="FILE",
                       help="PEM private key for --ssl-certfile")

    group = parser.add_argument_group("TDU data source")
    group.add_argument("--tdu-source", action="append", dest="tdu_sources",
                       metavar="[NAME=]URL",
                       help="a TDU to record from; repeatable. The name "
                            "defaults to the URL's host")
    group.add_argument("--tdu-url", dest="tdu_base_url", metavar="URL",
                       help="a single TDU; use --tdu-source for several")
    group.add_argument("--tdu-timeout", dest="tdu_timeout", type=float,
                       metavar="SECONDS", help="per-request timeout")
    group.add_argument("--tdu-retries", dest="tdu_retries", type=int,
                       metavar="N", help="retries per failed request")

    group = parser.add_argument_group("ingest")
    group.add_argument("--no-ingest", action="store_true",
                       help="serve the existing archive without polling the TDU")
    group.add_argument("--interval", dest="ingest_interval", type=float,
                       metavar="SECONDS", help="seconds between polls")
    group.add_argument("--batch-limit", dest="ingest_batch_limit", type=int,
                       metavar="N", help="maximum records per poll")
    group.add_argument("--overlap", dest="ingest_overlap", type=float,
                       metavar="SECONDS", help="seconds of overlap re-read per poll")
    group.add_argument("--backfill", dest="ingest_backfill", type=float,
                       metavar="SECONDS",
                       help="seconds of history to request on a cold start")

    group = parser.add_argument_group("storage")
    group.add_argument("-d", "--database", dest="storage_path", metavar="FILE",
                       help="SQLite archive file")
    group.add_argument("--retention-days", dest="storage_retention_days",
                       type=float, metavar="DAYS",
                       help="delete events older than this; 0 keeps everything")

    group = parser.add_argument_group("query defaults")
    group.add_argument("--timezone", dest="query_timezone", metavar="ZONE",
                       help="zone assumed for times that carry none")
    group.add_argument("--default-limit", dest="query_default_limit", type=int,
                       metavar="N", help="default maximum rows per query")
    group.add_argument("--max-limit", dest="query_max_limit", type=int,
                       metavar="N", help="hard ceiling on rows per query")

    group = parser.add_argument_group("authentication")
    group.add_argument("--auth", dest="auth_enabled", action="store_true",
                       default=None, help="require OIDC single sign-on")
    group.add_argument("--no-auth", dest="auth_enabled", action="store_false",
                       default=None, help="serve without authentication")
    group.add_argument("--oidc-issuer", dest="auth_issuer", metavar="URL")
    group.add_argument("--oidc-client-id", dest="auth_client_id", metavar="ID")
    group.add_argument("--oidc-client-secret-file", dest="auth_client_secret_file",
                       metavar="FILE",
                       help="file holding the client secret")
    group.add_argument("--oidc-redirect-url", dest="auth_redirect_url", metavar="URL")

    group = parser.add_argument_group("runtime configuration")
    group.add_argument("--admin-token-file", dest="admin_token_file",
                       metavar="FILE",
                       help="file holding the token that allows changing "
                            "sources from the /config page")

    group = parser.add_argument_group("logging")
    group.add_argument("-l", "--log-level", dest="logging_level",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                       help="logging verbosity")
    group.add_argument("--log-file", dest="logging_file", metavar="FILE",
                       help="also write logs to this file")
    group.add_argument("-v", "--verbose", action="store_true",
                       help="shorthand for --log-level DEBUG")

    return parser


#: Maps a parser destination onto ``(section, key)``.
_ARG_MAP = {
    "host": ("server", "host"),
    "port": ("server", "port"),
    "root_path": ("server", "root_path"),
    "cors_origins": ("server", "cors_origins"),
    "ssl_certfile": ("server", "ssl_certfile"),
    "ssl_keyfile": ("server", "ssl_keyfile"),
    "tdu_sources": ("tdu", "sources"),
    "tdu_base_url": ("tdu", "base_url"),
    "tdu_timeout": ("tdu", "timeout"),
    "tdu_retries": ("tdu", "retries"),
    "ingest_interval": ("ingest", "interval"),
    "ingest_batch_limit": ("ingest", "batch_limit"),
    "ingest_overlap": ("ingest", "overlap"),
    "ingest_backfill": ("ingest", "backfill"),
    "storage_path": ("storage", "path"),
    "storage_retention_days": ("storage", "retention_days"),
    "query_timezone": ("query", "timezone"),
    "query_default_limit": ("query", "default_limit"),
    "query_max_limit": ("query", "max_limit"),
    "auth_enabled": ("auth", "enabled"),
    "auth_issuer": ("auth", "issuer"),
    "auth_client_id": ("auth", "client_id"),
    "auth_client_secret_file": ("auth", "client_secret_file"),
    "auth_redirect_url": ("auth", "redirect_url"),
    "admin_token_file": ("admin", "token_file"),
    "logging_level": ("logging", "level"),
    "logging_file": ("logging", "file"),
}


def load_config(
    args: Optional[argparse.Namespace] = None,
    argv: Optional[Sequence[str]] = None,
    environ: Optional[Mapping[str, str]] = None,
    search_paths: Optional[Sequence[str]] = None,
) -> Config:
    """Build a :class:`Config` from all four layers.

    :param args: already-parsed arguments; if omitted, *argv* is parsed.
    :param argv: command line to parse when *args* is not given.
    :param environ: environment to read; defaults to :data:`os.environ`.
    :param search_paths: candidate config files; defaults to
        :data:`DEFAULT_CONFIG_PATHS`.
    :raises ConfigError: on any malformed or inconsistent setting.
    """
    if args is None:
        args = build_parser().parse_args(argv)
    environ = os.environ if environ is None else environ

    config = Config()

    # -- layer 2: YAML ----------------------------------------------------
    config_path: Optional[Path] = None
    explicit = getattr(args, "config", None) or environ.get(ENV_PREFIX + "CONFIG")
    if explicit:
        config_path = Path(explicit).expanduser()
        if not config_path.is_file():
            raise ConfigError("config file not found: {}".format(config_path))
    else:
        # An empty list means "search nowhere", not "use the defaults".
        for candidate in (DEFAULT_CONFIG_PATHS if search_paths is None
                          else search_paths):
            path = Path(candidate).expanduser()
            if path.is_file():
                config_path = path
                break

    if config_path is not None:
        _apply_mapping(config, _load_yaml(config_path), str(config_path))
        config.source = str(config_path)

    # -- layer 3: environment ---------------------------------------------
    env_data = _env_overrides(environ)
    if env_data:
        _apply_mapping(config, env_data, "the environment")

    # -- layer 4: command line ---------------------------------------------
    cli_data: Dict[str, Dict[str, Any]] = {}
    for dest, (section_name, key) in _ARG_MAP.items():
        value = getattr(args, dest, None)
        if value is not None:
            cli_data.setdefault(section_name, {})[key] = value

    if getattr(args, "no_ingest", False):
        cli_data.setdefault("ingest", {})["enabled"] = False
    if getattr(args, "verbose", False):
        cli_data.setdefault("logging", {})["level"] = "DEBUG"

    if cli_data:
        _apply_mapping(config, cli_data, "the command line")

    config.validate()
    return config


def configure_logging(config: LoggingConfig) -> None:
    """Install the configured logging handlers on the root logger.

    A log file that cannot be opened is reported on stderr and skipped rather
    than raised. Losing the log destination is a real problem, but it is a
    smaller one than refusing to serve accelerator data because a directory
    is missing, and the console handler still captures everything.
    """
    handlers: List[logging.Handler] = [logging.StreamHandler()]
    if config.file:
        log_path = Path(config.file).expanduser()
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            handlers.append(logging.FileHandler(str(log_path)))
        except OSError as exc:
            print(
                "warning: cannot write the log file {}: {}; "
                "logging to the console only".format(log_path, exc),
                file=sys.stderr,
            )

    logging.basicConfig(
        level=getattr(logging, config.level.upper(), logging.INFO),
        format=config.format,
        handlers=handlers,
        force=True,
    )
