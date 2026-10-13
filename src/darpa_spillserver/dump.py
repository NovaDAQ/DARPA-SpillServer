"""Bulk export of a time range to CSV and JSON (``darpa-spill-dump``).

``darpa-spill-query`` answers a question; this tool takes a copy.  It walks a
time range in windows and writes every matching event to a CSV file, a JSON
file, or both in the same pass, reading either a local archive or a running
server.

    darpa-spill-dump --start 2026-10-12T00:00 --end 2026-10-13T00:00 \\
        --csv day.csv --json day.json
    darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k \\
        --start=-6h --signal '$8f' --csv numi.csv

**Why windows.**  Both ``/api/events`` and :meth:`SpillStore.query` page with
``LIMIT ... OFFSET``, which SQLite satisfies by stepping over every skipped
row; a month dumped through one cursor is quadratic in the row count, and on
the server a single ``/api/export`` request is capped at
``query.max_export_rows`` and silently stops there.  Bounding each request to
a slice of time keeps it proportional to the slice.  A window holding more
rows than ``--page-size`` is halved and re-read rather than paged by offset,
so a concurrent ``darpa-spill-backfill`` inserting into the middle of the
range cannot shift a page boundary and drop or repeat a row.

**One fetch, two files.**  Rows are fetched once as the canonical row shape
of :func:`darpa_spillserver.formats.event_row` and fanned out to both
writers, so the CSV and JSON of one run always hold the same events.

**Snapshot and verification.**  Before the walk the matching rows are
counted; afterwards the count written is compared against it.  The range is
also clamped to the archive's extent at start-up, so rows that arrive while a
long dump runs are not half-included.

**No partial files.**  Output is written to ``FILE.part`` and renamed into
place only after the whole range has been read, so an interrupted dump never
leaves a plausible-looking but short file behind.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, time as dtime, timedelta, timezone
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

from . import __version__
from .config import ENV_PREFIX, ConfigError, load_config
from .formats import COLUMNS, event_row
from .novatime import (
    TICKS_PER_SECOND,
    NovaTimeError,
    convert,
    datetime_to_nova,
    get_timezone,
    nova_to_datetime,
    parse_range,
)
from .signals import parse_signal, parse_spill_type
from .storage import UNKNOWN_SIGNAL, SpillStore, StoreError

__all__ = ["main", "dump_range", "split_range", "segment_path", "CsvSink",
           "JsonSink", "LocalReader", "RemoteReader", "DumpError"]

#: Seconds of archive time per request.  The near detector archive holds
#: roughly 25k events per hour across three TDUs, so an hour is one request.
DEFAULT_WINDOW = 3600.0

#: Rows per request.  A window holding more is halved and re-read.
DEFAULT_PAGE_SIZE = 50000

#: Environment variables read by this tool alone.  They sit under the
#: server's ``DARPA_SPILL_`` prefix but name no configuration section, so the
#: server's loader ignores them.
ENV_SERVER = ENV_PREFIX + "DUMP_SERVER"
ENV_TOKEN = ENV_PREFIX + "DUMP_TOKEN"
ENV_CA_BUNDLE = ENV_PREFIX + "DUMP_CA_BUNDLE"
ENV_INSECURE = ENV_PREFIX + "DUMP_INSECURE"


class DumpError(RuntimeError):
    """A failure that ends the dump; the message is meant for the operator."""


# ----------------------------------------------------------------- readers


class LocalReader:
    """Read windows straight from a SQLite archive."""

    def __init__(self, store: SpillStore, spill_types, signal_codes, sources):
        self.store = store
        self.spill_types = spill_types or None
        self.signal_codes = signal_codes or None
        self.sources = sources or None
        self.description = "sqlite:" + str(store.path)

    def extent(self) -> Tuple[Optional[int], Optional[int]]:
        first = self.store.earliest(self.sources)
        last = self.store.latest(None, self.sources)
        return (first.nova_time if first else None,
                last.nova_time if last else None)

    def fetch(self, start_nova: int, end_nova: int,
              limit: int) -> Tuple[List[Dict[str, Any]], int]:
        page = self.store.query(
            start_nova=start_nova, end_nova=end_nova,
            spill_types=self.spill_types, signal_codes=self.signal_codes,
            limit=limit, offset=0, sources=self.sources,
        )
        return [event_row(e) for e in page.events], page.total

    def close(self) -> None:
        self.store.close()


class RemoteReader:
    """Read windows from a running server's ``/api/events``."""

    def __init__(self, base_url: str, signals: Sequence[str],
                 types: Sequence[str], sources: Sequence[str],
                 token: Optional[str] = None, verify: Any = True,
                 timeout: float = 120.0, retries: int = 3, client=None):
        if client is None:
            try:
                import httpx
            except ImportError:  # pragma: no cover - httpx is a dependency
                raise DumpError("reading a server needs httpx; run ./bootstrap.sh")
            headers = {"User-Agent": "darpa-spill-dump/" + __version__}
            if token:
                headers["Authorization"] = "Bearer " + token
            client = httpx.Client(headers=headers, verify=verify,
                                  timeout=timeout)
        self.client = client
        self.base = base_url.rstrip("/")
        self.retries = max(1, retries)
        self.description = self.base
        self.selection: List[Tuple[str, str]] = (
            [("signal", s) for s in signals]
            + [("type", t) for t in types]
            + [("source", s) for s in sources]
        )
        self._status: Optional[Dict[str, Any]] = None

    def _get(self, path: str, params=None) -> Dict[str, Any]:
        import httpx
        url = self.base + path
        delay = 1.0
        for attempt in range(1, self.retries + 1):
            try:
                response = self.client.get(url, params=params)
            except httpx.HTTPError as exc:
                if "CERTIFICATE_VERIFY_FAILED" in str(exc):
                    raise DumpError(
                        "cannot verify the TLS certificate of {}: {}\n"
                        "pass --ca-bundle FILE with the server's certificate, "
                        "or --insecure to skip verification".format(
                            self.base, exc))
                if attempt == self.retries:
                    raise DumpError("cannot reach {}: {}".format(url, exc))
                time.sleep(delay)
                delay *= 2
                continue
            if response.status_code >= 500 and attempt < self.retries:
                time.sleep(delay)
                delay *= 2
                continue
            if response.status_code >= 400:
                try:
                    detail = response.json().get("detail", response.text)
                except ValueError:
                    detail = response.text
                if isinstance(detail, dict):
                    detail = "; ".join(str(detail[k]) for k in ("error", "hint")
                                       if detail.get(k)) or str(detail)
                raise DumpError("{} returned {}: {}".format(
                    url, response.status_code, detail))
            try:
                return response.json()
            except ValueError as exc:
                raise DumpError("{} returned unparseable JSON: {}".format(url, exc))
        raise DumpError("cannot reach {}".format(url))  # pragma: no cover

    def status(self) -> Dict[str, Any]:
        if self._status is None:
            self._status = self._get("/api/status")
        return self._status

    def server_timezone(self) -> str:
        return self.status().get("query_defaults", {}).get("timezone", "UTC")

    def max_limit(self) -> Optional[int]:
        return self.status().get("query_defaults", {}).get("max_limit")

    def extent(self) -> Tuple[Optional[int], Optional[int]]:
        # The status summary covers the whole archive, not the selected
        # sources; that is fine for clamping, which only needs a bound.
        archive = self.status().get("archive", {})
        first, last = archive.get("earliest"), archive.get("latest")
        return (first["nova"] if first else None,
                last["nova"] if last else None)

    def fetch(self, start_nova: int, end_nova: int,
              limit: int) -> Tuple[List[Dict[str, Any]], int]:
        params = [
            ("start", "nova:{}".format(start_nova)),
            ("end", "nova:{}".format(end_nova)),
            ("order", "asc"),
            ("format", "json"),
            ("limit", str(limit)),
        ] + self.selection
        body = self._get("/api/events", params)
        return body.get("events", []), int(body["meta"]["total"])

    def close(self) -> None:
        self.client.close()


# ------------------------------------------------------------------- sinks


class _AtomicFile:
    """A text file written to ``PATH.part`` and renamed on success."""

    def __init__(self, path: str):
        self.path = path
        if path == "-":
            self.stream = sys.stdout
            self.part = None
        else:
            self.part = path + ".part"
            self.stream = open(self.part, "w", newline="", encoding="utf-8")

    def commit(self) -> None:
        if self.part is None:
            self.stream.flush()
            return
        self.stream.close()
        os.replace(self.part, self.path)

    def abort(self) -> None:
        if self.part is None:
            return
        self.stream.close()
        try:
            os.unlink(self.part)
        except OSError:
            pass


class CsvSink:
    """CSV in the same layout as ``/api/events?format=csv``."""

    def __init__(self, path: str, columns: Sequence[str],
                 comments: Sequence[str] = (), header: bool = True):
        self.file = _AtomicFile(path)
        for line in comments:
            self.file.stream.write("# {}\n".format(line))
        self.writer = csv.DictWriter(self.file.stream, fieldnames=list(columns),
                                     extrasaction="ignore", lineterminator="\n")
        if header:
            self.writer.writeheader()

    def write(self, rows: Sequence[Dict[str, Any]]) -> None:
        self.writer.writerows(rows)

    def close(self, summary: Dict[str, Any]) -> None:
        self.file.commit()

    def abort(self) -> None:
        self.file.abort()


class JsonSink:
    """A JSON document ``{"meta": ..., "events": [...], "summary": ...}``.

    ``meta`` and ``events`` match the ``/api/events`` envelope so the two are
    interchangeable to a reader.  ``summary`` is written last because the
    row count is not known until the walk ends.  Each event is on its own
    line, which keeps a multi-gigabyte file greppable.
    """

    def __init__(self, path: str, columns: Sequence[str],
                 meta: Dict[str, Any], indent: Optional[int] = None):
        self.file = _AtomicFile(path)
        self.columns = list(columns)
        self.indent = indent
        self.first = True
        out = self.file.stream
        out.write('{\n"meta": ')
        out.write(json.dumps(meta, indent=indent, default=str))
        out.write(',\n"events": [')

    def write(self, rows: Sequence[Dict[str, Any]]) -> None:
        out = self.file.stream
        for row in rows:
            record = {name: row.get(name) for name in self.columns}
            out.write("\n" if self.first else ",\n")
            self.first = False
            out.write(json.dumps(record, indent=self.indent, default=str))

    def close(self, summary: Dict[str, Any]) -> None:
        out = self.file.stream
        out.write("\n],\n" if not self.first else "],\n")
        out.write('"summary": ')
        out.write(json.dumps(summary, indent=self.indent, default=str))
        out.write("\n}\n")
        self.file.commit()

    def abort(self) -> None:
        self.file.abort()


# -------------------------------------------------------------------- walk


def dump_range(reader, sinks, start_nova: int, end_nova: int,
               window_seconds: float = DEFAULT_WINDOW,
               page_size: int = DEFAULT_PAGE_SIZE,
               progress=None) -> Tuple[int, int]:
    """Walk ``[start_nova, end_nova)`` and write every row to every sink.

    Returns ``(rows, requests)``.  Raises :class:`DumpError` if a window of
    one NOvA tick still exceeds *page_size*, which would mean more events
    share a single timestamp than a request may return.
    """
    full_window = max(1, int(window_seconds * TICKS_PER_SECOND))
    window = full_window
    cursor = start_nova
    rows_written = requests = 0

    while cursor < end_nova:
        upper = min(cursor + window, end_nova)
        rows, total = reader.fetch(cursor, upper, page_size)
        requests += 1
        if total > len(rows):
            # Too many for one request: narrow and re-read the same stretch
            # rather than paging by offset (see the module docstring).
            if upper - cursor <= 1:
                raise DumpError(
                    "{} events share timestamp nova:{}; raise --page-size "
                    "above {}".format(total, cursor, page_size))
            window = max(1, (upper - cursor) // 2)
            continue
        for sink in sinks:
            sink.write(rows)
        rows_written += len(rows)
        cursor = upper
        if progress is not None:
            progress(cursor, rows_written, requests)
        # Grow back after a dense stretch so a burst does not leave the rest
        # of a long range crawling in tiny windows.
        if window < full_window and total < page_size // 4:
            window = min(full_window, window * 2)

    return rows_written, requests


# ------------------------------------------------------------------- split


def split_range(start_nova: int, end_nova: int, unit: Optional[str],
                zone) -> List[Tuple[int, int, Optional[str]]]:
    """Cut ``[start_nova, end_nova)`` at calendar boundaries in *zone*.

    Returns ``(start, end, label)`` triples that tile the range exactly; the
    first and last are partial when the range does not begin or end on a
    boundary.  *unit* is ``"day"``, ``"hour"`` or ``None`` (one piece, no
    label).

    Boundaries are local midnights or local hours in *zone*, stepped in real
    time, so a daylight-saving changeover day is 23 or 25 hours long and the
    repeated autumn hour gets its own piece.  Hour labels carry the UTC
    offset outside UTC for exactly that reason: ``2026-11-01T01-0500`` and
    ``2026-11-01T01-0600`` are different hours.
    """
    if unit is None or end_nova <= start_nova:
        return [(start_nova, end_nova, None)]

    utc = zone.utcoffset(datetime(2000, 1, 1)) == timedelta(0) and \
        zone.utcoffset(datetime(2000, 7, 1)) == timedelta(0)
    local = nova_to_datetime(start_nova).astimezone(zone)
    if unit == "day":
        floor = datetime.combine(local.date(), dtime(0), tzinfo=zone)
    elif unit == "hour":
        floor = local.replace(minute=0, second=0, microsecond=0)
    else:
        raise ValueError("unknown split unit {!r}".format(unit))

    pieces: List[Tuple[int, int, Optional[str]]] = []
    lower = start_nova
    boundary = floor
    while lower < end_nova:
        if unit == "day":
            label = boundary.strftime("%Y-%m-%d")
            following = datetime.combine(boundary.date() + timedelta(days=1),
                                         dtime(0), tzinfo=zone)
        else:
            label = boundary.strftime("%Y-%m-%dT%H")
            if not utc:
                label += boundary.strftime("%z")
            # Step in UTC so the repeated autumn hour is not skipped.
            following = (boundary.astimezone(timezone.utc)
                         + timedelta(hours=1)).astimezone(zone)
        upper = min(datetime_to_nova(following), end_nova)
        if upper > lower:
            pieces.append((lower, upper, label))
        lower = max(lower, upper)
        boundary = following
    return pieces


def segment_path(template: str, label: Optional[str]) -> str:
    """The output path for one piece of a split dump.

    ``{start}`` in *template* is replaced by the piece's label, which allows
    a directory per day (``out/{start}/spills.csv``).  Otherwise the label
    goes before the extension: ``week.csv`` becomes ``week_2026-10-12.csv``.
    """
    if label is None:
        return template
    if "{start}" in template:
        return template.replace("{start}", label)
    root, ext = os.path.splitext(template)
    return "{}_{}{}".format(root, label, ext)


# --------------------------------------------------------------------- CLI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="darpa-spill-dump",
        description="Dump a time range of NOvA accelerator events to CSV "
                    "and/or JSON, from a local archive or a running server.",
        epilog=(
            "Write relative times with an equals sign -- --start=-6h, not\n"
            "--start -6h -- or argparse takes the value for an option.\n"
            "Quote hex signals in a shell: --signal '$74', or use 0x74.\n"
            "\n"
            "environment:\n"
            "  {}       server URL (as --server)\n"
            "  {}        bearer token (as --token)\n"
            "  {}    CA bundle (as --ca-bundle)\n"
            "  {}     1 to skip TLS verification (as --insecure)\n"
            "  {}_CONFIG, {}_STORAGE_PATH  archive location\n".format(
                ENV_SERVER, ENV_TOKEN, ENV_CA_BUNDLE, ENV_INSECURE,
                ENV_PREFIX.rstrip("_"), ENV_PREFIX.rstrip("_"))
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-V", "--version", action="store_true",
                        help="show version information and exit")

    source = parser.add_argument_group("data source (default: the archive "
                                       "named by the configuration)")
    source.add_argument("-c", "--config", metavar="FILE",
                        help="YAML configuration file (for the archive path)")
    source.add_argument("-d", "--database", metavar="FILE",
                        help="SQLite archive to read directly")
    source.add_argument("-s", "--server", metavar="URL",
                        help="read from a running server instead")
    source.add_argument("--token", metavar="TOKEN",
                        help="bearer ID token for a server with SSO enabled")
    source.add_argument("--ca-bundle", metavar="FILE",
                        help="CA certificate(s) to verify the server with")
    source.add_argument("-k", "--insecure", action="store_true", default=None,
                        help="do not verify the server's TLS certificate")

    selection = parser.add_argument_group("selection")
    selection.add_argument("--start", metavar="TIME",
                           help="start of the range, inclusive; default the "
                                "earliest event in the archive")
    selection.add_argument("--end", metavar="TIME",
                           help="end of the range, exclusive; default now. "
                                "A date covers the whole day")
    selection.add_argument("--signal", action="append", default=[],
                           metavar="SIG",
                           help="accelerator signal, e.g. '$74'; repeatable")
    selection.add_argument("--type", action="append", default=[],
                           metavar="TYPE", dest="types",
                           help="decoded spill type, by name or number; "
                                "repeatable")
    selection.add_argument("--source", action="append", default=[],
                           metavar="NAME", dest="sources",
                           help="only events from this TDU; repeatable or "
                                "comma-separated")
    selection.add_argument("--timezone", metavar="ZONE",
                           help="zone for times that carry none; default the "
                                "configured (or server's) query timezone")

    output = parser.add_argument_group("output (at least one of --csv/--json)")
    output.add_argument("--csv", metavar="FILE",
                        help="write CSV here; '-' for stdout")
    output.add_argument("--json", metavar="FILE",
                        help="write JSON here; '-' for stdout")
    output.add_argument("--columns", metavar="LIST",
                        help="comma-separated subset of columns, for both "
                             "files")
    output.add_argument("--no-header", action="store_true",
                        help="omit the CSV column-name row")
    output.add_argument("--no-comments", action="store_true",
                        help="omit the '#' provenance lines at the top of "
                             "the CSV")
    output.add_argument("--indent", type=int, metavar="N",
                        help="pretty-print JSON events with this indent")
    output.add_argument("--split", choices=["day", "hour"],
                        help="write one file per day or hour, cut at "
                             "boundaries in --timezone; FILE gains a _LABEL "
                             "suffix, or replaces {start} if it contains it")
    output.add_argument("--skip-empty", action="store_true",
                        help="with --split, write no file for a piece "
                             "holding no events")

    tuning = parser.add_argument_group("tuning")
    tuning.add_argument("--window", type=float, default=DEFAULT_WINDOW,
                        metavar="SECONDS",
                        help="seconds of archive time per request "
                             "(default: %(default)s)")
    tuning.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE,
                        metavar="N",
                        help="maximum rows per request; a fuller window is "
                             "halved (default: %(default)s)")
    tuning.add_argument("--timeout", type=float, default=120.0,
                        metavar="SECONDS",
                        help="per-request HTTP timeout (default: %(default)s)")
    tuning.add_argument("-q", "--quiet", action="store_true",
                        help="print nothing but errors")
    return parser


def _split(values: Sequence[str]) -> List[str]:
    names: List[str] = []
    for text in values:
        names.extend(t.strip() for t in text.split(",") if t.strip())
    return list(dict.fromkeys(names))


def _truthy(text: Optional[str]) -> bool:
    return (text or "").strip().lower() in ("1", "true", "yes", "on")


def _err(message: str) -> None:
    print("darpa-spill-dump: error: {}".format(message), file=sys.stderr)


def _open_reader(args, environ) -> Tuple[Any, str]:
    """Build the reader named by *args*; return it with the default zone."""
    server = args.server or environ.get(ENV_SERVER)
    if server:
        insecure = args.insecure if args.insecure is not None \
            else _truthy(environ.get(ENV_INSECURE))
        ca_bundle = args.ca_bundle or environ.get(ENV_CA_BUNDLE)
        verify: Any = False if insecure else (ca_bundle or True)
        if insecure:
            import warnings
            warnings.filterwarnings("ignore", message="Unverified HTTPS")
        reader = RemoteReader(
            server, args.signal, args.types, _split(args.sources),
            token=args.token or environ.get(ENV_TOKEN),
            verify=verify, timeout=args.timeout,
        )
        return reader, reader.server_timezone()

    # Validate the selection locally before touching the archive.
    try:
        signals = [parse_signal(s) for s in args.signal]
        spill_types = [parse_spill_type(t) for t in args.types]
    except ValueError as exc:
        raise DumpError("{}; run darpa-spill-query --list-signals".format(exc))
    signal_codes = [s.code for s in signals]
    spill_types.extend(s.spill_type for s in signals)
    if signal_codes:
        # Records stored without a raw event word match on type alone, as
        # in darpa-spill-query and the API.
        signal_codes.append(UNKNOWN_SIGNAL)

    database = args.database
    legacy_source = None
    zone = "UTC"
    if not database:
        try:
            config = load_config(
                argv=(["-c", args.config] if args.config else []),
                environ=environ)
        except ConfigError as exc:
            raise DumpError(str(exc))
        database = config.storage.path
        zone = config.query.timezone
        legacy_source = config.tdu.resolved_sources()[0].name
    if not os.path.exists(database):
        raise DumpError("no archive at {}".format(database))
    try:
        store = SpillStore(database, legacy_source=legacy_source)
    except StoreError as exc:
        raise DumpError(str(exc))

    sources = _split(args.sources)
    if sources:
        known = store.source_names()
        unknown = [name for name in sources if name not in known]
        if unknown:
            store.close()
            raise DumpError("no events from source(s) {} in {}; this archive "
                            "has: {}".format(", ".join(unknown), database,
                                             ", ".join(known) or "(none)"))
    reader = LocalReader(store, list(dict.fromkeys(spill_types)),
                         list(dict.fromkeys(signal_codes)), sources)
    return reader, zone


def _progress_printer(start_nova: int, end_nova: int, expected: int,
                      stream) -> Any:
    started = time.monotonic()
    state: Dict[str, Any] = {"last": 0.0, "shown": None}

    def report(cursor: int, rows: int, requests: int, final: bool = False):
        now = time.monotonic()
        if not final and now - state["last"] < 2.0:
            return
        if (cursor, rows) == state["shown"]:
            return
        state["last"] = now
        state["shown"] = (cursor, rows)
        span = max(1, end_nova - start_nova)
        fraction = min(1.0, (cursor - start_nova) / span)
        rate = rows / max(1e-6, now - started)
        print("[{:5.1f}%] {}  {:>10,}/{:,} rows  {:>6} req  {:>8,.0f} rows/s"
              .format(100 * fraction, convert(cursor).utc[:19] + "Z",
                      rows, expected, requests, rate),
              file=stream, flush=True)
    return report


def main(argv: Optional[Sequence[str]] = None,
         environ: Optional[Dict[str, str]] = None) -> int:
    environ = dict(os.environ if environ is None else environ)
    args = build_parser().parse_args(argv)

    if args.version:
        print("darpa-spill-dump {}".format(__version__))
        return 0
    if not args.csv and not args.json:
        _err("nothing to write; give --csv FILE and/or --json FILE")
        return 2
    if args.csv == "-" and args.json == "-":
        _err("--csv and --json cannot both write to stdout")
        return 2
    if args.split and "-" in (args.csv, args.json):
        _err("--split writes several files; it cannot write to stdout")
        return 2
    if args.skip_empty and not args.split:
        _err("--skip-empty only applies with --split")
        return 2
    if args.page_size < 1 or args.window <= 0:
        _err("--page-size and --window must be positive")
        return 2

    columns = list(COLUMNS)
    if args.columns:
        columns = [c.strip() for c in args.columns.split(",") if c.strip()]
        unknown = [c for c in columns if c not in COLUMNS]
        if unknown:
            _err("unknown column(s): {}\navailable: {}".format(
                ", ".join(unknown), ", ".join(COLUMNS)))
            return 2

    # Progress and the summary go to stderr, so '-' sinks stay clean.
    quiet = args.quiet
    log = sys.stderr

    try:
        reader, default_zone = _open_reader(args, environ)
    except DumpError as exc:
        _err(str(exc))
        return 1 if (args.server or environ.get(ENV_SERVER)) else 2

    sinks: List[Any] = []
    written: List[str] = []
    try:
        # A server refuses a limit above its query.max_limit.
        ceiling = reader.max_limit() if hasattr(reader, "max_limit") else None
        if ceiling and args.page_size > ceiling:
            args.page_size = ceiling

        try:
            zone = get_timezone(args.timezone or default_zone)
            req_start, req_end = parse_range(args.start, args.end, tz=zone)
        except NovaTimeError as exc:
            _err(str(exc))
            return 2

        first, last = reader.extent()
        # Clamp the walk to what the archive held when the dump began: it
        # skips the empty stretch before the first event and fixes the
        # snapshot, so rows arriving during a long dump are not half-included.
        snap_start = req_start if first is None else max(req_start, first)
        snap_end = req_start if last is None else min(req_end, last + 1)
        snap_end = max(snap_end, snap_start)

        pieces = split_range(req_start, req_end, args.split, zone)
        if args.split:
            # Pieces wholly after the snapshot have not happened yet.
            pieces = [p for p in pieces if p[0] < snap_end]

        _, expected_total = (reader.fetch(snap_start, snap_end, 1)
                             if snap_end > snap_start else (None, 0))
        if not quiet:
            print("darpa-spill-dump: {} -> {} from {}: {:,} events{}".format(
                convert(req_start).utc, convert(req_end).utc,
                reader.description, expected_total,
                ", {} {} file(s)".format(len(pieces), args.split)
                if args.split else ""), file=log, flush=True)

        base_meta = {
            "generator": "darpa-spill-dump {}".format(__version__),
            "origin": reader.description,
            "selection": (list(args.signal) + list(args.types))
                         or ["(all signals)"],
            "sources": _split(args.sources) or ["(all sources)"],
            "timezone": str(args.timezone or default_zone),
            "order": "asc",
            "columns": columns,
        }
        printer = None if quiet else _progress_printer(
            snap_start, snap_end, expected_total, log)
        started = time.monotonic()
        rows = requests = 0
        mismatched = []

        for seg_start, seg_end, label in pieces:
            walk_start = max(seg_start, snap_start)
            walk_end = min(seg_end, snap_end)
            expected = 0
            if walk_end > walk_start:
                if len(pieces) == 1:
                    expected = expected_total
                else:
                    _, expected = reader.fetch(walk_start, walk_end, 1)
            if args.skip_empty and expected == 0:
                continue

            generated = datetime.now(timezone.utc).isoformat()
            meta = dict(base_meta, generated=generated, range={
                "start": convert(seg_start).as_dict(),
                "end": convert(seg_end).as_dict(),
                "note": "half-open: start <= t < end",
            }, expected=expected)
            if label is not None:
                meta["piece"] = {"split": args.split, "label": label}

            paths = []
            if args.csv:
                path = segment_path(args.csv, label)
                comments = [] if args.no_comments else [
                    "darpa-spill-dump {} generated {}".format(
                        __version__, generated),
                    "origin: {}".format(reader.description),
                    "range: {} <= t < {}".format(convert(seg_start).utc,
                                                 convert(seg_end).utc),
                    "selection: {}; sources: {}".format(
                        ", ".join(meta["selection"]),
                        ", ".join(meta["sources"])),
                ]
                _make_parent(path)
                sinks.append(CsvSink(path, columns, comments,
                                     header=not args.no_header))
                paths.append(path)
            if args.json:
                path = segment_path(args.json, label)
                _make_parent(path)
                sinks.append(JsonSink(path, columns, meta, indent=args.indent))
                paths.append(path)

            def progress(cursor, seg_rows, seg_requests, _r=rows, _q=requests):
                printer(cursor, _r + seg_rows, _q + seg_requests)

            seg_started = time.monotonic()
            got = got_requests = 0
            if walk_end > walk_start:
                got, got_requests = dump_range(
                    reader, sinks, walk_start, walk_end,
                    window_seconds=args.window, page_size=args.page_size,
                    progress=progress if printer else None)
            summary = {
                "returned": got,
                "expected": expected,
                "complete": got == expected,
                "requests": got_requests,
                "seconds": round(time.monotonic() - seg_started, 3),
            }
            for sink in sinks:
                sink.close(summary)
            sinks = []
            rows += got
            requests += got_requests
            if got != expected:
                mismatched.append((label, got, expected))
            written.extend(p for p in paths if p != "-")
            if args.split and not quiet:
                print("darpa-spill-dump: {}  {:>10,} events  {}".format(
                    label, got, ", ".join(paths)), file=log, flush=True)
        seconds = time.monotonic() - started
    except DumpError as exc:
        for sink in sinks:
            sink.abort()
        _err(str(exc))
        _report_kept(written)
        return 1
    except KeyboardInterrupt:
        for sink in sinks:
            sink.abort()
        _err("interrupted")
        _report_kept(written)
        return 130
    finally:
        reader.close()

    if not quiet:
        if printer is not None:
            printer(snap_end, rows, requests, final=True)
        shown = written if len(written) <= 4 else written[:2] + [
            "... ({} files)".format(len(written))]
        print("darpa-spill-dump: wrote {:,} events in {:.1f} s{}".format(
            rows, seconds, " to " + ", ".join(shown) if shown else ""),
            file=log)
    for label, got, expected in mismatched:
        # Rows inserted or pruned mid-run inside the range (a backfill, or
        # retention) change the count; the files are still self-consistent.
        print("darpa-spill-dump: warning: {}wrote {:,} events but {:,} "
              "matched at start; the archive changed during the dump".format(
                  label + ": " if label else "", got, expected),
              file=sys.stderr)
    return 3 if mismatched else 0


def _make_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if path != "-" and parent:
        os.makedirs(parent, exist_ok=True)


def _report_kept(written: Sequence[str]) -> None:
    if written:
        print("darpa-spill-dump: {} complete file(s) kept: {}".format(
            len(written), ", ".join(written)), file=sys.stderr)
    else:
        print("darpa-spill-dump: no output files written", file=sys.stderr)

if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
