"""Command-line query tool (``darpa-spill-query``).

Answers the same questions as ``/api/events`` without a browser, reading
either a local archive file or a running server.  It exists because the
people who need this data most often are already in a terminal on the gateway
node, and because a shell pipeline is the natural home for a CSV.

    darpa-spill-query --signal '$74' --start 2026-07-01 --end today
    darpa-spill-query --signal '$8f' --start 09:15 --end 11:34 --format csv
    darpa-spill-query --source tdu-near-master-ppc-02 --last
    darpa-spill-query --server http://localhost:8080 --last

Quote ``$74`` in a shell, or write it as ``0x74`` or ``74``: an unquoted
``$74`` is a shell variable and expands to nothing.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional, Sequence

from . import __version__
from .config import ConfigError, load_config
from .formats import COLUMNS, event_row, iter_csv, iter_json
from .novatime import NovaTimeError, convert, get_timezone, parse_range
from .signals import SIGNALS, parse_signal, parse_spill_type, signals_for_type
from .storage import SpillStore, StoreError

__all__ = ["main"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="darpa-spill-query",
        description="Query NOvA accelerator event timestamps.",
        epilog=(
            "Quote hex signals in a shell: --signal '$74', or use 0x74 / 74. "
            "Run with --list-signals to see every signal this tool accepts."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-V", "--version", action="store_true",
                        help="show version information and exit")
    parser.add_argument("--list-signals", action="store_true",
                        help="list the accelerator signals and exit")

    source = parser.add_argument_group("data source")
    source.add_argument("-c", "--config", metavar="FILE",
                        help="YAML configuration file (for the archive path)")
    source.add_argument("--env-file", metavar="FILE",
                        help=".env file to read (default ./.env if present)")
    source.add_argument("-d", "--database", metavar="FILE",
                        help="SQLite archive to read directly")
    source.add_argument("-s", "--server", metavar="URL",
                        help="query a running server instead of a local file")

    selection = parser.add_argument_group("selection")
    selection.add_argument("--start", metavar="TIME",
                           help="start of the range, inclusive "
                                "(e.g. 2026-07-01, 09:15, -2h)")
    selection.add_argument("--end", metavar="TIME",
                           help="end of the range, exclusive; a date covers "
                                "the whole day")
    selection.add_argument("--signal", action="append", metavar="SIG",
                           help="accelerator signal, e.g. '$74'; repeatable")
    selection.add_argument("--type", action="append", metavar="TYPE",
                           dest="types",
                           help="decoded spill type, by name or number; repeatable")
    selection.add_argument("--source", action="append", metavar="NAME",
                           dest="sources",
                           help="only events from this TDU source; repeatable "
                                "or comma-separated. Default: every source")
    selection.add_argument("--last", action="store_true",
                           help="show only the most recent matching event")
    selection.add_argument("--limit", type=int, metavar="N",
                           help="maximum rows to return")
    selection.add_argument("--desc", action="store_true",
                           help="newest first")
    selection.add_argument("--timezone", metavar="ZONE",
                           help="zone for times that carry none")

    output = parser.add_argument_group("output")
    output.add_argument("-f", "--format", choices=["table", "csv", "json"],
                        default="table", help="output format (default: table)")
    output.add_argument("--columns", metavar="LIST",
                        help="comma-separated subset of columns")
    output.add_argument("-o", "--output", metavar="FILE",
                        help="write to a file instead of stdout")
    output.add_argument("--no-header", action="store_true",
                        help="omit the CSV header row")

    return parser


def _list_signals() -> int:
    print("{:<6} {:<8} {:<20} {:<18} {}".format(
        "HEX", "CARRIER", "NAME", "TYPE", "DESCRIPTION"))
    print("-" * 100)
    for signal in SIGNALS:
        print("{:<6} {:<8} {:<20} {:<18} {}".format(
            signal.hex, signal.carrier_name, signal.name,
            signal.spill_type.name, signal.description))
    print()
    print("Types marked below map to more than one signal, so a stored record")
    print("carrying only the type cannot be narrowed to one of them:")
    for signal in SIGNALS:
        options = signals_for_type(signal.spill_type)
        if len(options) > 1:
            print("  {:<18} <- {}".format(
                signal.spill_type.name, " or ".join(s.hex for s in options)))
            break
    return 0


def _print_table(events, stream) -> None:
    """Render events as an aligned text table."""
    header = "{:<20} {:<28} {:<8} {:<18} {:>26} {:>12}  {}".format(
        "NOVA TIME", "UTC", "SIGNAL", "TYPE", "GPS TIME", "DELTA", "SOURCE")
    print(header, file=stream)
    print("-" * len(header), file=stream)

    count = 0
    for event in events:
        row = event_row(event)
        print("{:<20} {:<28} {:<8} {:<18} {:>26} {:>12}  {}".format(
            row["nova_time"],
            row["utc"][:27],
            row["signal"] or "-",
            row["spill_type_name"],
            row["gps"],
            row["delta"] if row["delta"] is not None else "-",
            row["source"],
        ), file=stream)
        count += 1

    print("-" * len(header), file=stream)
    print("{} event(s)".format(count), file=stream)


def _query_server(args, columns) -> int:
    """Fetch results from a running server over HTTP."""
    try:
        import httpx
    except ImportError:
        print("error: querying a server needs httpx; run ./bootstrap.sh",
              file=sys.stderr)
        return 1

    params: List[tuple] = []
    if args.start:
        params.append(("start", args.start))
    if args.end:
        params.append(("end", args.end))
    for signal in args.signal or []:
        params.append(("signal", signal))
    for spill_type in args.types or []:
        params.append(("type", spill_type))
    for name in _source_names(args):
        params.append(("source", name))
    if args.limit:
        params.append(("limit", str(args.limit)))
    if args.timezone:
        params.append(("tz", args.timezone))
    if args.desc:
        params.append(("order", "desc"))
    params.append(("format", "csv" if args.format == "csv" else "json"))
    if columns and args.format == "csv":
        params.append(("columns", ",".join(columns)))

    url = args.server.rstrip("/") + ("/api/latest" if args.last else "/api/events")
    try:
        response = httpx.get(url, params=params, timeout=60.0)
    except httpx.HTTPError as exc:
        print("error: cannot reach {}: {}".format(url, exc), file=sys.stderr)
        return 1

    if response.status_code >= 400:
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        print("error: server returned {}: {}".format(
            response.status_code, detail), file=sys.stderr)
        return 1

    stream = open(args.output, "w") if args.output else sys.stdout
    try:
        stream.write(response.text)
        if not response.text.endswith("\n"):
            stream.write("\n")
    finally:
        if args.output:
            stream.close()
    return 0


def _source_names(args) -> List[str]:
    names: List[str] = []
    for text in args.sources or []:
        names.extend(t.strip() for t in text.split(",") if t.strip())
    return list(dict.fromkeys(names))


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print("darpa-spill-query {}".format(__version__))
        return 0

    if args.list_signals:
        return _list_signals()

    columns = None
    if args.columns:
        columns = [name.strip() for name in args.columns.split(",") if name.strip()]
        unknown = [name for name in columns if name not in COLUMNS]
        if unknown:
            print("error: unknown column(s): {}\navailable: {}".format(
                ", ".join(unknown), ", ".join(COLUMNS)), file=sys.stderr)
            return 2

    if args.server:
        return _query_server(args, columns)

    # -- local archive ----------------------------------------------------
    database = args.database
    legacy_source = None
    if not database:
        try:
            config = load_config(argv=(
            (["-c", args.config] if args.config else [])
            + (["--env-file", args.env_file] if args.env_file else [])))
        except ConfigError as exc:
            print("error: {}".format(exc), file=sys.stderr)
            return 2
        database = config.storage.path
        timezone_name = args.timezone or config.query.timezone
        default_limit = config.query.default_limit
        legacy_source = config.tdu.resolved_sources()[0].name
    else:
        timezone_name = args.timezone or "UTC"
        default_limit = 10000

    try:
        zone = get_timezone(timezone_name)
    except NovaTimeError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2

    try:
        spill_types = [parse_spill_type(t) for t in (args.types or [])]
        signal_objects = [parse_signal(s) for s in (args.signal or [])]
    except ValueError as exc:
        print("error: {}\nrun --list-signals to see what is accepted".format(exc),
              file=sys.stderr)
        return 2

    signal_codes = [s.code for s in signal_objects]
    spill_types.extend(s.spill_type for s in signal_objects)
    if signal_codes:
        from .storage import UNKNOWN_SIGNAL
        signal_codes.append(UNKNOWN_SIGNAL)

    try:
        start_nova, end_nova = parse_range(args.start, args.end, tz=zone)
    except NovaTimeError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2

    try:
        store = SpillStore(database, legacy_source=legacy_source)
    except StoreError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2
    try:
        sources = _source_names(args)
        if sources:
            known = store.source_names()
            unknown = [name for name in sources if name not in known]
            if unknown:
                print("error: no events from source(s) {} in {}\n"
                      "sources in this archive: {}".format(
                          ", ".join(unknown), database,
                          ", ".join(known) or "(none)"), file=sys.stderr)
                return 2
        if args.last:
            event = store.latest(
                spill_types[0] if len(spill_types) == 1 else None,
                sources or None,
            )
            events = [event] if event is not None else []
        else:
            page = store.query(
                start_nova=start_nova,
                end_nova=end_nova,
                spill_types=list(dict.fromkeys(spill_types)) or None,
                signal_codes=list(dict.fromkeys(signal_codes)) or None,
                limit=args.limit or default_limit,
                descending=args.desc,
                sources=sources or None,
            )
            events = page.events
            if page.truncated:
                print(
                    "note: {} of {} matching events shown; raise --limit to "
                    "see more".format(len(events), page.total),
                    file=sys.stderr,
                )

        stream = open(args.output, "w") if args.output else sys.stdout
        try:
            if args.format == "csv":
                for chunk in iter_csv(events, columns=columns,
                                      header=not args.no_header):
                    stream.write(chunk)
            elif args.format == "json":
                meta = {
                    "range": {
                        "start": convert(start_nova).as_dict(),
                        "end": convert(end_nova).as_dict(),
                    },
                    "database": database,
                    "sources": sources or ["(all sources)"],
                }
                for chunk in iter_json(events, meta=meta, indent=2):
                    stream.write(chunk)
                stream.write("\n")
            else:
                _print_table(events, stream)
        finally:
            if args.output:
                stream.close()
    finally:
        store.close()

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
