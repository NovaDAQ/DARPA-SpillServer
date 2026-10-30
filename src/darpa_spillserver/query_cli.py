"""!
@file src/darpa_spillserver/query_cli.py
@brief ``darpa-spill-query``: query events from a local archive or a running server.

Command-line query tool (``darpa-spill-query``).

Answers the same questions as ``/api/events`` without a browser, reading
either a local archive file or a running server.  It exists because the
people who need this data most often are already in a terminal on the gateway
node, and because a shell pipeline is the natural home for a CSV.

    darpa-spill-query --signal '$74' --start 2026-07-01 --end today
    darpa-spill-query --signal '$8f' --start 09:15 --end 11:34 --format csv
    darpa-spill-query --source tdu-near-master-ppc-02 --last
    darpa-spill-query --server http://localhost:8080 --last
    darpa-spill-query -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --last

Quote ``$74`` in a shell, or write it as ``0x74`` or ``74``: an unquoted
``$74`` is a shell variable and expands to nothing.

Two back ends with deliberately parallel semantics: ``--server`` forwards the
selection as API query parameters; otherwise the archive is opened directly
(path from ``--database``, or from the merged configuration, which then also
supplies the default time zone and row limit).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List, Optional, Sequence

from . import __version__
from .config import ConfigError, load_config
from .formats import COLUMNS, event_row, iter_csv, iter_json
from .novatime import NovaTimeError, convert, get_timezone, parse_range
from .signals import SIGNALS, signals_for_type, resolve_selection
from .storage import SpillStore, StoreError

__all__ = ["main"]


def build_parser() -> argparse.ArgumentParser:
    """!
    @brief Build the ``darpa-spill-query`` command-line parser.
    @return the parser.
    """
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
    source.add_argument("-d", "--database", metavar="FILE",
                        help="SQLite archive to read directly")
    source.add_argument("-s", "--server", metavar="URL",
                        help="query a running server instead of a local file")
    source.add_argument("--ca-bundle", metavar="FILE",
                        help="CA certificate(s) to verify the server with")
    source.add_argument("-k", "--insecure", action="store_true",
                        help="do not verify the server's TLS certificate")

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
    """!
    @brief Print the signal registry and an example of an ambiguous type.

    The ambiguity footer stops at the first many-to-one type it meets
    (``break``), so it shows one example, not every such type.

    @return exit status 0.
    """
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


def _print_table(rows, stream) -> None:
    """!
    @brief Render rows shaped by :func:`event_row` as an aligned text table.
    @param rows iterable of row dicts (consumed once; may be a generator).
    @param stream file-like object to write to.
    """
    header = "{:<20} {:<28} {:<8} {:<18} {:>26} {:>12}  {}".format(
        "NOVA TIME", "UTC", "SIGNAL", "TYPE", "GPS TIME", "DELTA", "SOURCE")
    print(header, file=stream)
    print("-" * len(header), file=stream)

    count = 0
    for row in rows:
        print("{:<20} {:<28} {:<8} {:<18} {:>26} {:>12}  {}".format(
            row["nova_time"],
            row["utc"][:27],
            # Empty signal = record stored without a raw event word.
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
    """!
    @brief Fetch results from a running server over HTTP.

    Maps the CLI selection onto ``/api/events`` query parameters; selection
    semantics are then the server's.  ``--last`` is the same request with
    ``limit=1&order=desc``, so ranges and every filter still apply.
    ``table`` output is rendered locally from the JSON response.

    @param args parsed command line.
    @param columns validated column subset, or None.
    @return exit status: 0 success, 1 on transport or HTTP error.
    """
    try:
        import httpx
    except ImportError:
        print("error: querying a server needs httpx; run ./bootstrap.sh",
              file=sys.stderr)
        return 1

    # List of pairs, not a dict, so repeated parameters survive.
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
    if args.limit and not args.last:
        params.append(("limit", str(args.limit)))
    if args.timezone:
        params.append(("tz", args.timezone))
    if args.desc and not args.last:
        params.append(("order", "desc"))
    # The server has no "table"; ask for JSON and render it here.
    params.append(("format", "csv" if args.format == "csv" else "json"))
    if columns and args.format == "csv":
        params.append(("columns", ",".join(columns)))

    # --last goes through /api/events so the JSON envelope, CSV output, range
    # and every selection filter behave exactly as for an ordinary query.
    if args.last:
        params.extend([("limit", "1"), ("order", "desc")])
    url = args.server.rstrip("/") + "/api/events"
    # httpx verify: False, a CA bundle path, or True for the system store.
    verify = False if args.insecure else (args.ca_bundle or True)
    try:
        response = httpx.get(url, params=params, timeout=60.0, verify=verify)
    except httpx.HTTPError as exc:
        print("error: cannot reach {}: {}".format(url, exc), file=sys.stderr)
        if "CERTIFICATE_VERIFY_FAILED" in str(exc):
            print("pass --ca-bundle FILE with the server's certificate, or "
                  "--insecure to skip verification", file=sys.stderr)
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
        if args.format == "table":
            # The server has no table format; render its JSON rows here, the
            # same way a local query is rendered.
            body = response.json()
            if "events" in body:
                rows = body["events"]
                meta = body.get("meta", {})
                if meta.get("truncated") and not args.last:
                    print("note: {} of {} matching events shown; raise "
                          "--limit to see more".format(
                              meta.get("returned"), meta.get("total")),
                          file=sys.stderr)
            else:
                # /api/latest shape: {"event": {...}}.
                rows = [body["event"]] if body.get("event") else []
            _print_table(rows, stream)
        else:
            text = response.text
            if args.last and args.format == "json":
                body = response.json()
                rows = body.get("events", [])
                text = json.dumps({"event": rows[0] if rows else body.get("event")})
            if args.format == "csv" and args.no_header:
                # The API always sends the column-name row; drop the first
                # line that is not a '#' comment.
                lines = text.splitlines(True)
                for index, line in enumerate(lines):
                    if not line.startswith("#"):
                        del lines[index]
                        break
                text = "".join(lines)
            stream.write(text)
            if text and not text.endswith("\n"):
                stream.write("\n")
    finally:
        if args.output:
            stream.close()
    return 0


def _source_names(args) -> List[str]:
    """!
    @brief Flatten ``--source`` values (repeatable, comma-separated).
    @param args parsed command line.
    @return de-duplicated names in first-seen order.
    """
    names: List[str] = []
    for text in args.sources or []:
        names.extend(t.strip() for t in text.split(",") if t.strip())
    return list(dict.fromkeys(names))


def main(argv: Optional[Sequence[str]] = None) -> int:
    """!
    @brief Entry point for ``darpa-spill-query``.
    @param argv command line (default ``sys.argv[1:]``).
    @return exit status; 141 (128 + SIGPIPE) when the output pipe closes early.
    """
    try:
        return _main(argv)
    except BrokenPipeError:
        # The reader went away (| head): stop quietly, as cat would, and keep
        # the interpreter's final flush from raising again.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        return 141


def _main(argv: Optional[Sequence[str]] = None) -> int:
    """!
    @brief The body of main(), without the broken-pipe handling.
    @param argv command line.
    @return exit status: 0 success, 1 server error, 2 usage/configuration error.
    """
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print("darpa-spill-query {}".format(__version__))
        return 0

    if args.list_signals:
        return _list_signals()

    # Validate --columns up front so both back ends reject the same input.
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
    # With --database the configuration is not loaded at all: zone defaults
    # to UTC and limit to 10000, regardless of any YAML/env settings.
    database = args.database
    legacy_source = None
    if not database:
        try:
            config = load_config(argv=(["-c", args.config] if args.config else []))
        except ConfigError as exc:
            print("error: {}".format(exc), file=sys.stderr)
            return 2
        database = config.storage.path
        timezone_name = args.timezone or config.query.timezone
        default_limit = config.query.default_limit
        # Name given to rows of a pre-multi-source (version 1) archive.
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
        spill_types, signal_codes, _, explicit_types = resolve_selection(args.signal, args.types)
    except ValueError as exc:
        print("error: {}\nrun --list-signals to see what is accepted".format(exc), file=sys.stderr)
        return 2

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
            # Validated against the archive, not the configuration: a local
            # file may hold sources this host does not configure.
            known = store.source_names()
            unknown = [name for name in sources if name not in known]
            if unknown:
                print("error: no events from source(s) {} in {}\n"
                      "sources in this archive: {}".format(
                          ", ".join(unknown), database,
                          ", ".join(known) or "(none)"), file=sys.stderr)
                return 2
        page = store.query(
            start_nova=start_nova, end_nova=end_nova,
            spill_types=spill_types, signal_codes=signal_codes,
            unrestricted_types=explicit_types,
            limit=1 if args.last else (args.limit or default_limit),
            descending=args.last or args.desc, sources=sources or None,
            count_total=not args.last)
        events = page.events
        if not args.last:
            if page.truncated:
                # To stderr, so a CSV on stdout stays clean.
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
                _print_table((event_row(e) for e in events), stream)
        finally:
            if args.output:
                stream.close()
    finally:
        store.close()

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
