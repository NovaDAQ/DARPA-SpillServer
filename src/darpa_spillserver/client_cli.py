"""``darpa-spill-client`` --- drive a running server's API from the shell.

Where ``darpa-spill-query`` reads an archive file directly, this program talks
to a server over HTTP, so it works from any host that can reach one and needs
no access to the database.  Every call goes through
:class:`darpa_spillserver.client.SpillClient`.

The commands, options and output rules are specified in ``docs/CLIENT.md``;
the C++ ``darpa-spill-client-cpp`` implements the same specification, and the
test suite checks that both write identical output.
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from typing import IO, Any, Dict, List, Optional, Sequence

from . import __version__
from .client import (
    Selection,
    SpillClient,
    SpillConnectionError,
    SpillHTTPError,
    load_client_config,
)
from .config import ConfigError

__all__ = ["main", "build_parser", "cell", "flatten", "render_table", "render_csv"]

#: Columns requested for ``events`` in table form when ``--columns`` is not
#: given: enough to identify each event, narrow enough for a terminal.
DEFAULT_EVENT_COLUMNS = ("utc_string,gps_week,gps_tow_exact,signal,"
                         "spill_type_name,event_number,source")

#: Columns shown by the list commands in table and CSV form.
TABLE_COLUMNS = {
    "sources": ("sources", ["name", "base_url", "enabled", "overridden", "events"]),
    "signals": ("signals", ["hex", "name", "spill_type_name", "description"]),
    "types": ("types", ["value", "name", "ambiguous", "signals"]),
}

EXIT_OK, EXIT_HTTP, EXIT_USAGE, EXIT_CONNECT = 0, 1, 2, 3


# --------------------------------------------------------------- rendering


def cell(value: Any) -> str:
    """Render one JSON value as text, by the rules in docs/CLIENT.md."""
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        text = "%.6f" % value
        return text.rstrip("0").rstrip(".") if "." in text else text
    if isinstance(value, list):
        return ",".join(cell(item) for item in value)
    if isinstance(value, dict):
        return "{}" if not value else ",".join(
            "{}={}".format(k, cell(v)) for k, v in value.items())
    return str(value)


def flatten(value: Any, prefix: str = "") -> List[str]:
    """``key: value`` lines for every leaf of *value*, in document order."""
    if isinstance(value, dict):
        if not value:
            return ["{}: {{}}".format(prefix)] if prefix else []
        lines: List[str] = []
        for key, item in value.items():
            lines += flatten(item, "{}.{}".format(prefix, key) if prefix else str(key))
        return lines
    if isinstance(value, list):
        if not value:
            return ["{}: []".format(prefix)]
        lines = []
        for index, item in enumerate(value):
            lines += flatten(item, "{}.{}".format(prefix, index) if prefix else str(index))
        return lines
    return ["{}: {}".format(prefix, cell(value))]


def render_table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    """Left-aligned columns separated by two spaces, trailing space trimmed."""
    widths = [len(h) for h in header]
    for row in rows:
        for index, text in enumerate(row):
            widths[index] = max(widths[index], len(text))
    lines = []
    for row in [list(header)] + [list(r) for r in rows]:
        lines.append("  ".join(text.ljust(widths[i])
                               for i, text in enumerate(row)).rstrip())
    return "\n".join(lines) + "\n"


def _csv_field(text: str) -> str:
    if any(c in text for c in ',"\n'):
        return '"' + text.replace('"', '""') + '"'
    return text


def render_csv(header: Sequence[str], rows: Sequence[Sequence[str]]) -> str:
    return "".join(",".join(_csv_field(t) for t in row) + "\n"
                   for row in [list(header)] + [list(r) for r in rows])


def _rows(records: Sequence[Dict[str, Any]], columns: Sequence[str]) -> List[List[str]]:
    return [[cell(record.get(column)) for column in columns] for record in records]


def _table_from_csv(body: str, err: IO[str]) -> str:
    """Lay the server's CSV out as a table, passing its warnings to *err*."""
    data_lines = []
    for line in body.splitlines(keepends=True):
        if line.startswith("#"):
            comment = line[1:].strip()
            if comment.startswith("WARNING:"):
                err.write("warning: {}\n".format(comment[len("WARNING:"):].strip()))
            continue
        data_lines.append(line)
    rows = list(csv.reader(io.StringIO("".join(data_lines))))
    if not rows:
        return ""
    return render_table(rows[0], rows[1:])


# ------------------------------------------------------------------ parser


def _selection_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start", metavar="TIME", help="start of the range, inclusive")
    parser.add_argument("--end", metavar="TIME", help="end of the range, exclusive")
    parser.add_argument("--signal", action="append", default=[], metavar="SIG",
                        help="accelerator signal such as $74; repeatable")
    parser.add_argument("--type", action="append", default=[], metavar="TYPE",
                        help="decoded spill type; repeatable")
    parser.add_argument("--source", action="append", default=[], metavar="NAME",
                        help="source (TDU) name; repeatable")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="darpa-spill-client",
        description="Query and administer a DARPA Spill Information Server "
                    "over its HTTP API.",
        epilog="Settings: command line > environment > .env > config file > "
               "defaults. See darpa-spill-client(1).",
    )
    parser.add_argument("-c", "--config", metavar="FILE", help="YAML configuration file")
    parser.add_argument("--env-file", metavar="FILE", help=".env file to read")
    parser.add_argument("-u", "--url", help="server base URL")
    parser.add_argument("-t", "--timeout", type=float, metavar="SECONDS",
                        help="per-request timeout")
    parser.add_argument("--admin-token", metavar="TOKEN", help="admin token")
    parser.add_argument("--admin-token-file", metavar="FILE",
                        help="file holding the admin token")
    parser.add_argument("--ca-file", metavar="FILE", help="CA bundle for https")
    parser.add_argument("-k", "--insecure", action="store_true",
                        help="skip TLS certificate verification")
    parser.add_argument("-f", "--format", choices=("table", "json", "csv"),
                        help="output format (default table)")
    parser.add_argument("--tz", metavar="ZONE",
                        help="zone for time inputs that carry none")
    parser.add_argument("--print-config", action="store_true",
                        help="print the merged configuration and exit")
    parser.add_argument("-V", "--version", action="version",
                        version="darpa-spill-client {}".format(__version__))

    commands = parser.add_subparsers(dest="command", metavar="COMMAND")
    commands.add_parser("health", help="liveness check")
    commands.add_parser("status", help="server and ingest status")
    commands.add_parser("sources", help="the TDUs the server records from")
    commands.add_parser("signals", help="accelerator signals the server knows")
    commands.add_parser("types", help="decoded spill types")
    convert = commands.add_parser("convert", help="convert a time between timescales")
    convert.add_argument("time", metavar="TIME")
    commands.add_parser("time-help", help="accepted time expressions")

    latest = commands.add_parser("latest", help="the most recent stored event")
    latest.add_argument("--signal", metavar="SIG")
    latest.add_argument("--source", action="append", default=[], metavar="NAME")

    events = commands.add_parser("events", help="one page of matching events")
    _selection_options(events)
    events.add_argument("--limit", type=int, metavar="N", help="maximum rows")
    events.add_argument("--offset", type=int, default=0, metavar="N", help="rows to skip")
    events.add_argument("--desc", action="store_true", help="newest first")
    events.add_argument("--columns", metavar="LIST", help="comma-separated columns")

    export = commands.add_parser("export", help="stream a whole range")
    _selection_options(export)
    export.add_argument("--columns", metavar="LIST", help="comma-separated columns (CSV)")
    export.add_argument("-o", "--output", metavar="FILE", help="write to FILE")

    commands.add_parser("admin-check", help="check the admin credentials")

    source = commands.add_parser("source", help="change a source at runtime")
    actions = source.add_subparsers(dest="action", metavar="ACTION")
    actions.required = True
    for action, text in (("enable", "start recording from NAME"),
                         ("disable", "stop recording from NAME"),
                         ("reset", "return NAME to the configuration file")):
        actions.add_parser(action, help=text).add_argument("name", metavar="NAME")
    set_url = actions.add_parser("set-url", help="point NAME at a different TDU")
    set_url.add_argument("name", metavar="NAME")
    set_url.add_argument("new_url", metavar="URL")
    return parser


def _selection(args: argparse.Namespace) -> Selection:
    return Selection(start=args.start, end=args.end, signals=args.signal,
                     types=args.type, sources=args.source)


# -------------------------------------------------------------------- main


def _run(args: argparse.Namespace, client: SpillClient, fmt: str,
         out: IO[bytes], err: IO[str]) -> None:
    command = args.command

    def emit(text: str) -> None:
        out.write(text.encode("utf-8"))

    def emit_raw(body: bytes) -> None:
        out.write(body)
        if not body.endswith(b"\n"):
            out.write(b"\n")

    if command in ("events", "export"):
        selection = _selection(args)
        if command == "export":
            wire = "json" if fmt == "json" else "csv"
            params = client.export_params(selection, wire, None, args.columns)
            if args.output:
                with open(args.output, "wb") as target:
                    for chunk in client.stream("GET", "/api/export", params):
                        target.write(chunk)
            else:
                for chunk in client.stream("GET", "/api/export", params):
                    out.write(chunk)
            return
        wire = "json" if fmt == "json" else "csv"
        columns = args.columns
        if fmt == "table" and not columns:
            columns = DEFAULT_EVENT_COLUMNS
        response = client.request("GET", "/api/events", client.events_params(
            selection, wire, args.limit, args.offset, args.desc,
            client._tz(None), columns))
        if fmt == "table":
            emit(_table_from_csv(response.text, err))
        else:
            emit_raw(response.body)
        return

    if command == "source":
        path = "/api/sources/" + _quote(args.name)
        if args.action == "reset":
            response = client.request("POST", path + "/reset")
        else:
            body: Dict[str, Any] = (
                {"enabled": True} if args.action == "enable" else
                {"enabled": False} if args.action == "disable" else
                {"base_url": args.new_url})
            response = client.request("PATCH", path, body=body)
        if fmt == "json":
            emit_raw(response.body)
        else:
            emit("".join(line + "\n" for line in flatten(response.json()["source"])))
        return

    simple = {
        "health": ("/api/health", []),
        "status": ("/api/status", []),
        "sources": ("/api/sources", []),
        "signals": ("/api/signals", []),
        "types": ("/api/types", []),
        "time-help": ("/api/time/help", []),
        "admin-check": ("/api/admin/check", []),
    }
    if command in simple:
        path, params = simple[command]
    elif command == "convert":
        path, params = "/api/time/convert", [("t", args.time)] + client._tz(None)
    elif command == "latest":
        path = "/api/latest"
        params = ([("signal", args.signal)] if args.signal else []) + [
            ("source", s) for s in args.source]
    else:  # pragma: no cover - argparse rejects anything else
        raise AssertionError(command)

    response = client.request("GET", path, params)
    if fmt == "json":
        emit_raw(response.body)
        return
    data = response.json()
    if command in TABLE_COLUMNS:
        key, columns = TABLE_COLUMNS[command]
        rows = _rows(data[key], columns)
        emit(render_csv(columns, rows) if fmt == "csv" else render_table(columns, rows))
        return
    if command == "latest":
        data = data["event"]
    emit("".join(line + "\n" for line in flatten(data)))


def _quote(name: str) -> str:
    from urllib.parse import quote
    return quote(name, safe="")


def main(argv: Optional[Sequence[str]] = None,
         out: Optional[IO[bytes]] = None, err: Optional[IO[str]] = None) -> int:
    out = out if out is not None else sys.stdout.buffer
    err = err if err is not None else sys.stderr
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_client_config(
            config_file=args.config,
            env_file=args.env_file,
            overrides={
                "url": args.url,
                "timeout": args.timeout,
                "admin_token": args.admin_token,
                "admin_token_file": args.admin_token_file,
                "ca_file": args.ca_file,
                "verify_tls": False if args.insecure else None,
                "format": args.format,
                "timezone": args.tz,
            },
        )
        if args.print_config:
            out.write(config.to_yaml().encode("utf-8"))
            return EXIT_OK
        if not args.command:
            parser.print_usage(err)
            err.write("error: a COMMAND is required\n")
            return EXIT_USAGE
        client = SpillClient.from_config(config)
    except ConfigError as exc:
        err.write("error: {}\n".format(exc))
        return EXIT_USAGE

    try:
        _run(args, client, config.format, out, err)
    except SpillHTTPError as exc:
        err.write("error: HTTP {}: {}\n".format(exc.status, exc.message))
        if exc.hint:
            err.write("hint: {}\n".format(exc.hint))
        return EXIT_HTTP
    except SpillConnectionError as exc:
        err.write("error: {}\n".format(exc))
        return EXIT_CONNECT
    except OSError as exc:
        err.write("error: {}\n".format(exc))
        return EXIT_USAGE
    finally:
        out.flush()
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
