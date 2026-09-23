"""Rendering query results as CSV and JSON.

Design.md asks for a structured data table in CSV and JSON.  Both are produced
from one canonical row shape (:func:`event_row`) so the two formats can never
drift apart: a column added for CSV appears in JSON automatically.

Both writers are **streaming generators**.  A query spanning months can return
millions of rows, and materialising that into a string before sending it would
hold the whole result in memory on a gateway node that is also running the
DAQ.  Yielding chunk by chunk keeps the footprint flat and lets the client see
the first row immediately.

Every row carries the timestamp in all four timescales at once --- NOvA ticks,
UNIX, UTC and GPS --- rather than making the client choose up front.  The
conversions are cheap integer arithmetic, and a saved CSV that omitted GPS
would be useless to the next person who needed it.

GPS appears both as ``gps``, a full-precision decimal string good to the
picosecond, and as the separate ``gps_seconds`` / ``gps_nsec`` / ``gps_week``
/ ``gps_tow`` integers.  Prefer ``gps``: a NOvA tick is 15.625 ns, so
``gps_nsec`` cannot land on a tick boundary and truncates 625 ps of every
one.  ``gps_psec`` carries the same exact remainder as an integer for callers
that would rather not parse a decimal.
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any, Dict, Iterable, Iterator, Optional, Sequence

from .novatime import convert
from .signals import SpillType, describe_type, signal_for_code, signals_for_type
from .storage import UNKNOWN_SIGNAL, SpillEvent

__all__ = [
    "COLUMNS",
    "event_row",
    "iter_csv",
    "iter_json",
    "render_csv",
    "render_json",
]

#: Canonical column order, shared by CSV and JSON.
COLUMNS: Sequence[str] = (
    "nova_time",
    "utc",
    "utc_string",
    "unix_sec",
    "unix_nsec",
    "gps",
    "gps_seconds",
    "gps_nsec",
    "gps_psec",
    "gps_week",
    "gps_tow",
    "gps_tow_exact",
    "spill_type",
    "spill_type_name",
    "signal",
    "signal_name",
    "event_number",
    "delta",
    "pps_offset",
    "source",
    "route",
)


def event_row(event: SpillEvent) -> Dict[str, Any]:
    """Render one event as a flat dictionary keyed by :data:`COLUMNS`.

    When the hardware did not preserve the raw event word, ``signal`` and
    ``signal_name`` are empty rather than guessed.  A type such as
    ``BNB_TCLK`` maps to both ``$1D`` and ``$1F``, and inventing one of them
    would put a value in the table that the instrument never measured.
    """
    times = convert(event.nova_time)
    signal = (
        signal_for_code(event.signal_code)
        if event.signal_code != UNKNOWN_SIGNAL
        else None
    )

    return {
        "nova_time": event.nova_time,
        "utc": times.utc,
        "utc_string": times.utc_string,
        "unix_sec": times.unix_sec,
        "unix_nsec": times.unix_nsec,
        "gps": times.gps,
        "gps_seconds": times.gps_seconds,
        "gps_nsec": times.gps_nsec,
        "gps_psec": times.gps_psec,
        "gps_week": times.gps_week,
        "gps_tow": times.gps_tow,
        "gps_tow_exact": times.gps_tow_exact,
        "spill_type": int(event.spill_type),
        "spill_type_name": event.spill_type.name,
        "signal": signal.hex if signal else "",
        "signal_name": signal.name if signal else "",
        "event_number": event.event_number,
        "delta": event.delta,
        "pps_offset": event.pps_offset,
        "source": event.source,
        "route": event.route,
    }


def iter_csv(
    events: Iterable[SpillEvent],
    columns: Optional[Sequence[str]] = None,
    header: bool = True,
    comments: Optional[Sequence[str]] = None,
) -> Iterator[str]:
    """Stream *events* as CSV text.

    :param columns: subset and order of :data:`COLUMNS` to emit.
    :param header: whether to emit the column-name row.
    :param comments: lines written before the header, each prefixed with
        ``#``.  Used to record the query that produced the file and any
        caveat about it, so a saved CSV explains itself later.
    """
    selected = list(columns or COLUMNS)
    unknown = [name for name in selected if name not in COLUMNS]
    if unknown:
        raise ValueError(
            "unknown column(s): {}; available: {}".format(
                ", ".join(unknown), ", ".join(COLUMNS)
            )
        )

    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=selected, extrasaction="ignore", lineterminator="\n"
    )

    def drain() -> str:
        text = buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)
        return text

    if comments:
        for line in comments:
            buffer.write("# {}\n".format(line))
        yield drain()

    if header:
        writer.writeheader()
        yield drain()

    for event in events:
        writer.writerow(event_row(event))
        chunk = drain()
        if chunk:
            yield chunk


def iter_json(
    events: Iterable[SpillEvent],
    meta: Optional[Dict[str, Any]] = None,
    indent: Optional[int] = None,
) -> Iterator[str]:
    """Stream *events* as a JSON object with ``meta`` and ``events`` keys.

    The envelope is assembled by hand rather than with a single
    :func:`json.dumps` so that the event array never has to exist in memory
    all at once.  Only individual rows are serialised at a time.
    """
    separator = "\n" if indent else ""
    pad = " " * (indent or 0)

    yield "{" + separator
    if meta is not None:
        yield '{}"meta": {},{}'.format(
            pad, json.dumps(meta, default=str, indent=indent), separator
        )
    yield '{}"events": ['.format(pad)

    first = True
    for event in events:
        prefix = "" if first else ","
        first = False
        row = json.dumps(event_row(event), default=str, indent=indent)
        if indent:
            row = "\n" + "\n".join(pad * 2 + line for line in row.splitlines())
            yield prefix + row
        else:
            yield prefix + row

    if indent and not first:
        yield separator + pad
    yield "]" + separator + "}"


def render_csv(
    events: Iterable[SpillEvent],
    columns: Optional[Sequence[str]] = None,
    header: bool = True,
    comments: Optional[Sequence[str]] = None,
) -> str:
    """Collect :func:`iter_csv` into a single string (tests, small results)."""
    return "".join(iter_csv(events, columns=columns, header=header, comments=comments))


def render_json(
    events: Iterable[SpillEvent],
    meta: Optional[Dict[str, Any]] = None,
    indent: Optional[int] = None,
) -> str:
    """Collect :func:`iter_json` into a single string (tests, small results)."""
    return "".join(iter_json(events, meta=meta, indent=indent))


def ambiguity_note(spill_types: Sequence[SpillType]) -> Optional[str]:
    """Describe any signal ambiguity implied by querying *spill_types*.

    Returns ``None`` when every requested type maps to exactly one signal.
    Otherwise returns a sentence naming the types whose stored records cannot
    be narrowed further, so the caller can put it in the response metadata
    instead of leaving the user to discover it.
    """
    ambiguous = []
    for spill_type in dict.fromkeys(spill_types):
        options = signals_for_type(spill_type)
        if len(options) > 1:
            ambiguous.append(
                "{} ({})".format(
                    spill_type.name, " or ".join(s.hex for s in options)
                )
            )
    if not ambiguous:
        return None
    return (
        "Records stored without a raw event word cannot distinguish the "
        "signals behind these types: " + "; ".join(ambiguous) + ". "
        "Rows with an empty 'signal' column are of this kind."
    )
