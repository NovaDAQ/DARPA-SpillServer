"""!
@file src/darpa_spillserver/formats.py
@brief Streaming CSV and JSON rendering of query results.

Rendering query results as CSV and JSON.

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

## Canonical column order, shared by CSV and JSON.
## This order is the default column layout of every darpa-spill-dump file.
## The timestamp blur (shift.py) keeps whatever header a file carries, so it
## does not depend on this order, but it recomputes the time columns listed
## in its DERIVED_COLUMNS from nova_time: a new nova_time-derived column
## added here must be added there too, or a shifted file would leak the
## true time through it.  Append new columns at the end rather than
## reordering, so existing readers keep working.
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
    """!
    @brief Render one event as a flat dictionary keyed by :data:`COLUMNS`.

    When the hardware did not preserve the raw event word, ``signal`` and
    ``signal_name`` are empty rather than guessed.  A type such as
    ``BNB_TCLK`` maps to both ``$1D`` and ``$1F``, and inventing one of them
    would put a value in the table that the instrument never measured.

    @details Every time column is derived from ``nova_time`` alone via
    novatime.convert(); nothing else in the row depends on the clock.  That
    is what lets the shift tool move ``nova_time`` and recompute the rest.
    @param event Stored event.
    @return Dict with exactly the COLUMNS keys, in COLUMNS order.
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
    """!
    @brief Stream *events* as CSV text.

    :param columns: subset and order of :data:`COLUMNS` to emit.
    :param header: whether to emit the column-name row.
    :param comments: lines written before the header, each prefixed with
        ``#``.  Used to record the query that produced the file and any
        caveat about it, so a saved CSV explains itself later.

    @param events Events to render, consumed lazily.
    @param columns Subset and order of COLUMNS; None means all.
    @param header Emit the column-name row.
    @param comments Lines emitted as ``# <line>`` before the header.  Not
                    escaped: a line must not contain a newline.
    @return Generator of text chunks (one per comment block, header, row).
    @throws ValueError if *columns* names a column not in COLUMNS (raised
            on the first next(), since this is a generator).
    """
    selected = list(columns or COLUMNS)
    unknown = [name for name in selected if name not in COLUMNS]
    if unknown:
        raise ValueError(
            "unknown column(s): {}; available: {}".format(
                ", ".join(unknown), ", ".join(COLUMNS)
            )
        )

    # One reusable buffer: the csv module only writes to file objects, so each
    # row is written into it and drained, keeping memory O(one row).
    # "\n" line endings (not csv's default "\r\n") keep files diff- and
    # byte-stable across platforms.
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer, fieldnames=selected, extrasaction="ignore", lineterminator="\n"
    )

    def drain() -> str:
        """!
        @brief Return and clear everything written to the buffer so far.

        @return Buffered text.
        """
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
    """!
    @brief Stream *events* as a JSON object with ``meta`` and ``events`` keys.

    The envelope is assembled by hand rather than with a single
    :func:`json.dumps` so that the event array never has to exist in memory
    all at once.  Only individual rows are serialised at a time.

    @param events Events to render, consumed lazily.
    @param meta Optional metadata object emitted first; non-JSON values are
                rendered with str().
    @param indent None for compact output; otherwise the indent width.
    @return Generator of text chunks forming one JSON object.
    """
    # Hand-rolled pretty-printing: the envelope keys sit at one pad, each row
    # object at two.  The meta object's own inner lines are left as
    # json.dumps produced them (not re-indented).
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
        # Commas go before every row but the first, so nothing needs to know
        # in advance whether another row is coming.
        prefix = "" if first else ","
        first = False
        row = json.dumps(event_row(event), default=str, indent=indent)
        if indent:
            row = "\n" + "\n".join(pad * 2 + line for line in row.splitlines())
            yield prefix + row
        else:
            yield prefix + row

    # Close the array on its own line only if it had rows; an empty array
    # stays "[]".
    if indent and not first:
        yield separator + pad
    yield "]" + separator + "}"


def render_csv(
    events: Iterable[SpillEvent],
    columns: Optional[Sequence[str]] = None,
    header: bool = True,
    comments: Optional[Sequence[str]] = None,
) -> str:
    """!
    @brief Collect :func:`iter_csv` into a single string (tests, small results).

    @param events Events to render.
    @param columns Subset and order of COLUMNS; None means all.
    @param header Emit the column-name row.
    @param comments Lines emitted as ``# <line>`` before the header.
    @return The complete CSV text.
    @throws ValueError if *columns* names an unknown column.
    """
    return "".join(iter_csv(events, columns=columns, header=header, comments=comments))


def render_json(
    events: Iterable[SpillEvent],
    meta: Optional[Dict[str, Any]] = None,
    indent: Optional[int] = None,
) -> str:
    """!
    @brief Collect :func:`iter_json` into a single string (tests, small results).

    @param events Events to render.
    @param meta Optional metadata object.
    @param indent None for compact output; otherwise the indent width.
    @return The complete JSON text.
    """
    return "".join(iter_json(events, meta=meta, indent=indent))


def ambiguity_note(spill_types: Sequence[SpillType]) -> Optional[str]:
    """!
    @brief Describe any signal ambiguity implied by querying *spill_types*.

    Returns ``None`` when every requested type maps to exactly one signal.
    Otherwise returns a sentence naming the types whose stored records cannot
    be narrowed further, so the caller can put it in the response metadata
    instead of leaving the user to discover it.

    @param spill_types Types requested by the query (duplicates allowed).
    @return The note text, or None if no requested type is ambiguous.
    """
    ambiguous = []
    # dict.fromkeys de-duplicates while keeping the caller's order, so the
    # note reads in the order the types were asked for.
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
