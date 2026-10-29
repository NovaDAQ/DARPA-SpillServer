#!/usr/bin/env python3
"""!
@file contrib/logbackfill/backfill_from_nsstduapp_logs.py
@brief Backfill the archive from NssTDUApp log files.

Backfill the archive from NssTDUApp log files.

Copyright 2010-2026 Andrew Norman for Fermi Forward Discovery Group LLC.
All rights reserved.

A one-off recovery tool.  ``darpa-spill-backfill`` reads history out of a
TDU's ring buffer, which is no use once the TDU has lost power and the ring
has been cleared.  NssTDUApp, though, logs every event it forwards from that
same ring::

    %MSG-i SpillSent:  Main 13-Oct-2026 14:43:49 UTC MF-online
    Spilltype: BNBtclk(3) (evt 0x11f) novaTime 0x786a8c46cab631 33894147920934449
    %MSG

and those logs survive in ``/daqlogs/NovaSpillServer/NssTDUApp``.  This
script parses the ``Spilltype:`` lines and inserts them into the archive as
rows of the TDU the logs came from.

**Matching the ingest's rows.**  The rows are written the way the poller
writes them from ``/spill_history``, so that an event the archive already
holds is recognised as a duplicate rather than stored twice:

* ``signal_code`` is ``-1`` and ``event_word`` NULL, as the TDU's history
  route supplies no event word.  The log does carry one, but recording it
  would change the primary key and duplicate every event already present.
* ``pps_offset`` is ``nova_time`` modulo one second, as the TDU reports it.
* ``event_number`` is the TDU's own, recovered as described below, or NULL
  where it cannot be.
* ``delta`` is the time since the previous event of any type in the log, i.e.
  since hardware event ``event_number - 1``, which is how the TDU defines it.
* ``route`` is ``nsstduapp_log``, so every row this adds can be found, or
  removed, with ``WHERE route = 'nsstduapp_log'``.

**Event numbers.**  ``darpa-spill-shift`` keys each event's offset on
``(source, event_number)``, so a row without one cannot be published.  The
log does not print the number, but it does not need to: the TDU numbers every
event it writes to the ring, SuperCycle included, and NssTDUApp logs every
one of them.  Sorted by time, an event's position in the log differs from its
``event_number`` by a constant for the whole TCRMonitor run (checked on 3.3
million archived rows in October 2026: the constant was 0 or 1 per log).
Each event the archive already holds from the TDU is an *anchor* giving that
constant; an event between two anchors that agree is numbered from them.  An
event whose anchors disagree because the log swapped it with a neighbour a
tick away takes the one number left free around it.  Anything else with no
anchor on one side, or anchors that disagree, is left NULL, as is every event
of a log with no anchors at all.  A number already used by another archived
event is never assigned.

Only the spill types the ingest records are inserted by default (NuMI, BNB,
BNB TCLK and 1 Hz TCLK); SuperCycle is logged by NssTDUApp but is not in the
TDU's history, so adding it would make the backfilled stretch unlike the
rest of the archive.  It still counts towards numbering and ``delta``.
``Fake`` events and the zero and saturated timestamps NssTDUApp logs while
the TDU is unsynchronised are dropped.

Inserts use ``ON CONFLICT DO NOTHING``, so re-running is harmless and it is
safe against the live archive while the server is ingesting.  A run also
updates ``event_number`` and ``delta`` of rows an earlier run inserted
(``route = 'nsstduapp_log'``); rows from the TDU itself are never changed.

Usage (from the repository root, in the venv)::

    python contrib/logbackfill/backfill_from_nsstduapp_logs.py --verify \\
        /daqlogs/NovaSpillServer/NssTDUApp/NssTDUApp_Temporary_202610*.log
    python contrib/logbackfill/backfill_from_nsstduapp_logs.py \\
        /daqlogs/NovaSpillServer/NssTDUApp/NssTDUApp_Temporary_202610*.log

Exit status: 0 on completion (including files with nothing in range); 2 for
an unparsable --start/--end.  Needs ``darpa_spillserver`` importable (run in
the project venv, or with PYTHONPATH=src).
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from collections import Counter
from typing import Dict, List, Optional, Sequence, Tuple

from darpa_spillserver.novatime import (
    TICKS_PER_SECOND, NovaTimeError, convert, parse_range,
)
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import UNKNOWN_SIGNAL, SpillEvent, SpillStore

## Value of the ``route`` column on every row this script writes; the tag
## that distinguishes log-derived rows from TDU-ingested ones.
ROUTE = "nsstduapp_log"
## TDU the logs are assumed to come from unless --source says otherwise.
DEFAULT_SOURCE = "tdu-near-master-ppc-01"
## Spill types inserted by default: those the live ingest records.
DEFAULT_TYPES = (SpillType.NUMI, SpillType.BNB, SpillType.BNB_TCLK,
                 SpillType.ACCEL_ONE_HZ_TCLK)
## Rows per insert/renumber transaction.
BATCH = 50000

## Events either side searched for a free number; see :func:`number`.
WINDOW = 4

## How far out of time order the ring may hold two events: 1 ms.
SWAP_TICKS = TICKS_PER_SECOND // 1000

## NssTDUApp logs 0x7fff... for an unsynchronised TDU; nothing real is near it.
_LATEST = 1 << 62

## One ``Spilltype:`` line: group 1 is the decoded type number in
## parentheses, group 2 the decimal novaTime.  The evt word and hex time are
## matched but not captured (see the module docstring for why the event word
## is not stored).
_LINE = re.compile(
    r"^Spilltype: \w+\((\d+)\) \(evt 0x[0-9a-fA-F]+\) "
    r"novaTime 0x[0-9a-fA-F]+ (\d+)\s*$"
)

## (nova_time, spill_type) pairs, sorted by time.
Logged = List[Tuple[int, int]]


def read_log(path: str) -> Logged:
    """!@brief Every real event in *path*, sorted by time and de-duplicated.

    The log is not in time order --- NssTDUApp forwards each type from its
    own position in the ring --- and the whole file is kept, not just the
    range being backfilled, because an event's place in it is its number.

    De-duplication is on (nova_time, spill_type), so an event logged more
    than once occupies one position.  Sorting the tuples orders by time,
    then type for simultaneous events.

    @param path  NssTDUApp log file; undecodable bytes are replaced.
    @return Logged list, possibly empty.
    @throws OSError  *path* cannot be opened.
    """
    found = set()
    with open(path, "r", errors="replace") as handle:
        for line in handle:
            if not line.startswith("Spilltype:"):
                continue
            match = _LINE.match(line)
            if match is None:
                continue
            spill_type = int(match.group(1))
            nova_time = int(match.group(2))
            if spill_type != SpillType.FAKE and 0 < nova_time < _LATEST:
                found.add((nova_time, spill_type))
    return sorted(found)


def number(db: sqlite3.Connection, source: str, logged: Logged,
           tally: Counter) -> List[Optional[int]]:
    """!@brief The TDU event number of each logged event, or None where unknown.

    Implements the anchor scheme of the module docstring in three passes:
    forward and backward sweeps carry the (event_number - position) offset
    of the nearest anchor on each side; an event is numbered only when both
    sides agree and the number is unused; a final pass resolves single
    neighbour swaps.

    @param db      archive connection (read only here).
    @param source  TDU name whose archived rows serve as anchors.
    @param logged  non-empty Logged list from read_log.
    @param tally   Counter updated with per-outcome counts.
    @return list parallel to *logged* of event numbers or None.
    """
    anchors: Dict[Tuple[int, int], int] = {}
    used = set()
    for nova_time, spill_type, event_number in db.execute(
            "SELECT nova_time, spill_type, event_number FROM events "
            "WHERE source = ? AND route != ? AND nova_time >= ? "
            "AND nova_time <= ? AND event_number IS NOT NULL",
            (source, ROUTE, logged[0][0], logged[-1][0])):
        anchors[(nova_time, spill_type)] = event_number
        used.add(event_number)

    # before[i]/after[i]: offset (event_number - position) implied by the
    # nearest anchor at or before / at or after position i, None if none.
    count = len(logged)
    before: List[Optional[int]] = [None] * count
    after: List[Optional[int]] = [None] * count
    offset = None
    for i in range(count):
        anchor = anchors.get(logged[i])
        if anchor is not None:
            offset = anchor - i
        before[i] = offset
    offset = None
    for i in range(count - 1, -1, -1):
        anchor = anchors.get(logged[i])
        if anchor is not None:
            offset = anchor - i
        after[i] = offset

    numbers: List[Optional[int]] = []
    for i in range(count):
        anchor = anchors.get(logged[i])
        if anchor is not None:
            numbers.append(anchor)
            tally["anchored"] += 1
        # Anchors on the two sides disagree (an event missing from, or
        # extra in, the log between them) or one side has none: the
        # position no longer determines the number.
        elif before[i] is None or before[i] != after[i]:
            numbers.append(None)
            tally["no number"] += 1
        elif i + before[i] in used:
            numbers.append(None)
            tally["number taken"] += 1
        else:
            numbers.append(i + before[i])
            used.add(i + before[i])
            tally["numbered"] += 1

    # Events a tick apart are sometimes logged in the other order from the
    # ring (a BNB TCLK at x.999 s and the BNB at x+1 s), so the anchors on
    # either side disagree by one.  Where exactly one number is free among
    # the neighbours and exactly one event lacks one, it is that event's.
    for i in range(count):
        if numbers[i] is not None:
            continue
        window = range(max(0, i - WINDOW), min(count, i + WINDOW + 1))
        known = [numbers[j] for j in window if numbers[j] is not None]
        unknown = [j for j in window if numbers[j] is None]
        if not known or unknown != [i]:
            continue
        free = set(range(min(known), max(known) + 1)) - used
        if len(free) != 1:
            continue
        candidate = free.pop()
        # The free number must fall where this event does in time, give or
        # take the swap: a free number can also belong to an event the log
        # missed, e.g. at the edge of a gap in the log.
        at = {numbers[j]: logged[j][0] for j in window if numbers[j] is not None}
        lower = at.get(candidate - 1)
        upper = at.get(candidate + 1)
        t = logged[i][0]
        if (lower is not None and t < lower - SWAP_TICKS) or \
                (upper is not None and t > upper + SWAP_TICKS):
            continue
        numbers[i] = candidate
        used.add(candidate)
        # Note: this assumes the event was counted as "no number" above; one
        # that was counted as "number taken" makes this tally off by one.
        tally["no number"] -= 1
        tally["numbered from neighbours"] += 1
    return numbers


def to_events(logged: Logged, numbers: Sequence[Optional[int]], source: str,
              types: Sequence[int], start: int, end: int) -> List[SpillEvent]:
    """!@brief Rows for the wanted *types* in ``[start, end)``.

    ``delta`` is taken over every logged event, wanted or not, in or out of
    range, since it is the gap to the previous hardware event.

    @param logged   Logged list from read_log.
    @param numbers  event numbers parallel to *logged* (from number()).
    @param source   TDU name written to the ``source`` column.
    @param types    spill-type integers to keep.
    @param start    range start, NOvA ticks, inclusive.
    @param end      range end, NOvA ticks, exclusive.
    @return SpillEvent list in time order, all with route ROUTE.
    @throws ValueError  a type number is not a valid SpillType.
    """
    now = int(time.time())
    events = []
    previous = None
    for (nova_time, spill_type), event_number in zip(logged, numbers):
        delta = None if previous is None else nova_time - previous
        previous = nova_time
        if spill_type not in types or not start <= nova_time < end:
            continue
        events.append(SpillEvent(
            nova_time=nova_time,
            spill_type=SpillType(spill_type),
            signal_code=UNKNOWN_SIGNAL,
            event_word=None,
            event_number=event_number,
            delta=delta,
            pps_offset=nova_time % TICKS_PER_SECOND,
            source=source,
            route=ROUTE,
            ingested_at=now,
        ))
    return events


def renumber(db: sqlite3.Connection, events: List[SpillEvent]) -> int:
    """!@brief Bring rows an earlier run inserted up to date.

    Only rows with route ROUTE are touched, and only when event_number or
    delta actually differ (``IS NOT`` is NULL-safe), so TDU rows are never
    modified and the change count is meaningful.

    @param db      writable archive connection.
    @param events  rows as to_events produced them.
    @return number of rows changed.
    @throws sqlite3.Error  on a database error (the batch is rolled back).
    """
    with db:
        before = db.total_changes
        db.executemany(
            "UPDATE events SET event_number = ?, delta = ? "
            "WHERE nova_time = ? AND spill_type = ? AND signal_code = ? "
            "AND source = ? AND route = ? AND (event_number IS NOT ? "
            "OR delta IS NOT ?)",
            [(e.event_number, e.delta, e.nova_time, int(e.spill_type),
              UNKNOWN_SIGNAL, e.source, ROUTE, e.event_number, e.delta)
             for e in events])
        return db.total_changes - before


def verify(db: sqlite3.Connection, events: List[SpillEvent], tally: Counter) -> None:
    """!@brief Compare *events* with the rows the archive already holds for them.

    One range scan per file rather than a lookup per event: the archive is
    on NFS, where millions of point queries take hours.

    @param db      archive connection (read only).
    @param events  time-ordered rows from to_events; all share one source.
    @param tally   Counter updated with missing / present / agreement counts.
    @return None.
    """
    if not events:
        return
    held = {
        (nova_time, spill_type): (delta, pps_offset, route)
        for nova_time, spill_type, delta, pps_offset, route in db.execute(
            "SELECT nova_time, spill_type, delta, pps_offset, route FROM events "
            "WHERE source = ? AND signal_code = ? AND nova_time >= ? "
            "AND nova_time <= ?",
            (events[0].source, UNKNOWN_SIGNAL, events[0].nova_time,
             events[-1].nova_time),
        )
    }
    for event in events:
        row = held.get((event.nova_time, int(event.spill_type)))
        if row is None:
            tally["missing"] += 1
            continue
        if row[2] == ROUTE:
            tally["present from a log"] += 1
            continue
        tally["present"] += 1
        tally["pps_offset agrees" if row[1] == event.pps_offset
              else "pps_offset differs"] += 1
        if event.delta is not None and row[0] is not None:
            tally["delta agrees" if row[0] == event.delta
                  else "delta differs"] += 1


def main(argv: Sequence[str] = None) -> int:
    """!@brief Parse the given logs and insert (or, with --verify, compare).

    @param argv  command-line arguments (default: sys.argv[1:]).
    @return process exit status: 0 done, 2 bad time range.
    @throws sqlite3.Error  the archive cannot be opened or written.
    @throws OSError        a log file cannot be read.
    """
    parser = argparse.ArgumentParser(
        description="Backfill the archive from NssTDUApp log files.")
    parser.add_argument("logs", nargs="+", metavar="LOG",
                        help="NssTDUApp_Temporary_*.log files")
    parser.add_argument("-d", "--database", default="spills.db", metavar="FILE",
                        help="archive to write into (default: %(default)s)")
    parser.add_argument("--source", default=DEFAULT_SOURCE, metavar="NAME",
                        help="TDU the logs came from (default: %(default)s)")
    parser.add_argument("--start", default="2026-10-01T00:00Z", metavar="TIME",
                        help="earliest event time kept (default: %(default)s)")
    parser.add_argument("--end", default="2026-11-01T00:00Z", metavar="TIME",
                        help="end of the range, exclusive (default: %(default)s)")
    parser.add_argument("--types", default=",".join(str(int(t)) for t in DEFAULT_TYPES),
                        metavar="N,N,...",
                        help="spill types to insert (default: %(default)s)")
    parser.add_argument("--verify", action="store_true",
                        help="compare against the archive and write nothing")
    args = parser.parse_args(argv)

    try:
        start, end = parse_range(args.start, args.end)
    except NovaTimeError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2
    types = [int(t) for t in args.types.split(",")]
    print("range {} .. {}, source {}, types {}".format(
        convert(start).utc[:19], convert(end).utc[:19], args.source, types))

    # --verify opens the archive read-only by URI, so it cannot write even
    # by accident; otherwise SpillStore applies the schema and its insert
    # logic (ON CONFLICT DO NOTHING) is reused.
    if args.verify:
        db = sqlite3.connect("file:{}?mode=ro".format(args.database), uri=True)
        store = None
    else:
        store = SpillStore(args.database)
        db = store.connection

    total_parsed = total_inserted = total_updated = 0
    tally = Counter()
    try:
        for path in args.logs:
            logged = read_log(path)
            if not logged:
                print("{}: no events".format(path))
                continue
            file_tally = Counter()
            numbers = number(db, args.source, logged, file_tally)
            events = to_events(logged, numbers, args.source, types, start, end)
            if not events:
                print("{}: nothing in range".format(path))
                continue
            unnumbered = sum(1 for e in events if e.event_number is None)
            line = "{}: {} .. {}  {} of wanted types, {} without a number".format(
                path, convert(events[0].nova_time).utc[:19],
                convert(events[-1].nova_time).utc[:19], len(events), unnumbered)
            if args.verify:
                verify(db, events, tally)
                print(line)
            else:
                inserted = updated = 0
                # Insert then renumber per batch: the insert skips rows that
                # already exist, the renumber refreshes those an earlier run
                # inserted.
                for i in range(0, len(events), BATCH):
                    inserted += store.insert_events(events[i:i + BATCH])
                    updated += renumber(db, events[i:i + BATCH])
                print(line + ", {} new, {} updated".format(inserted, updated))
                total_inserted += inserted
                total_updated += updated
            print("  numbering: " + ", ".join(
                "{} {}".format(v, k) for k, v in sorted(file_tally.items())))
            total_parsed += len(events)
    finally:
        if store is not None:
            store.close()
        elif db is not None:
            db.close()

    if args.verify:
        for key in sorted(tally):
            print("  {:20} {}".format(key, tally[key]))
    else:
        print("done: {} event(s) read, {} new row(s), {} updated".format(
            total_parsed, total_inserted, total_updated))
    return 0


if __name__ == "__main__":
    sys.exit(main())
