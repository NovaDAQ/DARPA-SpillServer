#!/usr/bin/env python3
"""Backfill the archive from NssTDUApp log files.

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
* ``delta`` is the time since the previous event of any type in the same log
  file, which is how the TDU computes it; the first event of each file has
  none.  ``--verify`` checks this against rows the archive already has.
* ``event_number`` is NULL: the ring's counter is not in the log.
* ``route`` is ``nsstduapp_log``, so every row this adds can be found, or
  removed, with ``WHERE route = 'nsstduapp_log'``.

Only the spill types the ingest records are inserted by default (NuMI, BNB,
BNB TCLK and 1 Hz TCLK); SuperCycle is logged by NssTDUApp but is not in the
TDU's history, so adding it would make the backfilled stretch unlike the
rest of the archive.  ``Fake`` events, and the zero and saturated timestamps
NssTDUApp logs while the TDU is unsynchronised, fall outside any sensible
range and are dropped by the ``--start``/``--end`` window.

Inserts use ``ON CONFLICT DO NOTHING``, so re-running is harmless and it is
safe against the live archive while the server is ingesting.

Usage (from the repository root, in the venv)::

    python contrib/logbackfill/backfill_from_nsstduapp_logs.py --verify \\
        /daqlogs/NovaSpillServer/NssTDUApp/NssTDUApp_Temporary_202610*.log
    python contrib/logbackfill/backfill_from_nsstduapp_logs.py \\
        /daqlogs/NovaSpillServer/NssTDUApp/NssTDUApp_Temporary_202610*.log
"""

from __future__ import annotations

import argparse
import re
import sqlite3
import sys
import time
from collections import Counter
from typing import Iterator, List, Sequence, Tuple

from darpa_spillserver.novatime import (
    TICKS_PER_SECOND, NovaTimeError, convert, parse_range,
)
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import UNKNOWN_SIGNAL, SpillEvent, SpillStore

ROUTE = "nsstduapp_log"
DEFAULT_SOURCE = "tdu-near-master-ppc-01"
DEFAULT_TYPES = (SpillType.NUMI, SpillType.BNB, SpillType.BNB_TCLK,
                 SpillType.ACCEL_ONE_HZ_TCLK)
BATCH = 50000

_LINE = re.compile(
    r"^Spilltype: \w+\((\d+)\) \(evt 0x[0-9a-fA-F]+\) "
    r"novaTime 0x[0-9a-fA-F]+ (\d+)\s*$"
)

#: (nova_time, spill_type) pairs, sorted by time.
Logged = List[Tuple[int, int]]


def read_log(path: str, start: int, end: int) -> Logged:
    """Every event in *path* logged with a time in ``[start, end)``.

    The log is not in time order --- NssTDUApp forwards each type from its
    own position in the ring --- so the result is sorted and de-duplicated.
    """
    found = set()
    with open(path, "r", errors="replace") as handle:
        for line in handle:
            if not line.startswith("Spilltype:"):
                continue
            match = _LINE.match(line)
            if match is None:
                continue
            nova_time = int(match.group(2))
            if start <= nova_time < end:
                found.add((nova_time, int(match.group(1))))
    return sorted(found)


def to_events(logged: Logged, source: str, types: Sequence[int]) -> Iterator[SpillEvent]:
    """Rows for the wanted *types*, with delta taken over all logged types."""
    now = int(time.time())
    previous = None
    for nova_time, spill_type in logged:
        delta = None if previous is None else nova_time - previous
        previous = nova_time
        if spill_type not in types:
            continue
        yield SpillEvent(
            nova_time=nova_time,
            spill_type=SpillType(spill_type),
            signal_code=UNKNOWN_SIGNAL,
            event_word=None,
            event_number=None,
            delta=delta,
            pps_offset=nova_time % TICKS_PER_SECOND,
            source=source,
            route=ROUTE,
            ingested_at=now,
        )


def verify(db: sqlite3.Connection, events: List[SpillEvent], tally: Counter) -> None:
    """Compare *events* with the rows the archive already holds for them.

    One range scan per file rather than a lookup per event: the archive is
    on NFS, where millions of point queries take hours.
    """
    if not events:
        return
    held = {
        (nova_time, spill_type): (delta, pps_offset)
        for nova_time, spill_type, delta, pps_offset in db.execute(
            "SELECT nova_time, spill_type, delta, pps_offset FROM events "
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
        tally["present"] += 1
        tally["pps_offset agrees" if row[1] == event.pps_offset
              else "pps_offset differs"] += 1
        if event.delta is not None and row[0] is not None:
            tally["delta agrees" if row[0] == event.delta
                  else "delta differs"] += 1


def main(argv: Sequence[str] = None) -> int:
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

    if args.verify:
        db = sqlite3.connect("file:{}?mode=ro".format(args.database), uri=True)
        store = None
    else:
        db = None
        store = SpillStore(args.database)

    total_parsed = total_inserted = 0
    tally = Counter()
    try:
        for path in args.logs:
            logged = read_log(path, start, end)
            if not logged:
                print("{}: nothing in range".format(path))
                continue
            events = list(to_events(logged, args.source, types))
            line = "{}: {} .. {}  {} logged, {} of wanted types".format(
                path, convert(logged[0][0]).utc[:19],
                convert(logged[-1][0]).utc[:19], len(logged), len(events))
            if args.verify:
                verify(db, events, tally)
                print(line)
            else:
                inserted = 0
                for i in range(0, len(events), BATCH):
                    inserted += store.insert_events(events[i:i + BATCH])
                print(line + ", {} new".format(inserted))
                total_inserted += inserted
            total_parsed += len(events)
    finally:
        if store is not None:
            store.close()
        if db is not None:
            db.close()

    if args.verify:
        for key in sorted(tally):
            print("  {:20} {}".format(key, tally[key]))
    else:
        print("done: {} event(s) read, {} new row(s)".format(
            total_parsed, total_inserted))
    return 0


if __name__ == "__main__":
    sys.exit(main())
