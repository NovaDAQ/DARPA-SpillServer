"""Backfill the archive from a TDU's ring buffer (``darpa-spill-backfill``).

The ingest poller only ever moves forward.  Its ``ingest.backfill`` setting
applies on a cold start and nowhere else, so once a source has any rows at
all the poller will never reach back past them.  That leaves no way to pick
up what the TDU already held before the server was first pointed at it ---
which, on a ring holding thirty hours, is most of the data.

This tool fills that gap.  It walks a time range in windows, asking the TDU
for one window at a time and inserting what comes back.

**Why windows rather than paging by count.**  ``/spill_history`` applies its
``limit`` after collecting everything at or after ``since``, so a request with
a wide ``since`` and a small ``limit`` still makes the TDU produce the whole
remainder of the ring --- tens of megabytes, past the server's output cap, and
minutes of PowerPC time for a handful of rows.  Bounding both ends keeps each
request proportional to the window, which is the only shape that finishes.

Inserts are idempotent, keyed on ``(nova_time, spill_type, signal_code)``, so
running this over a range the archive already covers costs time and changes
nothing.  It is safe to run against a live archive while the poller is
running; SQLite is in WAL mode and the two writers do not block each other.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from typing import List, Optional, Sequence

from . import __version__
from .config import Config, ConfigError, load_config
from .novatime import TICKS_PER_SECOND, NovaTimeError, convert, parse_range
from .storage import SpillStore
from .tdu_client import TDUClient, TDUError

__all__ = ["main", "backfill_source"]

log = logging.getLogger(__name__)

#: Seconds of TDU time requested per call.  Small enough that one window is
#: well inside the server's output cap and history timeout even on the busiest
#: TDU, large enough that a long range does not turn into thousands of calls.
DEFAULT_WINDOW = 300.0


async def backfill_source(
    client: TDUClient,
    store: SpillStore,
    start_nova: int,
    end_nova: int,
    window_seconds: float = DEFAULT_WINDOW,
    batch_limit: int = 20000,
    progress=None,
) -> "tuple[int, int, int]":
    """Walk *start_nova* to *end_nova* in windows, inserting what is found.

    Returns ``(fetched, inserted, windows)``.
    """
    window_ticks = int(window_seconds * TICKS_PER_SECOND)
    fetched = inserted = windows = 0
    cursor = start_nova

    while cursor < end_nova:
        upper = min(cursor + window_ticks, end_nova)
        result = await client.fetch_history(
            since_nova=cursor, until_nova=upper, limit=batch_limit
        )
        added = await asyncio.to_thread(store.insert_events, result.events)

        fetched += len(result.events)
        inserted += added
        windows += 1

        if result.truncated:
            # More matched than the limit returned, so the window is too wide
            # for this stretch of the ring. Advancing past it would silently
            # skip events; narrow instead and re-read the same stretch.
            log.warning(
                "window %s .. %s hit the limit of %d; halving it",
                convert(cursor).utc, convert(upper).utc, batch_limit,
            )
            if window_ticks > TICKS_PER_SECOND:
                window_ticks = max(TICKS_PER_SECOND, window_ticks // 2)
                continue
            log.error(
                "a one-second window still exceeds the limit at %s; skipping "
                "ahead, some events in that second will be missing",
                convert(cursor).utc,
            )

        if progress is not None:
            progress(cursor, upper, end_nova, fetched, inserted)
        cursor = upper

    return fetched, inserted, windows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="darpa-spill-backfill",
        description="Fill the archive from a TDU's ring buffer, backwards in "
                    "time from where ingest happens to have started.",
        epilog="Write relative times with an equals sign -- --start=-6h, not\n"
               "--start -6h -- or the shell hands argparse something that\n"
               "looks like an option.\n"
               "\n"
               "The ring is finite: once TCRMonitor has written past a slot\n"
               "that event is gone, so a backfill reaches only as far back as\n"
               "the TDU still holds. It can reach further than the current\n"
               "TCRMonitor run, though -- that process resets its event\n"
               "counter on start but does not clear the segment, so older\n"
               "runs' events survive beyond the insert point. Their\n"
               "timestamps are genuine; only the Number sequence restarts,\n"
               "so event_number is not continuous across that boundary.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-V", "--version", action="store_true",
                        help="show version information and exit")
    parser.add_argument("-c", "--config", metavar="FILE",
                        help="YAML configuration file")
    parser.add_argument("--env-file", metavar="FILE",
                        help=".env file to read (default ./.env if present)")
    parser.add_argument("-d", "--database", metavar="FILE",
                        help="archive to write into; overrides the config")
    parser.add_argument("-s", "--source", action="append", metavar="NAME",
                        help="restrict to this source; repeatable, "
                             "default all configured sources")
    parser.add_argument("--start", metavar="TIME",
                        help="start of the range; default the earliest the "
                             "TDU still holds, found by probing")
    parser.add_argument("--end", metavar="TIME", default="now",
                        help="end of the range (default: now)")
    parser.add_argument("--window", type=float, default=DEFAULT_WINDOW,
                        metavar="SECONDS",
                        help="seconds of TDU time per request")
    parser.add_argument("--limit", type=int, default=20000, metavar="N",
                        help="maximum rows per request")
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="report what would be fetched without writing")
    parser.add_argument("-q", "--quiet", action="store_true",
                        help="only report the final summary")
    return parser


async def _probe_earliest(client: TDUClient, end_nova: int) -> Optional[int]:
    """Find where the TDU's ring begins, to within a few minutes.

    Steps backwards in doubling strides until a narrow window comes back
    empty, then binary-searches the bracket between the last stride that had
    data and the first that did not. Doubling alone would under-report by up
    to half the distance, which on a multi-day ring is many hours of history
    silently left behind.

    Note that the ring can span more than the current TCRMonitor run.  That
    process resets its event counter on start but does not clear the segment,
    so slots beyond the current insert point still hold the previous run's
    events.  Their timestamps are genuine; only the ``Number`` sequence
    restarts.  This probe deliberately reaches into that older data, because
    it is real accelerator history that is otherwise lost when the ring
    wraps over it.
    """
    probe_ticks = int(60 * TICKS_PER_SECOND)

    async def has_data(seconds_back: float) -> bool:
        point = end_nova - int(seconds_back * TICKS_PER_SECOND)
        if point < 0:
            return False
        try:
            result = await client.fetch_history(
                since_nova=point, until_nova=point + probe_ticks, limit=5
            )
        except TDUError as exc:
            log.debug("probe at -%.0f s failed: %s", seconds_back, exc)
            return False
        return bool(result.events)

    # Expand until a window is empty, keeping the last one that was not.
    good = 0.0
    bad: Optional[float] = None
    back = 3600.0
    while back <= 30 * 24 * 3600:
        if await has_data(back):
            good = back
            back *= 2
        else:
            bad = back
            break

    if good == 0.0:
        return None
    if bad is None:
        bad = back

    # Narrow the bracket. Ten steps takes the uncertainty below a minute for
    # any ring this side of a month.
    for _ in range(10):
        if bad - good < 300:
            break
        middle = (good + bad) / 2
        if await has_data(middle):
            good = middle
        else:
            bad = middle

    return end_nova - int(good * TICKS_PER_SECOND)


async def _run(args: argparse.Namespace, config: Config) -> int:
    store = SpillStore(config.storage.path, timeout=config.storage.timeout)
    sources = config.tdu.resolved_sources()
    if args.source:
        wanted = set(args.source)
        sources = [s for s in sources if s.name in wanted]
        if not sources:
            print("error: no configured source matches {}".format(
                ", ".join(args.source)), file=sys.stderr)
            return 2

    total_fetched = total_inserted = 0
    started = time.time()

    try:
        for source in sources:
            client = TDUClient(
                base_url=source.base_url,
                timeout=config.tdu.timeout,
                history_timeout=config.tdu.history_timeout,
                retries=config.tdu.retries,
                retry_backoff=config.tdu.retry_backoff,
                name=source.name,
            )
            async with client:
                if not await client.supports_history():
                    print("{}: no bulk history route; cannot backfill from "
                          "this TDU. See contrib/tduweb/README.md.".format(
                              source.name), file=sys.stderr)
                    continue

                try:
                    _, end_nova = parse_range(None, args.end)
                except NovaTimeError as exc:
                    print("error: {}".format(exc), file=sys.stderr)
                    return 2

                if args.start:
                    try:
                        start_nova, _ = parse_range(args.start, args.end)
                    except NovaTimeError as exc:
                        print("error: {}".format(exc), file=sys.stderr)
                        return 2
                else:
                    if not args.quiet:
                        print("{}: probing for the start of the ring..."
                              .format(source.name))
                    probed = await _probe_earliest(client, end_nova)
                    if probed is None:
                        print("{}: the TDU returned nothing; skipping"
                              .format(source.name), file=sys.stderr)
                        continue
                    start_nova = probed

                hours = (end_nova - start_nova) / float(TICKS_PER_SECOND) / 3600
                print("{}: {} .. {}  ({:.1f} hours)".format(
                    source.name, convert(start_nova).utc[:19],
                    convert(end_nova).utc[:19], hours))

                if args.dry_run:
                    windows = int((end_nova - start_nova)
                                  / (args.window * TICKS_PER_SECOND)) + 1
                    print("  dry run: {} window(s) of {:.0f} s would be "
                          "requested".format(windows, args.window))
                    continue

                def report(lo, hi, end, fetched, inserted, _s=source):
                    if args.quiet:
                        return
                    done = (hi - start_nova) / float(end - start_nova) * 100
                    sys.stdout.write(
                        "\r  {:5.1f}%  up to {}  fetched {:>7}  new {:>7}".format(
                            done, convert(hi).utc[:19], fetched, inserted))
                    sys.stdout.flush()

                fetched, inserted, windows = await backfill_source(
                    client, store, start_nova, end_nova,
                    window_seconds=args.window, batch_limit=args.limit,
                    progress=report,
                )
                if not args.quiet:
                    sys.stdout.write("\n")
                print("  {} window(s), {} fetched, {} new".format(
                    windows, fetched, inserted))
                total_fetched += fetched
                total_inserted += inserted
    finally:
        store.close()

    elapsed = time.time() - started
    print("\ndone in {:.0f} s: {} fetched, {} new row(s)".format(
        elapsed, total_fetched, total_inserted))
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.version:
        print("darpa-spill-backfill {}".format(__version__))
        return 0

    try:
        config = load_config(argv=(
            (["-c", args.config] if args.config else [])
            + (["--env-file", args.env_file] if args.env_file else [])))
    except ConfigError as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 2

    if args.database:
        config.storage.path = args.database

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    try:
        return asyncio.run(_run(args, config))
    except KeyboardInterrupt:
        print("\ninterrupted; rows already inserted are kept", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
