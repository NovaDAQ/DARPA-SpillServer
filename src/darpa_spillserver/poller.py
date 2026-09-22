"""Background ingest: copy events from the TDU into the local archive.

The TDU holds only a small ring buffer, so this loop is what turns a live
instrument into a queryable history.  It runs as an asyncio task for the life
of the server.

Each pass asks the TDU for everything since a **watermark** --- the newest
timestamp already stored --- minus a configurable overlap.  Re-reading a few
seconds that are already archived is deliberate: inserts are idempotent
(see :mod:`darpa_spillserver.storage`), and the overlap closes the window that
clock skew, a slow poll, or a restart would otherwise leave empty.  Progress is
measured by rows *inserted*, never by rows fetched.

On a cold start there is no watermark, so the loop reaches back
``ingest.backfill`` seconds instead.

**Degraded mode.** When the TDU has no ``/spill_history`` route, the client
falls back to ``/tcr_status``, which returns only the newest event.  Events
arrive at roughly 6--15 Hz, so a 1 Hz poll then captures well under a tenth of
them.  The loop detects this, warns once at startup, and records
``degraded: true`` in its status so the API can tell clients their history has
holes rather than letting them assume it is complete.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from .config import Config
from .novatime import TICKS_PER_SECOND, nova_now
from .storage import SpillStore
from .tdu_client import HISTORY_ROUTE, TDUClient, TDUError

__all__ = ["IngestStatus", "Poller"]

log = logging.getLogger(__name__)


@dataclass
class IngestStatus:
    """A snapshot of the poller's health, surfaced by ``/api/status``."""

    running: bool = False
    degraded: bool = False
    """True when the TDU lacks the history route, so events are being missed."""

    route: str = ""
    polls: int = 0
    events_fetched: int = 0
    events_inserted: int = 0
    consecutive_errors: int = 0
    last_poll_at: Optional[float] = None
    last_success_at: Optional[float] = None
    last_error: str = ""
    last_error_at: Optional[float] = None
    watermark_nova: Optional[int] = None
    pruned_total: int = 0

    def as_dict(self) -> dict:
        data = {
            "running": self.running,
            "degraded": self.degraded,
            "route": self.route,
            "polls": self.polls,
            "events_fetched": self.events_fetched,
            "events_inserted": self.events_inserted,
            "consecutive_errors": self.consecutive_errors,
            "last_poll_at": self.last_poll_at,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "last_error_at": self.last_error_at,
            "watermark_nova": self.watermark_nova,
            "pruned_total": self.pruned_total,
        }
        if self.degraded:
            data["degraded_reason"] = (
                "The TDU does not expose {}, so only the single newest event "
                "is readable per poll. Accelerator events arrive at roughly "
                "6-15 Hz, so the archive is sampling, not a complete history. "
                "See contrib/tduweb/README.md.".format(HISTORY_ROUTE)
            )
        return data


class Poller:
    """Polls one TDU and writes what it finds into the archive.

    :param config: the merged server configuration.
    :param store: archive to write into.
    :param client: TDU client; one is created from *config* if omitted.
    """

    def __init__(
        self,
        config: Config,
        store: SpillStore,
        client: Optional[TDUClient] = None,
    ) -> None:
        self.config = config
        self.store = store
        self.status = IngestStatus()
        self._owns_client = client is None
        self._client = client or TDUClient(
            base_url=config.tdu.base_url,
            timeout=config.tdu.timeout,
            retries=config.tdu.retries,
            retry_backoff=config.tdu.retry_backoff,
        )
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        self._last_prune = 0.0

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        """Open the client and launch the polling task."""
        if self._task is not None:
            return
        await self._client.open()
        self._stopping.clear()
        self.status.running = True
        self._task = asyncio.create_task(self._run(), name="darpa-spill-ingest")
        log.info(
            "ingest started: polling %s every %.3gs",
            self.config.tdu.base_url, self.config.ingest.interval,
        )

    async def stop(self) -> None:
        """Ask the loop to finish the current pass, then wait for it."""
        self._stopping.set()
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=30)
            except asyncio.TimeoutError:
                log.warning("ingest task did not stop in time; cancelling")
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self._task = None
        if self._owns_client:
            await self._client.close()
        self.status.running = False
        log.info(
            "ingest stopped after %d poll(s); %d event(s) inserted",
            self.status.polls, self.status.events_inserted,
        )

    # -- the loop -----------------------------------------------------------

    async def _run(self) -> None:
        interval = self.config.ingest.interval

        # Probe once up front so the warning appears at startup rather than
        # buried in the first poll's debug output.
        try:
            supported = await self._client.supports_history()
            self.status.degraded = not supported
            self.status.route = HISTORY_ROUTE if supported else "/tcr_status"
        except TDUError as exc:
            log.warning("could not probe the TDU for a history route: %s", exc)

        while not self._stopping.is_set():
            started = time.time()
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # the loop must survive any single failure
                self._record_error(exc)
                limit = self.config.ingest.max_consecutive_errors
                if limit and self.status.consecutive_errors >= limit:
                    log.error(
                        "stopping ingest after %d consecutive errors",
                        self.status.consecutive_errors,
                    )
                    break

            await self._maybe_prune()

            # Subtract the work already done so the period is the poll
            # interval, not the interval plus however long the poll took.
            elapsed = time.time() - started
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=max(0.0, interval - elapsed)
                )
            except asyncio.TimeoutError:
                pass

        self.status.running = False

    async def poll_once(self) -> int:
        """Run one ingest pass; returns the number of rows inserted."""
        started_at = int(time.time())
        since = await asyncio.to_thread(self._watermark)

        result = await self._client.fetch_history(
            since_nova=since,
            limit=self.config.ingest.batch_limit,
        )

        inserted = await asyncio.to_thread(self.store.insert_events, result.events)

        self.status.polls += 1
        self.status.events_fetched += len(result.events)
        self.status.events_inserted += inserted
        self.status.consecutive_errors = 0
        self.status.last_poll_at = time.time()
        self.status.last_success_at = self.status.last_poll_at
        self.status.route = result.route
        self.status.degraded = result.route != HISTORY_ROUTE
        if result.events:
            self.status.watermark_nova = max(e.nova_time for e in result.events)

        await asyncio.to_thread(
            self.store.record_ingest,
            result.route, started_at, int(time.time()),
            len(result.events), inserted, None,
        )

        if inserted:
            log.debug(
                "ingest: %d fetched, %d new, via %s",
                len(result.events), inserted, result.route,
            )
        if result.truncated and result.route == HISTORY_ROUTE:
            # The TDU had more than batch_limit waiting, so the next poll
            # should not wait a full interval before catching up.
            log.info(
                "ingest: TDU reported more events than the batch limit (%d); "
                "consider raising ingest.batch_limit or lowering ingest.interval",
                self.config.ingest.batch_limit,
            )
        return inserted

    # -- helpers ------------------------------------------------------------

    def _watermark(self) -> int:
        """The NOvA tick to resume from, allowing for the configured overlap."""
        latest = self.store.latest()
        overlap_ticks = int(self.config.ingest.overlap * TICKS_PER_SECOND)

        if latest is None:
            backfill_ticks = int(self.config.ingest.backfill * TICKS_PER_SECOND)
            start = max(0, nova_now() - backfill_ticks)
            log.info(
                "archive is empty; backfilling %.0f s of history",
                self.config.ingest.backfill,
            )
            return start

        return max(0, latest.nova_time - overlap_ticks)

    def _record_error(self, exc: Exception) -> None:
        self.status.consecutive_errors += 1
        self.status.last_error = "{}: {}".format(type(exc).__name__, exc)
        self.status.last_error_at = time.time()
        self.status.last_poll_at = self.status.last_error_at

        # Log the first failure loudly, then back off to debug so that a TDU
        # that is down overnight does not fill the disk with identical lines.
        if self.status.consecutive_errors == 1:
            log.error("ingest poll failed: %s", self.status.last_error)
        elif self.status.consecutive_errors % 60 == 0:
            log.error(
                "ingest poll still failing after %d attempts: %s",
                self.status.consecutive_errors, self.status.last_error,
            )
        else:
            log.debug("ingest poll failed: %s", self.status.last_error)

    async def _maybe_prune(self) -> None:
        """Apply the retention policy, at most once per prune interval."""
        retention_days = self.config.storage.retention_days
        if retention_days <= 0:
            return

        now = time.time()
        if now - self._last_prune < self.config.storage.prune_interval:
            return
        self._last_prune = now

        cutoff = nova_now() - int(retention_days * 86400 * TICKS_PER_SECOND)
        if cutoff <= 0:
            return

        removed = await asyncio.to_thread(self.store.prune_before, cutoff)
        await asyncio.to_thread(
            self.store.trim_ingest_log, self.config.storage.ingest_log_keep
        )
        if removed:
            self.status.pruned_total += removed
            log.info(
                "retention: removed %d event(s) older than %.3g day(s)",
                removed, retention_days,
            )
