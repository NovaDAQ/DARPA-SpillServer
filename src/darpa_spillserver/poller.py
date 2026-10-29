"""!
@file src/darpa_spillserver/poller.py
@brief Background ingest: one asyncio polling task per TDU source, plus the manager that owns them.

Background ingest: copy events from the TDUs into the local archive.

A TDU holds only a small ring buffer, so these loops are what turn live
instruments into a queryable history.  Each source gets its own
:class:`Poller`, an asyncio task that runs for the life of the server or
until the source is disabled.  :class:`IngestManager` owns the pollers and
applies changes made to sources at runtime.

Each pass asks the TDU for everything since a **watermark** --- the newest
timestamp already stored from that source --- minus a configurable overlap.  Re-reading a few
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

**Concurrency model.**  Pollers are *asyncio tasks* on the server's single
event loop, not OS threads.  Network I/O is awaited on the loop (the TDU
client is async); every blocking SQLite call (watermark read, insert, prune,
ingest log) is pushed to the default executor with ``asyncio.to_thread`` so
it never stalls the loop or the API handlers sharing it.  Because all state
mutation happens on the loop thread, :class:`IngestStatus` needs no lock;
the only lock is :attr:`IngestManager._lock`, an ``asyncio.Lock`` that
serialises start/stop and runtime source changes so two PATCH requests (or a
PATCH racing shutdown) cannot interleave poller teardown and creation.
Thread safety of the archive itself is :class:`~darpa_spillserver.storage.SpillStore`'s
responsibility.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, replace
from typing import Any, Callable, Dict, List, Optional

from .config import Config, ConfigError, Source, check_source_url
from .novatime import TICKS_PER_SECOND, nova_now
from .storage import SpillStore
from .tdu_client import HISTORY_ROUTE, TDUClient, TDUError

__all__ = ["IngestStatus", "Poller", "IngestManager", "SourceError"]

## Module logger.
log = logging.getLogger(__name__)


@dataclass
class IngestStatus:
    """!
    @brief Per-source ingest health counters.

    A snapshot of the poller's health, surfaced by ``/api/status``.

    Mutated only from the event-loop thread (by its Poller), read by API
    handlers on the same loop, so no locking is needed.  Times ending in
    ``_at`` are UNIX seconds (float); ``*_nova`` fields are NOvA 64 MHz ticks.
    """

    ## Source name this status belongs to.
    source: str = ""
    ## TDU base URL being polled.
    base_url: str = ""
    ## True while the polling task is alive.
    running: bool = False
    ## True when the TDU lacks the history route, so events are being missed.
    degraded: bool = False

    ## Route used by the last successful poll (``/spill_history`` or ``/tcr_status``).
    route: str = ""
    ## Successful polls completed.
    polls: int = 0
    ## Events returned by the TDU, overlap re-reads included.
    events_fetched: int = 0
    ## Events actually new to the archive.
    events_inserted: int = 0
    ## Failures since the last success; reset to 0 on success.
    consecutive_errors: int = 0
    ## UNIX time of the last poll attempt, successful or not.
    last_poll_at: Optional[float] = None
    ## UNIX time of the last successful poll.
    last_success_at: Optional[float] = None
    ## ``"<ExceptionType>: <message>"`` of the last failure.
    last_error: str = ""
    ## UNIX time of the last failure.
    last_error_at: Optional[float] = None
    ## Events dropped for lying more than ``ingest.max_future`` ahead.
    events_rejected: int = 0
    ## Newest event time fetched in the last non-empty poll (NOvA ticks).
    watermark_nova: Optional[int] = None
    ## NOvA time up to which the TDU has been read completely: the start of
    # the last successful poll that was not cut short by the batch limit.
    # Unlike :attr:`watermark_nova` it advances while no events arrive, so a
    # quiet beam (shutdown, a silent TDU) still counts as caught up.
    polled_through_nova: Optional[int] = None

    ## Events removed by retention sweeps since start.
    pruned_total: int = 0

    def as_dict(self) -> dict:
        """!
        @brief Render as a JSON-ready dict for ``/api/status`` and ``/api/sources``.
        @return the fields, plus ``degraded_reason`` when degraded.
        """
        data = {
            "source": self.source,
            "base_url": self.base_url,
            "running": self.running,
            "degraded": self.degraded,
            "route": self.route,
            "polls": self.polls,
            "events_fetched": self.events_fetched,
            "events_inserted": self.events_inserted,
            "events_rejected": self.events_rejected,
            "consecutive_errors": self.consecutive_errors,
            "last_poll_at": self.last_poll_at,
            "last_success_at": self.last_success_at,
            "last_error": self.last_error,
            "last_error_at": self.last_error_at,
            "watermark_nova": self.watermark_nova,
            "polled_through_nova": self.polled_through_nova,
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
    """!
    @brief Polls one TDU and writes what it finds into the archive.

    Polls one TDU and writes what it finds into the archive.

    Lifecycle: ``start()`` opens the client and spawns ``_run()`` as an
    asyncio task; ``stop()`` sets ``_stopping`` (an ``asyncio.Event``) and
    waits up to 30 s for the task before cancelling it.  The event doubles as
    an interruptible sleep between polls and as the abort signal for an
    in-flight fetch.

    @param config the merged server configuration.
    @param store archive to write into.
    @param source the TDU to poll; the first configured source if omitted.
    @param client TDU client; one is created for *source* if omitted.
    """

    def __init__(
        self,
        config: Config,
        store: SpillStore,
        source: Optional[Source] = None,
        client: Optional[TDUClient] = None,
    ) -> None:
        """!
        @brief Bind to a source and build (or adopt) its TDU client.
        @param config the merged server configuration.
        @param store archive to write into.
        @param source the TDU to poll; the first configured source if omitted.
        @param client TDU client; one is created for *source* if omitted.
        """
        self.config = config
        self.store = store
        self.source = source or config.tdu.resolved_sources()[0]
        self.status = IngestStatus(
            source=self.source.name, base_url=self.source.base_url
        )
        # Only close a client we created; an injected one (tests) belongs to
        # the caller.
        self._owns_client = client is None
        self._client = client or TDUClient(
            base_url=self.source.base_url,
            timeout=config.tdu.timeout,
            history_timeout=config.tdu.history_timeout,
            retries=config.tdu.retries,
            retry_backoff=config.tdu.retry_backoff,
            name=self.source.name,
        )
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()
        # UNIX time of the last retention sweep; 0 forces one on the first pass.
        self._last_prune = 0.0

    # -- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        """!
        @brief Open the client and launch the polling task.

        Idempotent: a second call while the task exists does nothing.
        """
        if self._task is not None:
            return
        await self._client.open()
        self._stopping.clear()
        self.status.running = True
        self._task = asyncio.create_task(
            self._run(), name="darpa-spill-ingest:" + self.source.name
        )
        log.info(
            "ingest started: polling %s (%s) every %.3gs",
            self.source.name, self.source.base_url, self.config.ingest.interval,
        )

    async def stop(self) -> None:
        """!
        @brief Ask the loop to finish the current pass, then wait for it.

        Waits at most 30 s, then cancels the task.  poll_once abandons an
        in-flight fetch as soon as the stop event is set, so the wait is
        normally bounded by an insert, not by a slow TDU.
        """
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
            "ingest from %s stopped after %d poll(s); %d event(s) inserted",
            self.source.name, self.status.polls, self.status.events_inserted,
        )

    # -- the loop -----------------------------------------------------------

    async def _run(self) -> None:
        """!
        @brief The polling loop: probe once, then poll/prune/sleep until stopped.

        Any exception from a single pass is recorded and swallowed so the
        task survives; only ``ingest.max_consecutive_errors`` (if non-zero)
        or a stop request ends it.
        """
        interval = self.config.ingest.interval

        # Probe once up front so the warning appears at startup rather than
        # buried in the first poll's debug output.
        try:
            supported = await self._client.supports_history()
            self.status.degraded = not supported
            self.status.route = HISTORY_ROUTE if supported else "/tcr_status"
        except TDUError as exc:
            # Unreachable at startup is not fatal: the first poll will retry
            # and set route/degraded from its result.
            log.warning("could not probe %s for a history route: %s",
                        self.source.name, exc)

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
                        "stopping ingest from %s after %d consecutive errors",
                        self.source.name, self.status.consecutive_errors,
                    )
                    break

            await self._maybe_prune()

            # Subtract the work already done so the period is the poll
            # interval, not the interval plus however long the poll took.
            # Waiting on the stop event (rather than asyncio.sleep) makes the
            # sleep end immediately when stop() is called.
            elapsed = time.time() - started
            try:
                await asyncio.wait_for(
                    self._stopping.wait(), timeout=max(0.0, interval - elapsed)
                )
            except asyncio.TimeoutError:
                pass

        self.status.running = False

    async def poll_once(self) -> int:
        """!
        @brief Run one ingest pass; returns the number of rows inserted.

        Steps: read the watermark, fetch from the TDU (abortable by stop),
        tag events with this source, drop future-dated events, insert,
        update counters, append to the ingest log.

        @return rows newly inserted (0 if the fetch was abandoned for a stop).
        @throws TDUError (or any other exception) from the fetch or the
            store; _run records it.
        """
        # Wall-clock start (UNIX s) for the ingest log; NOvA start for the
        # future-time ceiling and polled_through_nova.
        started_at = int(time.time())
        started_nova = nova_now()
        since = await asyncio.to_thread(self._watermark)

        # A fetch from a slow TDU can take timeout x (retries + 1) seconds,
        # longer than stop() is willing to wait. Abandoning the fetch is safe,
        # since nothing has been written yet, so a stop request cancels it at
        # once; the insert below is left to finish, so the archive, the
        # counters and the ingest log always agree.
        fetch = asyncio.ensure_future(self._client.fetch_history(
            since_nova=since,
            limit=self.config.ingest.batch_limit,
        ))
        stopping = asyncio.ensure_future(self._stopping.wait())
        try:
            # Race the fetch against the stop event.
            await asyncio.wait({fetch, stopping},
                               return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            fetch.cancel()
            raise
        finally:
            stopping.cancel()
        if not fetch.done():
            # Stop won the race.
            fetch.cancel()
            try:
                await fetch
            except (asyncio.CancelledError, Exception):
                pass
            log.debug("ingest from %s: stop requested; abandoned a fetch",
                      self.source.name)
            return 0
        # Re-raises the fetch's exception, if any, into _run.
        result = fetch.result()

        # The tag is applied here rather than trusted from the client: which
        # source an event belongs to is decided by which poller fetched it.
        for event in result.events:
            event.source = self.source.name
        ceiling = self._future_ceiling(started_nova)
        if ceiling is not None:
            kept = [e for e in result.events if e.nova_time < ceiling]
            if len(kept) != len(result.events):
                dropped = [e for e in result.events if e.nova_time >= ceiling]
                self.status.events_rejected += len(dropped)
                log.warning(
                    "ingest from %s: dropped %d event(s) stamped more than "
                    "%.0f s in the future (e.g. event %s at %d ticks); is the "
                    "TDU clock set?", self.source.name, len(dropped),
                    self.config.ingest.max_future, dropped[0].event_number,
                    dropped[0].nova_time,
                )
                result = replace(result, events=kept)
        # Idempotent insert: overlap re-reads return 0 new rows for them.
        inserted = await asyncio.to_thread(self.store.insert_events, result.events)

        self.status.polls += 1
        self.status.events_fetched += len(result.events)
        self.status.events_inserted += inserted
        self.status.consecutive_errors = 0
        self.status.last_poll_at = time.time()
        self.status.last_success_at = self.status.last_poll_at
        self.status.route = result.route
        # Re-derived every poll: the client falls back per request, so a TDU
        # can move in or out of degraded mode without a restart.
        self.status.degraded = result.route != HISTORY_ROUTE
        if result.events:
            self.status.watermark_nova = max(e.nova_time for e in result.events)
        # A history read that hit the batch limit left events behind, so it
        # covers only up to the newest one fetched; otherwise everything the
        # TDU held when the poll began has been read.
        if result.truncated and result.route == HISTORY_ROUTE:
            through = self.status.watermark_nova
        else:
            through = started_nova
        if through is not None:
            # Monotone: never move polled_through backwards.
            self.status.polled_through_nova = max(
                through, self.status.polled_through_nova or 0)

        await asyncio.to_thread(
            self.store.record_ingest,
            self.source.name, result.route, started_at, int(time.time()),
            len(result.events), inserted, None,
        )

        if inserted:
            log.debug(
                "ingest from %s: %d fetched, %d new, via %s",
                self.source.name, len(result.events), inserted, result.route,
            )
        if result.truncated and result.route == HISTORY_ROUTE:
            # The TDU had more than batch_limit waiting, so the next poll
            # should not wait a full interval before catching up.
            log.info(
                "ingest from %s: TDU reported more events than the batch limit "
                "(%d); consider raising ingest.batch_limit or lowering "
                "ingest.interval",
                self.source.name, self.config.ingest.batch_limit,
            )
        return inserted

    # -- helpers ------------------------------------------------------------

    def _future_ceiling(self, now_nova: int) -> Optional[int]:
        """!
        @brief Events at or after this tick are treated as bad clocks; None when
        ``ingest.max_future`` is 0.

        @param now_nova the current NOvA time in ticks.
        @return ``now_nova + max_future`` in ticks, or None if disabled.
        """
        if self.config.ingest.max_future <= 0:
            return None
        return now_nova + int(self.config.ingest.max_future * TICKS_PER_SECOND)

    def _watermark(self) -> int:
        """!
        @brief The NOvA tick to resume from, allowing for the configured overlap.

        Events beyond ``ingest.max_future`` (stored before that check
        existed) are ignored, so one bad clock cannot stall the source.

        Blocking (SQLite); called via ``asyncio.to_thread``.

        @return ``latest - overlap`` in ticks, or ``now - backfill`` on a cold
            start; never negative.
        """
        latest = self.store.latest(sources=[self.source.name],
                                   before=self._future_ceiling(nova_now()))
        overlap_ticks = int(self.config.ingest.overlap * TICKS_PER_SECOND)

        if latest is None:
            backfill_ticks = int(self.config.ingest.backfill * TICKS_PER_SECOND)
            start = max(0, nova_now() - backfill_ticks)
            log.info(
                "archive holds nothing from %s; backfilling %.0f s of history",
                self.source.name, self.config.ingest.backfill,
            )
            return start

        return max(0, latest.nova_time - overlap_ticks)

    def _record_error(self, exc: Exception) -> None:
        """!
        @brief Update the status counters for a failed pass and log it, rate-limited.
        @param exc the exception the pass raised.
        """
        self.status.consecutive_errors += 1
        self.status.last_error = "{}: {}".format(type(exc).__name__, exc)
        self.status.last_error_at = time.time()
        self.status.last_poll_at = self.status.last_error_at

        # Log the first failure loudly, then back off to debug so that a TDU
        # that is down overnight does not fill the disk with identical lines.
        # Every 60th consecutive failure is re-logged at ERROR as a heartbeat.
        name = self.source.name
        if self.status.consecutive_errors == 1:
            log.error("ingest poll of %s failed: %s", name, self.status.last_error)
        elif self.status.consecutive_errors % 60 == 0:
            log.error(
                "ingest poll of %s still failing after %d attempts: %s",
                name, self.status.consecutive_errors, self.status.last_error,
            )
        else:
            log.debug("ingest poll of %s failed: %s", name, self.status.last_error)

    async def _maybe_prune(self) -> None:
        """!
        @brief Apply the retention policy, at most once per prune interval.

        No-op when ``storage.retention_days`` is 0.  Also trims the ingest log
        to ``storage.ingest_log_keep`` rows.
        """
        retention_days = self.config.storage.retention_days
        if retention_days <= 0:
            return

        now = time.time()
        if now - self._last_prune < self.config.storage.prune_interval:
            return
        self._last_prune = now

        # days -> seconds -> 64 MHz ticks.
        cutoff = nova_now() - int(retention_days * 86400 * TICKS_PER_SECOND)
        if cutoff <= 0:
            return

        # Each poller prunes only its own source, so a disabled source's
        # history is kept rather than aged out by its neighbours.
        removed = await asyncio.to_thread(
            self.store.prune_before, cutoff, self.source.name
        )
        await asyncio.to_thread(
            self.store.trim_ingest_log, self.config.storage.ingest_log_keep
        )
        if removed:
            self.status.pruned_total += removed
            log.info(
                "retention: removed %d event(s) from %s older than %.3g day(s)",
                removed, self.source.name, retention_days,
            )


class SourceError(ValueError):
    """!
    @brief A runtime change to a source was refused.

    The API maps it to 404 (unknown source) or 400 (bad value).
    """


class IngestManager:
    """!
    @brief Runs one :class:`Poller` per enabled source, and applies changes.

    Runs one :class:`Poller` per enabled source, and applies changes.

    The effective sources are the configuration file's, with any runtime
    overrides from the archive's ``source_overrides`` table applied on top.
    An override stores the source's whole state (URL and enabled flag), is
    written only when that state differs from the file, and is dropped by
    :meth:`reset_source` or by a change that makes the two agree again.

    Locking: ``_lock`` (``asyncio.Lock``) is held across start, stop and
    update_source, each of which may await poller start/stop.  Readers
    (names, describe, summary, degraded_sources) are synchronous and run on
    the loop thread, so they see a consistent view without the lock.

    @param config the merged server configuration.
    @param store the archive, which also holds the overrides.
    @param client_factory builds the TDU client for a source; tests pass a
        stub.  The default makes a real :class:`TDUClient`.
    """

    def __init__(
        self,
        config: Config,
        store: SpillStore,
        client_factory: Optional[Callable[[Source], Any]] = None,
    ) -> None:
        """!
        @brief Load configured sources and apply saved runtime overrides.

        Does not start polling; see start().

        @param config the merged server configuration.
        @param store the archive, which also holds the overrides.
        @param client_factory optional ``Source -> client`` builder.
        """
        self.config = config
        self.store = store
        self._client_factory = client_factory
        # As the configuration file says; never modified at runtime.
        self.configured: Dict[str, Source] = {
            source.name: source for source in config.tdu.resolved_sources()
        }
        # Effective state: configured + overrides.
        self.sources: Dict[str, Source] = {}
        self.overrides: Dict[str, Dict[str, Any]] = {}
        self.pollers: Dict[str, Poller] = {}
        # Kept after a poller stops, so the last health stays reportable.
        self.statuses: Dict[str, IngestStatus] = {}
        self.running = False
        self._lock = asyncio.Lock()
        self._load_overrides()

    # -- effective configuration ----------------------------------------------

    def _load_overrides(self) -> None:
        """!
        @brief Read ``source_overrides`` from the archive and build ``self.sources``.

        Overrides for names no longer in the configuration file are ignored
        (with a warning) but left in the table.
        """
        rows = self.store.source_overrides()
        for name, row in rows.items():
            if name not in self.configured:
                log.warning(
                    "ignoring a saved change to source %r, which the "
                    "configuration file no longer lists", name,
                )
                continue
            self.overrides[name] = {
                "base_url": row["base_url"],
                "enabled": bool(row["enabled"]),
                "updated_at": row["updated_at"],
                "updated_by": row["updated_by"],
            }
        for name, configured in self.configured.items():
            override = self.overrides.get(name)
            if override is None:
                # Copy, so runtime edits never alias the configured object.
                self.sources[name] = replace(configured)
                continue
            self.sources[name] = Source(
                name=name,
                base_url=override["base_url"],
                enabled=override["enabled"],
            )
            log.warning(
                "source %s: using the change saved at runtime (%s, %s) "
                "instead of the configuration file (%s, enabled); reset it "
                "from /config to return to the file",
                name, override["base_url"],
                "enabled" if override["enabled"] else "disabled",
                configured.base_url,
            )

    def names(self) -> List[str]:
        """!
        @brief Names of every configured source, in configuration order.
        @return list of source names.
        """
        return list(self.sources)

    def _make_poller(self, source: Source) -> Poller:
        """!
        @brief Build a Poller for *source* and register its status object.
        @param source the effective source.
        @return the new (not yet started) poller.
        """
        client = self._client_factory(source) if self._client_factory else None
        poller = Poller(self.config, self.store, source=source, client=client)
        # A replacement poller replaces the status too, so counters restart.
        self.statuses[source.name] = poller.status
        return poller

    # -- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        """!
        @brief Start a poller for every enabled source.
        """
        async with self._lock:
            self.running = True
            for source in self.sources.values():
                if source.enabled:
                    await self._start_poller(source)
                else:
                    log.info("source %s is disabled; not polling it", source.name)

    async def stop(self) -> None:
        """!
        @brief Stop every poller concurrently and mark ingest as not running.
        """
        async with self._lock:
            self.running = False
            # list(): _stop_poller pops from self.pollers while we iterate.
            await asyncio.gather(
                *(self._stop_poller(name) for name in list(self.pollers))
            )

    async def _start_poller(self, source: Source) -> None:
        """!
        @brief Create, register and start a poller.  Caller holds ``_lock``.
        @param source the effective source.
        """
        poller = self._make_poller(source)
        self.pollers[source.name] = poller
        await poller.start()

    async def _stop_poller(self, name: str) -> None:
        """!
        @brief Unregister and stop a poller, if one is running.  Caller holds ``_lock``.
        @param name source name.
        """
        poller = self.pollers.pop(name, None)
        if poller is not None:
            await poller.stop()

    # -- runtime changes ---------------------------------------------------------

    async def update_source(
        self,
        name: str,
        base_url: Optional[str] = None,
        enabled: Optional[bool] = None,
        updated_by: str = "",
    ) -> Source:
        """!
        @brief Change a source's URL or enabled flag, and apply it immediately.

        Change a source's URL or enabled flag, and apply it immediately.

        @param name source name.
        @param base_url new URL, or None to keep the current one.
        @param enabled new enabled flag, or None to keep the current one.
        @param updated_by who made the change, stored with the override.
        @return the new effective Source.
        @throws SourceError for an unknown source, a malformed URL, or a URL
            another source already uses.
        """
        async with self._lock:
            current = self.sources.get(name)
            if current is None:
                raise SourceError(
                    "no source named {!r}; known sources: {}".format(
                        name, ", ".join(self.sources) or "(none)")
                )
            new = replace(current)
            if base_url is not None:
                try:
                    new.base_url = check_source_url(base_url, "base_url")
                except ConfigError as exc:
                    raise SourceError(str(exc)) from None
                # Two sources on one TDU would double-ingest under two names.
                for other in self.sources.values():
                    if other.name != name and other.base_url == new.base_url:
                        raise SourceError(
                            "{} is already the URL of source {!r}".format(
                                new.base_url, other.name)
                        )
            if enabled is not None:
                new.enabled = bool(enabled)

            # The configuration file cannot express a disabled source, so
            # "agrees with the file" means same URL and enabled.  In that case
            # the override row is deleted rather than stored redundantly.
            configured = self.configured[name]
            if (new.base_url, new.enabled) == (configured.base_url, True):
                await asyncio.to_thread(self.store.clear_source_override, name)
                self.overrides.pop(name, None)
            else:
                await asyncio.to_thread(
                    self.store.set_source_override,
                    name, new.base_url, new.enabled, updated_by,
                )
                self.overrides[name] = {
                    "base_url": new.base_url,
                    "enabled": new.enabled,
                    "updated_at": int(time.time()),
                    "updated_by": updated_by,
                }
            log.warning(
                "source %s changed at runtime%s: %s, %s",
                name, " by " + updated_by if updated_by else "",
                new.base_url, "enabled" if new.enabled else "disabled",
            )
            await self._apply(current, new)
            return new

    async def reset_source(self, name: str, updated_by: str = "") -> Source:
        """!
        @brief Return a source to what the configuration file says.

        Implemented as an update to the configured URL with enabled=True,
        which update_source recognises as "matches the file" and so deletes
        the override.

        @param name source name.
        @param updated_by who asked, for the log.
        @return the restored Source.
        @throws SourceError for an unknown source.
        """
        configured = self.configured.get(name)
        if configured is None:
            raise SourceError("no source named {!r}".format(name))
        return await self.update_source(
            name, base_url=configured.base_url, enabled=True,
            updated_by=updated_by,
        )

    async def _apply(self, old: Source, new: Source) -> None:
        """!
        @brief Make the running pollers match a changed source.  Caller holds ``_lock``.
        @param old the effective source before the change.
        @param new the effective source after it.
        """
        self.sources[new.name] = new
        if not self.running:
            # Ingest is off for the whole server; the change is saved and
            # takes effect when it next starts with ingest enabled.
            return
        # A new URL means a new client, so the old poller finishes its pass
        # and a fresh one takes over; its watermark carries on from the
        # source's newest archived event.
        if old.base_url != new.base_url or not new.enabled:
            await self._stop_poller(new.name)
        if new.enabled and new.name not in self.pollers:
            await self._start_poller(new)

    # -- reporting ---------------------------------------------------------------

    def degraded_sources(self, names: Optional[List[str]] = None) -> List[Source]:
        """!
        @brief Enabled sources, among *names* if given, that ingest degraded.
        @param names restrict to these source names; None or empty means all.
        @return the matching Source objects.
        """
        result = []
        for name, source in self.sources.items():
            if names and name not in names:
                continue
            status = self.statuses.get(name)
            if source.enabled and status is not None and status.degraded:
                result.append(source)
        return result

    def describe(self) -> List[Dict[str, Any]]:
        """!
        @brief Every source, its configured and effective state, and its health.
        @return one dict per source, in configuration order.
        """
        described = []
        for name, source in self.sources.items():
            configured = self.configured[name]
            override = self.overrides.get(name)
            status = self.statuses.get(name)
            described.append({
                "name": name,
                "base_url": source.base_url,
                "enabled": source.enabled,
                # Configured sources are always enabled (see update_source).
                "configured": {"base_url": configured.base_url,
                               "enabled": True},
                "overridden": override is not None,
                "updated_at": override["updated_at"] if override else None,
                "updated_by": override["updated_by"] if override else None,
                "ingest": status.as_dict() if status else {"running": False},
            })
        return described

    def summary(self) -> Dict[str, Any]:
        """!
        @brief Health across every enabled source, for ``/api/status``.

        ``running`` means every enabled source is being polled; ``degraded``
        that at least one is sampling rather than recording a full history;
        ``consecutive_errors`` is the worst of them.

        @return the ``ingest`` block of ``/api/status``.
        """
        enabled = [s for s in self.sources.values() if s.enabled]
        statuses = [self.statuses.get(s.name) for s in enabled]
        running = self.running and bool(enabled) and all(
            status is not None and status.running for status in statuses
        )
        degraded = [s.name for s in self.degraded_sources()]
        data: Dict[str, Any] = {
            "running": running,
            "degraded": bool(degraded),
            "consecutive_errors": max(
                [status.consecutive_errors for status in statuses if status]
                or [0]
            ),
            # Over all statuses, disabled sources included; counts restart
            # when a poller is replaced.
            "events_inserted": sum(
                status.events_inserted for status in self.statuses.values()
            ),
            "sources": {
                name: (self.statuses[name].as_dict()
                       if name in self.statuses else {"running": False})
                for name in self.sources
            },
        }
        failing = [
            "{}: {}".format(s.name, status.last_error)
            for s, status in zip(enabled, statuses)
            if status and status.consecutive_errors
        ]
        if failing:
            data["last_error"] = "; ".join(failing)
        if degraded:
            data["degraded_sources"] = degraded
            data["degraded_reason"] = (
                "{} {} not expose {}, so only the single newest event is "
                "readable per poll. Accelerator events arrive at roughly "
                "6-15 Hz, so {} archive is sampling, not a complete history. "
                "See contrib/tduweb/README.md.".format(
                    ", ".join(degraded),
                    "does" if len(degraded) == 1 else "do",
                    HISTORY_ROUTE,
                    "that source's" if len(degraded) == 1 else "their",
                )
            )
        return data
