"""Background ingest: copy events from the TDUs into the local archive.

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

log = logging.getLogger(__name__)


@dataclass
class IngestStatus:
    """A snapshot of the poller's health, surfaced by ``/api/status``."""

    source: str = ""
    base_url: str = ""
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
            "source": self.source,
            "base_url": self.base_url,
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
    :param source: the TDU to poll; the first configured source if omitted.
    :param client: TDU client; one is created for *source* if omitted.
    """

    def __init__(
        self,
        config: Config,
        store: SpillStore,
        source: Optional[Source] = None,
        client: Optional[TDUClient] = None,
    ) -> None:
        self.config = config
        self.store = store
        self.source = source or config.tdu.resolved_sources()[0]
        self.status = IngestStatus(
            source=self.source.name, base_url=self.source.base_url
        )
        self._owns_client = client is None
        self._client = client or TDUClient(
            base_url=self.source.base_url,
            timeout=config.tdu.timeout,
            retries=config.tdu.retries,
            retry_backoff=config.tdu.retry_backoff,
            name=self.source.name,
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
        self._task = asyncio.create_task(
            self._run(), name="darpa-spill-ingest:" + self.source.name
        )
        log.info(
            "ingest started: polling %s (%s) every %.3gs",
            self.source.name, self.source.base_url, self.config.ingest.interval,
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
            "ingest from %s stopped after %d poll(s); %d event(s) inserted",
            self.source.name, self.status.polls, self.status.events_inserted,
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
            await asyncio.wait({fetch, stopping},
                               return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            fetch.cancel()
            raise
        finally:
            stopping.cancel()
        if not fetch.done():
            fetch.cancel()
            try:
                await fetch
            except (asyncio.CancelledError, Exception):
                pass
            log.debug("ingest from %s: stop requested; abandoned a fetch",
                      self.source.name)
            return 0
        result = fetch.result()

        # The tag is applied here rather than trusted from the client: which
        # source an event belongs to is decided by which poller fetched it.
        for event in result.events:
            event.source = self.source.name
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

    def _watermark(self) -> int:
        """The NOvA tick to resume from, allowing for the configured overlap."""
        latest = self.store.latest(sources=[self.source.name])
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
        self.status.consecutive_errors += 1
        self.status.last_error = "{}: {}".format(type(exc).__name__, exc)
        self.status.last_error_at = time.time()
        self.status.last_poll_at = self.status.last_error_at

        # Log the first failure loudly, then back off to debug so that a TDU
        # that is down overnight does not fill the disk with identical lines.
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
    """A runtime change to a source was refused."""


class IngestManager:
    """Runs one :class:`Poller` per enabled source, and applies changes.

    The effective sources are the configuration file's, with any runtime
    overrides from the archive's ``source_overrides`` table applied on top.
    An override stores the source's whole state (URL and enabled flag), is
    written only when that state differs from the file, and is dropped by
    :meth:`reset_source` or by a change that makes the two agree again.

    :param config: the merged server configuration.
    :param store: the archive, which also holds the overrides.
    :param client_factory: builds the TDU client for a source; tests pass a
        stub.  The default makes a real :class:`TDUClient`.
    """

    def __init__(
        self,
        config: Config,
        store: SpillStore,
        client_factory: Optional[Callable[[Source], Any]] = None,
    ) -> None:
        self.config = config
        self.store = store
        self._client_factory = client_factory
        self.configured: Dict[str, Source] = {
            source.name: source for source in config.tdu.resolved_sources()
        }
        self.sources: Dict[str, Source] = {}
        self.overrides: Dict[str, Dict[str, Any]] = {}
        self.pollers: Dict[str, Poller] = {}
        self.statuses: Dict[str, IngestStatus] = {}
        self.running = False
        self._lock = asyncio.Lock()
        self._load_overrides()

    # -- effective configuration ----------------------------------------------

    def _load_overrides(self) -> None:
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
        return list(self.sources)

    def _make_poller(self, source: Source) -> Poller:
        client = self._client_factory(source) if self._client_factory else None
        poller = Poller(self.config, self.store, source=source, client=client)
        self.statuses[source.name] = poller.status
        return poller

    # -- lifecycle -------------------------------------------------------------

    async def start(self) -> None:
        """Start a poller for every enabled source."""
        async with self._lock:
            self.running = True
            for source in self.sources.values():
                if source.enabled:
                    await self._start_poller(source)
                else:
                    log.info("source %s is disabled; not polling it", source.name)

    async def stop(self) -> None:
        async with self._lock:
            self.running = False
            await asyncio.gather(
                *(self._stop_poller(name) for name in list(self.pollers))
            )

    async def _start_poller(self, source: Source) -> None:
        poller = self._make_poller(source)
        self.pollers[source.name] = poller
        await poller.start()

    async def _stop_poller(self, name: str) -> None:
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
        """Change a source's URL or enabled flag, and apply it immediately.

        :raises SourceError: for an unknown source, a malformed URL, or a URL
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
                for other in self.sources.values():
                    if other.name != name and other.base_url == new.base_url:
                        raise SourceError(
                            "{} is already the URL of source {!r}".format(
                                new.base_url, other.name)
                        )
            if enabled is not None:
                new.enabled = bool(enabled)

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
        """Return a source to what the configuration file says."""
        configured = self.configured.get(name)
        if configured is None:
            raise SourceError("no source named {!r}".format(name))
        return await self.update_source(
            name, base_url=configured.base_url, enabled=True,
            updated_by=updated_by,
        )

    async def _apply(self, old: Source, new: Source) -> None:
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
        """Enabled sources, among *names* if given, that ingest degraded."""
        result = []
        for name, source in self.sources.items():
            if names and name not in names:
                continue
            status = self.statuses.get(name)
            if source.enabled and status is not None and status.degraded:
                result.append(source)
        return result

    def describe(self) -> List[Dict[str, Any]]:
        """Every source, its configured and effective state, and its health."""
        described = []
        for name, source in self.sources.items():
            configured = self.configured[name]
            override = self.overrides.get(name)
            status = self.statuses.get(name)
            described.append({
                "name": name,
                "base_url": source.base_url,
                "enabled": source.enabled,
                "configured": {"base_url": configured.base_url,
                               "enabled": True},
                "overridden": override is not None,
                "updated_at": override["updated_at"] if override else None,
                "updated_by": override["updated_by"] if override else None,
                "ingest": status.as_dict() if status else {"running": False},
            })
        return described

    def summary(self) -> Dict[str, Any]:
        """Health across every enabled source, for ``/api/status``.

        ``running`` means every enabled source is being polled; ``degraded``
        that at least one is sampling rather than recording a full history;
        ``consecutive_errors`` is the worst of them.
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
