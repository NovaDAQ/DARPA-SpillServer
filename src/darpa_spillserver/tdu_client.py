"""!
@file src/darpa_spillserver/tdu_client.py
@brief Async HTTP client for a TDU's TDUWeb bottle server: history and legacy routes, tolerant JSON.

HTTP client for the TDU's embedded bottle server (``TDUWeb``).

The TDU runs a very small bottle application that shells out to the
``SHM_Utilities`` tools and returns their stdout.  Two generations of that
server are in play and this client speaks to both:

**Legacy routes** --- ``/tcr_status`` and ``/onehz_status`` --- run
``DumpSpillHistory --last`` and return exactly one event, the newest in the
shared-memory ring.  Note that ``--last`` in ``DumpSpillHistory.cc`` indexes
``data_entries - 1`` directly and never applies the ``--tcr`` / ``--onehertz``
filters, so **both routes return the same record**.  They are useful for a
liveness check and for "what just happened", but they cannot yield a history:
events arrive at roughly 6--15 Hz, so any poll interval drops most of them.

**History route** --- ``/spill_history`` --- is the addition proposed in
``contrib/tduweb/``.  It returns a window of events with their raw event words
intact, which is what makes real backfill and per-signal queries possible.
:meth:`TDUClient.fetch_history` degrades to the legacy route automatically when
the TDU has not been upgraded, so this server runs against either.

**Empty replies.** ``/tcr_status`` occasionally answers ``200`` with an empty
body.  It happens on both generations of ``tdu_webserver`` and clears on a
retry, so it originates below them -- in ``DumpSpillHistory`` or the
shared-memory read it performs.  :meth:`TDUClient._get` retries it rather than
failing the poll.

**Malformed JSON.** ``DumpSpillHistory``'s bulk mode emits
``printf("{\\nEvents: [\\n")`` --- an unquoted key, which is not valid JSON.
:func:`loads_tolerant` repairs that shape rather than failing, so the client
works against TDUs running the unpatched utility.

Protocol summary (all plain HTTP GET, JSON bodies):

- ``/spill_history_info`` --- cheap capability probe; any non-404 answer
  means the history route exists.
- ``/spill_history?limit=N&since=T&until=T&signals=hh,hh`` --- window of
  events; ``since``/``until`` in NOvA 64 MHz ticks (half-open), ``signals``
  as two-digit hex low bytes.  Body is a list, or an object with an
  ``events``/``Events`` list and an optional ``truncated`` flag.
- ``/tcr_status``, ``/onehz_status`` --- one record, the newest.
- ``/tcr_running`` --- liveness only.

A record carries ``Time`` (NOvA ticks), and either ``Event`` (raw 16-bit event
word; preferred) or ``Type`` (decoded SpillType), plus optional ``Number``,
``Delta`` and ``Offset``.  Key spelling varies by generation; see
:func:`event_from_record`.

The client is coroutine-based (httpx.AsyncClient) and is used from a single
event loop; it holds no locks.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urljoin

import httpx

from .signals import SpillType, decode_event_word, signal_for_code
from .storage import UNKNOWN_SIGNAL, UNKNOWN_SOURCE, SpillEvent

__all__ = [
    "TDUError",
    "TDUUnavailable",
    "TDUResponseError",
    "TDUClient",
    "loads_tolerant",
    "LEGACY_ROUTES",
    "HISTORY_ROUTE",
]

## Module logger.
log = logging.getLogger(__name__)

## Routes that return a single most-recent event.
LEGACY_ROUTES = ("/tcr_status", "/onehz_status")

## The bulk-history route added by ``contrib/tduweb``.
HISTORY_ROUTE = "/spill_history"

## Capability endpoint beside it.  Answering it costs the TDU nothing, which
## is the whole point: it reports whether the history route exists without
## walking the ring to prove it.
HISTORY_INFO_ROUTE = "/spill_history_info"

## Seconds before an inconclusive history probe is tried again.  A definite
## answer -- a 404, or a successful reply -- is kept; an error or a timeout is
## not, because it says nothing about whether the route exists.
HISTORY_PROBE_RETRY = 120.0

## Matches a bare (unquoted) object key, as emitted by DumpSpillHistory.
_BARE_KEY_RE = re.compile(r"(?P<prefix>[{,]\s*)(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*:")

## Matches a trailing comma before a closing bracket or brace.
_TRAILING_COMMA_RE = re.compile(r",\s*(?P<close>[}\]])")


class TDUError(RuntimeError):
    """!
    @brief Base class for TDU communication failures.
    """


class TDUUnavailable(TDUError):
    """!
    @brief The TDU could not be reached, or did not answer in time.

    Raised after all retries are exhausted, whatever the last failure was
    (transport error, non-2xx status, or empty 200 body).
    """


class TDUResponseError(TDUError):
    """!
    @brief The TDU answered, but the body could not be understood.
    """


def loads_tolerant(text: str) -> Any:
    """!
    @brief Parse JSON, repairing the malformations ``DumpSpillHistory`` emits.

    Strict parsing is tried first, so well-formed input is never touched.  On
    failure two repairs are applied --- quoting bare object keys and dropping
    trailing commas --- and parsing is retried once.

    >>> loads_tolerant('{ Events: [ {"Type": 4}, ] }')
    {'Events': [{'Type': 4}]}

    @param text the response body.
    @return the decoded JSON value.
    @throws TDUResponseError if the text is not JSON even after repair.
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # The repairs are regex-based, not a parser: they would also rewrite a
    # "{x:" or ", x:" sequence inside a string value.  Acceptable because
    # event records carry numeric values, and the strict parse runs first.
    repaired = _BARE_KEY_RE.sub(
        lambda m: '{}"{}":'.format(m.group("prefix"), m.group("key")), text
    )
    repaired = _TRAILING_COMMA_RE.sub(lambda m: m.group("close"), repaired)

    try:
        return json.loads(repaired)
    except json.JSONDecodeError as exc:
        excerpt = text[:200].replace("\n", " ")
        raise TDUResponseError(
            "could not parse the TDU response as JSON ({}); body began: {!r}".format(
                exc, excerpt
            )
        ) from exc


def _as_int(record: Dict[str, Any], *names: str) -> Optional[int]:
    """!
    @brief Return the first present key among *names*, coerced to int.

    Only the first present, non-null key is considered: if it does not
    coerce, None is returned without trying later names.

    @param record one TDU JSON record.
    @param names candidate key spellings, in priority order.
    @return the integer value, or None.
    """
    for name in names:
        if name in record and record[name] is not None:
            try:
                return int(record[name])
            except (TypeError, ValueError):
                return None
    return None


def event_from_record(
    record: Dict[str, Any],
    route: str,
    source: str = UNKNOWN_SOURCE,
) -> Optional[SpillEvent]:
    """!
    @brief Build a :class:`SpillEvent` from one TDU JSON record.

    *route* is the TDU route the record came from and *source* the name of
    the TDU; both are stored with the event.

    Accepts both the legacy capitalised keys (``Type``, ``Time``, ``Number``)
    and the lower-case keys used by the history route, and tolerates either
    supplying the raw event word.  Returns ``None`` for a record with no
    usable timestamp, so a single malformed entry cannot abort a whole batch.

    @param record one decoded JSON object.
    @param route route name stored on the event (without leading slash).
    @param source source name stored on the event.
    @return the event, or None if the record is unusable.
    """
    # NOvA time: 64 MHz ticks since the NOvA epoch.
    nova_time = _as_int(record, "Time", "time", "nova_time", "novatime")
    if nova_time is None:
        log.warning("dropping TDU record with no timestamp: %r", record)
        return None

    event_word = _as_int(record, "Event", "event", "event_word", "evt", "Evt")

    spill_type_value = _as_int(record, "Type", "type", "spill_type")
    if event_word is not None:
        # The raw word is authoritative: decoding it recovers the exact signal,
        # which the stored SpillType alone cannot distinguish.
        # A supplied Type is ignored in this branch, even if it disagrees.
        spill_type = decode_event_word(event_word)
        # Low byte is the signal code ($74, $8F, ...); high bits carry the
        # carrier and parity.
        signal_code = event_word & 0xFF
        if signal_for_code(signal_code) is None:
            signal_code = UNKNOWN_SIGNAL
    elif spill_type_value is not None:
        try:
            spill_type = SpillType(spill_type_value)
        except ValueError:
            log.warning("dropping TDU record with unknown type %r", spill_type_value)
            return None
        # Type alone is many-to-one; the exact signal is unrecoverable.
        signal_code = UNKNOWN_SIGNAL
    else:
        log.warning("dropping TDU record with neither event word nor type: %r", record)
        return None

    return SpillEvent(
        nova_time=nova_time,
        spill_type=spill_type,
        signal_code=signal_code,
        event_word=event_word,
        event_number=_as_int(record, "Number", "number", "event_number"),
        delta=_as_int(record, "Delta", "delta"),
        pps_offset=_as_int(record, "Offset", "offset", "pps_offset"),
        source=source,
        route=route,
        ingested_at=int(time.time()),
    )


@dataclass
class FetchResult:
    """!
    @brief Outcome of one fetch from the TDU.
    """

    ## Usable events, in the order the TDU returned them.
    events: List[SpillEvent]
    ## The route that actually answered, after any fallback.
    route: str

    ## True when the TDU said it had more events than it returned.
    truncated: bool = False

    ## Records received before any were dropped as unusable.
    raw_count: int = 0


class TDUClient:
    """!
    @brief Async client for one TDU's bottle server.

    Async client for one TDU's bottle server.

    Usable as ``async with TDUClient(...) as c:`` or via explicit
    open()/close().  Caches the history-route capability per instance.

    @param base_url e.g. ``http://tdu-near-master-ppc-01:8080``.
    @param name the source name events from this TDU are tagged with.
    @param timeout per-request timeout in seconds.
    @param history_timeout timeout in seconds for a bulk history read.
    @param retries how many times to retry a failed request.
    @param retry_backoff seconds to wait before the first retry; each
        subsequent retry doubles it.
    @param client an existing httpx.AsyncClient to use (not closed by us).
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        history_timeout: float = 600.0,
        retries: int = 2,
        retry_backoff: float = 0.5,
        client: Optional[httpx.AsyncClient] = None,
        name: str = UNKNOWN_SOURCE,
    ) -> None:
        """!
        @brief Store settings; no connection is made until open().
        @param base_url TDU base URL.
        @param timeout per-request timeout in seconds.
        @param history_timeout timeout for a bulk history read, in seconds.
        @param retries retries per failed request (negative treated as 0).
        @param retry_backoff initial retry delay in seconds, doubled each retry.
        @param client optional shared httpx.AsyncClient.
        @param name source name for event tagging and log lines.
        """
        # Trailing slash so urljoin appends routes instead of replacing the
        # last path segment.
        self.base_url = base_url.rstrip("/") + "/"
        self.name = name
        self.timeout = timeout
        self.history_timeout = history_timeout
        self.retries = max(0, int(retries))
        self.retry_backoff = retry_backoff
        self._client = client
        self._owns_client = client is None
        # Tri-state: None = unknown, True/False = definite probe answer.
        self._history_supported: Optional[bool] = None
        # UNIX time of the last probe attempt; 0 = never probed.
        self._history_probed_at: float = 0.0

    async def __aenter__(self) -> "TDUClient":
        """!
        @brief Open the client for an ``async with`` block.
        @return self.
        """
        await self.open()
        return self

    async def __aexit__(self, *exc_info) -> None:
        """!
        @brief Close the client at the end of an ``async with`` block.
        @param exc_info exception triple, ignored.
        """
        await self.close()

    async def open(self) -> None:
        """!
        @brief Create the underlying httpx.AsyncClient if none was supplied.
        """
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
            self._owns_client = True

    async def close(self) -> None:
        """!
        @brief Close the underlying client, only if this instance created it.
        """
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        """!
        @brief The open httpx client.
        @return the AsyncClient.
        @throws RuntimeError if open() has not been called.
        """
        if self._client is None:
            raise RuntimeError("TDUClient used before open(); use 'async with'")
        return self._client

    # -- low-level ----------------------------------------------------------

    async def _get(
        self,
        route: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> httpx.Response:
        """!
        @brief GET *route*, retrying transient failures with a growing backoff.

        *timeout* overrides the client default for this request. A bulk
        history read is a different kind of work from a status read --- it
        walks the ring and can take minutes on the TDU --- so it gets its own
        budget rather than being judged against the one sized for a register
        read.

        Retry policy: transport errors, non-2xx statuses other than 404, and
        empty 200 bodies are retried up to ``retries`` times with delays
        ``retry_backoff * 2**k``.  A 404 or a non-empty 2xx returns at once.
        Worst-case wall time is therefore about
        ``(retries + 1) * timeout + retry_backoff * (2**retries - 1)``.

        @param route path relative to base_url, with or without leading slash.
        @param params query parameters.
        @param timeout per-request timeout override in seconds.
        @return the response (2xx with a body, or 404).
        @throws TDUUnavailable when every attempt failed.
        """
        import asyncio

        url = urljoin(self.base_url, route.lstrip("/"))
        delay = self.retry_backoff
        last_error: Optional[Exception] = None

        for attempt in range(self.retries + 1):
            try:
                response = await self.client.get(
                    url, params=params,
                    timeout=timeout if timeout is not None else self.timeout,
                )
            except httpx.HTTPError as exc:
                last_error = exc
                log.debug("TDU request to %s failed (attempt %d): %s",
                          url, attempt + 1, exc)
            else:
                # A 404 is a definite answer -- the route is absent -- so it is
                # returned rather than retried; the caller decides what to do.
                if response.status_code == 404:
                    return response

                if response.is_success:
                    # The TDU occasionally answers 200 with nothing in the
                    # body. It happens on both the old and the new
                    # tdu_webserver, so it originates below them, in
                    # DumpSpillHistory or the shared-memory read it does --
                    # and it clears on a retry a moment later. Treating it as
                    # a parse failure threw away an entire poll for what is a
                    # transient blip, so it is retried like any other
                    # transient fault.
                    if response.text.strip():
                        return response
                    last_error = TDUResponseError(
                        "{} returned HTTP 200 with an empty body".format(url)
                    )
                    log.debug("TDU request to %s returned an empty body "
                              "(attempt %d)", url, attempt + 1)
                else:
                    last_error = TDUResponseError(
                        "{} returned HTTP {}".format(url, response.status_code)
                    )
                    log.debug("TDU request to %s returned %d (attempt %d)",
                              url, response.status_code, attempt + 1)

            # No sleep after the final attempt.
            if attempt < self.retries:
                await asyncio.sleep(delay)
                delay *= 2

        raise TDUUnavailable(
            "could not fetch {} after {} attempt(s): {}".format(
                url, self.retries + 1, last_error
            )
        )

    # -- high-level ---------------------------------------------------------

    async def ping(self) -> bool:
        """!
        @brief Return True if the TDU answers a cheap request.

        Uses ``/tcr_running``; a 404 counts as not answering.

        @return True on a 2xx reply.
        """
        try:
            response = await self._get("/tcr_running")
        except TDUError:
            return False
        return response.is_success

    async def fetch_latest(self, route: str = "/tcr_status") -> Optional[SpillEvent]:
        """!
        @brief Fetch the single most recent event via a legacy route.
        @param route a legacy single-event route.
        @return the event, or None if the record was unusable.
        @throws TDUResponseError if the route is absent or the body is not a
            JSON object.
        @throws TDUUnavailable if the TDU did not answer.
        """
        response = await self._get(route)
        if response.status_code == 404:
            raise TDUResponseError("the TDU has no route {}".format(route))

        payload = loads_tolerant(response.text)
        if not isinstance(payload, dict):
            raise TDUResponseError(
                "expected a JSON object from {}, got {}".format(
                    route, type(payload).__name__
                )
            )
        # Stored route names carry no leading slash.
        return event_from_record(payload, route=route.lstrip("/"),
                                 source=self.name)

    async def supports_history(self) -> bool:
        """!
        @brief Whether this TDU exposes the bulk-history route.

        Asked of ``/spill_history_info``, which exists to answer exactly this
        and costs the TDU nothing.  The obvious probe --- fetching
        ``/spill_history?limit=1`` --- is not cheap at all: it walks the whole
        ring, takes a minute or two on the hardware, and so times out against
        any sensible request timeout.  Reading that timeout as "the route is
        absent" is how a working TDU came to be reported as having no history
        route at all.

        A 404 on the info route means the TDU predates it, so the older probe
        is tried before concluding anything.

        Only a definite answer is remembered.  An error or a timeout says
        nothing about whether the route exists, so it is retried after
        :data:`HISTORY_PROBE_RETRY` seconds rather than condemning the source
        to degraded mode for the life of the process.

        Called on every poll by fetch_history; the cache makes that free once
        a definite answer exists.  A definite "no" is kept for the life of the
        client, so upgrading a TDU in place needs the source restarted (e.g. a
        PATCH that disables and re-enables it).

        @return True if the history route is known to exist; False if it is
            absent or not yet determined.
        """
        import asyncio  # noqa: F401  (kept for symmetry with _get)

        now = time.time()
        if self._history_supported is not None:
            return self._history_supported
        # Inside the retry window after an inconclusive probe: report
        # "unavailable" without contacting the TDU.
        if now - self._history_probed_at < HISTORY_PROBE_RETRY and self._history_probed_at:
            return False

        self._history_probed_at = now
        supported: Optional[bool] = None

        try:
            response = await self._get(HISTORY_INFO_ROUTE)
        except TDUError as exc:
            log.debug("%s probe of %s failed: %s", self.name, HISTORY_INFO_ROUTE, exc)
        else:
            if response.status_code != 404:
                supported = True
            else:
                # Older builds have the history route but not the info route,
                # so fall back to asking for the smallest possible window.
                # Uses the short timeout, so on such a build this probe can
                # time out and stay inconclusive (see above).
                try:
                    older = await self._get(HISTORY_ROUTE, params={"limit": 1})
                except TDUError as exc:
                    log.debug("%s probe of %s failed: %s",
                              self.name, HISTORY_ROUTE, exc)
                else:
                    supported = older.status_code != 404

        if supported is None:
            # Inconclusive: the TDU did not answer either probe. Do not record
            # it, so the next poll tries again once the retry interval passes.
            log.warning(
                "%s: could not determine whether %s exposes %s -- neither probe "
                "answered. Treating it as unavailable for now and retrying in "
                "%.0f s.",
                self.name, self.base_url, HISTORY_ROUTE, HISTORY_PROBE_RETRY,
            )
            return False

        self._history_supported = supported
        if not supported:
            log.warning(
                "TDU at %s has no %s route; falling back to %s, which "
                "returns only the newest event and will miss most of them. "
                "See contrib/tduweb/README.md to enable history.",
                self.base_url, HISTORY_ROUTE, LEGACY_ROUTES[0],
            )
        return supported

    async def fetch_history(
        self,
        since_nova: Optional[int] = None,
        until_nova: Optional[int] = None,
        limit: int = 5000,
        signal_codes: Optional[Sequence[int]] = None,
    ) -> FetchResult:
        """!
        @brief Fetch a window of events, falling back to the legacy route.

        Fetch a window of events, falling back to the legacy route.

        @param since_nova lower bound in NOvA ticks, inclusive.
        @param until_nova upper bound in NOvA ticks, exclusive.
        @param limit maximum records to request.
        @param signal_codes restrict to these signal low-bytes, if the TDU
            supports filtering; ignored on the fallback path.
        @return the events and the route that served them.  On the fallback
            path ``truncated`` is always True.
        @throws TDUUnavailable if the TDU did not answer.
        @throws TDUResponseError if the body cannot be parsed.
        """
        if await self.supports_history():
            params: Dict[str, Any] = {"limit": int(limit)}
            if since_nova is not None:
                params["since"] = int(since_nova)
            if until_nova is not None:
                params["until"] = int(until_nova)
            if signal_codes:
                # Two-digit lower-case hex low bytes, comma-separated.
                params["signals"] = ",".join(
                    "{:02x}".format(code) for code in signal_codes
                )

            # Long timeout: the TDU walks its whole ring to answer.
            response = await self._get(
                HISTORY_ROUTE, params=params, timeout=self.history_timeout)
            payload = loads_tolerant(response.text)
            records, truncated = _extract_records(payload)
            # Unusable records are dropped individually (event_from_record
            # logs and returns None) so one bad entry cannot lose the batch.
            events = [
                event
                for event in (
                    event_from_record(record, route="spill_history",
                                      source=self.name)
                    for record in records
                )
                if event is not None
            ]
            return FetchResult(
                events=events,
                route=HISTORY_ROUTE,
                truncated=truncated,
                raw_count=len(records),
            )

        # Degraded path: one event regardless of since/until/limit.
        event = await self.fetch_latest(LEGACY_ROUTES[0])
        events = [event] if event is not None else []
        return FetchResult(
            events=events,
            route=LEGACY_ROUTES[0],
            truncated=True,  # by construction: one event of many
            raw_count=len(events),
        )

    async def raw(self, route: str) -> str:
        """!
        @brief Return a route's body verbatim, for diagnostics and pass-through.
        @param route TDU route.
        @return the response text.
        @throws TDUResponseError if the route is absent.
        @throws TDUUnavailable if the TDU did not answer.
        """
        response = await self._get(route)
        if response.status_code == 404:
            raise TDUResponseError("the TDU has no route {}".format(route))
        return response.text


def _extract_records(payload: Any) -> "tuple[List[Dict[str, Any]], bool]":
    """!
    @brief Pull the event list out of any of the shapes the TDU may return.

    Accepted shapes: a bare list of records; an object holding the list under
    ``events``/``Events``/``records``/``Records`` with an optional
    ``truncated``/``Truncated`` flag; or a single record object.  Non-dict
    list entries are skipped.

    @param payload decoded JSON.
    @return ``(records, truncated)``.
    @throws TDUResponseError if no event list can be found.
    """
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)], False

    if isinstance(payload, dict):
        for key in ("events", "Events", "records", "Records"):
            if key in payload and isinstance(payload[key], list):
                records = [r for r in payload[key] if isinstance(r, dict)]
                truncated = bool(
                    payload.get("truncated", payload.get("Truncated", False))
                )
                return records, truncated
        # A bare single-event object, as the legacy routes return.
        if any(k in payload for k in ("Time", "time", "nova_time")):
            return [payload], False

    raise TDUResponseError(
        "the TDU response contained no recognisable event list "
        "(got {})".format(type(payload).__name__)
    )
