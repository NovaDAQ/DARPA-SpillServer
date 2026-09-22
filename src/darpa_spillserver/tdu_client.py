"""HTTP client for the TDU's embedded bottle server (``TDUWeb``).

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

**Malformed JSON.** ``DumpSpillHistory``'s bulk mode emits
``printf("{\\nEvents: [\\n")`` --- an unquoted key, which is not valid JSON.
:func:`loads_tolerant` repairs that shape rather than failing, so the client
works against TDUs running the unpatched utility.
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
from .storage import UNKNOWN_SIGNAL, SpillEvent

__all__ = [
    "TDUError",
    "TDUUnavailable",
    "TDUResponseError",
    "TDUClient",
    "loads_tolerant",
    "LEGACY_ROUTES",
    "HISTORY_ROUTE",
]

log = logging.getLogger(__name__)

#: Routes that return a single most-recent event.
LEGACY_ROUTES = ("/tcr_status", "/onehz_status")

#: The bulk-history route added by ``contrib/tduweb``.
HISTORY_ROUTE = "/spill_history"

#: Matches a bare (unquoted) object key, as emitted by DumpSpillHistory.
_BARE_KEY_RE = re.compile(r"(?P<prefix>[{,]\s*)(?P<key>[A-Za-z_][A-Za-z0-9_]*)\s*:")

#: Matches a trailing comma before a closing bracket or brace.
_TRAILING_COMMA_RE = re.compile(r",\s*(?P<close>[}\]])")


class TDUError(RuntimeError):
    """Base class for TDU communication failures."""


class TDUUnavailable(TDUError):
    """The TDU could not be reached, or did not answer in time."""


class TDUResponseError(TDUError):
    """The TDU answered, but the body could not be understood."""


def loads_tolerant(text: str) -> Any:
    """Parse JSON, repairing the malformations ``DumpSpillHistory`` emits.

    Strict parsing is tried first, so well-formed input is never touched.  On
    failure two repairs are applied --- quoting bare object keys and dropping
    trailing commas --- and parsing is retried once.

    >>> loads_tolerant('{ Events: [ {"Type": 4}, ] }')
    {'Events': [{'Type': 4}]}
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

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
    """Return the first present key among *names*, coerced to int."""
    for name in names:
        if name in record and record[name] is not None:
            try:
                return int(record[name])
            except (TypeError, ValueError):
                return None
    return None


def event_from_record(record: Dict[str, Any], source: str) -> Optional[SpillEvent]:
    """Build a :class:`SpillEvent` from one TDU JSON record.

    Accepts both the legacy capitalised keys (``Type``, ``Time``, ``Number``)
    and the lower-case keys used by the history route, and tolerates either
    supplying the raw event word.  Returns ``None`` for a record with no
    usable timestamp, so a single malformed entry cannot abort a whole batch.
    """
    nova_time = _as_int(record, "Time", "time", "nova_time", "novatime")
    if nova_time is None:
        log.warning("dropping TDU record with no timestamp: %r", record)
        return None

    event_word = _as_int(record, "Event", "event", "event_word", "evt", "Evt")

    spill_type_value = _as_int(record, "Type", "type", "spill_type")
    if event_word is not None:
        # The raw word is authoritative: decoding it recovers the exact signal,
        # which the stored SpillType alone cannot distinguish.
        spill_type = decode_event_word(event_word)
        signal_code = event_word & 0xFF
        if signal_for_code(signal_code) is None:
            signal_code = UNKNOWN_SIGNAL
    elif spill_type_value is not None:
        try:
            spill_type = SpillType(spill_type_value)
        except ValueError:
            log.warning("dropping TDU record with unknown type %r", spill_type_value)
            return None
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
        ingested_at=int(time.time()),
    )


@dataclass
class FetchResult:
    """Outcome of one fetch from the TDU."""

    events: List[SpillEvent]
    route: str
    """The route that actually answered, after any fallback."""

    truncated: bool = False
    """True when the TDU said it had more events than it returned."""

    raw_count: int = 0
    """Records received before any were dropped as unusable."""


class TDUClient:
    """Async client for one TDU's bottle server.

    :param base_url: e.g. ``http://tdu-near-master-ppc-01:8080``.
    :param timeout: per-request timeout in seconds.
    :param retries: how many times to retry a failed request.
    :param retry_backoff: seconds to wait before the first retry; each
        subsequent retry doubles it.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        retries: int = 2,
        retry_backoff: float = 0.5,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.timeout = timeout
        self.retries = max(0, int(retries))
        self.retry_backoff = retry_backoff
        self._client = client
        self._owns_client = client is None
        self._history_supported: Optional[bool] = None

    async def __aenter__(self) -> "TDUClient":
        await self.open()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.close()

    async def open(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
            self._owns_client = True

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("TDUClient used before open(); use 'async with'")
        return self._client

    # -- low-level ----------------------------------------------------------

    async def _get(self, route: str, params: Optional[Dict[str, Any]] = None) -> httpx.Response:
        """GET *route*, retrying transient failures with a growing backoff."""
        import asyncio

        url = urljoin(self.base_url, route.lstrip("/"))
        delay = self.retry_backoff
        last_error: Optional[Exception] = None

        for attempt in range(self.retries + 1):
            try:
                response = await self.client.get(url, params=params)
            except httpx.HTTPError as exc:
                last_error = exc
                log.debug("TDU request to %s failed (attempt %d): %s",
                          url, attempt + 1, exc)
            else:
                # A 404 is a definite answer -- the route is absent -- so it is
                # returned rather than retried; the caller decides what to do.
                if response.status_code == 404 or response.is_success:
                    return response
                last_error = TDUResponseError(
                    "{} returned HTTP {}".format(url, response.status_code)
                )
                log.debug("TDU request to %s returned %d (attempt %d)",
                          url, response.status_code, attempt + 1)

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
        """Return True if the TDU answers a cheap request."""
        try:
            response = await self._get("/tcr_running")
        except TDUError:
            return False
        return response.is_success

    async def fetch_latest(self, route: str = "/tcr_status") -> Optional[SpillEvent]:
        """Fetch the single most recent event via a legacy route."""
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
        return event_from_record(payload, source=route.lstrip("/"))

    async def supports_history(self) -> bool:
        """Whether this TDU exposes the bulk-history route.

        The answer is probed once and cached; a TDU is not upgraded underneath
        a running server without a restart.
        """
        if self._history_supported is None:
            try:
                response = await self._get(HISTORY_ROUTE, params={"limit": 1})
            except TDUError:
                self._history_supported = False
            else:
                self._history_supported = response.status_code != 404
            if not self._history_supported:
                log.warning(
                    "TDU at %s has no %s route; falling back to %s, which "
                    "returns only the newest event and will miss most of them. "
                    "See contrib/tduweb/README.md to enable history.",
                    self.base_url, HISTORY_ROUTE, LEGACY_ROUTES[0],
                )
        return self._history_supported

    async def fetch_history(
        self,
        since_nova: Optional[int] = None,
        until_nova: Optional[int] = None,
        limit: int = 5000,
        signal_codes: Optional[Sequence[int]] = None,
    ) -> FetchResult:
        """Fetch a window of events, falling back to the legacy route.

        :param since_nova: lower bound in NOvA ticks, inclusive.
        :param until_nova: upper bound in NOvA ticks, exclusive.
        :param limit: maximum records to request.
        :param signal_codes: restrict to these signal low-bytes, if the TDU
            supports filtering; ignored on the fallback path.
        """
        if await self.supports_history():
            params: Dict[str, Any] = {"limit": int(limit)}
            if since_nova is not None:
                params["since"] = int(since_nova)
            if until_nova is not None:
                params["until"] = int(until_nova)
            if signal_codes:
                params["signals"] = ",".join(
                    "{:02x}".format(code) for code in signal_codes
                )

            response = await self._get(HISTORY_ROUTE, params=params)
            payload = loads_tolerant(response.text)
            records, truncated = _extract_records(payload)
            events = [
                event
                for event in (
                    event_from_record(record, source="spill_history")
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

        event = await self.fetch_latest(LEGACY_ROUTES[0])
        events = [event] if event is not None else []
        return FetchResult(
            events=events,
            route=LEGACY_ROUTES[0],
            truncated=True,  # by construction: one event of many
            raw_count=len(events),
        )

    async def raw(self, route: str) -> str:
        """Return a route's body verbatim, for diagnostics and pass-through."""
        response = await self._get(route)
        if response.status_code == 404:
            raise TDUResponseError("the TDU has no route {}".format(route))
        return response.text


def _extract_records(payload: Any) -> "tuple[List[Dict[str, Any]], bool]":
    """Pull the event list out of any of the shapes the TDU may return."""
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
