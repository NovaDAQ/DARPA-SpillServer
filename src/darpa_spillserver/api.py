"""The HTTP API.

Design.md asks for a structured data table selectable by time range and by
accelerator event number in the operators' hex notation (``$74``, ``$8f``),
returned as CSV or JSON, with timestamps available in GPS and UTC.  That is
what :func:`build_router` assembles.

Three decisions shape the routes:

**One query endpoint, two renderings.**  ``/api/events`` answers in JSON or
CSV depending on ``?format=`` (or the ``Accept`` header).  Keeping a single
endpoint means the filtering semantics cannot diverge between the format a
person tries in a browser and the one their script asks for.

**Results are streamed.**  A range query can match millions of rows, so both
renderings are served from a generator over
:meth:`~darpa_spillserver.storage.SpillStore.iter_query` rather than a list.

**Caveats travel with the data.**  When a query cannot be answered exactly ---
because the stored records lack raw event words, or because ingest is running
degraded against a TDU with no history route --- the response says so in its
metadata (and in CSV, in a comment header).  A client that saves the file
keeps the caveat with it.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from .config import Config
from .formats import COLUMNS, ambiguity_note, event_row, iter_csv, iter_json
from .novatime import UTC, NovaTimeError, convert, get_timezone, nova_now, parse_instant, parse_range
from .signals import (
    SIGNALS,
    SpillType,
    describe_type,
    parse_signal,
    parse_spill_type,
    signals_for_type,
)
from .storage import SpillStore

__all__ = ["build_router"]

log = logging.getLogger(__name__)


def _error(status: int, message: str, hint: Optional[str] = None) -> HTTPException:
    """Build an HTTPException whose body explains how to fix the problem."""
    detail: Dict[str, Any] = {"error": message}
    if hint:
        detail["hint"] = hint
    return HTTPException(status_code=status, detail=detail)


def build_router(
    config: Config,
    store: SpillStore,
    state: Any,
) -> APIRouter:
    """Assemble the API router.

    :param config: merged server configuration.
    :param store: the event archive.
    :param state: the application state object, carrying ``poller``,
        ``authenticator`` and ``started_at``.
    """
    router = APIRouter(prefix="/api", tags=["spills"])
    authenticator = state.authenticator

    async def current_user(request: Request):
        return await authenticator.require_user(request)

    # ---------------------------------------------------------------- health

    @router.get("/health", summary="Liveness check", tags=["status"])
    async def health() -> Dict[str, Any]:
        """Return OK if the process is serving. Never requires authentication."""
        return {"status": "ok", "time": datetime.now(tz=UTC).isoformat()}

    @router.get("/status", summary="Server and ingest status", tags=["status"])
    async def status(request: Request) -> Dict[str, Any]:
        """Report configuration, archive extent and ingest health.

        Deliberately readable without authentication so that monitoring can
        scrape it, but it carries no event data and no secrets.
        """
        import asyncio

        earliest = await asyncio.to_thread(store.earliest)
        latest = await asyncio.to_thread(store.latest)
        total = await asyncio.to_thread(store.count)
        by_type = await asyncio.to_thread(store.counts_by_type)

        poller = getattr(state, "poller", None)
        return {
            "status": "ok",
            "version": state.version,
            "started_at": state.started_at,
            "uptime_seconds": round(time.time() - state.started_at, 1),
            "config_source": config.source or "(defaults)",
            "tdu": {"base_url": config.tdu.base_url},
            "auth": authenticator.describe(),
            "archive": {
                "path": store.path,
                "events": total,
                "earliest": convert(earliest.nova_time).as_dict() if earliest else None,
                "latest": convert(latest.nova_time).as_dict() if latest else None,
                "by_type": {
                    spill_type.name: count for spill_type, count in sorted(
                        by_type.items(), key=lambda item: item[0].value
                    )
                },
                "retention_days": config.storage.retention_days or None,
            },
            "ingest": poller.status.as_dict() if poller else {"running": False},
            "query_defaults": {
                "timezone": config.query.timezone,
                "default_limit": config.query.default_limit,
                "max_limit": config.query.max_limit,
            },
        }

    # -------------------------------------------------------------- registry

    @router.get("/signals", summary="Accelerator signals this server knows",
                tags=["reference"])
    async def signals() -> Dict[str, Any]:
        """List every decodable accelerator signal.

        Use the ``hex`` value (``$74``) or ``name`` (``numi``) as the
        ``signal`` parameter of ``/api/events``.
        """
        return {
            "signals": [
                {
                    "hex": signal.hex,
                    "code": signal.code,
                    "name": signal.name,
                    "carrier": signal.carrier_name,
                    "event_word": signal.event_word,
                    "spill_type": int(signal.spill_type),
                    "spill_type_name": signal.spill_type.name,
                    "description": signal.description,
                }
                for signal in SIGNALS
            ]
        }

    @router.get("/types", summary="Decoded spill types", tags=["reference"])
    async def types() -> Dict[str, Any]:
        """List the spill-type enumeration stored by the hardware.

        ``signals`` names every signal that decodes to each type.  More than
        one entry means a record carrying only that type is ambiguous.
        """
        return {
            "types": [
                {
                    "value": int(spill_type),
                    "name": spill_type.name,
                    "description": describe_type(spill_type),
                    "signals": [s.hex for s in signals_for_type(spill_type)],
                    "ambiguous": len(signals_for_type(spill_type)) > 1,
                }
                for spill_type in SpillType
            ]
        }

    # ------------------------------------------------------------ conversion

    @router.get("/time/convert", summary="Convert a time between timescales",
                tags=["reference"])
    async def time_convert(
        t: str = Query(..., description="Any accepted time expression, "
                                        "e.g. 2026-07-01T09:15:00Z, now, -2h, "
                                        "nova:33778458638680249, gps:1025136016"),
        tz: Optional[str] = Query(None, description="Zone for inputs carrying none"),
    ) -> Dict[str, Any]:
        """Render one instant as NOvA ticks, UNIX, UTC and GPS.

        Provided because every other endpoint speaks NOvA time, and a client
        building a query needs a way to check that it means what they think.
        """
        zone = _resolve_zone(tz, config)
        try:
            nova = parse_instant(t, tz=zone)
        except NovaTimeError as exc:
            raise _error(400, str(exc),
                         "See /api/time/help for the accepted forms.")
        return convert(nova).as_dict()

    @router.get("/time/help", summary="Accepted time expressions",
                tags=["reference"])
    async def time_help() -> Dict[str, Any]:
        return {
            "default_timezone": config.query.timezone,
            "note": (
                "Times that carry no zone are interpreted in "
                "{}. Pass tz= to override per request, or set query.timezone "
                "in the configuration file.".format(config.query.timezone)
            ),
            "forms": [
                {"example": "now", "means": "the current instant"},
                {"example": "today", "means": "midnight at the start of today"},
                {"example": "yesterday", "means": "midnight at the start of yesterday"},
                {"example": "-2h", "means": "two hours ago; also -30m, -7d, -1w"},
                {"example": "09:15", "means": "that clock time today"},
                {"example": "2026-07-01", "means": "that calendar date"},
                {"example": "Jul 1, 2026", "means": "the same date, written out"},
                {"example": "2026-07-01T09:15:00Z", "means": "an ISO 8601 instant"},
                {"example": "nova:33778458638680249", "means": "NOvA 64 MHz ticks"},
                {"example": "unix:1790092413", "means": "UNIX seconds"},
                {"example": "gps:1025136016", "means": "GPS seconds"},
                {"example": "gps:1695:259216", "means": "GPS week:time-of-week"},
            ],
            "range_note": (
                "A date-only end bound covers that whole day, so "
                "start=2026-07-01&end=2026-07-01 returns everything on 1 July."
            ),
        }

    # ----------------------------------------------------------------- latest

    @router.get("/latest", summary="The most recent stored event", tags=["spills"])
    async def latest(
        signal: Optional[str] = Query(None, description="Restrict to one signal, e.g. $8F"),
        user=Depends(current_user),
    ) -> Dict[str, Any]:
        """Return the newest archived event, optionally of one signal."""
        import asyncio

        spill_type = None
        if signal:
            try:
                spill_type = parse_signal(signal).spill_type
            except ValueError as exc:
                raise _error(400, str(exc), "See /api/signals for the full list.")

        event = await asyncio.to_thread(store.latest, spill_type)
        if event is None:
            raise _error(
                404,
                "the archive holds no matching events yet",
                "Check /api/status to see whether ingest is running.",
            )
        return {"event": event_row(event)}

    # ----------------------------------------------------------------- events

    @router.get("/events", summary="Query events by time range and signal",
                tags=["spills"])
    async def events(
        request: Request,
        start: Optional[str] = Query(
            None, description="Start of the range, inclusive. Any time "
                              "expression; see /api/time/help."),
        end: Optional[str] = Query(
            None, description="End of the range, exclusive. A date-only value "
                              "covers the whole day."),
        signal: Optional[List[str]] = Query(
            None, description="Accelerator signal such as $74 or $8f, or a "
                              "name such as one-hertz. Repeatable."),
        type: Optional[List[str]] = Query(
            None, alias="type",
            description="Decoded spill type, by name or number. Repeatable."),
        format: str = Query("json", pattern="^(json|csv)$",
                            description="Response format."),
        limit: Optional[int] = Query(None, ge=1, description="Maximum rows."),
        offset: int = Query(0, ge=0, description="Rows to skip."),
        order: str = Query("asc", pattern="^(asc|desc)$",
                           description="Sort by time ascending or descending."),
        tz: Optional[str] = Query(
            None, description="Zone for time inputs that carry none."),
        columns: Optional[str] = Query(
            None, description="Comma-separated subset of columns (CSV only)."),
        user=Depends(current_user),
    ):
        """Return every event matching a time range and signal selection.

        The range is half-open --- ``start <= t < end`` --- so adjacent
        queries tile without returning an event twice.

        Signals and types combine as a union: asking for ``$74`` and ``$8F``
        returns both.  Asking for a signal narrows to that signal's raw code
        where the archive has one, and otherwise falls back to its decoded
        type, which is the best the hardware preserved.
        """
        zone = _resolve_zone(tz, config)

        try:
            start_nova, end_nova = parse_range(start, end, tz=zone)
        except NovaTimeError as exc:
            raise _error(400, str(exc), "See /api/time/help for accepted forms.")

        spill_types, signal_codes, requested = _resolve_selection(signal, type)

        effective_limit = limit or config.query.default_limit
        if effective_limit > config.query.max_limit:
            raise _error(
                400,
                "limit {} exceeds the server maximum of {}".format(
                    effective_limit, config.query.max_limit
                ),
                "Page through the result with offset, or raise "
                "query.max_limit in the configuration.",
            )

        selected_columns = _resolve_columns(columns)
        descending = order == "desc"

        import asyncio
        page = await asyncio.to_thread(
            store.query,
            start_nova=start_nova,
            end_nova=end_nova,
            spill_types=spill_types or None,
            signal_codes=signal_codes or None,
            limit=effective_limit,
            offset=offset,
            descending=descending,
        )

        meta = _build_meta(
            config=config,
            state=state,
            start_nova=start_nova,
            end_nova=end_nova,
            requested=requested,
            spill_types=spill_types,
            page_total=page.total,
            returned=len(page.events),
            limit=effective_limit,
            offset=offset,
            order=order,
            timezone_name=str(tz or config.query.timezone),
        )

        if format == "csv":
            filename = "spills_{}.csv".format(int(time.time()))
            comments = _csv_comments(meta)
            return StreamingResponse(
                iter_csv(page.events, columns=selected_columns, comments=comments),
                media_type="text/csv; charset=utf-8",
                headers={
                    "Content-Disposition": 'attachment; filename="{}"'.format(filename),
                    "X-Total-Count": str(page.total),
                },
            )

        return StreamingResponse(
            iter_json(page.events, meta=meta),
            media_type="application/json",
            headers={"X-Total-Count": str(page.total)},
        )

    @router.get("/events.csv", summary="Query events, always as CSV",
                tags=["spills"], include_in_schema=False)
    async def events_csv(request: Request, user=Depends(current_user)):
        """Convenience alias so a browser or ``curl -O`` gets a file."""
        params = dict(request.query_params)
        params["format"] = "csv"
        scope_query = "&".join(
            "{}={}".format(k, v) for k, v in params.items()
        )
        from fastapi.responses import RedirectResponse
        return RedirectResponse(
            url=str(request.url_for("events")) + "?" + scope_query,
            status_code=307,
        )

    @router.get("/export", summary="Stream a whole range without paging",
                tags=["spills"])
    async def export(
        start: Optional[str] = Query(None),
        end: Optional[str] = Query(None),
        signal: Optional[List[str]] = Query(None),
        type: Optional[List[str]] = Query(None, alias="type"),
        format: str = Query("csv", pattern="^(json|csv)$"),
        tz: Optional[str] = Query(None),
        columns: Optional[str] = Query(None),
        user=Depends(current_user),
    ):
        """Stream every matching event, bypassing the per-query row limit.

        ``/api/events`` is paged so that an accidental unbounded query cannot
        exhaust the server.  This endpoint is the deliberate opposite: it
        streams the full range in constant memory, bounded only by
        ``query.max_export_rows``.
        """
        zone = _resolve_zone(tz, config)
        try:
            start_nova, end_nova = parse_range(start, end, tz=zone)
        except NovaTimeError as exc:
            raise _error(400, str(exc), "See /api/time/help for accepted forms.")

        spill_types, signal_codes, requested = _resolve_selection(signal, type)
        selected_columns = _resolve_columns(columns)
        max_rows = config.query.max_export_rows

        def bounded():
            """Yield events, stopping at the export ceiling."""
            for index, event in enumerate(
                store.iter_query(
                    start_nova=start_nova,
                    end_nova=end_nova,
                    spill_types=spill_types or None,
                    signal_codes=signal_codes or None,
                )
            ):
                if index >= max_rows:
                    log.warning(
                        "export truncated at query.max_export_rows (%d)", max_rows
                    )
                    return
                yield event

        meta = _build_meta(
            config=config, state=state,
            start_nova=start_nova, end_nova=end_nova,
            requested=requested, spill_types=spill_types,
            page_total=None, returned=None,
            limit=max_rows, offset=0, order="asc",
            timezone_name=str(tz or config.query.timezone),
        )

        if format == "csv":
            return StreamingResponse(
                iter_csv(bounded(), columns=selected_columns,
                         comments=_csv_comments(meta)),
                media_type="text/csv; charset=utf-8",
                headers={
                    "Content-Disposition":
                        'attachment; filename="spills_export.csv"'
                },
            )
        return StreamingResponse(
            iter_json(bounded(), meta=meta), media_type="application/json"
        )

    return router


# ---------------------------------------------------------------- helpers


def _resolve_zone(tz: Optional[str], config: Config):
    try:
        return get_timezone(tz or config.query.timezone)
    except NovaTimeError as exc:
        raise _error(400, str(exc),
                     "Use an IANA zone name such as America/Chicago, or UTC.")


def _resolve_columns(columns: Optional[str]) -> Optional[Sequence[str]]:
    if not columns:
        return None
    requested = [name.strip() for name in columns.split(",") if name.strip()]
    unknown = [name for name in requested if name not in COLUMNS]
    if unknown:
        raise _error(
            400,
            "unknown column(s): {}".format(", ".join(unknown)),
            "Available columns: {}".format(", ".join(COLUMNS)),
        )
    return requested


def _resolve_selection(
    signals_requested: Optional[Sequence[str]],
    types_requested: Optional[Sequence[str]],
) -> "tuple[List[SpillType], List[int], List[str]]":
    """Turn the ``signal``/``type`` parameters into store filters.

    Returns ``(spill_types, signal_codes, human_readable_request)``.

    A named signal contributes *both* its raw code and its decoded type.  The
    archive holds raw codes only for records ingested from a TDU that supplies
    event words; older records carry the type alone.  Filtering on the union
    means a query for ``$8F`` finds both, instead of silently returning
    nothing on an archive built before the history route existed.
    """
    spill_types: List[SpillType] = []
    signal_codes: List[int] = []
    requested: List[str] = []

    for text in signals_requested or []:
        for token in str(text).split(","):
            token = token.strip()
            if not token:
                continue
            try:
                signal = parse_signal(token)
            except ValueError as exc:
                raise _error(400, str(exc),
                             "See /api/signals for every accepted signal.")
            signal_codes.append(signal.code)
            spill_types.append(signal.spill_type)
            requested.append(signal.hex)

    for text in types_requested or []:
        for token in str(text).split(","):
            token = token.strip()
            if not token:
                continue
            try:
                spill_type = parse_spill_type(token)
            except ValueError as exc:
                raise _error(400, str(exc),
                             "See /api/types for every accepted type.")
            spill_types.append(spill_type)
            requested.append(spill_type.name)
            # A type names every signal that decodes to it, so that a query by
            # type still matches records that do carry a raw code.
            for signal in signals_for_type(spill_type):
                signal_codes.append(signal.code)

    if signal_codes:
        # Records with no raw event word store -1; include them so a query by
        # signal does not hide everything the older ingest path collected.
        from .storage import UNKNOWN_SIGNAL
        signal_codes.append(UNKNOWN_SIGNAL)

    return (
        list(dict.fromkeys(spill_types)),
        list(dict.fromkeys(signal_codes)),
        requested,
    )


def _build_meta(
    config: Config,
    state: Any,
    start_nova: int,
    end_nova: int,
    requested: Sequence[str],
    spill_types: Sequence[SpillType],
    page_total: Optional[int],
    returned: Optional[int],
    limit: int,
    offset: int,
    order: str,
    timezone_name: str,
) -> Dict[str, Any]:
    """Assemble the metadata block that accompanies every result."""
    meta: Dict[str, Any] = {
        "generated": datetime.now(tz=UTC).isoformat(),
        "range": {
            "start": convert(start_nova).as_dict(),
            "end": convert(end_nova).as_dict(),
            "note": "half-open: start <= t < end",
        },
        "selection": list(requested) or ["(all signals)"],
        "timezone": timezone_name,
        "order": order,
        "limit": limit,
        "offset": offset,
        "columns": list(COLUMNS),
    }
    if page_total is not None:
        meta["total"] = page_total
        meta["returned"] = returned
        meta["truncated"] = (offset + (returned or 0)) < page_total

    warnings: List[str] = []

    note = ambiguity_note(spill_types)
    if note:
        warnings.append(note)

    poller = getattr(state, "poller", None)
    if poller is not None and poller.status.degraded:
        warnings.append(
            "Ingest is running in degraded mode: the TDU at {} does not "
            "expose a bulk history route, so only the newest event is "
            "readable per poll and most events are never captured. This "
            "result is a sample, not a complete history. See "
            "contrib/tduweb/README.md.".format(config.tdu.base_url)
        )

    if warnings:
        meta["warnings"] = warnings
    return meta


def _csv_comments(meta: Dict[str, Any]) -> List[str]:
    """Render the metadata block as CSV comment lines.

    A CSV file outlives the HTTP response that produced it, so the query and
    any caveat are written into the file itself.
    """
    lines = [
        "DARPA Spill Information Server",
        "generated: {}".format(meta["generated"]),
        "range: {} .. {} ({})".format(
            meta["range"]["start"]["utc"],
            meta["range"]["end"]["utc"],
            meta["range"]["note"],
        ),
        "selection: {}".format(", ".join(meta["selection"])),
        "timezone for unqualified inputs: {}".format(meta["timezone"]),
    ]
    if "total" in meta:
        lines.append("matching rows: {}".format(meta["total"]))
    for warning in meta.get("warnings", []):
        lines.append("WARNING: {}".format(warning))
    return lines
