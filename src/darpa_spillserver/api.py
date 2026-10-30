"""!
@file src/darpa_spillserver/api.py
@brief HTTP API router: event queries, export, reference tables, source administration.

The HTTP API.

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

**Sources.**  Every query route takes ``source=`` (repeatable, or
comma-separated) to restrict it to named TDUs; leaving it out means every
source.  ``/api/sources`` lists them, and its ``PATCH`` form changes one at
runtime --- allowed only to a caller holding the admin token, or a signed-in
member of ``admin.allowed_groups``.

Documentation note: FastAPI copies a route handler's docstring into the
OpenAPI ``description`` served at ``/docs``, and a pydantic model's docstring
into its schema description.  Those docstrings are therefore left as plain
runtime text, and their Doxygen tags live in ``##`` comments above the
``def``/``class`` instead.

Concurrency: every handler is a coroutine on the server's event loop.  All
SQLite access goes through ``asyncio.to_thread`` so a slow query never stalls
the loop (and with it the ingest poller, which shares the loop).  Streaming
responses iterate the store's generator from Starlette's threadpool.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlencode

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from .config import Config
from .formats import COLUMNS, ambiguity_note, event_row, iter_csv, iter_json
from .novatime import UTC, NovaTimeError, convert, get_timezone, nova_now, parse_instant, parse_range
from .signals import (
    SIGNALS,
    resolve_selection,
    SpillType,
    describe_type,
    signals_for_type,
)
from .poller import SourceError
from .storage import SpillStore

__all__ = ["build_router"]

## Module logger.
log = logging.getLogger(__name__)


def _error(status: int, message: str, hint: Optional[str] = None) -> HTTPException:
    """!
    @brief Build an HTTPException whose body explains how to fix the problem.

    The body is ``{"error": message, "hint": hint}``; FastAPI wraps it as
    ``{"detail": {...}}``.  Callers ``raise`` the result.

    @param status HTTP status code.
    @param message what went wrong.
    @param hint optional advice on how to correct the request.
    @return the exception, not yet raised.
    """
    detail: Dict[str, Any] = {"error": message}
    if hint:
        detail["hint"] = hint
    return HTTPException(status_code=status, detail=detail)


## @brief Request body of ``PATCH /api/sources/{name}``.
#
# The class docstring is the OpenAPI schema description, so it is kept as
# plain text.  Both fields are optional; ``None`` means "leave unchanged".
class SourceUpdate(BaseModel):
    """A change to one source.  Omitted fields are left as they are."""

    ## New enabled state, or None to keep the current one.
    enabled: Optional[bool] = Field(
        None, description="Start (true) or stop (false) recording from it.")
    ## New TDU base URL, or None to keep the current one.
    base_url: Optional[str] = Field(
        None, description="New TDU URL, e.g. http://tdu-near-master-ppc-04:8080")


## Header carrying the admin token on requests that change configuration.
ADMIN_HEADER = "X-Admin-Token"


def build_router(
    config: Config,
    store: SpillStore,
    state: Any,
) -> APIRouter:
    """!
    @brief Assemble the API router.

    Assemble the API router.

    Route handlers are closures over *config*, *store* and *state*, so the
    same factory can be instantiated per test application without globals.

    @param config merged server configuration.
    @param store the event archive.
    @param state the application state object, carrying ``ingest``,
        ``authenticator``, ``admin_token`` and ``started_at``.
    @return an ``APIRouter`` mounted under ``/api``.
    """
    router = APIRouter(prefix="/api", tags=["spills"])
    authenticator = state.authenticator
    ingest = state.ingest

    async def current_user(request: Request):
        """!
        @brief FastAPI dependency: the authenticated principal for a read route.

        Delegates to the configured authenticator; with ``auth.enabled`` false
        that is the no-op authenticator, which admits everyone.

        @param request the incoming request.
        @return the principal the authenticator resolved.
        @throws HTTPException 401 from the authenticator when sign-in is
            required and missing.
        """
        return await authenticator.require_user(request)

    async def require_admin(request: Request) -> str:
        """!
        @brief FastAPI dependency guarding the source-changing routes.

        Allow a configuration change, returning who made it.

        The token is compared in constant time.  It travels in a header
        rather than a cookie, so a page on another site cannot make a
        browser send it.

        Order of checks: (1) neither token nor SSO admin groups configured
        means the feature is off (403); (2) a supplied header is judged
        against the token and is final either way, so a wrong token does
        not fall through to SSO; (3) otherwise an SSO principal in one of
        ``admin.allowed_groups`` is accepted; (4) otherwise 401.

        @param request the incoming request.
        @return a short description of the caller, recorded as ``updated_by``.
        @throws HTTPException 403 if changes are disabled, the token is wrong,
            or the SSO user is in none of the allowed groups; 401 if no
            credential was offered.
        """
        token = state.admin_token
        groups = list(config.admin.allowed_groups)
        # Group-based admin is only meaningful when SSO actually produces
        # principals with groups.
        via_sso = config.auth.enabled and bool(groups)
        if not token and not via_sso:
            raise _error(
                403,
                "changing sources at runtime is disabled on this server",
                "Set admin.token_file in the configuration to a file holding "
                "a secret (e.g. from 'openssl rand -hex 32'), and restart.",
            )

        supplied = request.headers.get(ADMIN_HEADER, "")
        if token and supplied:
            # compare_digest: timing does not leak how many leading bytes match.
            if hmac.compare_digest(supplied.encode(), token.encode()):
                host = request.client.host if request.client else "?"
                return "admin token from {}".format(host)
            raise _error(403, "the admin token is not correct")

        if via_sso:
            principal = await authenticator.require_user(request)
            if set(groups) & set(principal.groups):
                return principal.subject
            raise _error(
                403,
                "your account is not in a group allowed to change sources "
                "(need one of: {})".format(", ".join(groups)),
            )
        raise _error(
            401,
            "changing sources needs the admin token",
            "Send it in the {} header, or enter it on the /config page.".format(
                ADMIN_HEADER),
        )

    async def resolve_sources(requested: Optional[Sequence[str]]) -> List[str]:
        """!
        @brief Validate ``source=`` values; an empty result means every source.

        Accepts repeated parameters and comma-separated lists, in any mix.
        A name is valid if it is configured *or* still present in the archive,
        so data from a source removed from the configuration stays queryable.

        @param requested raw ``source`` query values, or None.
        @return de-duplicated names in request order; ``[]`` for "all".
        @throws HTTPException 400 naming any unknown source.
        """
        names: List[str] = []
        for text in requested or []:
            names.extend(t.strip() for t in str(text).split(",") if t.strip())
        if not names:
            return []
        known = set(ingest.names()) | set(
            await asyncio.to_thread(store.source_names)
        )
        unknown = [name for name in names if name not in known]
        if unknown:
            raise _error(
                400,
                "unknown source(s): {}".format(", ".join(unknown)),
                "Known sources: {}. See /api/sources.".format(
                    ", ".join(sorted(known)) or "(none)"),
            )
        # dict.fromkeys de-duplicates while preserving first-seen order.
        return list(dict.fromkeys(names))

    # ---------------------------------------------------------------- health

    ## @brief GET /api/health: liveness probe; no auth, no database access.
    # @return ``{"status": "ok", "time": <ISO 8601 UTC>}``.
    @router.get("/health", summary="Liveness check", tags=["status"])
    async def health() -> Dict[str, Any]:
        """Return OK if the process is serving. Never requires authentication."""
        return {"status": "ok", "time": datetime.now(tz=UTC).isoformat()}

    ## @brief GET /api/status: configuration, archive extent and ingest health.
    # Unauthenticated by design (monitoring scrapes it).  Five separate
    # archive reads, each off-loop via asyncio.to_thread; they are not one
    # transaction, so counts may be mutually inconsistent by a poll's worth.
    # @param request the incoming request (unused).
    # @return status document; see docs/API.md.
    @router.get("/status", summary="Server and ingest status", tags=["status"])
    async def status(request: Request) -> Dict[str, Any]:
        """Report configuration, archive extent and ingest health.

        Deliberately readable without authentication so that monitoring can
        scrape it, but it carries no event data and no secrets.
        """
        earliest = await asyncio.to_thread(store.earliest)
        latest = await asyncio.to_thread(store.latest)
        total = await asyncio.to_thread(store.count)
        by_type = await asyncio.to_thread(store.counts_by_type)
        by_source = await asyncio.to_thread(store.counts_by_source)

        return {
            "status": "ok",
            "version": state.version,
            "started_at": state.started_at,
            "uptime_seconds": round(time.time() - state.started_at, 1),
            "config_source": config.source or "(defaults)",
            "sources": [
                {"name": s.name, "base_url": s.base_url, "enabled": s.enabled}
                for s in ingest.sources.values()
            ],
            "auth": authenticator.describe(),
            "archive": {
                "path": store.path,
                "events": total,
                "earliest": convert(earliest.nova_time).as_dict() if earliest else None,
                "latest": convert(latest.nova_time).as_dict() if latest else None,
                "by_type": {
                    # Ordered by enum value so the output is stable.
                    spill_type.name: count for spill_type, count in sorted(
                        by_type.items(), key=lambda item: item[0].value
                    )
                },
                "by_source": by_source,
                # 0 means "keep forever"; report it as null.
                "retention_days": config.storage.retention_days or None,
            },
            "ingest": ingest.summary(),
            "query_defaults": {
                "timezone": config.query.timezone,
                "default_limit": config.query.default_limit,
                "max_limit": config.query.max_limit,
            },
        }

    # --------------------------------------------------------------- sources

    ## @brief GET /api/sources: every source with URL, enabled state,
    # runtime overrides, ingest health and archived event count.
    # Unauthenticated.  ``archived_only`` lists names present in the archive
    # but no longer configured.
    # @return sources document; see docs/API.md.
    @router.get("/sources", summary="The TDUs this server records from",
                tags=["sources"])
    async def sources() -> Dict[str, Any]:
        """List every source: its URL, whether it is enabled, how that
        differs from the configuration file, and its ingest health.

        Readable without authentication, like ``/api/status``; it carries no
        event data.  ``admin.enabled`` says whether this server accepts
        changes at all.
        """
        counts = await asyncio.to_thread(store.counts_by_source)
        described = ingest.describe()
        for entry in described:
            entry["events"] = counts.get(entry["name"], 0)
        return {
            "sources": described,
            "archived_only": sorted(set(counts) - set(ingest.names())),
            "ingest_enabled": ingest.running,
            "admin": {
                # Mirrors the first test in require_admin: either credential
                # path makes the PATCH routes usable.
                "enabled": bool(state.admin_token) or (
                    config.auth.enabled and bool(config.admin.allowed_groups)),
                "token_header": ADMIN_HEADER,
                "sso_groups": list(config.admin.allowed_groups)
                if config.auth.enabled else [],
            },
        }

    ## @brief GET /api/admin/check: verify admin credentials without changing
    # anything.  Used by the /config page to validate a typed-in token.
    # @param who caller description from require_admin.
    # @return ``{"ok": true, "as": who}``.
    @router.get("/admin/check", summary="Check admin credentials",
                tags=["sources"])
    async def admin_check(who: str = Depends(require_admin)) -> Dict[str, Any]:
        """Succeeds only for a caller allowed to change sources."""
        return {"ok": True, "as": who}

    def _describe_one(name: str) -> Dict[str, Any]:
        """!
        @brief The ``ingest.describe()`` entry for one source.
        @param name source name; must exist (callers have just updated it).
        @return that source's description dict.
        """
        return next(e for e in ingest.describe() if e["name"] == name)

    ## @brief PATCH /api/sources/{name}: enable/disable a source or change its
    # URL at runtime.  Admin only.  The override is persisted in the archive
    # and outranks the configuration file until reset.
    # @param name source name from the path.
    # @param update body: ``enabled`` and/or ``base_url``.
    # @param who caller description from require_admin, stored as updated_by.
    # @return ``{"source": <description>}``.
    # @throws HTTPException 400 for an empty or invalid change, 404 for an
    #   unknown source.
    @router.patch("/sources/{name}", summary="Change a source at runtime",
                  tags=["sources"])
    async def update_source(
        name: str,
        update: SourceUpdate,
        who: str = Depends(require_admin),
    ) -> Dict[str, Any]:
        """Enable or disable a source, or point it at a different URL.

        The change applies immediately, is saved in the archive so that it
        survives a restart, and wins over the configuration file until the
        source is reset.
        """
        if update.enabled is None and update.base_url is None:
            raise _error(400, "nothing to change",
                         "Send enabled, base_url, or both.")
        try:
            await ingest.update_source(
                name, base_url=update.base_url, enabled=update.enabled,
                updated_by=who,
            )
        except SourceError as exc:
            # SourceError covers both "no such source" and "bad value";
            # distinguish them by whether the name exists.
            status_code = 404 if name not in ingest.sources else 400
            raise _error(status_code, str(exc))
        return {"source": _describe_one(name)}

    ## @brief POST /api/sources/{name}/reset: drop runtime overrides so the
    # source reverts to the configuration file.  Admin only.
    # @param name source name from the path.
    # @param who caller description from require_admin.
    # @return ``{"source": <description>}``.
    # @throws HTTPException 404 for an unknown source.
    @router.post("/sources/{name}/reset",
                 summary="Return a source to the configuration file",
                 tags=["sources"])
    async def reset_source(
        name: str, who: str = Depends(require_admin)
    ) -> Dict[str, Any]:
        """Discard runtime changes to a source."""
        try:
            await ingest.reset_source(name, updated_by=who)
        except SourceError as exc:
            raise _error(404, str(exc))
        return {"source": _describe_one(name)}

    # -------------------------------------------------------------- registry

    ## @brief GET /api/signals: the static signal registry (hex, name,
    # carrier, event word, decoded spill type).  Unauthenticated.
    # @return ``{"signals": [...]}``.
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

    ## @brief GET /api/types: the SpillType enumeration with the signals that
    # decode to each; ``ambiguous`` marks many-to-one types.  Unauthenticated.
    # @return ``{"types": [...]}``.
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

    ## @brief GET /api/time/convert: render one instant in every timescale.
    # @param t time expression (see /api/time/help).
    # @param tz IANA zone for inputs without an offset; default query.timezone.
    # @return NOvA ticks, UNIX, UTC and GPS for the instant.
    # @throws HTTPException 400 for an unparseable time or zone.
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

    ## @brief GET /api/time/help: the accepted time-expression forms.
    # Deliberately has no docstring: adding one would add a description to
    # the published OpenAPI schema.  The list is static text and must be kept
    # in step with novatime.parse_instant by hand.
    # @return default zone, examples and the range note.
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

    ## @brief GET /api/latest: newest archived event, optionally filtered.
    # Filters by the signal's decoded *type*, not its raw code, so for a
    # many-to-one type it may return a sibling signal's event.
    # @param signal optional signal (``$8F``, ``one-hertz``).
    # @param source optional source names; repeatable or comma-separated.
    # @param user authenticated principal (dependency; unused).
    # @return ``{"event": <row>}``.
    # @throws HTTPException 400 for a bad signal/source, 404 if nothing matches.
    @router.get("/latest", summary="The most recent stored event", tags=["spills"])
    async def latest(
        signal: Optional[str] = Query(None, description="Restrict to one signal, e.g. $8F"),
        source: Optional[List[str]] = Query(
            None, description="Restrict to these sources. Repeatable."),
        user=Depends(current_user),
    ) -> Dict[str, Any]:
        """Return the newest archived event, optionally of one signal or
        from given sources."""
        selected_sources = await resolve_sources(source)
        # The same selection rule as /api/events, newest first, without a
        # whole-archive count.  `or None` normalises "no source= given" ([])
        # to the store's "every source" (None).
        spill_types, codes, _, explicit = _resolve_selection([signal] if signal else None, None)
        page = await asyncio.to_thread(
            store.query, spill_types=spill_types, signal_codes=codes,
            sources=selected_sources or None, limit=1, descending=True,
            unrestricted_types=explicit, count_total=False)
        event = page.events[0] if page.events else None
        if event is None:
            raise _error(
                404,
                "the archive holds no matching events yet",
                "Check /api/status to see whether ingest is running.",
            )
        record = event_row(event)
        record["nova_time_exact"] = str(event.nova_time)
        return {"event": record}

    # ----------------------------------------------------------------- events

    ## @brief GET /api/events: the main paged query, JSON or CSV.
    # Half-open range ``[start, end)``; signals, types and sources each
    # combine as a union and the three intersect.  ``limit`` defaults to
    # query.default_limit and may not exceed query.max_limit.  Responses are
    # streamed and carry ``X-Total-Count``.
    # @param request the incoming request (unused; kept for url_for symmetry).
    # @param start range start, inclusive.
    # @param end range end, exclusive; a bare date covers the whole day.
    # @param signal signals, repeatable or comma-separated.
    # @param type decoded spill types, by name or number.
    # @param source source names.
    # @param format ``json`` or ``csv``.
    # @param limit maximum rows on this page.
    # @param offset rows to skip.
    # @param order ``asc`` or ``desc`` by time.
    # @param tz zone for inputs without an offset.
    # @param columns CSV column subset.
    # @param user authenticated principal (dependency; unused).
    # @return a StreamingResponse in the requested format.
    # @throws HTTPException 400 for any invalid parameter.
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
        source: Optional[List[str]] = Query(
            None, description="Source (TDU) name, e.g. tdu-near-master-ppc-02. "
                              "Repeatable; omit for every source."),
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

        Sources combine as a union too, and narrow everything else: asking
        for ``$8F`` from two sources returns that signal as each recorded it.
        """
        zone = _resolve_zone(tz, config)

        try:
            # Both bounds in NOvA 64 MHz ticks.
            start_nova, end_nova = parse_range(start, end, tz=zone)
        except NovaTimeError as exc:
            raise _error(400, str(exc), "See /api/time/help for accepted forms.")

        spill_types, signal_codes, requested, unrestricted_types = _resolve_selection(signal, type)
        selected_sources = await resolve_sources(source)

        # Reject rather than clamp, so a client never mistakes a truncated
        # page for the whole answer.
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

        # One page plus the total match count (for X-Total-Count and
        # meta.truncated), read off the event loop.
        page = await asyncio.to_thread(
            store.query,
            start_nova=start_nova,
            end_nova=end_nova,
            spill_types=spill_types or None,
            signal_codes=signal_codes or None,
            unrestricted_types=unrestricted_types or None,
            limit=effective_limit,
            offset=offset,
            descending=descending,
            sources=selected_sources or None,
        )

        meta = _build_meta(
            config=config,
            state=state,
            start_nova=start_nova,
            end_nova=end_nova,
            requested=requested,
            spill_types=spill_types,
            sources=selected_sources,
            page_total=page.total,
            returned=len(page.events),
            limit=effective_limit,
            offset=offset,
            order=order,
            timezone_name=str(tz or config.query.timezone),
        )

        if format == "csv":
            # Timestamped filename so successive downloads do not collide.
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

    ## @brief GET /api/events.csv: 307 redirect to /api/events with
    # ``format=csv`` forced.  Hidden from the schema.
    # @param request the incoming request; its query string is carried over.
    # @param user authenticated principal (dependency; unused).
    # @return a 307 RedirectResponse.
    @router.get("/events.csv", summary="Query events, always as CSV",
                tags=["spills"], include_in_schema=False)
    async def events_csv(request: Request, user=Depends(current_user)):
        """Convenience alias so a browser or ``curl -O`` gets a file."""
        # multi_items() keeps every value of a repeated parameter, and
        # urlencode re-escapes values that the request decoded.
        params = [(key, value) for key, value in request.query_params.multi_items()
                  if key != "format"]
        params.append(("format", "csv"))
        scope_query = urlencode(params)
        from fastapi.responses import RedirectResponse
        # 307 preserves the method; url_for honours root_path.
        return RedirectResponse(
            url=str(request.url_for("events")) + "?" + scope_query,
            status_code=307,
        )

    ## @brief GET /api/export: stream every matching event, unpaged.
    # Same selection semantics as /api/events; capped at
    # query.max_export_rows, beyond which the stream ends silently (a
    # warning is logged server-side only).  No total count is computed.
    # @param start range start, inclusive.
    # @param end range end, exclusive.
    # @param signal signals, repeatable or comma-separated.
    # @param type decoded spill types.
    # @param source source names.
    # @param format ``csv`` (default) or ``json``.
    # @param tz zone for inputs without an offset.
    # @param columns CSV column subset.
    # @param user authenticated principal (dependency; unused).
    # @return a StreamingResponse.
    # @throws HTTPException 400 for any invalid parameter.
    @router.get("/export", summary="Stream a whole range without paging",
                tags=["spills"])
    async def export(
        start: Optional[str] = Query(None),
        end: Optional[str] = Query(None),
        signal: Optional[List[str]] = Query(None),
        type: Optional[List[str]] = Query(None, alias="type"),
        source: Optional[List[str]] = Query(None),
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

        spill_types, signal_codes, requested, unrestricted_types = _resolve_selection(signal, type)
        selected_sources = await resolve_sources(source)
        selected_columns = _resolve_columns(columns)
        max_rows = config.query.max_export_rows

        counted = await asyncio.to_thread(
            store.query, start_nova=start_nova, end_nova=end_nova,
            spill_types=spill_types, signal_codes=signal_codes,
            unrestricted_types=unrestricted_types, sources=selected_sources,
            limit=0)
        if counted.total > max_rows:
            raise _error(400, "export has {} rows, exceeding the maximum of {}".format(
                counted.total, max_rows), "Narrow the range or use darpa-spill-dump.")

        def bounded():
            """!
            @brief Yield events, stopping at the export ceiling.

            Runs in Starlette's threadpool as the response body is consumed,
            so the SQLite cursor lives for the duration of the download.

            @return a generator of stored events, at most ``max_rows`` long.
            """
            for index, event in enumerate(
                store.iter_query(
                    start_nova=start_nova,
                    end_nova=end_nova,
                    spill_types=spill_types or None,
                    signal_codes=signal_codes or None,
                    unrestricted_types=unrestricted_types or None,
                    sources=selected_sources or None,
                )
            ):
                if index >= max_rows:
                    # The archive grew after the preflight count. Abort the
                    # stream rather than emit a successful incomplete export.
                    raise RuntimeError("archive grew beyond the export ceiling; retry a narrower range")
                yield event

        # page_total/returned None: an export never counts, so meta carries
        # no total/truncated fields.
        meta = _build_meta(
            config=config, state=state,
            start_nova=start_nova, end_nova=end_nova,
            requested=requested, spill_types=spill_types,
            sources=selected_sources,
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
    """!
    @brief Resolve the zone used for time inputs that carry no offset.
    @param tz per-request ``tz=`` value, or None for ``query.timezone``.
    @param config server configuration.
    @return a tzinfo.
    @throws HTTPException 400 for an unknown zone name.
    """
    try:
        return get_timezone(tz or config.query.timezone)
    except NovaTimeError as exc:
        raise _error(400, str(exc),
                     "Use an IANA zone name such as America/Chicago, or UTC.")


def _resolve_columns(columns: Optional[str]) -> Optional[Sequence[str]]:
    """!
    @brief Parse and validate the ``columns=`` CSV subset.
    @param columns comma-separated column names, or None/empty for all.
    @return the requested names in order, or None for every column.
    @throws HTTPException 400 naming any unknown column.
    """
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
) -> "tuple[List[SpillType], List[int], List[str], List[SpillType]]":
    """!
    @brief Turn the ``signal``/``type`` parameters into store filters.

    Delegates to signals.resolve_selection, which is shared with the query
    CLI and darpa-spill-dump so that every interface selects identically.

    @param signals_requested raw ``signal`` values (repeatable, comma lists).
    @param types_requested raw ``type`` values (repeatable, comma lists).
    @return ``(spill_types, signal_codes, requested, unrestricted_types)``.
    @throws HTTPException 400 for an unrecognised signal or type.
    """
    try:
        return resolve_selection(signals_requested, types_requested)
    except ValueError as exc:
        raise _error(400, str(exc), "See /api/signals and /api/types.") from None


def _build_meta(
    config: Config,
    state: Any,
    start_nova: int,
    end_nova: int,
    requested: Sequence[str],
    spill_types: Sequence[SpillType],
    sources: Sequence[str],
    page_total: Optional[int],
    returned: Optional[int],
    limit: int,
    offset: int,
    order: str,
    timezone_name: str,
) -> Dict[str, Any]:
    """!
    @brief Assemble the metadata block that accompanies every result.

    @param config server configuration (unused; kept for call symmetry).
    @param state application state; ``state.ingest`` supplies degraded sources.
    @param start_nova range start, NOvA ticks, inclusive.
    @param end_nova range end, NOvA ticks, exclusive.
    @param requested human-readable selection, e.g. ``["$74", "NUMI"]``.
    @param spill_types decoded types in the selection (for the ambiguity note).
    @param sources selected source names; empty means all.
    @param page_total total matches, or None for an unpaged export.
    @param returned rows on this page, or None for an export.
    @param limit page size (or the export ceiling).
    @param offset rows skipped.
    @param order ``asc`` or ``desc``.
    @param timezone_name zone used for unqualified inputs.
    @return the ``meta`` dict; includes ``warnings`` only when non-empty.
    """
    meta: Dict[str, Any] = {
        "generated": datetime.now(tz=UTC).isoformat(),
        "range": {
            "start": convert(start_nova).as_dict(),
            "end": convert(end_nova).as_dict(),
            "note": "half-open: start <= t < end",
        },
        "selection": list(requested) or ["(all signals)"],
        "sources": list(sources) or ["(all sources)"],
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

    # Many-to-one decoded types (e.g. BNB_TCLK <- $1D, $1F) cannot be split
    # for records without a raw event word.
    note = ambiguity_note(spill_types)
    if note:
        warnings.append(note)

    # A source polled without the history route yields only a sample; say so
    # whenever such a source contributes to the result.
    degraded = state.ingest.degraded_sources(list(sources) or None)
    if degraded:
        warnings.append(
            "Ingest is running in degraded mode for {}: {} not expose a bulk "
            "history route, so only the newest event is readable per poll "
            "and most events are never captured. Rows from {} are a sample, "
            "not a complete history. See contrib/tduweb/README.md.".format(
                ", ".join("{} ({})".format(s.name, s.base_url) for s in degraded),
                "it does" if len(degraded) == 1 else "they do",
                "that source" if len(degraded) == 1 else "those sources",
            )
        )

    if warnings:
        meta["warnings"] = warnings
    return meta


def _csv_comments(meta: Dict[str, Any]) -> List[str]:
    """!
    @brief Render the metadata block as CSV comment lines.

    A CSV file outlives the HTTP response that produced it, so the query and
    any caveat are written into the file itself.

    @param meta the dict from _build_meta.
    @return comment lines without the leading ``#`` (iter_csv adds it).
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
        "sources: {}".format(", ".join(meta["sources"])),
        "timezone for unqualified inputs: {}".format(meta["timezone"]),
    ]
    if "total" in meta:
        lines.append("matching rows: {}".format(meta["total"]))
    for warning in meta.get("warnings", []):
        lines.append("WARNING: {}".format(warning))
    return lines
