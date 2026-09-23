"""Application factory and lifecycle.

:func:`create_app` wires the pieces together --- archive, TDU poller,
authenticator, API router and web UI --- and returns a FastAPI application.
Everything it needs comes from a :class:`~darpa_spillserver.config.Config`, so
the same factory serves the production entry point and the tests; the tests
simply pass a config pointing at a temporary database and a stubbed TDU.

The ingest task is owned by the application's lifespan rather than started at
import time, so that an application created for a test, or for
``--print-config``, never reaches out to the network.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from urllib.parse import urlencode

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import COPYRIGHT, ISSUES_URL, PROJECT_URL, __version__
from .api import build_router
from .auth import AuthError, build_authenticator
from .config import Config, ConfigError
from .poller import IngestManager
from .signals import SIGNALS, SpillType, describe_type, signals_for_type
from .storage import SpillStore

__all__ = ["create_app"]

log = logging.getLogger(__name__)

_WEB_DIR = Path(__file__).parent / "web"

DESCRIPTION = """
Serves the accelerator event timestamps recorded by NOvA TDUs.

The server polls each configured TDU's embedded bottle server, stores decoded
events in a local archive tagged with the TDU they came from, and exposes them as a structured table selectable by time range
and by accelerator signal in the operators' hex notation (`$74`, `$8F`).
Timestamps are returned in NOvA base time, UNIX, UTC and GPS together.

* `/api/events` &mdash; the main query, as JSON or CSV
* `/api/export` &mdash; stream a whole range, unpaged
* `/api/signals`, `/api/types` &mdash; what can be asked for
* `/api/time/convert`, `/api/time/help` &mdash; timescale conversion
* `/api/sources` &mdash; the TDUs recorded from; changeable at runtime
* `/api/status` &mdash; archive extent and ingest health

---

{copyright}
"""


DESCRIPTION = DESCRIPTION.format(copyright=COPYRIGHT)


def create_app(
    config: Config,
    store: Optional[SpillStore] = None,
    ingest: Optional[IngestManager] = None,
    start_ingest: Optional[bool] = None,
) -> FastAPI:
    """Build the FastAPI application.

    :param config: merged configuration.
    :param store: archive to use; one is opened from *config* if omitted.
    :param ingest: the ingest manager; one is built from *config* if omitted.
    :param start_ingest: override ``config.ingest.enabled``.  Tests pass
        ``False`` so that creating an app never contacts a TDU.

    :raises ConfigError: if the admin token file cannot be read.
    """
    store = store or SpillStore(
        config.storage.path,
        timeout=config.storage.timeout,
        legacy_source=config.tdu.resolved_sources()[0].name,
    )
    authenticator = build_authenticator(config.auth)

    try:
        admin_token = config.admin.resolved_token()
    except OSError as exc:
        raise ConfigError("admin.token_file: cannot read {}: {}".format(
            config.admin.token_file, exc.strerror)) from None

    should_ingest = (
        config.ingest.enabled if start_ingest is None else bool(start_ingest)
    )
    # Built even when ingest is off, so that /config can show and change the
    # sources; nothing is polled until start() is called.
    ingest = ingest or IngestManager(config, store)

    state = SimpleNamespace(
        config=config,
        store=store,
        ingest=ingest,
        admin_token=admin_token,
        authenticator=authenticator,
        version=__version__,
        started_at=time.time(),
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            await authenticator.startup()
        except AuthError as exc:
            # Refuse to serve rather than silently fall back to no auth: a
            # deployment that asked for SSO must not come up unprotected.
            log.error("authentication setup failed: %s", exc)
            raise

        if should_ingest:
            await ingest.start()
        else:
            log.info("ingest disabled; serving the existing archive read-only")

        try:
            yield
        finally:
            await ingest.stop()
            await authenticator.shutdown()
            store.close()

    app = FastAPI(
        title="DARPA Spill Information Server",
        description=DESCRIPTION,
        version=__version__,
        root_path=config.server.root_path,
        lifespan=lifespan,
        contact={"name": "NOvA DAQ", "url": PROJECT_URL},
        license_info={"name": COPYRIGHT},
    )
    app.state.spill = state

    # Sessions are needed only by the OIDC login flow, which stores a nonce
    # between the redirect out and the callback back.
    if config.auth.enabled:
        try:
            from starlette.middleware.sessions import SessionMiddleware
        except ImportError:
            raise AuthError(
                "auth.enabled is true but itsdangerous is missing; "
                "install it with: pip install -e '.[oidc]'"
            ) from None
        app.add_middleware(
            SessionMiddleware,
            secret_key=config.auth.session_secret,
            max_age=config.auth.session_max_age,
            same_site="lax",
        )

    if config.server.cors_origins:
        from fastapi.middleware.cors import CORSMiddleware
        app.add_middleware(
            CORSMiddleware,
            allow_origins=config.server.cors_origins,
            allow_credentials=True,
            # GET only: the write routes behind /config are same-origin, and
            # a cross-origin page must not be able to reach them.
            allow_methods=["GET"],
            allow_headers=["*"],
        )

    app.include_router(build_router(config, store, state))
    authenticator.register_routes(app)

    _mount_web(app, config, store, state)
    return app


#: Form fields the report link fills in, keyed by the ``id`` of the matching
#: field in .github/ISSUE_TEMPLATE/bug_report.yml. GitHub addresses issue-form
#: fields by that id, so renaming one there without renaming it here silently
#: drops the prefill rather than failing.
BUG_REPORT_TEMPLATE = "bug_report.yml"

#: Value of the form's "Where" dropdown; it has to match the option text.
BUG_REPORT_SURFACE = "browser query page"


def _bug_report_url(config: Config) -> str:
    """The "report a bug" target, with the deployment's own details filled in.

    GitHub prefills an issue form from the query string, so the report arrives
    already carrying the server version and the TDU it was talking to --- the
    two facts a bug report from an operator is most often missing.  With
    several sources, ``tdu`` lists every configured URL.

    The ``bug`` label is declared by the form rather than passed here on
    purpose. A query parameter that performs an action needs the permission for
    that action, and GitHub answers 404 when the visitor lacks it: on a public
    repository ``labels=bug`` would turn the link into a dead end for anyone
    outside the organisation. A label the form declares is applied whoever
    files.
    """
    query = urlencode(
        {
            "template": BUG_REPORT_TEMPLATE,
            "version": __version__,
            "tdu": ", ".join(s.base_url for s in config.tdu.resolved_sources()),
            "surface": BUG_REPORT_SURFACE,
        }
    )
    return "{}?{}".format(ISSUES_URL, query)


def _mount_web(app: FastAPI, config: Config, store: SpillStore, state) -> None:
    """Attach the browser-facing pages."""
    templates = Jinja2Templates(directory=str(_WEB_DIR / "templates"))
    bug_report_url = _bug_report_url(config)
    static_dir = _WEB_DIR / "static"
    if static_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def index(request: Request):
        """The query form and results table."""
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "version": state.version,
                "copyright": COPYRIGHT,
                "signals": SIGNALS,
                "types": [
                    {
                        "value": int(t),
                        "name": t.name,
                        "description": describe_type(t),
                        "ambiguous": len(signals_for_type(t)) > 1,
                    }
                    for t in SpillType
                ],
                "sources": state.ingest.names(),
                "default_timezone": config.query.timezone,
                "auth_enabled": config.auth.enabled,
                "root_path": config.server.root_path,
                "project_url": PROJECT_URL,
                "bug_report_url": bug_report_url,
            },
        )

    @app.get("/config", response_class=HTMLResponse, include_in_schema=False)
    async def config_page(request: Request):
        """View the sources, and change them with the admin token."""
        return templates.TemplateResponse(
            request,
            "config.html",
            {
                "version": state.version,
                "copyright": COPYRIGHT,
                "root_path": config.server.root_path,
                "project_url": PROJECT_URL,
                "bug_report_url": bug_report_url,
                "auth_enabled": config.auth.enabled,
            },
        )

    @app.exception_handler(404)
    async def not_found(request: Request, exc):
        """Explain an unrouted path, without touching a route's own 404.

        This handler sees both kinds of 404: one raised by a route that looked
        for something and did not find it, and one produced by the router when
        no route matched at all. Only the second is ours to rewrite --- the
        first already carries a detail explaining what was missing, and
        replacing it would throw that away.
        """
        detail = getattr(exc, "detail", None)
        if detail is not None and not isinstance(detail, str):
            return JSONResponse(status_code=404, content={"detail": detail})
        if isinstance(detail, str) and detail != "Not Found":
            return JSONResponse(status_code=404, content={"detail": detail})

        if request.url.path.startswith("/api/"):
            return JSONResponse(
                status_code=404,
                content={
                    "error": "no such endpoint: {}".format(request.url.path),
                    "hint": "See /docs for the full API.",
                },
            )
        return JSONResponse(
            status_code=404,
            content={"error": "not found: {}".format(request.url.path)},
        )
