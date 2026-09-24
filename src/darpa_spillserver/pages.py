"""Content for the reference pages: ``/about``, ``/api`` and ``/sitemap``.

The API page and the sitemap are generated from the running application ---
its OpenAPI schema and its route table --- rather than written by hand, so
that adding a route updates both without anyone remembering to.  The about
page reads installed versions from package metadata for the same reason.
"""

from __future__ import annotations

import platform
import re
import sys
from typing import Any, Dict, List, Sequence, Tuple

__all__ = ["about_context", "api_context", "sitemap_groups", "sitemap_xml",
           "PYTHON_DEPENDENCIES", "OPTIONAL_DEPENDENCIES", "CPP_DEPENDENCIES"]

#: (distribution, required version, what it is used for).  Kept in step with
#: pyproject.toml by tests/test_pages.py.
PYTHON_DEPENDENCIES: Sequence[Tuple[str, str, str]] = (
    ("fastapi", ">=0.110", "HTTP routing, validation and the OpenAPI schema"),
    ("uvicorn", ">=0.27", "ASGI server; serves HTTP and HTTPS"),
    ("httpx", ">=0.26", "asynchronous HTTP client used to poll the TDUs"),
    ("pyyaml", ">=6.0", "YAML configuration files"),
    ("jinja2", ">=3.1", "templates for these pages"),
    ("nova-time-decoder", ">=1.2.0", "NOvA time to UTC and GPS conversion"),
)

OPTIONAL_DEPENDENCIES: Sequence[Tuple[str, str, str]] = (
    ("authlib", ">=1.3", "Fermilab SSO (OIDC) login; extra [oidc]"),
    ("itsdangerous", ">=2.1", "signed session cookies for OIDC; extra [oidc]"),
    ("pytest", ">=7.0", "test suite; extra [test]"),
    ("pytest-asyncio", ">=0.23", "asynchronous tests; extra [test]"),
    ("respx", ">=0.20", "stubbing the TDU in tests; extra [test]"),
)

#: The C/C++ client library's build dependencies (not needed by the server).
CPP_DEPENDENCIES: Sequence[Tuple[str, str, str]] = (
    ("CMake", ">=3.16", "build system"),
    ("C++ compiler", "C++17", "GCC, Clang or MSVC"),
    ("Boost", ">=1.75", "Beast (HTTP), Asio, JSON, Program_options"),
    ("OpenSSL", ">=1.1", "https support; optional (DARPA_SPILL_WITH_TLS)"),
    ("yaml-cpp", ">=0.7", "reading the client configuration file"),
    ("CppUnit", ">=1.14", "C++ unit tests; optional"),
)


def _installed(name: str) -> str:
    try:
        from importlib import metadata
    except ImportError:  # pragma: no cover - Python < 3.8
        return "unknown"
    try:
        return metadata.version(name)
    except metadata.PackageNotFoundError:
        return "not installed"


def _rows(deps: Sequence[Tuple[str, str, str]], probe: bool) -> List[Dict[str, str]]:
    return [{"name": name, "required": required,
             "installed": _installed(name) if probe else "build time",
             "purpose": purpose} for name, required, purpose in deps]


def about_context(config: Any, state: Any) -> Dict[str, Any]:
    return {
        "page_title": "About",
        "python_version": "{} ({})".format(platform.python_version(),
                                           sys.executable),
        "platform": platform.platform(),
        "config_source": config.source or "(defaults)",
        "archive_path": state.store.path,
        "sources": state.ingest.names(),
        "auth_enabled": config.auth.enabled,
        "dependency_groups": [
            {"title": "Python dependencies",
             "note": "Installed with the package; versions are read from this "
                     "server's environment.",
             "deps": _rows(PYTHON_DEPENDENCIES, True)},
            {"title": "Optional Python dependencies",
             "note": "Installed with pip extras: pip install -e '.[oidc]' or '.[test]'.",
             "deps": _rows(OPTIONAL_DEPENDENCIES, True)},
            {"title": "C/C++ client library",
             "note": "Needed only to build libdarpa_spill_client and "
                     "darpa-spill-client-cpp; see docs/CPP_LIBRARY.md.",
             "deps": _rows(CPP_DEPENDENCIES, False)},
        ],
    }


def _anchor(method: str, path: str) -> str:
    return "{}-{}".format(method.lower(), re.sub(r"[^a-z0-9]+", "-", path.lower()).strip("-"))


def api_context(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Group the OpenAPI operations by tag for the API page."""
    tags: Dict[str, List[Dict[str, Any]]] = {}
    for path, operations in schema.get("paths", {}).items():
        for method, operation in operations.items():
            if method not in ("get", "post", "put", "patch", "delete"):
                continue
            body = None
            content = operation.get("requestBody", {}).get("content", {})
            if "application/json" in content:
                ref = content["application/json"].get("schema", {}).get("$ref", "")
                body = ref.rsplit("/", 1)[-1] or "JSON"
            route = {
                "method": method.upper(),
                "path": path,
                "anchor": _anchor(method, path),
                "summary": operation.get("summary", ""),
                "description": operation.get("description", "").strip(),
                "parameters": [
                    {"name": p["name"], "location": p.get("in", ""),
                     "required": p.get("required", False),
                     "description": p.get("description", "")}
                    for p in operation.get("parameters", [])
                ],
                "body": body,
            }
            for tag in operation.get("tags", ["other"])[:1]:
                tags.setdefault(tag, []).append(route)
    return {
        "page_title": "API",
        "tags": [{"name": name, "routes": routes} for name, routes in tags.items()],
    }


#: Pages served outside the OpenAPI schema, with what each is.
_PAGES = (
    ("/", "GET", "query form and results table"),
    ("/config", "GET", "sources: view, and change with the admin token"),
    ("/api", "GET", "API reference, generated from the OpenAPI schema"),
    ("/about", "GET", "what this server is, and its dependencies"),
    ("/sitemap", "GET", "this page"),
    ("/sitemap.xml", "GET", "this page, for crawlers"),
    ("/docs", "GET", "interactive API documentation (Swagger UI)"),
    ("/redoc", "GET", "API documentation (ReDoc)"),
    ("/openapi.json", "GET", "OpenAPI schema"),
    ("/static/", "GET", "stylesheet and images"),
)

_AUTH_PAGES = (
    ("/auth/login", "GET", "start a Fermilab SSO login"),
    ("/auth/callback", "GET", "SSO redirect target"),
    ("/auth/logout", "GET", "end the session"),
    ("/auth/whoami", "GET", "the signed-in account"),
)


def sitemap_groups(schema: Dict[str, Any], auth_enabled: bool) -> List[Dict[str, Any]]:
    """Every page and API route, for the sitemap."""
    pages = [{"path": p, "link": p if not p.endswith("/") or p == "/" else None,
              "methods": m, "summary": s} for p, m, s in _PAGES]
    api = []
    for path, operations in schema.get("paths", {}).items():
        methods = [m.upper() for m in operations
                   if m in ("get", "post", "put", "patch", "delete")]
        summary = "; ".join(operations[m.lower()].get("summary", "") for m in methods)
        link = "/api#" + _anchor(methods[0], path) if methods else None
        api.append({"path": path, "link": link, "methods": ", ".join(methods),
                    "summary": summary})
    groups = [{"title": "Pages", "entries": pages},
              {"title": "API routes", "entries": api}]
    if auth_enabled:
        groups.append({"title": "Sign-on", "entries": [
            {"path": p, "link": None, "methods": m, "summary": s}
            for p, m, s in _AUTH_PAGES]})
    return groups


def sitemap_xml(base_url: str, schema: Dict[str, Any]) -> str:
    """A sitemaps.org ``urlset`` of the browsable pages and GET routes
    that take no required parameters."""
    base = base_url.rstrip("/")
    paths = [p for p, _, _ in _PAGES if p not in ("/static/", "/sitemap.xml")]
    for path, operations in schema.get("paths", {}).items():
        get = operations.get("get")
        if get and "{" not in path and not any(
                p.get("required") for p in get.get("parameters", [])):
            paths.append(path)
    from xml.sax.saxutils import escape
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">']
    lines += ["  <url><loc>{}</loc></url>".format(escape(base + p)) for p in paths]
    lines.append("</urlset>")
    return "\n".join(lines) + "\n"
