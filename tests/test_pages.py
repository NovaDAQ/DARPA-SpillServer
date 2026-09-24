"""The reference pages: /about, /api, /sitemap and /sitemap.xml."""

import re
import xml.etree.ElementTree as ET
from pathlib import Path

from darpa_spillserver import __version__
from darpa_spillserver.pages import OPTIONAL_DEPENDENCIES, PYTHON_DEPENDENCIES

ROOT = Path(__file__).resolve().parent.parent


def _pyproject_requirements():
    text = (ROOT / "pyproject.toml").read_text()
    project = text.split("[project]", 1)[1].split("[project.scripts]", 1)[0]
    names = re.findall(r'"([A-Za-z0-9_.\-]+)(?:\[[^\]]*\])?(?:>=|==|~=|<=|>|<)', project)
    return {name.lower() for name in names}


def test_dependency_tables_match_pyproject():
    listed = {name for name, _, _ in PYTHON_DEPENDENCIES + tuple(OPTIONAL_DEPENDENCIES)}
    assert listed == _pyproject_requirements()


def test_about_page(client):
    response = client.get("/about")
    assert response.status_code == 200
    body = response.text
    assert __version__ in body
    for name, _, _ in PYTHON_DEPENDENCIES:
        assert "<code>{}</code>".format(name) in body
    assert "Boost" in body and "CppUnit" in body


def test_api_page_lists_every_schema_route(client):
    body = client.get("/api").text
    schema = client.get("/openapi.json").json()
    for path, operations in schema["paths"].items():
        for method in operations:
            assert "<code>{}</code>".format(path) in body
            assert ">{}<".format(method.upper()) in body
    assert "<code>signal</code>" in body          # a query parameter
    assert "SourceUpdate" in body                  # a request body


def test_sitemap_links_resolve(client):
    body = client.get("/sitemap").text
    links = set(re.findall(r'href="(/[^"#]*)', body))
    assert {"/", "/config", "/about", "/api", "/docs"} <= links
    # API routes link to their entry on the API page.
    assert 'href="/api#get-api-events"' in body
    for link in links:
        assert client.get(link).status_code == 200, link


def test_sitemap_xml(client):
    response = client.get("/sitemap.xml")
    assert response.headers["content-type"].startswith("application/xml")
    tree = ET.fromstring(response.content)
    ns = {"s": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locations = [e.text for e in tree.findall("s:url/s:loc", ns)]
    assert "http://testserver/about" in locations
    assert "http://testserver/api/status" in locations
    assert not any("{" in loc for loc in locations)
    assert "http://testserver/api/time/convert" not in locations  # needs t=


def test_every_page_links_the_reference_pages(client):
    for page in ("/", "/config", "/about", "/api", "/sitemap"):
        body = client.get(page).text
        for target in ("/api", "/about", "/sitemap"):
            assert 'href="{}"'.format(target) in body, (page, target)
