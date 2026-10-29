"""!
@file tests/test_api.py
@brief Tests for the HTTP API.
"""

import csv
import html
import io
import json
import pathlib
import re

import pytest

from tests.conftest import BASE, SECOND


# ------------------------------------------------------------------ status


def test_health(client):
    """!
    @brief /api/health answers 200 with status 'ok'.
    """
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_status_reports_an_empty_archive(client):
    """!
    @brief /api/status on an empty archive reports zero events, no earliest
    event, and auth disabled.
    """
    body = client.get("/api/status").json()
    assert body["archive"]["events"] == 0
    assert body["archive"]["earliest"] is None
    assert body["auth"]["enabled"] is False


def test_status_reports_archive_extent(populated_client):
    """!
    @brief /api/status reports the event total, earliest instant and per-type
    counts of the populated archive.
    """
    body = populated_client.get("/api/status").json()
    # 60 1 Hz (SOURCE_A) + 5 NuMI (SOURCE_B) from the populated fixture.
    assert body["archive"]["events"] == 65
    assert body["archive"]["earliest"]["nova"] == BASE
    assert body["archive"]["by_type"]["ACCEL_ONE_HZ_TCLK"] == 60
    assert body["archive"]["by_type"]["NUMI"] == 5


# ---------------------------------------------------------------- registry


def test_signals_endpoint_lists_the_design_examples(client):
    """!
    @brief /api/signals lists the Design.md signals ($74, $8F) with their type
    and carrier.
    """
    signals = client.get("/api/signals").json()["signals"]
    by_hex = {entry["hex"]: entry for entry in signals}
    assert "$74" in by_hex and "$8F" in by_hex
    assert by_hex["$74"]["spill_type_name"] == "NUMI"
    assert by_hex["$8F"]["carrier"] == "TCLK"


def test_types_endpoint_flags_ambiguity(client):
    """!
    @brief /api/types marks BNB_TCLK as ambiguous ($1D/$1F) and the 1 Hz type
    as unambiguous.
    """
    types = {entry["name"]: entry for entry in client.get("/api/types").json()["types"]}
    assert types["BNB_TCLK"]["ambiguous"] is True
    assert types["BNB_TCLK"]["signals"] == ["$1D", "$1F"]
    assert types["ACCEL_ONE_HZ_TCLK"]["ambiguous"] is False


# -------------------------------------------------------------- conversion


def test_time_convert_matches_the_instrument(client):
    """!
    @brief /api/time/convert reproduces the TDU's own UNIX time and Timestring
    for the live reading.
    """
    body = client.get("/api/time/convert", params={"t": "nova:{}".format(BASE)}).json()
    # UnixtimeSec/Timestring as reported by the live TDU for BASE.
    assert body["unix_sec"] == 1790092413
    assert body["utc_string"] == "2026-Sep-22 15:53:33.229378890625 UTC"


def test_time_convert_rejects_nonsense_with_a_hint(client):
    """!
    @brief Unparseable time gives 400 with a hint.
    """
    response = client.get("/api/time/convert", params={"t": "banana"})
    assert response.status_code == 400
    assert "hint" in response.json()["detail"]


def test_time_help_documents_the_forms(client):
    """!
    @brief /api/time/help includes the Design.md example forms.
    """
    body = client.get("/api/time/help").json()
    examples = [form["example"] for form in body["forms"]]
    assert "09:15" in examples and "Jul 1, 2026" in examples


# ------------------------------------------------------------------ latest


def test_latest_on_empty_archive_is_404_with_a_hint(client):
    """!
    @brief /api/latest on an empty archive is 404 with a hint mentioning
    ingest.
    """
    response = client.get("/api/latest")
    assert response.status_code == 404
    assert "ingest" in response.json()["detail"]["hint"]


def test_latest_returns_the_newest_event(populated_client):
    """!
    @brief /api/latest returns the newest event (the 60th 1 Hz tick).
    """
    body = populated_client.get("/api/latest").json()
    # Newest of the 60 1 Hz events (indices 0..59); the last NuMI spill is at
    # +40.5 s.
    assert body["event"]["nova_time"] == BASE + 59 * SECOND


def test_latest_filtered_by_signal(populated_client):
    """!
    @brief /api/latest?signal=$74 returns the newest NuMI event.
    """
    body = populated_client.get("/api/latest", params={"signal": "$74"}).json()
    assert body["event"]["spill_type_name"] == "NUMI"


def test_latest_rejects_an_unknown_signal(populated_client):
    """!
    @brief /api/latest with an unknown signal is 400.
    """
    response = populated_client.get("/api/latest", params={"signal": "$99"})
    assert response.status_code == 400


# ------------------------------------------------------------------ events


def test_events_returns_everything_by_default(populated_client):
    """!
    @brief /api/events with no filters returns all 65 events.
    """
    body = populated_client.get("/api/events").json()
    assert body["meta"]["total"] == 65
    assert len(body["events"]) == 65


def test_events_are_ordered_oldest_first_by_default(populated_client):
    """!
    @brief /api/events is ascending in time by default.
    """
    events = populated_client.get("/api/events").json()["events"]
    times = [event["nova_time"] for event in events]
    assert times == sorted(times)


def test_events_descending(populated_client):
    """!
    @brief order=desc returns newest first.
    """
    events = populated_client.get(
        "/api/events", params={"order": "desc"}
    ).json()["events"]
    times = [event["nova_time"] for event in events]
    assert times == sorted(times, reverse=True)


def test_events_filtered_by_signal(populated_client):
    """!
    @brief A lower-case signal filter matches and is normalised to '$8F' in the
    output.
    """
    body = populated_client.get("/api/events", params={"signal": "$8f"}).json()
    assert body["meta"]["total"] == 60
    assert all(event["signal"] == "$8F" for event in body["events"])


def test_signal_filter_also_matches_records_lacking_a_raw_word(populated_client):
    """!
    @brief The NuMI rows were stored without an event word; a $74 query must
    still find them, or an archive built before the history route existed would
    answer 'nothing'.
    """
    body = populated_client.get("/api/events", params={"signal": "$74"}).json()
    assert body["meta"]["total"] == 5
    assert all(event["spill_type_name"] == "NUMI" for event in body["events"])


def test_multiple_signals_are_a_union(populated_client):
    """!
    @brief Repeated signal parameters select the union.
    """
    body = populated_client.get(
        "/api/events", params=[("signal", "$8f"), ("signal", "$74")]
    ).json()
    assert body["meta"]["total"] == 65


def test_comma_separated_signals_also_work(populated_client):
    """!
    @brief A comma-separated signal list selects the union.
    """
    body = populated_client.get("/api/events", params={"signal": "$8f,$74"}).json()
    assert body["meta"]["total"] == 65


def test_events_filtered_by_type(populated_client):
    """!
    @brief type=NUMI selects the 5 NuMI events.
    """
    body = populated_client.get("/api/events", params={"type": "NUMI"}).json()
    assert body["meta"]["total"] == 5


def test_time_range_narrows_the_result(populated_client):
    """!
    @brief start/end bound the result half-open.
    """
    body = populated_client.get("/api/events", params={
        "start": "nova:{}".format(BASE),
        "end": "nova:{}".format(BASE + 10 * SECOND),
        "signal": "$8f",
    }).json()
    # 1 Hz events at BASE + 0..9 s; the one at +10 s is excluded.
    assert body["meta"]["total"] == 10, "half-open: 0..9 inclusive"


def test_range_metadata_describes_the_bounds(populated_client):
    """!
    @brief meta.range echoes the bounds and notes the half-open convention.
    """
    body = populated_client.get("/api/events", params={
        "start": "nova:{}".format(BASE), "end": "nova:{}".format(BASE + SECOND),
    }).json()
    assert body["meta"]["range"]["start"]["nova"] == BASE
    assert "half-open" in body["meta"]["range"]["note"]


def test_paging_reports_the_full_total(populated_client):
    """!
    @brief A limited page reports the full total in meta and in X-Total-Count,
    with truncated=True.
    """
    body = populated_client.get("/api/events", params={"limit": 10}).json()
    assert len(body["events"]) == 10
    assert body["meta"]["total"] == 65
    assert body["meta"]["truncated"] is True

    header_total = populated_client.get(
        "/api/events", params={"limit": 10}
    ).headers["X-Total-Count"]
    assert header_total == "65"


def test_offset_pages_through(populated_client):
    """!
    @brief offset= advances to a different page.
    """
    first = populated_client.get("/api/events", params={"limit": 10}).json()
    second = populated_client.get(
        "/api/events", params={"limit": 10, "offset": 10}
    ).json()
    assert first["events"][0]["nova_time"] != second["events"][0]["nova_time"]


def test_limit_above_the_server_maximum_is_refused(populated_client):
    """!
    @brief limit above query.max_limit is 400 naming the maximum.
    """
    response = populated_client.get("/api/events", params={"limit": 99999})
    assert response.status_code == 400
    assert "maximum" in response.json()["detail"]["error"]


def test_unknown_signal_is_refused_with_the_valid_list(populated_client):
    """!
    @brief An unknown signal is 400 with a hint pointing at /api/signals.
    """
    response = populated_client.get("/api/events", params={"signal": "$99"})
    assert response.status_code == 400
    assert "/api/signals" in response.json()["detail"]["hint"]


def test_bad_time_is_refused_with_a_hint(populated_client):
    """!
    @brief An unparseable start is 400 with a hint pointing at /api/time/help.
    """
    response = populated_client.get("/api/events", params={"start": "banana"})
    assert response.status_code == 400
    assert "/api/time/help" in response.json()["detail"]["hint"]


def test_backwards_range_is_refused(populated_client):
    """!
    @brief end before start is 400.
    """
    response = populated_client.get(
        "/api/events", params={"start": "today", "end": "2020-01-01"}
    )
    assert response.status_code == 400


def test_unknown_timezone_is_refused(populated_client):
    """!
    @brief An unknown tz is 400.
    """
    response = populated_client.get("/api/events", params={"tz": "Mars/Olympus"})
    assert response.status_code == 400


# --------------------------------------------------------------------- CSV


def test_csv_format(populated_client):
    """!
    @brief format=csv returns a text/csv attachment that parses to all 65 rows.
    """
    response = populated_client.get("/api/events", params={"format": "csv"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]

    lines = [line for line in response.text.splitlines() if not line.startswith("#")]
    rows = list(csv.DictReader(io.StringIO("\n".join(lines))))
    assert len(rows) == 65
    assert rows[0]["utc"].startswith("2026-09-22T")


def test_csv_carries_the_query_as_comments(populated_client):
    """!
    @brief The CSV comment preamble names the server, the normalised signal and
    the match count.
    """
    response = populated_client.get(
        "/api/events", params={"format": "csv", "signal": "$8f"}
    )
    comments = [line for line in response.text.splitlines() if line.startswith("#")]
    joined = "\n".join(comments)
    assert "DARPA Spill Information Server" in joined
    assert "$8F" in joined
    assert "matching rows: 60" in joined


def test_csv_column_subset(populated_client):
    """!
    @brief columns= selects the CSV columns.
    """
    response = populated_client.get("/api/events", params={
        "format": "csv", "columns": "utc,signal", "limit": 1,
    })
    header = [l for l in response.text.splitlines() if not l.startswith("#")][0]
    assert header == "utc,signal"


def test_csv_rejects_unknown_columns(populated_client):
    """!
    @brief An unknown CSV column is 400.
    """
    response = populated_client.get(
        "/api/events", params={"format": "csv", "columns": "nope"}
    )
    assert response.status_code == 400


def test_invalid_format_is_refused(populated_client):
    """!
    @brief An unsupported format fails request validation (422).
    """
    assert populated_client.get(
        "/api/events", params={"format": "xml"}
    ).status_code == 422


# ------------------------------------------------------------------ export


def test_export_streams_past_the_page_limit(populated_client):
    """!
    @brief /api/export returns every matching row regardless of the page limit.
    """
    response = populated_client.get("/api/export", params={"format": "csv"})
    assert response.status_code == 200
    rows = [l for l in response.text.splitlines()
            if l and not l.startswith("#") and not l.startswith("nova_time")]
    assert len(rows) == 65


def test_export_as_json(populated_client):
    """!
    @brief /api/export?format=json returns every event.
    """
    payload = json.loads(
        populated_client.get("/api/export", params={"format": "json"}).text
    )
    assert len(payload["events"]) == 65


# --------------------------------------------------------------- ambiguity


def test_ambiguous_type_query_carries_a_warning(populated_client):
    """!
    @brief Querying an ambiguous type adds a meta warning naming its signals.
    """
    body = populated_client.get("/api/events", params={"type": "BNB_TCLK"}).json()
    warnings = body["meta"].get("warnings", [])
    assert any("$1D" in warning for warning in warnings)


def test_unambiguous_query_has_no_ambiguity_warning(populated_client):
    """!
    @brief A query for an unambiguous signal carries no ambiguity warning.
    """
    body = populated_client.get("/api/events", params={"signal": "$8f"}).json()
    warnings = body["meta"].get("warnings", [])
    assert not any("cannot distinguish" in warning for warning in warnings)


# ------------------------------------------------------------------- pages


def test_index_page_renders(client):
    """!
    @brief The query page renders with the title and the signal chips.
    """
    response = client.get("/")
    assert response.status_code == 200
    assert "DARPA Spill Information Server" in response.text
    assert "$8F" in response.text, "the signal chips should be rendered"


def test_query_page_shows_the_logo(client):
    """!
    @brief The masthead art is served from the package, not from a CDN.
    """
    body = client.get("/").text
    assert '/static/novadaq-logo.png' in body

    logo = client.get("/static/novadaq-logo.png")
    assert logo.status_code == 200
    assert logo.headers["content-type"] == "image/png"


# ------------------------------------------------------------ status pages


def _status_client(config, store, status_dir, root_path=""):
    """!
    @brief Build a TestClient whose app serves *status_dir* at /status.
    @param config shared config fixture (mutated: status_dir, root_path).
    @param store archive to serve.
    @param status_dir directory to mount at /status.
    @param root_path ASGI root path prefix, "" for none.
    @return the TestClient (not yet entered).
    """
    from fastapi.testclient import TestClient

    from darpa_spillserver.app import create_app

    config.server.status_dir = str(status_dir)
    config.server.root_path = root_path
    return TestClient(create_app(config, store=store, start_ingest=False))


@pytest.fixture
def status_dir(tmp_path):
    """!
    @brief Status directory with index.html, status.json and an empty
    subdirectory.
    @param tmp_path pytest per-test temporary directory.
    @return path of the status directory.
    """
    directory = tmp_path / "status"
    (directory / "empty").mkdir(parents=True)
    (directory / "index.html").write_text("<h1>publisher status</h1>")
    (directory / "status.json").write_text('{"ok": true}')
    return directory


def test_status_dir_is_served_at_status(config, store, status_dir):
    """!
    @brief Files in status_dir are served under /status/, index.html as the
    directory index.
    """
    with _status_client(config, store, status_dir) as client:
        page = client.get("/status/")
        assert page.status_code == 200
        assert "publisher status" in page.text
        assert page.headers["content-type"].startswith("text/html")
        assert client.get("/status/status.json").json() == {"ok": True}


def test_bare_status_redirects_to_the_slash(config, store, status_dir):
    """!
    @brief /status redirects (307/308) to /status/.
    """
    with _status_client(config, store, status_dir) as client:
        response = client.get("/status", follow_redirects=False)
        assert response.status_code in (307, 308)
        assert response.headers["location"].endswith("/status/")


def test_status_does_not_list_directories(config, store, status_dir):
    """!
    @brief A directory without an index and a missing file both give 404 (no
    listings).
    """
    with _status_client(config, store, status_dir) as client:
        assert client.get("/status/empty/").status_code == 404
        assert client.get("/status/nonexistent.html").status_code == 404


def test_status_respects_root_path(config, store, status_dir):
    """!
    @brief Served under the prefix exactly as /static is.
    """
    with _status_client(config, store, status_dir, "/spills") as client:
        assert "publisher status" in client.get("/spills/status/").text
        response = client.get("/spills/status", follow_redirects=False)
        assert response.headers["location"].endswith("/spills/status/")


def test_status_is_not_mounted_when_unset(client):
    """!
    @brief With status_dir unset, /status is 404.
    """
    assert client.get("/status/").status_code == 404
    assert client.get("/status/index.html").status_code == 404


def test_missing_status_dir_is_skipped_with_a_warning(
    config, store, tmp_path, caplog
):
    """!
    @brief A nonexistent status_dir is not mounted and not created; a warning
    is logged and the app still serves.
    """
    missing = tmp_path / "not-there"
    with caplog.at_level("WARNING", logger="darpa_spillserver.app"):
        with _status_client(config, store, missing) as client:
            assert client.get("/status/").status_code == 404
            assert client.get("/").status_code == 200
    assert not missing.exists(), "the directory must not be created"
    assert any("status_dir" in r.getMessage() for r in caplog.records)


#: The issue form the report link drives. It ships with the repository rather
#: than the package, so the tests reach for it by path.
ISSUE_FORM = (
    pathlib.Path(__file__).resolve().parents[1]
    / ".github" / "ISSUE_TEMPLATE" / "bug_report.yml"
)


def _bug_report_query(body):
    """!
    @brief The parsed query string of the report link rendered into *body*.
    @param body HTML of the query page.
    @return parse_qs dict of the issue-tracker link's query string.
    """
    from urllib.parse import parse_qs, urlparse

    from darpa_spillserver import ISSUES_URL

    match = re.search(r'href="({}[^"]*)"'.format(re.escape(ISSUES_URL)), body)
    assert match, "no link to the issue tracker on the query page"
    return parse_qs(urlparse(html.unescape(match.group(1))).query)


def test_query_page_links_a_bug_report(client, config):
    """!
    @brief The report link reaches the repository's issue tracker, prefilled.
    The prefill carries template, version, the configured TDU URLs and the
    surface.
    """
    from darpa_spillserver import PROJECT_URL, __version__
    from darpa_spillserver.app import BUG_REPORT_SURFACE, BUG_REPORT_TEMPLATE

    body = client.get("/").text
    assert PROJECT_URL in body
    assert "report a bug" in body

    query = _bug_report_query(body)
    assert query["template"] == [BUG_REPORT_TEMPLATE]
    assert query["version"] == [__version__]
    assert query["tdu"] == [", ".join(
        s.base_url for s in config.tdu.resolved_sources())]
    assert query["surface"] == [BUG_REPORT_SURFACE]


def test_bug_report_link_carries_no_permissioned_parameter(client):
    """!
    @brief A query parameter that acts needs the permission for that action.

    GitHub answers 404 when the visitor lacks it, so labels, assignees and
    milestones must not appear in a link a reader outside the organisation is
    invited to click. The form declares the label instead.
    """
    query = _bug_report_query(client.get("/").text)
    assert not {"labels", "assignees", "milestone", "projects"} & set(query)


def test_bug_report_prefill_matches_the_issue_form():
    """!
    @brief The prefill keys are the form's field ids; a rename must break here.
    """
    import yaml

    form = yaml.safe_load(ISSUE_FORM.read_text())
    from darpa_spillserver.app import BUG_REPORT_SURFACE

    assert "bug" in form["labels"], "the form must apply the label itself"

    fields = {field["id"]: field for field in form["body"] if "id" in field}
    assert {"version", "tdu", "surface"} <= set(fields)
    assert BUG_REPORT_SURFACE in fields["surface"]["attributes"]["options"]


def test_openapi_schema_is_served(client):
    """!
    @brief /openapi.json is served and documents /api/events.
    """
    schema = client.get("/openapi.json").json()
    assert "/api/events" in schema["paths"]


def test_unknown_api_path_gives_json_not_html(client):
    """!
    @brief Unknown /api/ paths answer 404 with a JSON error body.
    """
    response = client.get("/api/nope")
    assert response.status_code == 404
    assert "error" in response.json()


# ------------------------------------------------------------- copyright

COPYRIGHT_TEXT = (
    "Copyright 2010-2026 Andrew Norman for Fermi Forward Discovery Group LLC. "
    "All rights reserved."
)


def test_copyright_constant_is_exact():
    """!
    @brief Pinned here so a well-meaning reword cannot quietly alter the
    notice.
    """
    from darpa_spillserver import COPYRIGHT
    assert COPYRIGHT == COPYRIGHT_TEXT


def test_query_page_shows_the_copyright(client):
    """!
    @brief The query page shows the exact copyright notice.
    """
    body = client.get("/").text
    assert COPYRIGHT_TEXT in body


def test_query_page_has_a_copyright_meta_tag(client):
    """!
    @brief The query page carries the notice in a copyright meta tag.
    """
    body = client.get("/").text
    assert '<meta name="copyright" content="{}">'.format(COPYRIGHT_TEXT) in body


def test_openapi_schema_carries_the_copyright(client):
    """!
    @brief Covers the /docs and /redoc pages, which render from this schema.
    """
    schema = client.get("/openapi.json").json()
    assert schema["info"]["license"]["name"] == COPYRIGHT_TEXT
    assert COPYRIGHT_TEXT in schema["info"]["description"]


def test_no_conflicting_licence_claim_remains(client):
    """!
    @brief The notice reserves all rights; an MIT grant would contradict it.
    """
    schema = client.get("/openapi.json").json()
    assert "MIT" not in schema["info"]["license"]["name"]
    assert "MIT" not in schema["info"]["description"]
