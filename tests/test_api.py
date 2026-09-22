"""Tests for the HTTP API."""

import csv
import io
import json

import pytest

from tests.conftest import BASE, SECOND


# ------------------------------------------------------------------ status


def test_health(client):
    response = client.get("/api/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_status_reports_an_empty_archive(client):
    body = client.get("/api/status").json()
    assert body["archive"]["events"] == 0
    assert body["archive"]["earliest"] is None
    assert body["auth"]["enabled"] is False


def test_status_reports_archive_extent(populated_client):
    body = populated_client.get("/api/status").json()
    assert body["archive"]["events"] == 65
    assert body["archive"]["earliest"]["nova"] == BASE
    assert body["archive"]["by_type"]["ACCEL_ONE_HZ_TCLK"] == 60
    assert body["archive"]["by_type"]["NUMI"] == 5


# ---------------------------------------------------------------- registry


def test_signals_endpoint_lists_the_design_examples(client):
    signals = client.get("/api/signals").json()["signals"]
    by_hex = {entry["hex"]: entry for entry in signals}
    assert "$74" in by_hex and "$8F" in by_hex
    assert by_hex["$74"]["spill_type_name"] == "NUMI"
    assert by_hex["$8F"]["carrier"] == "TCLK"


def test_types_endpoint_flags_ambiguity(client):
    types = {entry["name"]: entry for entry in client.get("/api/types").json()["types"]}
    assert types["BNB_TCLK"]["ambiguous"] is True
    assert types["BNB_TCLK"]["signals"] == ["$1D", "$1F"]
    assert types["ACCEL_ONE_HZ_TCLK"]["ambiguous"] is False


# -------------------------------------------------------------- conversion


def test_time_convert_matches_the_instrument(client):
    body = client.get("/api/time/convert", params={"t": "nova:{}".format(BASE)}).json()
    assert body["unix_sec"] == 1790092413
    assert body["utc_string"] == "2026-Sep-22 15:53:33.229378890625 UTC"


def test_time_convert_rejects_nonsense_with_a_hint(client):
    response = client.get("/api/time/convert", params={"t": "banana"})
    assert response.status_code == 400
    assert "hint" in response.json()["detail"]


def test_time_help_documents_the_forms(client):
    body = client.get("/api/time/help").json()
    examples = [form["example"] for form in body["forms"]]
    assert "09:15" in examples and "Jul 1, 2026" in examples


# ------------------------------------------------------------------ latest


def test_latest_on_empty_archive_is_404_with_a_hint(client):
    response = client.get("/api/latest")
    assert response.status_code == 404
    assert "ingest" in response.json()["detail"]["hint"]


def test_latest_returns_the_newest_event(populated_client):
    body = populated_client.get("/api/latest").json()
    assert body["event"]["nova_time"] == BASE + 59 * SECOND


def test_latest_filtered_by_signal(populated_client):
    body = populated_client.get("/api/latest", params={"signal": "$74"}).json()
    assert body["event"]["spill_type_name"] == "NUMI"


def test_latest_rejects_an_unknown_signal(populated_client):
    response = populated_client.get("/api/latest", params={"signal": "$99"})
    assert response.status_code == 400


# ------------------------------------------------------------------ events


def test_events_returns_everything_by_default(populated_client):
    body = populated_client.get("/api/events").json()
    assert body["meta"]["total"] == 65
    assert len(body["events"]) == 65


def test_events_are_ordered_oldest_first_by_default(populated_client):
    events = populated_client.get("/api/events").json()["events"]
    times = [event["nova_time"] for event in events]
    assert times == sorted(times)


def test_events_descending(populated_client):
    events = populated_client.get(
        "/api/events", params={"order": "desc"}
    ).json()["events"]
    times = [event["nova_time"] for event in events]
    assert times == sorted(times, reverse=True)


def test_events_filtered_by_signal(populated_client):
    body = populated_client.get("/api/events", params={"signal": "$8f"}).json()
    assert body["meta"]["total"] == 60
    assert all(event["signal"] == "$8F" for event in body["events"])


def test_signal_filter_also_matches_records_lacking_a_raw_word(populated_client):
    """The NuMI rows were stored without an event word; a $74 query must
    still find them, or an archive built before the history route existed
    would answer 'nothing'."""
    body = populated_client.get("/api/events", params={"signal": "$74"}).json()
    assert body["meta"]["total"] == 5
    assert all(event["spill_type_name"] == "NUMI" for event in body["events"])


def test_multiple_signals_are_a_union(populated_client):
    body = populated_client.get(
        "/api/events", params=[("signal", "$8f"), ("signal", "$74")]
    ).json()
    assert body["meta"]["total"] == 65


def test_comma_separated_signals_also_work(populated_client):
    body = populated_client.get("/api/events", params={"signal": "$8f,$74"}).json()
    assert body["meta"]["total"] == 65


def test_events_filtered_by_type(populated_client):
    body = populated_client.get("/api/events", params={"type": "NUMI"}).json()
    assert body["meta"]["total"] == 5


def test_time_range_narrows_the_result(populated_client):
    body = populated_client.get("/api/events", params={
        "start": "nova:{}".format(BASE),
        "end": "nova:{}".format(BASE + 10 * SECOND),
        "signal": "$8f",
    }).json()
    assert body["meta"]["total"] == 10, "half-open: 0..9 inclusive"


def test_range_metadata_describes_the_bounds(populated_client):
    body = populated_client.get("/api/events", params={
        "start": "nova:{}".format(BASE), "end": "nova:{}".format(BASE + SECOND),
    }).json()
    assert body["meta"]["range"]["start"]["nova"] == BASE
    assert "half-open" in body["meta"]["range"]["note"]


def test_paging_reports_the_full_total(populated_client):
    body = populated_client.get("/api/events", params={"limit": 10}).json()
    assert len(body["events"]) == 10
    assert body["meta"]["total"] == 65
    assert body["meta"]["truncated"] is True

    header_total = populated_client.get(
        "/api/events", params={"limit": 10}
    ).headers["X-Total-Count"]
    assert header_total == "65"


def test_offset_pages_through(populated_client):
    first = populated_client.get("/api/events", params={"limit": 10}).json()
    second = populated_client.get(
        "/api/events", params={"limit": 10, "offset": 10}
    ).json()
    assert first["events"][0]["nova_time"] != second["events"][0]["nova_time"]


def test_limit_above_the_server_maximum_is_refused(populated_client):
    response = populated_client.get("/api/events", params={"limit": 99999})
    assert response.status_code == 400
    assert "maximum" in response.json()["detail"]["error"]


def test_unknown_signal_is_refused_with_the_valid_list(populated_client):
    response = populated_client.get("/api/events", params={"signal": "$99"})
    assert response.status_code == 400
    assert "/api/signals" in response.json()["detail"]["hint"]


def test_bad_time_is_refused_with_a_hint(populated_client):
    response = populated_client.get("/api/events", params={"start": "banana"})
    assert response.status_code == 400
    assert "/api/time/help" in response.json()["detail"]["hint"]


def test_backwards_range_is_refused(populated_client):
    response = populated_client.get(
        "/api/events", params={"start": "today", "end": "2020-01-01"}
    )
    assert response.status_code == 400


def test_unknown_timezone_is_refused(populated_client):
    response = populated_client.get("/api/events", params={"tz": "Mars/Olympus"})
    assert response.status_code == 400


# --------------------------------------------------------------------- CSV


def test_csv_format(populated_client):
    response = populated_client.get("/api/events", params={"format": "csv"})
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment" in response.headers["content-disposition"]

    lines = [line for line in response.text.splitlines() if not line.startswith("#")]
    rows = list(csv.DictReader(io.StringIO("\n".join(lines))))
    assert len(rows) == 65
    assert rows[0]["utc"].startswith("2026-09-22T")


def test_csv_carries_the_query_as_comments(populated_client):
    response = populated_client.get(
        "/api/events", params={"format": "csv", "signal": "$8f"}
    )
    comments = [line for line in response.text.splitlines() if line.startswith("#")]
    joined = "\n".join(comments)
    assert "DARPA Spill Information Server" in joined
    assert "$8F" in joined
    assert "matching rows: 60" in joined


def test_csv_column_subset(populated_client):
    response = populated_client.get("/api/events", params={
        "format": "csv", "columns": "utc,signal", "limit": 1,
    })
    header = [l for l in response.text.splitlines() if not l.startswith("#")][0]
    assert header == "utc,signal"


def test_csv_rejects_unknown_columns(populated_client):
    response = populated_client.get(
        "/api/events", params={"format": "csv", "columns": "nope"}
    )
    assert response.status_code == 400


def test_invalid_format_is_refused(populated_client):
    assert populated_client.get(
        "/api/events", params={"format": "xml"}
    ).status_code == 422


# ------------------------------------------------------------------ export


def test_export_streams_past_the_page_limit(populated_client):
    response = populated_client.get("/api/export", params={"format": "csv"})
    assert response.status_code == 200
    rows = [l for l in response.text.splitlines()
            if l and not l.startswith("#") and not l.startswith("nova_time")]
    assert len(rows) == 65


def test_export_as_json(populated_client):
    payload = json.loads(
        populated_client.get("/api/export", params={"format": "json"}).text
    )
    assert len(payload["events"]) == 65


# --------------------------------------------------------------- ambiguity


def test_ambiguous_type_query_carries_a_warning(populated_client):
    body = populated_client.get("/api/events", params={"type": "BNB_TCLK"}).json()
    warnings = body["meta"].get("warnings", [])
    assert any("$1D" in warning for warning in warnings)


def test_unambiguous_query_has_no_ambiguity_warning(populated_client):
    body = populated_client.get("/api/events", params={"signal": "$8f"}).json()
    warnings = body["meta"].get("warnings", [])
    assert not any("cannot distinguish" in warning for warning in warnings)


# ------------------------------------------------------------------- pages


def test_index_page_renders(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "DARPA Spill Information Server" in response.text
    assert "$8F" in response.text, "the signal chips should be rendered"


def test_openapi_schema_is_served(client):
    schema = client.get("/openapi.json").json()
    assert "/api/events" in schema["paths"]


def test_unknown_api_path_gives_json_not_html(client):
    response = client.get("/api/nope")
    assert response.status_code == 404
    assert "error" in response.json()


# ------------------------------------------------------------- copyright

COPYRIGHT_TEXT = (
    "Copyright 2010-2026 Andrew Norman for Fermi Forward Discovery Group LLC. "
    "All rights reserved."
)


def test_copyright_constant_is_exact():
    """Pinned here so a well-meaning reword cannot quietly alter the notice."""
    from darpa_spillserver import COPYRIGHT
    assert COPYRIGHT == COPYRIGHT_TEXT


def test_query_page_shows_the_copyright(client):
    body = client.get("/").text
    assert COPYRIGHT_TEXT in body


def test_query_page_has_a_copyright_meta_tag(client):
    body = client.get("/").text
    assert '<meta name="copyright" content="{}">'.format(COPYRIGHT_TEXT) in body


def test_openapi_schema_carries_the_copyright(client):
    """Covers the /docs and /redoc pages, which render from this schema."""
    schema = client.get("/openapi.json").json()
    assert schema["info"]["license"]["name"] == COPYRIGHT_TEXT
    assert COPYRIGHT_TEXT in schema["info"]["description"]


def test_no_conflicting_licence_claim_remains(client):
    """The notice reserves all rights; an MIT grant would contradict it."""
    schema = client.get("/openapi.json").json()
    assert "MIT" not in schema["info"]["license"]["name"]
    assert "MIT" not in schema["info"]["description"]
