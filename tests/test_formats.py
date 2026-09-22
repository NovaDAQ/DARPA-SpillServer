"""Tests for CSV and JSON rendering."""

import csv
import io
import json

import pytest

from darpa_spillserver.formats import (
    COLUMNS,
    ambiguity_note,
    event_row,
    render_csv,
    render_json,
)
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import UNKNOWN_SIGNAL, SpillEvent

BASE = 33778458638680249


def make_event(offset=0, signal_code=0x8F, spill_type=SpillType.ACCEL_ONE_HZ_TCLK):
    return SpillEvent(
        nova_time=BASE + offset,
        spill_type=spill_type,
        signal_code=signal_code,
        event_number=42,
        delta=64000000,
        pps_offset=0,
        source="spill_history",
    )


def test_row_carries_every_timescale():
    """A saved table must not force the reader back to the server to convert."""
    row = event_row(make_event())
    assert row["nova_time"] == BASE
    assert row["unix_sec"] == 1790092413
    assert row["utc"].startswith("2026-09-22T15:53:33")
    assert row["utc_string"] == "2026-Sep-22 15:53:33.229378890625 UTC"
    assert row["gps_seconds"] > 0
    assert row["gps_week"] > 0


def test_row_keys_match_the_declared_columns():
    assert set(event_row(make_event())) == set(COLUMNS)


def test_known_signal_is_rendered_in_hex():
    row = event_row(make_event(signal_code=0x8F))
    assert row["signal"] == "$8F"
    assert row["signal_name"] == "one-hertz"


def test_unknown_signal_is_blank_not_guessed():
    """$1D and $1F both decode to BNB_TCLK; inventing one would be a fiction."""
    row = event_row(make_event(signal_code=UNKNOWN_SIGNAL,
                               spill_type=SpillType.BNB_TCLK))
    assert row["signal"] == ""
    assert row["signal_name"] == ""
    assert row["spill_type_name"] == "BNB_TCLK"


# ----------------------------------------------------------------- CSV


def test_csv_round_trips_through_a_reader():
    text = render_csv([make_event(0), make_event(1)])
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) == 2
    assert rows[0]["nova_time"] == str(BASE)
    assert rows[0]["signal"] == "$8F"


def test_csv_header_can_be_suppressed():
    text = render_csv([make_event()], header=False)
    assert not text.startswith("nova_time")


def test_csv_column_subset_is_honoured():
    text = render_csv([make_event()], columns=["utc", "signal"])
    assert text.splitlines()[0] == "utc,signal"


def test_csv_rejects_unknown_columns():
    with pytest.raises(ValueError) as excinfo:
        render_csv([make_event()], columns=["nope"])
    assert "nope" in str(excinfo.value)


def test_csv_comments_precede_the_header():
    text = render_csv([make_event()], comments=["generated: now", "range: a..b"])
    lines = text.splitlines()
    assert lines[0] == "# generated: now"
    assert lines[1] == "# range: a..b"
    assert lines[2].startswith("nova_time")


def test_csv_of_no_events_still_has_a_header():
    text = render_csv([])
    assert text.strip() == ",".join(COLUMNS)


# ---------------------------------------------------------------- JSON


def test_json_envelope_is_valid():
    text = render_json([make_event(0), make_event(1)], meta={"total": 2})
    payload = json.loads(text)
    assert payload["meta"]["total"] == 2
    assert len(payload["events"]) == 2
    assert payload["events"][0]["signal"] == "$8F"


def test_json_without_meta_is_valid():
    payload = json.loads(render_json([make_event()]))
    assert "meta" not in payload
    assert len(payload["events"]) == 1


def test_json_of_no_events_is_valid():
    payload = json.loads(render_json([], meta={"total": 0}))
    assert payload["events"] == []


def test_indented_json_is_valid():
    payload = json.loads(render_json([make_event(0), make_event(1)],
                                     meta={"a": 1}, indent=2))
    assert len(payload["events"]) == 2


def test_json_streams_a_large_result():
    events = [make_event(i) for i in range(1000)]
    payload = json.loads(render_json(events, meta={"total": 1000}))
    assert len(payload["events"]) == 1000


# ---------------------------------------------------------- ambiguity


def test_ambiguity_note_names_the_affected_signals():
    note = ambiguity_note([SpillType.BNB_TCLK])
    assert "BNB_TCLK" in note
    assert "$1D" in note and "$1F" in note


def test_no_note_for_unambiguous_types():
    assert ambiguity_note([SpillType.ACCEL_ONE_HZ_TCLK, SpillType.NUMI]) is None


def test_no_note_for_an_empty_selection():
    assert ambiguity_note([]) is None
