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


# ------------------------------------------------- GPS full precision


def test_gps_is_rendered_at_full_precision():
    """The displayed GPS value must carry the sub-second part, not just
    whole seconds."""
    row = event_row(make_event())
    assert row["gps"] == "1474127631.229378890625"
    assert "." in row["gps"]


def test_gps_fraction_is_exact_and_matches_utc():
    """GPS is a leap-second-free offset from NOvA time, so its sub-second
    part is identical to the one the DAQ prints in utc_string. Any drift
    between them means a rounding bug."""
    row = event_row(make_event())
    utc_fraction = row["utc_string"].split(".")[1].split()[0]
    gps_fraction = row["gps"].split(".")[1]
    assert gps_fraction == utc_fraction == "229378890625"


def test_gps_picoseconds_are_exact_where_nanoseconds_truncate():
    """A NOvA tick is 15.625 ns, so nanoseconds cannot represent one."""
    row = event_row(make_event())
    assert row["gps_psec"] == 229378890625
    assert row["gps_nsec"] == 229378890
    assert row["gps_psec"] != row["gps_nsec"] * 1000, "nsec loses 625 ps here"


def test_gps_time_of_week_also_carries_the_fraction():
    row = event_row(make_event())
    assert row["gps_tow_exact"] == "230031.229378890625"
    assert row["gps_tow"] == 230031


def test_gps_fraction_always_has_twelve_digits():
    """Zero-padded, so values sort and align in a table."""
    from darpa_spillserver.novatime import convert
    from nova_time_decoder import NOVA_TIME_FACTOR
    # An instant one tick past a whole second.
    row = convert(2000 * NOVA_TIME_FACTOR + 1)
    assert row.gps.split(".")[1] == "000000015625"
    assert len(row.gps.split(".")[1]) == 12


def test_gps_on_an_exact_second_has_a_zero_fraction():
    from darpa_spillserver.novatime import convert
    from nova_time_decoder import NOVA_TIME_FACTOR
    row = convert(2000 * NOVA_TIME_FACTOR)
    assert row.gps.endswith(".000000000000")
    assert row.gps_psec == 0


def test_gps_string_combines_week_and_time_of_week():
    from darpa_spillserver.novatime import convert
    result = convert(33778458638680249)
    assert result.gps_string == (
        "week 2437, TOW 230031.229378890625 (1474127631.229378890625 s)"
    )


def test_legacy_gps_columns_are_still_present():
    """Kept so anything already reading gps_seconds/gps_nsec keeps working."""
    row = event_row(make_event())
    for name in ("gps_seconds", "gps_nsec", "gps_week", "gps_tow"):
        assert name in row
