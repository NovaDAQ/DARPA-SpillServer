"""!
@file tests/test_formats.py
@brief Tests for CSV and JSON rendering.
"""

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

# 2026-09-22 15:53:33.229378890625 UTC in NOvA ticks (live TDU reading).
BASE = 33778458638680249


def make_event(offset=0, signal_code=0x8F, spill_type=SpillType.ACCEL_ONE_HZ_TCLK):
    """!
    @brief Build a SpillEvent near the live reference instant.
    @param offset ticks after BASE.
    @param signal_code raw signal code, or UNKNOWN_SIGNAL.
    @param spill_type decoded SpillType.
    @return SpillEvent from tdu-near-master-ppc-01 via the history route.
    """
    return SpillEvent(
        nova_time=BASE + offset,
        spill_type=spill_type,
        signal_code=signal_code,
        event_number=42,
        # One second of the 64 MHz NOvA clock.
        delta=64000000,
        pps_offset=0,
        source="tdu-near-master-ppc-01",
        route="spill_history",
    )


def test_row_carries_every_timescale():
    """!
    @brief A saved table must not force the reader back to the server to
    convert.
    """
    row = event_row(make_event())
    assert row["nova_time"] == BASE
    assert row["unix_sec"] == 1790092413
    assert row["utc"].startswith("2026-09-22T15:53:33")
    assert row["utc_string"] == "2026-Sep-22 15:53:33.229378890625 UTC"
    assert row["gps_seconds"] > 0
    assert row["gps_week"] > 0


def test_row_keys_match_the_declared_columns():
    """!
    @brief event_row emits exactly the keys listed in COLUMNS, no more and no
    fewer.
    """
    assert set(event_row(make_event())) == set(COLUMNS)


def test_known_signal_is_rendered_in_hex():
    """!
    @brief A known signal code renders as '$8F' with its registry short name.
    """
    row = event_row(make_event(signal_code=0x8F))
    assert row["signal"] == "$8F"
    assert row["signal_name"] == "one-hertz"


def test_unknown_signal_is_blank_not_guessed():
    """!
    @brief $1D and $1F both decode to BNB_TCLK; inventing one would be a
    fiction.
    """
    row = event_row(make_event(signal_code=UNKNOWN_SIGNAL,
                               spill_type=SpillType.BNB_TCLK))
    assert row["signal"] == ""
    assert row["signal_name"] == ""
    assert row["spill_type_name"] == "BNB_TCLK"


# ----------------------------------------------------------------- CSV


def test_csv_round_trips_through_a_reader():
    """!
    @brief render_csv output parses with csv.DictReader, one row per event,
    values intact.
    """
    text = render_csv([make_event(0), make_event(1)])
    rows = list(csv.DictReader(io.StringIO(text)))
    assert len(rows) == 2
    assert rows[0]["nova_time"] == str(BASE)
    assert rows[0]["signal"] == "$8F"


def test_csv_header_can_be_suppressed():
    """!
    @brief header=False omits the column header line.
    """
    text = render_csv([make_event()], header=False)
    assert not text.startswith("nova_time")


def test_csv_column_subset_is_honoured():
    """!
    @brief columns= selects and orders the emitted columns.
    """
    text = render_csv([make_event()], columns=["utc", "signal"])
    assert text.splitlines()[0] == "utc,signal"


def test_csv_rejects_unknown_columns():
    """!
    @brief An unknown column name raises ValueError naming the offending
    column.
    """
    with pytest.raises(ValueError) as excinfo:
        render_csv([make_event()], columns=["nope"])
    assert "nope" in str(excinfo.value)


def test_csv_comments_precede_the_header():
    """!
    @brief comments= lines are emitted first, each prefixed with '# ', before
    the header.
    """
    text = render_csv([make_event()], comments=["generated: now", "range: a..b"])
    lines = text.splitlines()
    assert lines[0] == "# generated: now"
    assert lines[1] == "# range: a..b"
    assert lines[2].startswith("nova_time")


def test_csv_of_no_events_still_has_a_header():
    """!
    @brief An empty selection still yields the full header line.
    """
    text = render_csv([])
    assert text.strip() == ",".join(COLUMNS)


# ---------------------------------------------------------------- JSON


def test_json_envelope_is_valid():
    """!
    @brief render_json produces a valid {meta, events} envelope.
    """
    text = render_json([make_event(0), make_event(1)], meta={"total": 2})
    payload = json.loads(text)
    assert payload["meta"]["total"] == 2
    assert len(payload["events"]) == 2
    assert payload["events"][0]["signal"] == "$8F"


def test_json_without_meta_is_valid():
    """!
    @brief Without meta the envelope omits the meta key and is still valid
    JSON.
    """
    payload = json.loads(render_json([make_event()]))
    assert "meta" not in payload
    assert len(payload["events"]) == 1


def test_json_of_no_events_is_valid():
    """!
    @brief An empty selection yields valid JSON with an empty events list.
    """
    payload = json.loads(render_json([], meta={"total": 0}))
    assert payload["events"] == []


def test_indented_json_is_valid():
    """!
    @brief indent= output is still valid JSON with all events.
    """
    payload = json.loads(render_json([make_event(0), make_event(1)],
                                     meta={"a": 1}, indent=2))
    assert len(payload["events"]) == 2


def test_json_streams_a_large_result():
    """!
    @brief A 1000-event result renders to valid JSON containing every event.
    """
    events = [make_event(i) for i in range(1000)]
    payload = json.loads(render_json(events, meta={"total": 1000}))
    assert len(payload["events"]) == 1000


# ---------------------------------------------------------- ambiguity


def test_ambiguity_note_names_the_affected_signals():
    """!
    @brief For BNB_TCLK the note names the type and both signals ($1D, $1F) it
    cannot tell apart.
    """
    note = ambiguity_note([SpillType.BNB_TCLK])
    assert "BNB_TCLK" in note
    assert "$1D" in note and "$1F" in note


def test_no_note_for_unambiguous_types():
    """!
    @brief Types produced by a single signal produce no ambiguity note.
    """
    assert ambiguity_note([SpillType.ACCEL_ONE_HZ_TCLK, SpillType.NUMI]) is None


def test_no_note_for_an_empty_selection():
    """!
    @brief No types selected produces no ambiguity note.
    """
    assert ambiguity_note([]) is None


# ------------------------------------------------- GPS full precision


def test_gps_is_rendered_at_full_precision():
    """!
    @brief The displayed GPS value must carry the sub-second part, not just
    whole seconds.
    """
    row = event_row(make_event())
    # GPS s = unix 1790092413 - 315964800 (1980-01-06 epoch) + 18 leap seconds;
    # fraction = 14680249 ticks * 15.625 ns.
    assert row["gps"] == "1474127631.229378890625"
    assert "." in row["gps"]


def test_gps_fraction_is_exact_and_matches_utc():
    """!
    @brief GPS is a leap-second-free offset from NOvA time, so its sub-second
    part is identical to the one the DAQ prints in utc_string. Any drift
    between them means a rounding bug.
    """
    row = event_row(make_event())
    utc_fraction = row["utc_string"].split(".")[1].split()[0]
    gps_fraction = row["gps"].split(".")[1]
    assert gps_fraction == utc_fraction == "229378890625"


def test_gps_picoseconds_are_exact_where_nanoseconds_truncate():
    """!
    @brief A NOvA tick is 15.625 ns, so nanoseconds cannot represent one.
    """
    row = event_row(make_event())
    assert row["gps_psec"] == 229378890625
    assert row["gps_nsec"] == 229378890
    assert row["gps_psec"] != row["gps_nsec"] * 1000, "nsec loses 625 ps here"


def test_gps_time_of_week_also_carries_the_fraction():
    """!
    @brief gps_tow_exact carries the 12-digit picosecond fraction; gps_tow
    stays an integer.
    """
    row = event_row(make_event())
    # 1474127631 = 2437 weeks * 604800 s + 230031 s.
    assert row["gps_tow_exact"] == "230031.229378890625"
    assert row["gps_tow"] == 230031


def test_gps_fraction_always_has_twelve_digits():
    """!
    @brief Zero-padded, so values sort and align in a table.
    """
    from darpa_spillserver.novatime import convert
    from nova_time_decoder import NOVA_TIME_FACTOR
    # An instant one tick past a whole second.
    row = convert(2000 * NOVA_TIME_FACTOR + 1)
    # One tick = 1/64e6 s = 15625 ps.
    assert row.gps.split(".")[1] == "000000015625"
    assert len(row.gps.split(".")[1]) == 12


def test_gps_on_an_exact_second_has_a_zero_fraction():
    """!
    @brief An instant on a whole second renders a .000000000000 fraction and
    gps_psec 0.
    """
    from darpa_spillserver.novatime import convert
    from nova_time_decoder import NOVA_TIME_FACTOR
    row = convert(2000 * NOVA_TIME_FACTOR)
    assert row.gps.endswith(".000000000000")
    assert row.gps_psec == 0


def test_gps_string_combines_week_and_time_of_week():
    """!
    @brief gps_string renders week, time of week and total GPS seconds in one
    human-readable string.
    """
    from darpa_spillserver.novatime import convert
    result = convert(33778458638680249)
    assert result.gps_string == (
        "week 2437, TOW 230031.229378890625 (1474127631.229378890625 s)"
    )


def test_legacy_gps_columns_are_still_present():
    """!
    @brief Kept so anything already reading gps_seconds/gps_nsec keeps working.
    """
    row = event_row(make_event())
    for name in ("gps_seconds", "gps_nsec", "gps_week", "gps_tow"):
        assert name in row
