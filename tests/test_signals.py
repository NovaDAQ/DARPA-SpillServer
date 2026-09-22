"""Tests for the accelerator signal registry.

The decode rules here are a port of ``NssSpillInfo::getSpillTypeFromEvent``.
The truth table below is transcribed from that C++ source, so a divergence
between this server and the DAQ shows up as a test failure rather than as a
mislabelled event months later.
"""

import pytest

from darpa_spillserver.signals import (
    CARRIER_BNB,
    CARRIER_MIBS,
    CARRIER_TCLK,
    PARITY_ERROR,
    SIGNALS,
    SpillType,
    decode_event_word,
    describe_type,
    normalise_signal,
    parse_signal,
    parse_spill_type,
    signal_for_code,
    signals_for_type,
    spill_type_for_signal,
)

# (event word, expected type) transcribed from NssSpillInfo.cpp.
DECODE_TRUTH_TABLE = [
    (CARRIER_MIBS | 0x74, SpillType.NUMI),
    (CARRIER_BNB | 0x1B, SpillType.BNB),
    (CARRIER_TCLK | 0x1D, SpillType.BNB_TCLK),
    (CARRIER_TCLK | 0x1F, SpillType.BNB_TCLK),
    (CARRIER_TCLK | 0xAD, SpillType.NUMI_TCLK),
    (CARRIER_TCLK | 0xA9, SpillType.NUMI_TCLK),
    (CARRIER_TCLK | 0x8F, SpillType.ACCEL_ONE_HZ_TCLK),
    (CARRIER_TCLK | 0x00, SpillType.SUPER_CYCLE),
    (CARRIER_TCLK | 0xA4, SpillType.NUMI_SAMPLE_TRIG),
    (CARRIER_TCLK | 0xA5, SpillType.NUMI_RESET),
    (CARRIER_TCLK | 0x39, SpillType.TB_SPILL),
]


@pytest.mark.parametrize("event_word,expected", DECODE_TRUTH_TABLE)
def test_decode_matches_daq(event_word, expected):
    assert decode_event_word(event_word) is expected


def test_spill_type_values_are_wire_format():
    """The integers are what the hardware writes; reordering would corrupt data."""
    assert SpillType.NUMI == 0
    assert SpillType.BNB == 1
    assert SpillType.NUMI_TCLK == 2
    assert SpillType.BNB_TCLK == 3
    assert SpillType.ACCEL_ONE_HZ_TCLK == 4
    assert SpillType.FAKE == 5
    assert SpillType.TEST_CONNECTION == 6
    assert SpillType.SUPER_CYCLE == 7
    assert SpillType.NUMI_SAMPLE_TRIG == 8
    assert SpillType.NUMI_RESET == 9
    assert SpillType.TB_SPILL == 10
    assert SpillType.TB_TRIG == 11


def test_parity_error_decodes_to_fake():
    """A parity error makes the type unknowable, as the C++ comment notes."""
    assert decode_event_word(PARITY_ERROR | CARRIER_MIBS | 0x74) is SpillType.FAKE


def test_live_tcr_reference_word():
    """0x041B is the word TCRMonitor tests for when writing shared memory."""
    assert decode_event_word(0x041B) is SpillType.BNB


def test_unknown_code_on_known_carrier_is_fake():
    assert decode_event_word(CARRIER_MIBS | 0x99) is SpillType.FAKE
    assert decode_event_word(CARRIER_TCLK | 0xEE) is SpillType.FAKE


def test_no_carrier_bit_is_fake():
    assert decode_event_word(0x0074) is SpillType.FAKE


@pytest.mark.parametrize("spelling", ["$74", "74", "0x74", "0X74", "$74 ", " 74"])
def test_signal_spellings_all_resolve(spelling):
    assert parse_signal(spelling).code == 0x74


def test_lowercase_hex_is_accepted_and_normalised():
    """Design.md writes the 1 Hz signal as '$8f'; operators use either case."""
    assert normalise_signal("$8f") == "$8F"
    assert normalise_signal("0x8f") == "$8F"


def test_signal_short_names_resolve():
    assert parse_signal("one-hertz").code == 0x8F
    assert parse_signal("numi").code == 0x74


def test_unknown_signal_names_the_known_ones():
    with pytest.raises(ValueError) as excinfo:
        parse_signal("$99")
    message = str(excinfo.value)
    assert "$99" in message
    assert "$74" in message, "the error should list what is available"


@pytest.mark.parametrize("bad", ["", "   ", "zz", "$zz", "0x", "$123456"])
def test_malformed_signals_raise(bad):
    with pytest.raises(ValueError):
        parse_signal(bad)


def test_signal_to_type_mapping():
    assert spill_type_for_signal("$74") is SpillType.NUMI
    assert spill_type_for_signal("$8F") is SpillType.ACCEL_ONE_HZ_TCLK


def test_ambiguous_types_report_every_signal():
    """$1D and $1F are indistinguishable once decoded; the API must say so."""
    booster = [s.hex for s in signals_for_type(SpillType.BNB_TCLK)]
    assert booster == ["$1D", "$1F"]

    numi = [s.hex for s in signals_for_type(SpillType.NUMI_TCLK)]
    assert numi == ["$AD", "$A9"]


def test_unambiguous_types_map_to_one_signal():
    assert len(signals_for_type(SpillType.ACCEL_ONE_HZ_TCLK)) == 1
    assert len(signals_for_type(SpillType.NUMI)) == 1


def test_every_signal_round_trips_through_its_event_word():
    for signal in SIGNALS:
        assert decode_event_word(signal.event_word) is signal.spill_type
        assert signal_for_code(signal.code) is signal


def test_signal_hex_formatting_is_two_digits_upper():
    assert signal_for_code(0x00).hex == "$00"
    assert signal_for_code(0xAD).hex == "$AD"


@pytest.mark.parametrize("spelling,expected", [
    ("BNB_TCLK", SpillType.BNB_TCLK),
    ("bnb_tclk", SpillType.BNB_TCLK),
    ("bnb-tclk", SpillType.BNB_TCLK),
    ("kBNBtclk", SpillType.BNB_TCLK),
    ("3", SpillType.BNB_TCLK),
    ("kAccelOneHztclk", SpillType.ACCEL_ONE_HZ_TCLK),
    ("0", SpillType.NUMI),
])
def test_spill_type_spellings(spelling, expected):
    assert parse_spill_type(spelling) is expected


def test_out_of_range_spill_type_raises():
    with pytest.raises(ValueError):
        parse_spill_type("99")


def test_every_type_has_a_description():
    for spill_type in SpillType:
        assert describe_type(spill_type) != "Unknown spill type"


def test_carrier_names_are_populated():
    for signal in SIGNALS:
        assert signal.carrier_name in ("TCLK", "MIBS", "BNB")
