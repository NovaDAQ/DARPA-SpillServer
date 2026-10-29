"""!
@file tests/test_signals.py
@brief Tests for the accelerator signal registry.

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
# Carrier bits: TCLK = 1<<8, MIBS = 1<<9, BNB = 1<<10; low byte is the
# accelerator signal code.
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
    """!
    @brief decode_event_word reproduces the DAQ's type for every (event word,
    type) pair in DECODE_TRUTH_TABLE.
    @param event_word 16-bit TDU event word (carrier bit | 8-bit signal code).
    @param expected SpillType that NssSpillInfo assigns to that word.
    """
    assert decode_event_word(event_word) is expected


def test_spill_type_values_are_wire_format():
    """!
    @brief The integers are what the hardware writes; reordering would corrupt
    data.
    """
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
    """!
    @brief A parity error makes the type unknowable, as the C++ comment notes.
    """
    # PARITY_ERROR is bit 15; set on an otherwise valid $74 NuMI word.
    assert decode_event_word(PARITY_ERROR | CARRIER_MIBS | 0x74) is SpillType.FAKE


def test_live_tcr_reference_word():
    """!
    @brief 0x041B is the word TCRMonitor tests for when writing shared memory.
    """
    # 0x041B = BNB carrier (1<<10) | $1B.
    assert decode_event_word(0x041B) is SpillType.BNB


def test_unknown_code_on_known_carrier_is_fake():
    """!
    @brief A carrier bit with an unregistered signal code decodes to FAKE
    rather than to a guessed type.
    """
    assert decode_event_word(CARRIER_MIBS | 0x99) is SpillType.FAKE
    assert decode_event_word(CARRIER_TCLK | 0xEE) is SpillType.FAKE


def test_no_carrier_bit_is_fake():
    """!
    @brief A word with no carrier bit (8-10) set decodes to FAKE even if its
    low byte is a known code.
    """
    assert decode_event_word(0x0074) is SpillType.FAKE


@pytest.mark.parametrize("spelling", ["$74", "74", "0x74", "0X74", "$74 ", " 74"])
def test_signal_spellings_all_resolve(spelling):
    """!
    @brief parse_signal accepts $NN, bare NN, 0xNN/0XNN and surrounding
    whitespace.
    @param spelling one textual form of signal $74.
    """
    assert parse_signal(spelling).code == 0x74


def test_lowercase_hex_is_accepted_and_normalised():
    """!
    @brief Design.md writes the 1 Hz signal as '$8f'; operators use either
    case.
    """
    assert normalise_signal("$8f") == "$8F"
    assert normalise_signal("0x8f") == "$8F"


def test_signal_short_names_resolve():
    """!
    @brief Registry short names ("one-hertz", "numi") resolve to their signal
    codes.
    """
    assert parse_signal("one-hertz").code == 0x8F
    assert parse_signal("numi").code == 0x74


def test_unknown_signal_names_the_known_ones():
    """!
    @brief An unregistered signal raises ValueError whose message echoes the
    input and lists the valid signals.
    """
    with pytest.raises(ValueError) as excinfo:
        parse_signal("$99")
    message = str(excinfo.value)
    assert "$99" in message
    assert "$74" in message, "the error should list what is available"


@pytest.mark.parametrize("bad", ["", "   ", "zz", "$zz", "0x", "$123456"])
def test_malformed_signals_raise(bad):
    """!
    @brief Empty, non-hex, prefix-only and over-long spellings raise
    ValueError.
    @param bad malformed signal text.
    """
    with pytest.raises(ValueError):
        parse_signal(bad)


def test_signal_to_type_mapping():
    """!
    @brief spill_type_for_signal maps a signal to the SpillType its event word
    decodes to.
    """
    assert spill_type_for_signal("$74") is SpillType.NUMI
    assert spill_type_for_signal("$8F") is SpillType.ACCEL_ONE_HZ_TCLK


def test_ambiguous_types_report_every_signal():
    """!
    @brief $1D and $1F are indistinguishable once decoded; the API must say so.
    """
    booster = [s.hex for s in signals_for_type(SpillType.BNB_TCLK)]
    assert booster == ["$1D", "$1F"]

    numi = [s.hex for s in signals_for_type(SpillType.NUMI_TCLK)]
    assert numi == ["$AD", "$A9"]


def test_unambiguous_types_map_to_one_signal():
    """!
    @brief Types produced by a single signal report exactly one signal.
    """
    assert len(signals_for_type(SpillType.ACCEL_ONE_HZ_TCLK)) == 1
    assert len(signals_for_type(SpillType.NUMI)) == 1


def test_every_signal_round_trips_through_its_event_word():
    """!
    @brief For every registered signal, decoding its event word yields its type
    and looking up its code yields the same registry entry.
    """
    for signal in SIGNALS:
        assert decode_event_word(signal.event_word) is signal.spill_type
        assert signal_for_code(signal.code) is signal


def test_signal_hex_formatting_is_two_digits_upper():
    """!
    @brief Signal.hex is '$' plus two upper-case hex digits, zero-padded.
    """
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
    """!
    @brief parse_spill_type accepts enum names in any case, '-' for '_', the
    C++ k-constant names and the integer value.
    @param spelling textual form of a spill type.
    @param expected SpillType it must parse to.
    """
    assert parse_spill_type(spelling) is expected


def test_out_of_range_spill_type_raises():
    """!
    @brief An integer outside the SpillType enum raises ValueError.
    """
    with pytest.raises(ValueError):
        parse_spill_type("99")


def test_every_type_has_a_description():
    """!
    @brief describe_type has a specific description for every SpillType member.
    """
    for spill_type in SpillType:
        assert describe_type(spill_type) != "Unknown spill type"


def test_carrier_names_are_populated():
    """!
    @brief Every registered signal names one of the three timing carriers.
    """
    for signal in SIGNALS:
        assert signal.carrier_name in ("TCLK", "MIBS", "BNB")
