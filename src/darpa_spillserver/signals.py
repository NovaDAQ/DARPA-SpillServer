"""!
@file src/darpa_spillserver/signals.py
@brief Accelerator signal registry and the TDU event-word decode rules.

Accelerator signal registry.

The NOvA timing hardware reports accelerator events as a 16-bit *event word*.
The DAQ decodes that word into a coarse :class:`SpillType` enumeration before
storing it, so two representations are in play and clients use both:

``event word``
    The raw 16-bit value read from the TDU.  Its low byte is the accelerator
    signal number that operators quote in hex with a leading ``$`` --- ``$74``
    for NuMI proton extraction, ``$8F`` for the 1 Hz clock.  The high bits
    select the carrier: bit 8 TCLK, bit 9 MIBS, bit 10 BNB, bit 15 parity
    error.

``spill type``
    The decoded enumeration stored in the TDU shared-memory segment, mirroring
    ``nova::NovaSpillServer::SpillType`` in ``NovaSpillServer/NssSpillInfo.h``.

The mapping from signal to spill type is **many-to-one**: ``$1D`` and ``$1F``
both decode to :data:`SpillType.BNB_TCLK`, and ``$A9`` and ``$AD`` both decode
to :data:`SpillType.NUMI_TCLK`.  Records that carry only a spill type therefore
cannot be narrowed back to a single signal.  :func:`signals_for_type` makes
that ambiguity explicit rather than guessing, and the query layer reports it to
the caller so a result set is never silently over-specific.

The decode rules here are a direct port of
``NssSpillInfo::getSpillTypeFromEvent`` and are exercised against the C++
truth table in ``tests/test_signals.py``.
"""

from __future__ import annotations

from enum import IntEnum
from typing import Dict, List, NamedTuple, Optional, Tuple

__all__ = [
    "SpillType",
    "Signal",
    "SIGNALS",
    "CARRIER_TCLK",
    "CARRIER_MIBS",
    "CARRIER_BNB",
    "PARITY_ERROR",
    "decode_event_word",
    "normalise_signal",
    "parse_signal",
    "signal_for_code",
    "signals_for_type",
    "spill_type_for_signal",
    "parse_spill_type",
    "describe_type",
]


class SpillType(IntEnum):
    """!
    @brief Decoded spill type, matching the C++ ``SpillType`` enum value-for-value.

    The integer values are wire format: they are what the TDU writes into
    shared memory and what ``DumpSpillHistory`` reports as ``"Type"``.  They
    must not be reordered.

    @details They are also the stored ``spill_type`` column of the archive
    and the ``spill_type`` column of every CSV/JSON dump.
    """

    NUMI = 0             # MIBS $74, proton extraction into NuMI
    BNB = 1              # $1B, parasitic beam inhibit / TCR reference
    NUMI_TCLK = 2        # TCLK $A9 or $AD
    BNB_TCLK = 3         # TCLK $1D or $1F, booster extraction
    ACCEL_ONE_HZ_TCLK = 4  # TCLK $8F, 1 Hz clock
    FAKE = 5             # assigned on a parity error
    TEST_CONNECTION = 6
    SUPER_CYCLE = 7      # TCLK $00, super cycle and master clock reset
    NUMI_SAMPLE_TRIG = 8  # TCLK $A4, reference for $A5
    NUMI_RESET = 9       # TCLK $A5, NuMI reset for beam
    TB_SPILL = 10        # TCLK $39, start of test-beam slow extraction
    TB_TRIG = 11         # test-beam trigger card signal


## Carrier bits in the 16-bit event word.
## Bit 8 (0x0100): event arrived on the Tevatron clock (TCLK).
CARRIER_TCLK = 1 << 8
## Bit 9 (0x0200): event arrived on the Main Injector beam sync (MIBS).
CARRIER_MIBS = 1 << 9
## Bit 10 (0x0400): event arrived on the Booster Neutrino Beam (BNB) line.
CARRIER_BNB = 1 << 10
## Bit 15 (0x8000): the TDU flagged a parity error; the word is untrusted.
PARITY_ERROR = 1 << 15

## Human-readable name for each carrier bit.
_CARRIER_NAMES = {
    CARRIER_TCLK: "TCLK",
    CARRIER_MIBS: "MIBS",
    CARRIER_BNB: "BNB",
}


class Signal(NamedTuple):
    """!
    @brief One accelerator signal the DAQ knows how to decode.

    @details Immutable registry entry; the full set is SIGNALS.
    """

    ## Low byte of the event word (the ``$XX`` number).
    code: int
    """Low byte of the event word, e.g. ``0x74``."""

    ## Carrier bit the signal arrives on.
    carrier: int
    """Carrier bit: one of :data:`CARRIER_TCLK`, :data:`CARRIER_MIBS`,
    :data:`CARRIER_BNB`."""

    ## The SpillType this signal decodes to.
    spill_type: SpillType
    """The type this signal decodes to."""

    ## Short stable identifier (URL- and CSV-safe).
    name: str
    """Short stable identifier, safe in a URL or a CSV column."""

    ## One-line explanation for docs and the web UI.
    description: str
    """One-line explanation for documentation and the web UI."""

    @property
    def hex(self) -> str:
        """!
        @brief The operator-facing spelling, e.g. ``"$74"``.

        @return ``$`` followed by the code as two upper-case hex digits.
        """
        return "${:02X}".format(self.code)

    @property
    def event_word(self) -> int:
        """!
        @brief The full 16-bit event word for this signal.

        @return carrier | code, with the parity bit clear.
        """
        return self.carrier | self.code

    @property
    def carrier_name(self) -> str:
        """!
        @brief Name of the carrier bit: ``TCLK``, ``MIBS`` or ``BNB``.

        @return The carrier name.
        """
        return _CARRIER_NAMES[self.carrier]


## Every signal decoded by ``NssSpillInfo::getSpillTypeFromEvent``, in the
## order that function tests them.
SIGNALS: Tuple[Signal, ...] = (
    Signal(0x74, CARRIER_MIBS, SpillType.NUMI, "numi",
           "MIBS $74 -- 120 GeV proton extraction from MI into NuMI"),
    Signal(0x1B, CARRIER_BNB, SpillType.BNB, "bnb",
           "BNB $1B -- parasitic beam inhibit; the TCR reference signal"),
    Signal(0x1D, CARRIER_TCLK, SpillType.BNB_TCLK, "booster-reset",
           "TCLK $1D -- booster reset for a MiniBooNE beam cycle; "
           "expect $1F about 35 ms later"),
    Signal(0x1F, CARRIER_TCLK, SpillType.BNB_TCLK, "booster-extraction",
           "TCLK $1F -- booster extraction sync (BES); "
           "expect beam about 320 us later"),
    Signal(0xAD, CARRIER_TCLK, SpillType.NUMI_TCLK, "numi-mixed-mode",
           "TCLK $AD -- NuMI reset for a mixed-mode beamline ramp"),
    Signal(0xA9, CARRIER_TCLK, SpillType.NUMI_TCLK, "numi-tclk",
           "TCLK $A9 -- TCLK reflection of MIBS $74; the one to use for NuMI"),
    Signal(0x8F, CARRIER_TCLK, SpillType.ACCEL_ONE_HZ_TCLK, "one-hertz",
           "TCLK $8F -- 1 Hz accelerator pulser"),
    Signal(0x00, CARRIER_TCLK, SpillType.SUPER_CYCLE, "super-cycle",
           "TCLK $00 -- super cycle and master clock reset"),
    Signal(0xA4, CARRIER_TCLK, SpillType.NUMI_SAMPLE_TRIG, "numi-sample-trig",
           "TCLK $A4 -- NuMI cycle sample trigger; the reference for $A5"),
    Signal(0xA5, CARRIER_TCLK, SpillType.NUMI_RESET, "numi-reset",
           "TCLK $A5 -- NuMI reset for beam"),
    Signal(0x39, CARRIER_TCLK, SpillType.TB_SPILL, "testbeam-spill",
           "TCLK $39 -- start of test-beam slow extraction"),
)

## Lookup by low-byte code.  Keyed by code alone, which relies on every code
## in SIGNALS being unique across carriers (true of the C++ table today).
_BY_CODE: Dict[int, Signal] = {s.code: s for s in SIGNALS}
## Lookup by Signal.name.
_BY_NAME: Dict[str, Signal] = {s.name: s for s in SIGNALS}

## One-line description of every SpillType, including those with no Signal
## (FAKE, TEST_CONNECTION, TB_TRIG).
_TYPE_DESCRIPTIONS: Dict[SpillType, str] = {
    SpillType.NUMI: "NuMI proton extraction (MIBS $74)",
    SpillType.BNB: "Booster neutrino beam inhibit / TCR reference ($1B)",
    SpillType.NUMI_TCLK: "NuMI TCLK reflection ($A9 or $AD)",
    SpillType.BNB_TCLK: "Booster TCLK ($1D or $1F)",
    SpillType.ACCEL_ONE_HZ_TCLK: "1 Hz accelerator clock ($8F)",
    SpillType.FAKE: "Undecodable event (parity error)",
    SpillType.TEST_CONNECTION: "Test connection placeholder",
    SpillType.SUPER_CYCLE: "Super cycle / master clock reset ($00)",
    SpillType.NUMI_SAMPLE_TRIG: "NuMI cycle sample trigger ($A4)",
    SpillType.NUMI_RESET: "NuMI reset for beam ($A5)",
    SpillType.TB_SPILL: "Test-beam slow extraction ($39)",
    SpillType.TB_TRIG: "Test-beam trigger card signal",
}


def describe_type(spill_type: SpillType) -> str:
    """!
    @brief Return a human-readable description of *spill_type*.

    @param spill_type The type to describe.
    @return Its description, or ``"Unknown spill type"``.
    """
    return _TYPE_DESCRIPTIONS.get(spill_type, "Unknown spill type")


def decode_event_word(evt: int) -> SpillType:
    """!
    @brief Decode a raw 16-bit event word into a :class:`SpillType`.

    A direct port of ``NssSpillInfo::getSpillTypeFromEvent``.  An event word
    whose parity bit is set, or whose low byte is not a signal the DAQ knows
    on that carrier, decodes to :data:`SpillType.FAKE`.

    >>> decode_event_word(0x0274) is SpillType.NUMI
    True
    >>> decode_event_word(0x018F) is SpillType.ACCEL_ONE_HZ_TCLK
    True

    @param evt Raw 16-bit event word.
    @return The decoded SpillType; FAKE for anything undecodable.
    """
    if evt & PARITY_ERROR:
        return SpillType.FAKE

    code = evt & 0xFF
    # Carrier precedence MIBS > BNB > TCLK follows the C++ decode: only the
    # first carrier bit found is considered, and a code that is not
    # registered on *that* carrier is FAKE even if it exists on another.
    # A word with no carrier bit at all is FAKE.
    for carrier in (CARRIER_MIBS, CARRIER_BNB, CARRIER_TCLK):
        if evt & carrier:
            signal = _BY_CODE.get(code)
            if signal is not None and signal.carrier == carrier:
                return signal.spill_type
            return SpillType.FAKE
    return SpillType.FAKE


def normalise_signal(text: str) -> str:
    """!
    @brief Normalise an operator-supplied signal spelling to ``"$XX"`` form.

    Accepts the spellings people actually type --- ``$74``, ``74``, ``0x74``,
    ``0X74``, ``$8f`` --- and the registry's short names (``one-hertz``).
    Raises :class:`ValueError` on anything else.

    >>> normalise_signal("0x8f")
    '$8F'
    >>> normalise_signal("one-hertz")
    '$8F'

    @param text Signal spelling.
    @return The canonical ``$XX`` spelling.
    @throws ValueError if *text* is not a registered signal.
    """
    return parse_signal(text).hex


def parse_signal(text: str) -> Signal:
    """!
    @brief Resolve *text* to a registered :class:`Signal`.

    @param text Hex code (``$74``, ``74``, ``0x74``) or registry name.
    @return The matching Signal.
    @throws ValueError if *text* is not a recognised spelling, or names a
            code the DAQ does not decode.

    :raises ValueError: if *text* is not a recognised spelling, or names a
        code the DAQ does not decode.
    """
    if text is None:
        raise ValueError("no signal given")

    token = text.strip()
    if not token:
        raise ValueError("no signal given")

    lowered = token.lower()
    # Names are tried before hex so no name can be shadowed by a digit
    # string; no registered name is valid hex anyway.
    if lowered in _BY_NAME:
        return _BY_NAME[lowered]

    digits = lowered
    if digits.startswith("$"):
        digits = digits[1:]
    elif digits.startswith("0x"):
        digits = digits[2:]

    # At most two hex digits: a signal is the low byte only; a full event
    # word (carrier bits included) is deliberately not accepted here.
    if not digits or len(digits) > 2 or any(c not in "0123456789abcdef" for c in digits):
        raise ValueError(
            "{!r} is not a signal; expected a hex code such as $74, "
            "or a name such as {}".format(
                text, ", ".join(sorted(_BY_NAME)[:3]) + ", ..."
            )
        )

    code = int(digits, 16)
    signal = _BY_CODE.get(code)
    if signal is None:
        known = ", ".join(s.hex for s in SIGNALS)
        raise ValueError(
            "signal ${:02X} is not decoded by the NOvA DAQ; known signals "
            "are {}".format(code, known)
        )
    return signal


def signal_for_code(code: int) -> Optional[Signal]:
    """!
    @brief Return the :class:`Signal` for a low-byte *code*, or ``None``.

    @param code Low byte of the event word.
    @return The Signal, or None if the code is not registered (including
            the storage sentinel -1).
    """
    return _BY_CODE.get(code)


def spill_type_for_signal(text: str) -> SpillType:
    """!
    @brief Return the :class:`SpillType` that signal *text* decodes to.

    @param text Signal spelling accepted by parse_signal().
    @return The SpillType.
    @throws ValueError if *text* is not a registered signal.
    """
    return parse_signal(text).spill_type


def signals_for_type(spill_type: SpillType) -> List[Signal]:
    """!
    @brief Return every signal that decodes to *spill_type*.

    More than one entry means a stored record carrying this type is ambiguous:
    the hardware did not preserve which signal produced it.  Callers should
    surface that to the user rather than picking one.

    >>> [s.hex for s in signals_for_type(SpillType.BNB_TCLK)]
    ['$1D', '$1F']

    @param spill_type The type to invert.
    @return Signals in SIGNALS order; empty for FAKE, TEST_CONNECTION and
            TB_TRIG, which no registered signal produces.
    """
    return [s for s in SIGNALS if s.spill_type == spill_type]


def parse_spill_type(text: str) -> SpillType:
    """!
    @brief Resolve *text* to a :class:`SpillType`.

    Accepts the enum member name (``BNB_TCLK``, case-insensitive), the C++
    spelling (``kBNBtclk``), or the integer wire value.

    @param text Name, C++ spelling or integer (any object; str() is taken).
    @return The SpillType.
    @throws ValueError if *text* matches none of the accepted forms.
    """
    token = str(text).strip()
    if not token:
        raise ValueError("no spill type given")

    # A leading "-" is let through so "-1" gets the range error below rather
    # than the generic "not a spill type" message.
    if token.lstrip("-").isdigit():
        value = int(token)
        try:
            return SpillType(value)
        except ValueError:
            raise ValueError(
                "{} is not a valid spill type; expected 0..{}".format(
                    value, len(SpillType) - 1
                )
            ) from None

    # "bnb-tclk" and "BNB_TCLK" both name BNB_TCLK.
    canonical = token.lower().replace("-", "_")
    for member in SpillType:
        if member.name.lower() == canonical:
            return member

    # The C++ spellings, e.g. "kBNBtclk" / "kAccelOneHztclk".
    cxx = {
        "knumi": SpillType.NUMI,
        "kbnb": SpillType.BNB,
        "knumitclk": SpillType.NUMI_TCLK,
        "kbnbtclk": SpillType.BNB_TCLK,
        "kaccelonehztclk": SpillType.ACCEL_ONE_HZ_TCLK,
        "kfake": SpillType.FAKE,
        "ktestconnection": SpillType.TEST_CONNECTION,
        "ksupercycle": SpillType.SUPER_CYCLE,
        "knumisampletrig": SpillType.NUMI_SAMPLE_TRIG,
        "knumireset": SpillType.NUMI_RESET,
        "ktbspill": SpillType.TB_SPILL,
        "ktbtrig": SpillType.TB_TRIG,
    }
    key = token.lower().replace("_", "")
    if key in cxx:
        return cxx[key]

    raise ValueError(
        "{!r} is not a spill type; expected a name such as BNB_TCLK or an "
        "integer 0..{}".format(text, len(SpillType) - 1)
    )
