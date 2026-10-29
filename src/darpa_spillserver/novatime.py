"""!
@file src/darpa_spillserver/novatime.py
@brief Time parsing and conversion between NOvA ticks, UNIX, UTC and GPS.

Time parsing and conversion for spill queries.

Everything the TDU records is stamped in **NOvA time** --- a 64-bit count of
64 MHz ticks since 01-Jan-2010 00:00:00 UTC.  Clients, though, ask questions in
human terms ("all the $74 events between Jul 1, 2026 and today", "from 09:15
today until 11:34 today") and want answers back in UTC and GPS.  This module is
the boundary between the two.

**Timescales and units.** NOvA time is an atomic, leap-second-free count:
it advances by exactly 64,000,000 ticks per SI second (1 tick = 15.625 ns =
15625 ps), so it is a fixed integer-second offset from GPS time
(``NOVA_GPS_OFFSET`` in :mod:`nova_time_decoder`).  UNIX time does not count
leap seconds, so NOvA <-> UNIX/UTC goes through the decoder's post-epoch
leap-second table (2012-06-30, 2015-06-30, 2016-12-31).  The TDU-reported
``pps_offset`` stored elsewhere in the project is the tick phase within the
current second; with a GPS-aligned PPS it equals ``nova mod 64e6``.

The arithmetic itself is delegated to the :mod:`nova_time_decoder` package;
nothing here reimplements a conversion.  What this module adds is

* :func:`parse_instant`, which accepts the range endpoints people actually
  type, and
* :func:`convert`, which renders one NOvA tick count into every representation
  the API can emit.

**Timezone.** A bare clock time such as ``09:15`` is ambiguous without a zone.
The zone is an explicit argument, defaulting to UTC so that an unconfigured
server never silently shifts a result by six hours; deployments that want
operators to speak Fermilab local time set ``query.timezone`` in the config
file (see :mod:`darpa_spillserver.config`).  Absolute inputs that carry their
own offset (``...Z``, ``+01:00``) always win over the default.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta, timezone
from typing import Optional, Union
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from nova_time_decoder import (
    NOVA_EPOCH,
    NOVA_TIME_FACTOR,
    GpsTime,
    gps_to_nova,
    nova_to_gps,
    nova_to_string,
    nova_to_unix,
    unix_to_nova,
)

__all__ = [
    "UTC",
    "NovaTimeError",
    "ConvertedTime",
    "get_timezone",
    "parse_instant",
    "parse_range",
    "convert",
    "nova_now",
    "datetime_to_nova",
    "nova_to_datetime",
]

## The UTC tzinfo; the default zone for every input that carries none.
UTC = timezone.utc

## Ticks per second of the NOvA 64 MHz clock; re-exported for callers.
TICKS_PER_SECOND = NOVA_TIME_FACTOR

## A NOvA tick count large enough that it cannot be confused with a UNIX
## timestamp.  The NOvA epoch is 2010, so one year of ticks is ~2.0e15 while a
## plausible UNIX time is ~1.8e9; anything at or above this is unambiguous.
## (1e12 ticks is only ~4.3 h after the NOvA epoch, and 1e12 UNIX seconds is
## ~31,700 years out, so neither side of the cut is ever reached in practice.)
_NOVA_TICK_THRESHOLD = 10 ** 12

## Relative offsets: ``-2h``, ``now-30m``, ``+1.5d``.  The caller strips
## blanks before matching.  ``m`` is minutes, never months (months have no
## fixed length, so they are deliberately not offered).
_RELATIVE_RE = re.compile(
    r"^(?:now\s*)?(?P<sign>[-+])\s*(?P<value>\d+(?:\.\d+)?)\s*"
    r"(?P<unit>ns|us|ms|s|sec|secs|second|seconds|m|min|mins|minute|minutes|"
    r"h|hr|hrs|hour|hours|d|day|days|w|week|weeks)$",
    re.IGNORECASE,
)

## Seconds per unit for every unit spelling _RELATIVE_RE accepts.  Values
## are SI seconds; a "day" is 86400 s, i.e. relative offsets ignore DST and
## leap seconds (they are applied as timedelta on an aware datetime).
_UNIT_SECONDS = {
    "ns": 1e-9, "us": 1e-6, "ms": 1e-3,
    "s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
    "m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
    "h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
    "d": 86400, "day": 86400, "days": 86400,
    "w": 604800, "week": 604800, "weeks": 604800,
}

## Bare clock time ``HH:MM`` or ``HH:MM:SS[.frac]``, interpreted on the
## reference date in the query timezone.
_BARE_TIME_RE = re.compile(
    r"^(?P<hour>\d{1,2}):(?P<minute>\d{2})(?::(?P<second>\d{2}(?:\.\d+)?))?$"
)

## Calendar formats accepted in addition to ISO 8601, covering the spellings
## used in NOvA logbooks and in Design.md ("Jul 1, 2026").
_DATE_FORMATS = (
    "%Y-%m-%d", "%Y/%m/%d", "%d-%b-%Y", "%d %b %Y",
    "%b %d %Y", "%b %d, %Y", "%B %d %Y", "%B %d, %Y",
    "%m/%d/%Y",
)

## Every _DATE_FORMATS spelling followed by a clock time (space or ``T``
## separator; minute, second or microsecond resolution).  strptime's ``%f``
## takes at most 6 digits, so these cannot express sub-microsecond input.
_DATETIME_FORMATS = tuple(
    fmt + sep + tfmt
    for fmt in _DATE_FORMATS
    for sep in (" ", "T")
    for tfmt in ("%H:%M", "%H:%M:%S", "%H:%M:%S.%f")
)


class NovaTimeError(ValueError):
    """!
    @brief Raised when a time expression cannot be understood.

    @details Subclasses ValueError so callers that treat any bad input as a
    ValueError (e.g. the HTTP layer mapping to 400) need no special case.
    """


@dataclass(frozen=True)
class ConvertedTime:
    """!
    @brief One instant rendered in every representation the API emits.

    @details Immutable; built only by convert().  Integer fields are exact
    except gps_nsec and unix_nsec, which truncate the 15.625 ns tick to
    whole nanoseconds.
    """

    ## NOvA base time: 64 MHz ticks since 2010-01-01T00:00:00Z.
    nova: int
    """NOvA base time, in 64 MHz ticks since the NOvA epoch."""

    ## UNIX time_t (leap seconds removed via the decoder's table).
    unix_sec: int
    ## Sub-second part of the UNIX time in ns, truncated from the tick.
    unix_nsec: int
    ## ISO 8601 UTC string with 9 fractional digits and a trailing Z.
    utc: str
    """ISO 8601 UTC, e.g. ``2026-09-22T15:53:33.229378890Z``."""

    ## The DAQ's own UTC rendering (picosecond fraction).
    utc_string: str
    """The DAQ's own rendering, e.g. ``2026-Sep-22 15:53:33.229378890625 UTC``."""

    ## Whole GPS seconds since 1980-01-06T00:00:00Z.
    gps_seconds: int
    """Whole GPS seconds since the GPS epoch."""

    ## GPS sub-second in ns (truncated; see gps_psec for the exact value).
    gps_nsec: int
    """Sub-second remainder in nanoseconds, as :mod:`nova_time_decoder`
    reports it.  A NOvA tick is 15.625 ns, so this *truncates*: it cannot
    land on a tick boundary.  Use :attr:`gps_psec` or :attr:`gps` when the
    exact instant matters."""

    ## GPS sub-second in ps; exact, since 1 tick = 15625 ps.
    gps_psec: int
    """Sub-second remainder in picoseconds, exact.  1e12 / 64e6 is 15625
    exactly, so every NOvA tick maps to a whole number of picoseconds with
    nothing lost.  This is the same unit the DAQ's own ``utc_string`` uses."""

    ## Full GPS week number (not reduced modulo 1024).
    gps_week: int
    ## Whole-second GPS time-of-week, 0..604799.
    gps_tow: int
    """Whole-second GPS time-of-week; the sub-second part is in
    :attr:`gps_psec`."""

    ## GPS seconds with a 12-digit (picosecond) fraction.
    gps: str
    """Full-precision GPS seconds, e.g. ``1474127631.229378890625``."""

    ## GPS time-of-week with a 12-digit (picosecond) fraction.
    gps_tow_exact: str
    """Full-precision GPS time-of-week, e.g. ``230031.229378890625``."""

    ## Human-readable "week W, TOW t (s s)" rendering.
    gps_string: str
    """Both together, e.g.
    ``week 2437, TOW 230031.229378890625 (1474127631.229378890625 s)``."""

    def as_dict(self) -> dict:
        """!
        @brief Return the fields as a plain dict in declaration order.

        @details The key order is the column order of the JSON and CSV
        renderers that consume it; keep it stable.
        @return Mapping of field name to value.
        """
        return {
            "nova": self.nova,
            "unix_sec": self.unix_sec,
            "unix_nsec": self.unix_nsec,
            "utc": self.utc,
            "utc_string": self.utc_string,
            "gps_seconds": self.gps_seconds,
            "gps_nsec": self.gps_nsec,
            "gps_psec": self.gps_psec,
            "gps_week": self.gps_week,
            "gps_tow": self.gps_tow,
            "gps": self.gps,
            "gps_tow_exact": self.gps_tow_exact,
            "gps_string": self.gps_string,
        }


def get_timezone(name: str) -> timezone:
    """!
    @brief Resolve a timezone name, accepting ``UTC`` and any IANA zone.

    @param name Zone name (``UTC``/``Z``/``GMT``, case-insensitive, or an IANA
                name such as ``America/Chicago``); None means UTC.
    @return A tzinfo: the fixed UTC zone or a zoneinfo.ZoneInfo.
    @throws NovaTimeError if the zone is not installed on this host.

    :raises NovaTimeError: if the zone is not installed on this host.
    """
    if name is None or str(name).strip().upper() in ("UTC", "Z", "GMT"):
        return UTC
    try:
        return ZoneInfo(str(name).strip())
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise NovaTimeError("unknown timezone {!r}: {}".format(name, exc)) from None


def nova_now() -> int:
    """!
    @brief Return the current wall-clock instant as a NOvA tick count.

    @details Resolution is that of datetime (1 us); the host clock is assumed
    to be NTP-disciplined UTC.
    @return NOvA ticks for the current instant.
    """
    return datetime_to_nova(datetime.now(tz=UTC))


def datetime_to_nova(moment: datetime) -> int:
    """!
    @brief Convert an aware :class:`~datetime.datetime` to NOvA ticks.

    A naive datetime is rejected rather than assumed to be UTC, so that a
    caller which forgot to attach a zone gets an error instead of a six-hour
    offset.

    @param moment Timezone-aware instant.
    @return NOvA tick count (leap seconds added per the decoder's table).
    @throws NovaTimeError if moment is naive or predates the NOvA epoch.
    """
    if moment.tzinfo is None:
        raise NovaTimeError(
            "refusing to convert a naive datetime; attach a timezone first"
        )
    utc_moment = moment.astimezone(UTC)
    # Whole seconds and the fraction are taken separately: the float from
    # timestamp() only supplies the integer part (its ~240 ns resolution is
    # far finer than 1 s), and the sub-second part comes exactly from the
    # microsecond field.  us -> ns is exact; ns -> ticks in unix_to_nova
    # floors (ticks = nsec * 64e6 // 1e9).
    epoch_seconds = int(utc_moment.timestamp())
    nsec = utc_moment.microsecond * 1000
    ticks = unix_to_nova(epoch_seconds, nsec)
    if ticks is None:
        raise NovaTimeError(
            "{} predates the NOvA epoch (01-Jan-2010 UTC)".format(
                utc_moment.isoformat()
            )
        )
    return ticks


def nova_to_datetime(nova: int) -> datetime:
    """!
    @brief Convert NOvA ticks to an aware UTC :class:`~datetime.datetime`.

    ``datetime`` resolves to microseconds, so the returned value is truncated
    from the tick-level precision the hardware provides.  Use
    :func:`convert` when the full precision matters.

    @param nova NOvA tick count (non-negative).
    @return Aware UTC datetime, truncated to the microsecond.  During an
            inserted leap second the result repeats the preceding UNIX
            second, as UNIX time itself does.
    """
    unix = nova_to_unix(nova)
    # Build from the integer second plus a timedelta rather than from a float
    # timestamp, so the sub-second part is not rounded through float64.
    return datetime.fromtimestamp(unix.sec, tz=UTC) + timedelta(
        microseconds=unix.nsec // 1000
    )


def convert(nova: int) -> ConvertedTime:
    """!
    @brief Render one NOvA tick count in every supported representation.

    @param nova NOvA tick count.
    @return The ConvertedTime for that instant.
    @throws NovaTimeError if nova is negative.
    """
    if nova < 0:
        raise NovaTimeError("NOvA time cannot be negative: {}".format(nova))

    # UNIX/UTC go through the leap-second table; GPS is a fixed offset.
    unix = nova_to_unix(nova)
    gps: GpsTime = nova_to_gps(nova)
    # Only whole seconds go through datetime; the 9-digit fraction is
    # appended from the integer ns so no float rounding enters the string.
    moment = datetime.fromtimestamp(unix.sec, tz=UTC)
    iso = "{}.{:09d}Z".format(moment.strftime("%Y-%m-%dT%H:%M:%S"), unix.nsec)

    # GPS is a leap-second-free offset from NOvA time, so the sub-second part
    # of the two is the same value. Deriving it from the tick remainder rather
    # than from gps.nsec keeps it exact: 1e12 / 64e6 == 15625, so a tick is a
    # whole number of picoseconds, while nanoseconds truncate 625 ps of it.
    whole_ticks = nova // NOVA_TIME_FACTOR
    frac_ticks = nova - whole_ticks * NOVA_TIME_FACTOR
    picoseconds = frac_ticks * 1_000_000_000_000 // NOVA_TIME_FACTOR

    gps_exact = "{:d}.{:012d}".format(gps.seconds, picoseconds)
    gps_tow_exact = "{:d}.{:012d}".format(gps.tow, picoseconds)

    return ConvertedTime(
        nova=nova,
        unix_sec=unix.sec,
        unix_nsec=unix.nsec,
        utc=iso,
        utc_string=nova_to_string(nova),
        gps_seconds=gps.seconds,
        gps_nsec=gps.nsec,
        gps_psec=picoseconds,
        gps_week=gps.week,
        gps_tow=gps.tow,
        gps=gps_exact,
        gps_tow_exact=gps_tow_exact,
        gps_string="week {:d}, TOW {} ({} s)".format(
            gps.week, gps_tow_exact, gps_exact
        ),
    )


def _end_of_day(day: date, tz) -> datetime:
    """!
    @brief The first instant of the following day, used as an exclusive bound.

    @details Midnight is computed in *tz*, so a DST-transition day in a local
    zone is correctly 23 or 25 hours long.
    @param day Calendar date.
    @param tz tzinfo the date is interpreted in.
    @return Aware datetime of 00:00 on day + 1 in tz.
    """
    return datetime.combine(day + timedelta(days=1), dtime(0, 0), tzinfo=tz)


def parse_instant(
    text: Union[str, int, datetime, None],
    now: Optional[datetime] = None,
    tz=UTC,
    end_of_day: bool = False,
) -> int:
    """!
    @brief Parse a time expression into a NOvA tick count.

    @param text Expression to parse: str, int (bare-number rules), or an
                aware datetime.
    @param now Reference instant for relative and date-less forms; must be
               aware.  None means the real clock.
    @param tz Zone for inputs that carry none.
    @param end_of_day Return the exclusive end of a date-only day instead of
                      its start.
    @return NOvA tick count.
    @throws NovaTimeError if the expression cannot be understood, is
            negative, or predates the NOvA epoch.


    Accepted forms, tried in this order:

    ===========================  ==============================================
    ``now``                      the current instant
    ``today`` / ``yesterday``    midnight of that day (see *end_of_day*)
    ``-2h``, ``now-30m``, ``+1d``  relative to *now*
    ``09:15``, ``11:34:22``      that clock time on *now*'s date
    ``2026-07-01``               midnight on that date (see *end_of_day*)
    ``Jul 1, 2026``              same, in the spelling Design.md uses
    ``2026-07-01T09:15:00Z``     ISO 8601; a trailing ``Z`` or offset wins
    ``nova:337784596...``        an explicit NOvA tick count
    ``unix:1790092413``          an explicit UNIX time (``.frac`` allowed)
    ``gps:1025136016``           GPS seconds, or ``gps:1695:259216`` week:TOW
    a bare integer               NOvA ticks if huge, else a UNIX time
    ===========================  ==============================================

    :param now: the instant ``now``/``today`` resolve against; defaults to the
        real clock.  Supplying it makes callers and tests deterministic.
    :param tz: zone used for inputs that carry none.
    :param end_of_day: when the expression names a whole day with no clock
        time, return the day's *exclusive upper* bound rather than its start.
        Set this for the end of a range so that ``2026-07-01`` to
        ``2026-07-01`` covers that whole day instead of being empty.
    :raises NovaTimeError: if the expression cannot be understood.
    """
    if text is None:
        raise NovaTimeError("no time given")

    if isinstance(text, datetime):
        return datetime_to_nova(text)

    # bool is an int subclass; True/False are not times.
    if isinstance(text, int) and not isinstance(text, bool):
        return _from_bare_number(str(text))

    token = str(text).strip()
    if not token:
        raise NovaTimeError("no time given")

    reference = now if now is not None else datetime.now(tz=UTC)
    if reference.tzinfo is None:
        raise NovaTimeError("the reference 'now' must carry a timezone")
    # "today", bare clock times etc. are calendar notions, so they are taken
    # on the reference's date *in the query zone*, not in UTC.
    local_now = reference.astimezone(tz)

    lowered = token.lower()

    # --- explicitly tagged timescales -----------------------------------
    # Checked first so a tag always overrides every heuristic below.
    for prefix in ("nova:", "novatime:", "ticks:"):
        if lowered.startswith(prefix):
            return _require_nonnegative(token[len(prefix):].strip(), "NOvA ticks")

    if lowered.startswith("unix:") or lowered.startswith("epoch:"):
        return _unix_string_to_nova(token.split(":", 1)[1].strip())

    if lowered.startswith("gps:"):
        return _gps_string_to_nova(token[4:].strip())

    # --- keywords --------------------------------------------------------
    if lowered in ("now", "currently"):
        return datetime_to_nova(reference)

    if lowered in ("today", "midnight"):
        day = local_now.date()
        moment = _end_of_day(day, tz) if end_of_day else datetime.combine(
            day, dtime(0, 0), tzinfo=tz
        )
        return datetime_to_nova(moment)

    if lowered == "yesterday":
        day = local_now.date() - timedelta(days=1)
        moment = _end_of_day(day, tz) if end_of_day else datetime.combine(
            day, dtime(0, 0), tzinfo=tz
        )
        return datetime_to_nova(moment)

    # NOvA tick 0 is 2010-01-01T00:00:00Z, the earliest representable instant.
    if lowered in ("epoch", "beginning", "start"):
        return 0

    # --- relative offsets -------------------------------------------------
    match = _RELATIVE_RE.match(lowered.replace(" ", ""))
    if match:
        # Offsets are applied to the aware reference instant, i.e. in elapsed
        # civil (UNIX) seconds; float precision is ample at s..week scales.
        seconds = float(match.group("value")) * _UNIT_SECONDS[match.group("unit")]
        if match.group("sign") == "-":
            seconds = -seconds
        return datetime_to_nova(reference + timedelta(seconds=seconds))

    # --- bare clock time, meaning "today at" -------------------------------
    match = _BARE_TIME_RE.match(lowered)
    if match:
        # Seconds may carry a fraction; split it into datetime's whole
        # second + microsecond.  Precision below 1 us is dropped.
        second_text = match.group("second") or "0"
        whole_second = int(float(second_text))
        microsecond = int(round((float(second_text) - whole_second) * 1e6))
        try:
            clock = dtime(
                int(match.group("hour")),
                int(match.group("minute")),
                whole_second,
                microsecond,
            )
        except ValueError as exc:
            raise NovaTimeError("{!r} is not a valid clock time: {}".format(token, exc))
        return datetime_to_nova(
            datetime.combine(local_now.date(), clock, tzinfo=tz)
        )

    # --- ISO 8601 ----------------------------------------------------------
    # datetime.fromisoformat() before Python 3.11 rejects a trailing "Z",
    # so it is rewritten to the equivalent explicit offset.
    iso_candidate = token[:-1] + "+00:00" if token.endswith(("Z", "z")) else token
    try:
        parsed = datetime.fromisoformat(iso_candidate)
    except ValueError:
        parsed = None
    if parsed is not None:
        # A date-only ISO string (no T/space/colon) names a whole day and is
        # eligible for end-of-day widening; anything with a clock is exact.
        # An explicit offset in the string wins over tz.
        had_clock = any(sep in token for sep in ("T", " ", ":"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=tz)
        if not had_clock and end_of_day:
            return datetime_to_nova(_end_of_day(parsed.date(), parsed.tzinfo))
        return datetime_to_nova(parsed)

    # --- other calendar spellings -------------------------------------------
    # Date+time spellings first: they are exact instants and never widened.
    for fmt in _DATETIME_FORMATS:
        try:
            parsed = datetime.strptime(token, fmt)
        except ValueError:
            continue
        return datetime_to_nova(parsed.replace(tzinfo=tz))

    # Date-only spellings: midnight, or the next midnight if end_of_day.
    for fmt in _DATE_FORMATS:
        try:
            parsed = datetime.strptime(token, fmt)
        except ValueError:
            continue
        day = parsed.date()
        moment = _end_of_day(day, tz) if end_of_day else datetime.combine(
            day, dtime(0, 0), tzinfo=tz
        )
        return datetime_to_nova(moment)

    # --- a bare number ------------------------------------------------------
    # Last: the threshold heuristic is the least specific rule, so every
    # tagged, keyword and calendar form gets first refusal.
    try:
        return _from_bare_number(token)
    except NovaTimeError:
        pass

    raise NovaTimeError(
        "cannot understand the time {!r}; try an ISO 8601 instant "
        "(2026-07-01T09:15:00Z), a date (2026-07-01), a clock time (09:15), "
        "a relative offset (-2h), or a tagged value "
        "(nova:..., unix:..., gps:...)".format(text)
    )


def _from_bare_number(token: str) -> int:
    """!
    @brief Interpret a bare number as NOvA ticks or a UNIX time.

    Values at or above :data:`_NOVA_TICK_THRESHOLD` are NOvA ticks; smaller
    ones are UNIX seconds.  The two ranges do not overlap for any instant this
    server can serve, so the guess is safe --- but callers who want certainty
    should use the ``nova:`` or ``unix:`` prefix.

    @details A ``0x`` prefix is always read as NOvA ticks, regardless of
    magnitude.  A fractional value must be a UNIX time.
    @param token Numeric text.
    @return NOvA tick count.
    @throws NovaTimeError if not a number, negative, a fractional value
            above the threshold, or before the NOvA epoch.
    """
    stripped = token.strip()
    if stripped.lower().startswith("0x"):
        try:
            return _require_nonnegative(str(int(stripped, 16)), "NOvA ticks")
        except ValueError:
            raise NovaTimeError("{!r} is not a hex number".format(token)) from None

    # A pure integer is parsed as an int, never via float: a NOvA tick count
    # is ~3.4e16, far beyond float64's exact-integer range (2**53), so going
    # through a float would silently shift the timestamp by a tick or more.
    if re.fullmatch(r"[+-]?\d+", stripped):
        value_int = int(stripped)
        if value_int < 0:
            raise NovaTimeError("a time cannot be negative: {}".format(token))
        if value_int >= _NOVA_TICK_THRESHOLD:
            return value_int
        return _unix_string_to_nova(stripped)

    try:
        value = float(stripped)
    except ValueError:
        raise NovaTimeError("{!r} is not a number".format(token)) from None

    if value < 0:
        raise NovaTimeError("a time cannot be negative: {}".format(token))

    if value >= _NOVA_TICK_THRESHOLD:
        raise NovaTimeError(
            "{!r} is too large to be a fractional time; write a NOvA tick "
            "count as a whole number, or tag it as nova:...".format(token)
        )
    return _unix_string_to_nova(stripped)


def _unix_string_to_nova(token: str) -> int:
    """!
    @brief Parse a UNIX time, keeping the seconds and the fraction separate.

    float64 has about 240 ns of resolution near the present epoch, which is
    coarser than the 15.625 ns NOvA tick, so the two halves are parsed
    independently rather than through a single float.

    @param token ``SECONDS[.FRACTION]`` text.
    @return NOvA tick count (leap seconds added per the decoder's table).
    @throws NovaTimeError if malformed, negative, or before the NOvA epoch.
    """
    stripped = token.strip()
    whole, _, fraction = stripped.partition(".")
    try:
        seconds = int(whole)
        # The fraction alone is < 1, so float64 holds it to ~1e-16 s; the
        # result is rounded to whole ns and then floored to ticks by
        # unix_to_nova.  A fraction like .9999999999 rounds to 1e9 ns, which
        # unix_to_nova turns into a full extra second of ticks (correct).
        nsec = int(round(float("0." + fraction) * 1e9)) if fraction else 0
    except ValueError:
        raise NovaTimeError("{!r} is not a UNIX time".format(token)) from None
    if seconds < 0:
        raise NovaTimeError("a UNIX time cannot be negative: {}".format(token))
    ticks = unix_to_nova(seconds, nsec)
    if ticks is None:
        raise NovaTimeError(
            "UNIX time {} predates the NOvA epoch ({})".format(token, NOVA_EPOCH)
        )
    return ticks


def _gps_string_to_nova(token: str) -> int:
    """!
    @brief Parse a GPS time as ``SECONDS[.frac]`` or ``WEEK:TOW[.frac]``.

    @details GPS and NOvA time are both leap-second-free, so the conversion
    is a fixed integer-second offset with no table lookup.  Unlike
    _unix_string_to_nova, the fraction here goes through a float of the
    whole value, so sub-microsecond digits of a plain-seconds input are not
    preserved exactly.
    @param token GPS seconds since 1980-01-06, or week number and
                 time-of-week separated by a colon.
    @return NOvA tick count.
    @throws NovaTimeError if malformed or before the NOvA epoch.
    """
    if ":" in token:
        # seconds = week * 604800 + whole TOW; the TOW fraction becomes ns.
        week_text, _, tow_text = token.partition(":")
        try:
            seconds = int(week_text) * 604800 + int(float(tow_text))
            nsec = int(round((float(tow_text) % 1) * 1e9))
        except ValueError:
            raise NovaTimeError(
                "{!r} is not a GPS week:TOW pair".format(token)
            ) from None
    else:
        try:
            value = float(token)
        except ValueError:
            raise NovaTimeError("{!r} is not a GPS time".format(token)) from None
        seconds = int(value)
        nsec = int(round((value - seconds) * 1e9))

    ticks = gps_to_nova(seconds, nsec)
    if ticks is None:
        raise NovaTimeError(
            "GPS time {} predates the NOvA epoch".format(token)
        )
    return ticks


def _require_nonnegative(token: str, what: str) -> int:
    """!
    @brief Parse a decimal integer and reject negatives.

    @param token Integer text.
    @param what Name of the quantity, used in the error message.
    @return The parsed non-negative integer.
    @throws NovaTimeError if not an integer or negative.
    """
    try:
        value = int(token)
    except ValueError:
        raise NovaTimeError("{!r} is not a valid {} value".format(token, what)) from None
    if value < 0:
        raise NovaTimeError("{} cannot be negative: {}".format(what, token))
    return value


def _swap_hint(start, end) -> str:
    """!
    @brief Spell out the corrected range when both ends are bare dates.

    Only then is the widening of a date-only end worth mentioning: swapping
    ``2026-10-11`` / ``2026-10-10`` gives two whole days, which is not what
    swapping two clock times gives.

    @param start Start expression as the caller supplied it.
    @param end End expression as the caller supplied it.
    @return A sentence to append to the error, or "" when not applicable.
    """
    bare = re.compile(r"^\s*\d{4}-\d{2}-\d{2}\s*$")
    if isinstance(start, str) and isinstance(end, str) \
            and bare.match(start) and bare.match(end):
        return (" A date given as the end includes that whole day, so start "
                "{} with end {} covers both days.".format(end.strip(),
                                                          start.strip()))
    return ""


def parse_range(
    start: Union[str, int, datetime, None],
    end: Union[str, int, datetime, None],
    now: Optional[datetime] = None,
    tz=UTC,
) -> "tuple[int, int]":
    """!
    @brief Resolve a start/end pair into an inclusive-exclusive NOvA tick range.

    @param start Start expression (see parse_instant); None or "" means tick 0.
    @param end End expression; None or "" means now.
    @param now Reference instant (aware) for relative forms; None means the
               real clock.  Both ends share it so "-1h" to "now" is exactly
               one hour.
    @param tz Zone for inputs that carry none.
    @return (start, end) NOvA ticks with start <= end; start inclusive,
            end exclusive.
    @throws NovaTimeError if either end is unparseable or the range runs
            backwards.

    An omitted *start* means the beginning of time; an omitted *end* means
    now.  The *end* is parsed with ``end_of_day=True`` so that a date-only
    upper bound covers that whole day --- ``2026-07-01`` to ``2026-07-01``
    returns everything on 1 July, which is what an operator means.

    :raises NovaTimeError: if either endpoint is unparseable, or if the range
        runs backwards.
    """
    reference = now if now is not None else datetime.now(tz=UTC)

    start_nova = 0 if start in (None, "") else parse_instant(
        start, now=reference, tz=tz, end_of_day=False
    )

    if end in (None, ""):
        return start_nova, datetime_to_nova(reference)

    # The endpoint is parsed twice. The unadjusted reading is what decides
    # whether the caller wrote the range backwards: after the end-of-day
    # widening, "today" to "yesterday" would collapse to an empty range and
    # look like a legitimate query for nothing, hiding the mistake.
    plain_end = parse_instant(end, now=reference, tz=tz, end_of_day=False)
    if plain_end < start_nova:
        raise NovaTimeError(
            "the end of the range ({}) is before its start ({}); are start "
            "and end swapped?{}".format(
                nova_to_string(plain_end), nova_to_string(start_nova),
                _swap_hint(start, end),
            )
        )

    end_nova = parse_instant(end, now=reference, tz=tz, end_of_day=True)
    # end_of_day only ever moves the end later, so this max() is a guard
    # that keeps the returned range well-formed rather than a correction.
    return start_nova, max(end_nova, start_nova)
