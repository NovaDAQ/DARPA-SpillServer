"""!
@file tests/test_novatime.py
@brief Tests for time parsing and timescale conversion.

The reference values here are not invented: they come from a live reading of
``http://tdu-near-master-ppc-01:8080/tcr_status``, whose JSON carries the
NOvA tick count, the DAQ's own rendered ``Timestring``, and the UNIX time it
derived.  Testing against the instrument's own arithmetic is what makes this
server's timestamps trustworthy.
"""

from datetime import datetime, timedelta, timezone

import pytest

from darpa_spillserver.novatime import (
    TICKS_PER_SECOND,
    UTC,
    ConvertedTime,
    NovaTimeError,
    convert,
    datetime_to_nova,
    get_timezone,
    nova_to_datetime,
    parse_instant,
    parse_range,
)

# Captured live from the TDU on 2026-09-22.
LIVE_NOVA = 33778458638680249
LIVE_UNIX_SEC = 1790092413
LIVE_UNIX_USEC = 229378
LIVE_TIMESTRING = "2026-Sep-22 15:53:33.229378890625 UTC"

# A fixed reference so "today" and "now" are deterministic.
NOW = datetime(2026, 9, 22, 15, 53, 33, tzinfo=UTC)


def test_conversion_matches_the_instrument():
    """!
    @brief Our conversion must agree with what the TDU itself reported.
    """
    result = convert(LIVE_NOVA)
    assert result.unix_sec == LIVE_UNIX_SEC
    assert result.unix_nsec // 1000 == LIVE_UNIX_USEC
    assert result.utc_string == LIVE_TIMESTRING
    assert result.utc.startswith("2026-09-22T15:53:33.")


def test_conversion_reports_every_timescale():
    """!
    @brief convert() returns a ConvertedTime whose GPS week/TOW are consistent
    with gps_seconds and whose as_dict() exposes exactly the documented keys.
    """
    result = convert(LIVE_NOVA)
    assert isinstance(result, ConvertedTime)
    assert result.nova == LIVE_NOVA
    assert result.gps_seconds > 0
    # 604800 = seconds per GPS week.
    assert result.gps_week == result.gps_seconds // 604800
    assert result.gps_tow == result.gps_seconds % 604800
    assert set(result.as_dict()) == {
        "nova", "unix_sec", "unix_nsec", "utc", "utc_string",
        "gps", "gps_seconds", "gps_nsec", "gps_psec",
        "gps_week", "gps_tow", "gps_tow_exact", "gps_string",
    }


def test_negative_nova_time_is_rejected():
    """!
    @brief A negative tick count raises NovaTimeError.
    """
    with pytest.raises(NovaTimeError):
        convert(-1)


def test_datetime_round_trip():
    """!
    @brief datetime -> NOvA ticks -> datetime is lossless for an aware UTC
    instant.
    """
    moment = datetime(2026, 7, 1, 9, 15, 0, tzinfo=UTC)
    assert nova_to_datetime(datetime_to_nova(moment)) == moment


def test_naive_datetime_is_refused_not_assumed_utc():
    """!
    @brief Silently assuming UTC would shift a Chicago operator's query by
    hours.
    """
    with pytest.raises(NovaTimeError):
        datetime_to_nova(datetime(2026, 7, 1, 9, 15, 0))


# -------------------------------------------------------------- keywords


def test_now_and_today():
    """!
    @brief 'now' is the reference instant; 'today' is its UTC midnight.
    """
    assert parse_instant("now", now=NOW) == datetime_to_nova(NOW)

    midnight = datetime(2026, 9, 22, 0, 0, tzinfo=UTC)
    assert parse_instant("today", now=NOW) == datetime_to_nova(midnight)


def test_yesterday():
    """!
    @brief 'yesterday' is the previous UTC midnight.
    """
    expected = datetime(2026, 9, 21, 0, 0, tzinfo=UTC)
    assert parse_instant("yesterday", now=NOW) == datetime_to_nova(expected)


def test_epoch_keyword_is_zero():
    """!
    @brief 'epoch' is NOvA tick 0.
    """
    assert parse_instant("epoch", now=NOW) == 0


# ------------------------------------------------------------- relatives


@pytest.mark.parametrize("text,delta", [
    ("-2h", timedelta(hours=-2)),
    ("-30m", timedelta(minutes=-30)),
    ("-7d", timedelta(days=-7)),
    ("-1w", timedelta(weeks=-1)),
    ("+15s", timedelta(seconds=15)),
    ("now-2h", timedelta(hours=-2)),
    ("-90min", timedelta(minutes=-90)),
])
def test_relative_offsets(text, delta):
    """!
    @brief Signed s/m/min/h/d/w offsets, optionally prefixed with 'now', are
    applied to the reference instant.
    @param text relative-time expression.
    @param delta timedelta it must denote.
    """
    assert parse_instant(text, now=NOW) == datetime_to_nova(NOW + delta)


# ------------------------------------------------------------ clock times


def test_bare_clock_time_means_today():
    """!
    @brief Design.md: 'from 09:15 today until 11:34 today'.
    """
    expected = datetime(2026, 9, 22, 9, 15, tzinfo=UTC)
    assert parse_instant("09:15", now=NOW) == datetime_to_nova(expected)


def test_bare_clock_time_with_seconds():
    """!
    @brief HH:MM:SS is accepted and means today.
    """
    expected = datetime(2026, 9, 22, 11, 34, 22, tzinfo=UTC)
    assert parse_instant("11:34:22", now=NOW) == datetime_to_nova(expected)


def test_invalid_clock_time_raises():
    """!
    @brief An out-of-range clock time raises NovaTimeError rather than
    wrapping.
    """
    with pytest.raises(NovaTimeError):
        parse_instant("25:99", now=NOW)


# ----------------------------------------------------------- calendar


@pytest.mark.parametrize("text", [
    "2026-07-01", "2026/07/01", "01-Jul-2026", "1 Jul 2026",
    "Jul 1 2026", "Jul 1, 2026", "July 1, 2026", "07/01/2026",
])
def test_calendar_spellings(text):
    """!
    @brief Design.md writes dates as 'Jul 1, 2026'; logbooks use other forms.
    @param text one spelling of 2026-07-01.
    """
    expected = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
    assert parse_instant(text, now=NOW) == datetime_to_nova(expected)


def test_iso_with_explicit_zone_wins_over_the_default():
    """!
    @brief An explicit 'Z' in an ISO string overrides the tz= default.
    """
    chicago = get_timezone("America/Chicago")
    explicit = parse_instant("2026-07-01T09:15:00Z", now=NOW, tz=chicago)
    assert explicit == datetime_to_nova(
        datetime(2026, 7, 1, 9, 15, tzinfo=UTC)
    )


def test_unqualified_time_uses_the_supplied_zone():
    """!
    @brief A time without a zone is interpreted in tz=, with DST applied.
    """
    chicago = get_timezone("America/Chicago")
    result = parse_instant("2026-07-01 09:15", now=NOW, tz=chicago)
    # Chicago is UTC-5 in July (CDT), so this is 14:15 UTC.
    assert result == datetime_to_nova(datetime(2026, 7, 1, 14, 15, tzinfo=UTC))


def test_unknown_timezone_raises():
    """!
    @brief get_timezone rejects an unknown IANA name with NovaTimeError.
    """
    with pytest.raises(NovaTimeError):
        get_timezone("Mars/Olympus_Mons")


# ------------------------------------------------------- tagged timescales


def test_explicit_nova_ticks():
    """!
    @brief `nova:<ticks>` is taken verbatim.
    """
    assert parse_instant("nova:{}".format(LIVE_NOVA), now=NOW) == LIVE_NOVA


def test_explicit_unix():
    """!
    @brief `unix:<sec>` maps to the instant with that UNIX second.
    """
    result = parse_instant("unix:{}".format(LIVE_UNIX_SEC), now=NOW)
    assert convert(result).unix_sec == LIVE_UNIX_SEC


def test_explicit_gps_seconds_round_trips():
    """!
    @brief `gps:<sec>` maps back to the whole-second tick count of the original
    instant.
    """
    gps = convert(LIVE_NOVA).gps_seconds
    assert parse_instant("gps:{}".format(gps), now=NOW) == (
        # gps_seconds is integral, so the sub-second part is lost on the round
        # trip.
        LIVE_NOVA // TICKS_PER_SECOND * TICKS_PER_SECOND
    )


def test_explicit_gps_week_and_tow():
    """!
    @brief `gps:<week>:<tow>` maps to an instant with that week and time of
    week.
    """
    reference = convert(LIVE_NOVA)
    text = "gps:{}:{}".format(reference.gps_week, reference.gps_tow)
    parsed = convert(parse_instant(text, now=NOW))
    assert parsed.gps_week == reference.gps_week
    assert parsed.gps_tow == reference.gps_tow


def test_bare_large_number_is_nova_ticks():
    """!
    @brief An untagged integer of NOvA magnitude is read as ticks.
    """
    assert parse_instant(str(LIVE_NOVA), now=NOW) == LIVE_NOVA


def test_bare_small_number_is_unix():
    """!
    @brief An untagged integer of UNIX-second magnitude is read as UNIX time.
    """
    assert convert(parse_instant(str(LIVE_UNIX_SEC), now=NOW)).unix_sec == LIVE_UNIX_SEC


def test_hex_number_is_nova_ticks():
    """!
    @brief A 0x-prefixed integer is read as ticks.
    """
    assert parse_instant(hex(LIVE_NOVA), now=NOW) == LIVE_NOVA


def test_unparseable_time_explains_the_options():
    """!
    @brief Unparseable text raises NovaTimeError listing the accepted forms
    (ISO 8601, nova:, ...).
    """
    with pytest.raises(NovaTimeError) as excinfo:
        parse_instant("last Tuesday afternoon", now=NOW)
    message = str(excinfo.value)
    assert "ISO 8601" in message and "nova:" in message


def test_pre_epoch_time_is_rejected():
    """!
    @brief An instant before the NOvA epoch (2010-01-01) raises NovaTimeError.
    """
    with pytest.raises(NovaTimeError):
        parse_instant("1999-01-01", now=NOW)


# ------------------------------------------------------------------ ranges


def test_range_defaults_span_everything_up_to_now():
    """!
    @brief With neither bound given the range is [epoch, now).
    """
    start, end = parse_range(None, None, now=NOW)
    assert start == 0
    assert end == datetime_to_nova(NOW)


def test_date_only_end_covers_the_whole_day():
    """!
    @brief Otherwise start=end=2026-07-01 would return nothing, surprising a
    user.
    """
    start, end = parse_range("2026-07-01", "2026-07-01", now=NOW)
    assert start == datetime_to_nova(datetime(2026, 7, 1, tzinfo=UTC))
    assert end == datetime_to_nova(datetime(2026, 7, 2, tzinfo=UTC))
    assert end > start


def test_design_example_july_to_today():
    """!
    @brief Design.md: 'all the time stamps for the $74 events between Jul 1,
    2026 and today'.
    """
    start, end = parse_range("Jul 1, 2026", "today", now=NOW)
    assert start == datetime_to_nova(datetime(2026, 7, 1, tzinfo=UTC))
    # "today" as an end bound covers all of today.
    assert end == datetime_to_nova(datetime(2026, 9, 23, tzinfo=UTC))


def test_design_example_clock_times():
    """!
    @brief Design.md: 'from 09:15 today until 11:34 today'.
    """
    start, end = parse_range("09:15", "11:34", now=NOW)
    assert start == datetime_to_nova(datetime(2026, 9, 22, 9, 15, tzinfo=UTC))
    assert end == datetime_to_nova(datetime(2026, 9, 22, 11, 34, tzinfo=UTC))


def test_backwards_range_is_rejected():
    """!
    @brief end < start raises with a 'swapped' hint, and no whole-day hint for
    keyword bounds.
    """
    with pytest.raises(NovaTimeError) as excinfo:
        parse_range("today", "yesterday", now=NOW)
    assert "before its start" in str(excinfo.value)
    assert "swapped" in str(excinfo.value)
    assert "whole day" not in str(excinfo.value)


def test_backwards_date_range_spells_out_the_fix():
    """!
    @brief For date-only bounds the error spells out the inclusive whole-day
    range to use instead.
    """
    with pytest.raises(NovaTimeError) as excinfo:
        parse_range("2026-10-11", "2026-10-10", now=NOW)
    assert "start 2026-10-10 with end 2026-10-11 covers both days" \
        in str(excinfo.value)


def test_backwards_clock_times_get_no_date_hint():
    """!
    @brief Reversed clock times get the 'swapped' hint but not the date-only
    whole-day hint.
    """
    with pytest.raises(NovaTimeError) as excinfo:
        parse_range("11:34", "09:15", now=NOW)
    assert "swapped" in str(excinfo.value)
    assert "whole day" not in str(excinfo.value)


def test_equal_instants_make_an_empty_range():
    """!
    @brief Equal non-date bounds give an empty [t, t) range rather than an
    error.
    """
    start, end = parse_range("09:15", "09:15", now=NOW)
    assert start == end
