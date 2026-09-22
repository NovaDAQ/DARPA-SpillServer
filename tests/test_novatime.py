"""Tests for time parsing and timescale conversion.

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
    """Our conversion must agree with what the TDU itself reported."""
    result = convert(LIVE_NOVA)
    assert result.unix_sec == LIVE_UNIX_SEC
    assert result.unix_nsec // 1000 == LIVE_UNIX_USEC
    assert result.utc_string == LIVE_TIMESTRING
    assert result.utc.startswith("2026-09-22T15:53:33.")


def test_conversion_reports_every_timescale():
    result = convert(LIVE_NOVA)
    assert isinstance(result, ConvertedTime)
    assert result.nova == LIVE_NOVA
    assert result.gps_seconds > 0
    assert result.gps_week == result.gps_seconds // 604800
    assert result.gps_tow == result.gps_seconds % 604800
    assert set(result.as_dict()) == {
        "nova", "unix_sec", "unix_nsec", "utc", "utc_string",
        "gps_seconds", "gps_nsec", "gps_week", "gps_tow",
    }


def test_negative_nova_time_is_rejected():
    with pytest.raises(NovaTimeError):
        convert(-1)


def test_datetime_round_trip():
    moment = datetime(2026, 7, 1, 9, 15, 0, tzinfo=UTC)
    assert nova_to_datetime(datetime_to_nova(moment)) == moment


def test_naive_datetime_is_refused_not_assumed_utc():
    """Silently assuming UTC would shift a Chicago operator's query by hours."""
    with pytest.raises(NovaTimeError):
        datetime_to_nova(datetime(2026, 7, 1, 9, 15, 0))


# -------------------------------------------------------------- keywords


def test_now_and_today():
    assert parse_instant("now", now=NOW) == datetime_to_nova(NOW)

    midnight = datetime(2026, 9, 22, 0, 0, tzinfo=UTC)
    assert parse_instant("today", now=NOW) == datetime_to_nova(midnight)


def test_yesterday():
    expected = datetime(2026, 9, 21, 0, 0, tzinfo=UTC)
    assert parse_instant("yesterday", now=NOW) == datetime_to_nova(expected)


def test_epoch_keyword_is_zero():
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
    assert parse_instant(text, now=NOW) == datetime_to_nova(NOW + delta)


# ------------------------------------------------------------ clock times


def test_bare_clock_time_means_today():
    """Design.md: 'from 09:15 today until 11:34 today'."""
    expected = datetime(2026, 9, 22, 9, 15, tzinfo=UTC)
    assert parse_instant("09:15", now=NOW) == datetime_to_nova(expected)


def test_bare_clock_time_with_seconds():
    expected = datetime(2026, 9, 22, 11, 34, 22, tzinfo=UTC)
    assert parse_instant("11:34:22", now=NOW) == datetime_to_nova(expected)


def test_invalid_clock_time_raises():
    with pytest.raises(NovaTimeError):
        parse_instant("25:99", now=NOW)


# ----------------------------------------------------------- calendar


@pytest.mark.parametrize("text", [
    "2026-07-01", "2026/07/01", "01-Jul-2026", "1 Jul 2026",
    "Jul 1 2026", "Jul 1, 2026", "July 1, 2026", "07/01/2026",
])
def test_calendar_spellings(text):
    """Design.md writes dates as 'Jul 1, 2026'; logbooks use other forms."""
    expected = datetime(2026, 7, 1, 0, 0, tzinfo=UTC)
    assert parse_instant(text, now=NOW) == datetime_to_nova(expected)


def test_iso_with_explicit_zone_wins_over_the_default():
    chicago = get_timezone("America/Chicago")
    explicit = parse_instant("2026-07-01T09:15:00Z", now=NOW, tz=chicago)
    assert explicit == datetime_to_nova(
        datetime(2026, 7, 1, 9, 15, tzinfo=UTC)
    )


def test_unqualified_time_uses_the_supplied_zone():
    chicago = get_timezone("America/Chicago")
    result = parse_instant("2026-07-01 09:15", now=NOW, tz=chicago)
    # Chicago is UTC-5 in July (CDT), so this is 14:15 UTC.
    assert result == datetime_to_nova(datetime(2026, 7, 1, 14, 15, tzinfo=UTC))


def test_unknown_timezone_raises():
    with pytest.raises(NovaTimeError):
        get_timezone("Mars/Olympus_Mons")


# ------------------------------------------------------- tagged timescales


def test_explicit_nova_ticks():
    assert parse_instant("nova:{}".format(LIVE_NOVA), now=NOW) == LIVE_NOVA


def test_explicit_unix():
    result = parse_instant("unix:{}".format(LIVE_UNIX_SEC), now=NOW)
    assert convert(result).unix_sec == LIVE_UNIX_SEC


def test_explicit_gps_seconds_round_trips():
    gps = convert(LIVE_NOVA).gps_seconds
    assert parse_instant("gps:{}".format(gps), now=NOW) == (
        LIVE_NOVA // TICKS_PER_SECOND * TICKS_PER_SECOND
    )


def test_explicit_gps_week_and_tow():
    reference = convert(LIVE_NOVA)
    text = "gps:{}:{}".format(reference.gps_week, reference.gps_tow)
    parsed = convert(parse_instant(text, now=NOW))
    assert parsed.gps_week == reference.gps_week
    assert parsed.gps_tow == reference.gps_tow


def test_bare_large_number_is_nova_ticks():
    assert parse_instant(str(LIVE_NOVA), now=NOW) == LIVE_NOVA


def test_bare_small_number_is_unix():
    assert convert(parse_instant(str(LIVE_UNIX_SEC), now=NOW)).unix_sec == LIVE_UNIX_SEC


def test_hex_number_is_nova_ticks():
    assert parse_instant(hex(LIVE_NOVA), now=NOW) == LIVE_NOVA


def test_unparseable_time_explains_the_options():
    with pytest.raises(NovaTimeError) as excinfo:
        parse_instant("last Tuesday afternoon", now=NOW)
    message = str(excinfo.value)
    assert "ISO 8601" in message and "nova:" in message


def test_pre_epoch_time_is_rejected():
    with pytest.raises(NovaTimeError):
        parse_instant("1999-01-01", now=NOW)


# ------------------------------------------------------------------ ranges


def test_range_defaults_span_everything_up_to_now():
    start, end = parse_range(None, None, now=NOW)
    assert start == 0
    assert end == datetime_to_nova(NOW)


def test_date_only_end_covers_the_whole_day():
    """Otherwise start=end=2026-07-01 would return nothing, surprising a user."""
    start, end = parse_range("2026-07-01", "2026-07-01", now=NOW)
    assert start == datetime_to_nova(datetime(2026, 7, 1, tzinfo=UTC))
    assert end == datetime_to_nova(datetime(2026, 7, 2, tzinfo=UTC))
    assert end > start


def test_design_example_july_to_today():
    """Design.md: 'all the time stamps for the $74 events between Jul 1, 2026
    and today'."""
    start, end = parse_range("Jul 1, 2026", "today", now=NOW)
    assert start == datetime_to_nova(datetime(2026, 7, 1, tzinfo=UTC))
    # "today" as an end bound covers all of today.
    assert end == datetime_to_nova(datetime(2026, 9, 23, tzinfo=UTC))


def test_design_example_clock_times():
    """Design.md: 'from 09:15 today until 11:34 today'."""
    start, end = parse_range("09:15", "11:34", now=NOW)
    assert start == datetime_to_nova(datetime(2026, 9, 22, 9, 15, tzinfo=UTC))
    assert end == datetime_to_nova(datetime(2026, 9, 22, 11, 34, tzinfo=UTC))


def test_backwards_range_is_rejected():
    with pytest.raises(NovaTimeError) as excinfo:
        parse_range("today", "yesterday", now=NOW)
    assert "before its start" in str(excinfo.value)


def test_equal_instants_make_an_empty_range():
    start, end = parse_range("09:15", "09:15", now=NOW)
    assert start == end
