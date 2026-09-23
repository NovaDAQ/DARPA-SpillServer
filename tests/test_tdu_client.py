"""Tests for the TDU HTTP client.

The sample payloads here are real: ``LIVE_TCR_STATUS`` is the exact body
returned by ``http://tdu-near-master-ppc-01:8080/tcr_status`` on 2026-09-22,
and ``LEGACY_BULK_JSON`` reproduces the malformed shape that
``DumpSpillHistory --json`` emits for a multi-event dump.
"""

import httpx
import pytest
import respx

from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import UNKNOWN_SIGNAL
from darpa_spillserver.tdu_client import (
    HISTORY_ROUTE,
    TDUClient,
    TDUResponseError,
    TDUUnavailable,
    event_from_record,
    loads_tolerant,
)

BASE_URL = "http://tdu-test:8080"

LIVE_TCR_STATUS = """{
\t"Type": 3,
\t"Number": 10733,
\t"Time": 33778458638680249,
\t"Delta": 4266769,
\t"Offset": 14680249,
\t"Timestring": "2026-Sep-22 15:53:33.229378890625 UTC",
\t"UnixtimeSec": 1790092413,
\t"UnixtimeUSec": 229378
}
"""

# DumpSpillHistory writes printf("{\nEvents: [\n") -- an unquoted key.
LEGACY_BULK_JSON = """{
Events: [
{
\t"Type": 4,
\t"Number": 1,
\t"Time": 33778458638680249,
\t"Delta": 0,
\t"Offset": 0
}
,{
\t"Type": 4,
\t"Number": 2,
\t"Time": 33778458702680249,
\t"Delta": 64000000,
\t"Offset": 0
}
]
}
"""


# ------------------------------------------------------------ JSON repair


def test_valid_json_is_untouched():
    assert loads_tolerant('{"a": 1}') == {"a": 1}


def test_live_single_event_parses_strictly():
    payload = loads_tolerant(LIVE_TCR_STATUS)
    assert payload["Type"] == 3
    assert payload["Time"] == 33778458638680249


def test_unquoted_key_is_repaired():
    """DumpSpillHistory's bulk output is not valid JSON; parse it anyway."""
    payload = loads_tolerant(LEGACY_BULK_JSON)
    assert len(payload["Events"]) == 2
    assert payload["Events"][0]["Type"] == 4


def test_trailing_comma_is_repaired():
    assert loads_tolerant('{"a": [1, 2,]}') == {"a": [1, 2]}


def test_unrepairable_body_raises_with_an_excerpt():
    with pytest.raises(TDUResponseError) as excinfo:
        loads_tolerant("Failed to attach to shared memory segment")
    assert "Failed to attach" in str(excinfo.value)


# --------------------------------------------------------- record mapping


def test_event_from_legacy_record_has_no_signal():
    """The legacy routes report a decoded type only, never the raw word."""
    record = loads_tolerant(LIVE_TCR_STATUS)
    event = event_from_record(record, route="tcr_status")

    assert event.nova_time == 33778458638680249
    assert event.spill_type is SpillType.BNB_TCLK
    assert event.signal_code == UNKNOWN_SIGNAL, "must not guess $1D vs $1F"
    assert event.event_number == 10733
    assert event.delta == 4266769


def test_raw_event_word_is_authoritative_over_the_reported_type():
    """When both are present the word wins: it recovers the exact signal."""
    event = event_from_record(
        {"Time": 1, "Type": 99, "Event": 0x018F}, route="spill_history"
    )
    assert event.spill_type is SpillType.ACCEL_ONE_HZ_TCLK
    assert event.signal_code == 0x8F


def test_lowercase_keys_are_accepted():
    event = event_from_record(
        {"time": 42, "type": 4, "number": 7}, route="spill_history"
    )
    assert event.nova_time == 42
    assert event.spill_type is SpillType.ACCEL_ONE_HZ_TCLK


def test_record_without_a_timestamp_is_dropped_not_fatal():
    assert event_from_record({"Type": 4}, route="x") is None


def test_record_with_unknown_type_is_dropped():
    assert event_from_record({"Time": 1, "Type": 99}, route="x") is None


def test_unknown_signal_code_falls_back_to_sentinel():
    event = event_from_record({"Time": 1, "Event": 0x0199}, route="x")
    assert event.signal_code == UNKNOWN_SIGNAL
    assert event.spill_type is SpillType.FAKE


# ------------------------------------------------------------- HTTP paths


@respx.mock
async def test_fetch_latest():
    respx.get(BASE_URL + "/tcr_status").mock(
        return_value=httpx.Response(200, text=LIVE_TCR_STATUS)
    )
    async with TDUClient(BASE_URL) as client:
        event = await client.fetch_latest()
    assert event.nova_time == 33778458638680249


@respx.mock
async def test_history_route_is_used_when_present():
    respx.get(BASE_URL + HISTORY_ROUTE).mock(
        return_value=httpx.Response(200, json={
            "events": [
                {"Time": 100, "Event": 0x018F, "Number": 1},
                {"Time": 200, "Event": 0x018F, "Number": 2},
            ],
            "count": 2,
            "truncated": False,
        })
    )
    async with TDUClient(BASE_URL) as client:
        result = await client.fetch_history(since_nova=0)

    assert result.route == HISTORY_ROUTE
    assert len(result.events) == 2
    assert result.events[0].signal_code == 0x8F
    assert result.truncated is False


@respx.mock
async def test_falls_back_to_the_legacy_route_on_404():
    """An un-upgraded TDU must still work, degraded rather than broken."""
    respx.get(BASE_URL + HISTORY_ROUTE).mock(return_value=httpx.Response(404))
    respx.get(BASE_URL + "/tcr_status").mock(
        return_value=httpx.Response(200, text=LIVE_TCR_STATUS)
    )
    async with TDUClient(BASE_URL) as client:
        result = await client.fetch_history()

    assert result.route == "/tcr_status"
    assert len(result.events) == 1
    assert result.truncated is True, "one of many events is by definition partial"


@respx.mock
async def test_history_support_is_probed_only_once():
    route = respx.get(BASE_URL + HISTORY_ROUTE).mock(
        return_value=httpx.Response(404)
    )
    respx.get(BASE_URL + "/tcr_status").mock(
        return_value=httpx.Response(200, text=LIVE_TCR_STATUS)
    )
    async with TDUClient(BASE_URL) as client:
        await client.fetch_history()
        await client.fetch_history()
        await client.fetch_history()

    assert route.call_count == 1, "the probe result should be cached"


@respx.mock
async def test_legacy_bulk_json_is_parsed():
    respx.get(BASE_URL + HISTORY_ROUTE).mock(
        return_value=httpx.Response(200, text=LEGACY_BULK_JSON)
    )
    async with TDUClient(BASE_URL) as client:
        result = await client.fetch_history()
    assert len(result.events) == 2


@respx.mock
async def test_transient_failures_are_retried():
    route = respx.get(BASE_URL + "/tcr_status")
    route.side_effect = [
        httpx.ConnectError("refused"),
        httpx.Response(200, text=LIVE_TCR_STATUS),
    ]
    async with TDUClient(BASE_URL, retries=2, retry_backoff=0.01) as client:
        event = await client.fetch_latest()
    assert event is not None
    assert route.call_count == 2


@respx.mock
async def test_persistent_failure_raises_after_the_retry_budget():
    respx.get(BASE_URL + "/tcr_status").mock(
        side_effect=httpx.ConnectError("refused")
    )
    async with TDUClient(BASE_URL, retries=1, retry_backoff=0.01) as client:
        with pytest.raises(TDUUnavailable):
            await client.fetch_latest()


@respx.mock
async def test_ping_reports_false_when_unreachable():
    respx.get(BASE_URL + "/tcr_running").mock(
        side_effect=httpx.ConnectError("refused")
    )
    async with TDUClient(BASE_URL, retries=0) as client:
        assert await client.ping() is False


@respx.mock
async def test_bare_list_response_is_accepted():
    respx.get(BASE_URL + HISTORY_ROUTE).mock(
        return_value=httpx.Response(200, json=[{"Time": 1, "Type": 4}])
    )
    async with TDUClient(BASE_URL) as client:
        result = await client.fetch_history()
    assert len(result.events) == 1


def test_client_rejects_use_before_open():
    client = TDUClient(BASE_URL)
    with pytest.raises(RuntimeError):
        _ = client.client
