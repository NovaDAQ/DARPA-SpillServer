"""!
@file tests/test_backfill.py
@brief Tests for the ring-buffer backfill tool.
"""

import httpx
import pytest
import respx

from darpa_spillserver.backfill import DEFAULT_WINDOW, _probe_earliest, backfill_source
from darpa_spillserver.novatime import TICKS_PER_SECOND
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import SpillStore
from darpa_spillserver.tdu_client import HISTORY_ROUTE, TDUClient

BASE_URL = "http://tdu-test:8080"
# 2026-09-22 15:53:33 UTC in NOvA ticks (64 MHz); the live tcr_status reading
# used throughout the suite.
START = 33778458638680249


def event(offset_seconds, number):
    """!
    @brief Build one TDU history record for a 1 Hz TCLK event.
    @param offset_seconds seconds after START (may be fractional).
    @param number TDU event number.
    @return dict in the TDU's JSON record shape (Time, Type, Number, Delta,
    Offset).
    """
    return {
        "Time": START + int(offset_seconds * TICKS_PER_SECOND),
        "Type": int(SpillType.ACCEL_ONE_HZ_TCLK),
        "Number": number,
        "Delta": 0,
        "Offset": 0,
    }


@pytest.fixture
def store(tmp_path):
    """!
    @brief Empty SpillStore in a per-test temporary file; closed on teardown.
    @param tmp_path pytest per-test temporary directory.
    @return the open SpillStore.
    """
    s = SpillStore(str(tmp_path / "bf.db"))
    yield s
    s.close()


def mock_info():
    """!
    @brief Mock /spill_history_info so the client sees a TDU that supports the
    history route (avoids the fallback probe).
    """
    respx.get(BASE_URL + "/spill_history_info").mock(
        return_value=httpx.Response(200, json={"spill_history": True})
    )


@respx.mock
async def test_range_is_walked_in_windows(store):
    """!
    @brief The whole point: bound both ends of each request, or the TDU tries
    to return the rest of the ring for every call.
    """
    mock_info()
    seen = []

    def handler(request):
        """!
        @brief Record each request's [since, until) and answer with an empty
        window.
        """
        since = int(request.url.params["since"])
        until = int(request.url.params["until"])
        seen.append((since, until))
        assert "until" in request.url.params, "both ends must be bounded"
        return httpx.Response(200, json={"events": [], "count": 0})

    respx.get(BASE_URL + HISTORY_ROUTE).mock(side_effect=handler)

    async with TDUClient(BASE_URL) as client:
        await backfill_source(client, store, START,
                              START + int(600 * TICKS_PER_SECOND),
                              window_seconds=120.0)

    assert len(seen) == 5, "600 s in 120 s windows"
    # Windows must tile: each starts where the previous ended.
    for (a_lo, a_hi), (b_lo, _) in zip(seen, seen[1:]):
        assert a_hi == b_lo


@respx.mock
async def test_events_are_inserted(store):
    """!
    @brief A single window's records are fetched and inserted; backfill_source
    returns (fetched, inserted, windows) = (2, 2, 1).
    """
    mock_info()
    respx.get(BASE_URL + HISTORY_ROUTE).mock(
        return_value=httpx.Response(200, json={
            "events": [event(0, 1), event(1, 2)], "count": 2})
    )
    async with TDUClient(BASE_URL) as client:
        fetched, inserted, windows = await backfill_source(
            client, store, START, START + int(120 * TICKS_PER_SECOND),
            window_seconds=120.0)

    assert windows == 1
    assert fetched == 2
    assert inserted == 2
    assert store.count() == 2


@respx.mock
async def test_rerunning_inserts_nothing_new(store):
    """!
    @brief Safe to run over a range already covered.
    """
    mock_info()
    respx.get(BASE_URL + HISTORY_ROUTE).mock(
        return_value=httpx.Response(200, json={
            "events": [event(0, 1), event(1, 2)], "count": 2})
    )
    end = START + int(120 * TICKS_PER_SECOND)
    async with TDUClient(BASE_URL) as client:
        await backfill_source(client, store, START, end, window_seconds=120.0)
        _, inserted, _ = await backfill_source(
            client, store, START, end, window_seconds=120.0)

    assert inserted == 0
    assert store.count() == 2


@respx.mock
async def test_a_truncated_window_is_narrowed_not_skipped(store):
    """!
    @brief Advancing past a truncated window would drop events silently.
    """
    mock_info()
    calls = []

    def handler(request):
        """!
        @brief Report truncation for any window wider than 150 s, otherwise
        return nothing.
        """
        since = int(request.url.params["since"])
        until = int(request.url.params["until"])
        calls.append(until - since)
        # Report truncation until the window is small enough.
        # 150 s threshold: the 600 s request must be halved twice (600 -> 300
        # -> 150) before the TDU stops truncating.
        if until - since > int(150 * TICKS_PER_SECOND):
            return httpx.Response(200, json={
                "events": [event(0, 1)], "count": 1, "truncated": True})
        return httpx.Response(200, json={"events": [], "count": 0})

    respx.get(BASE_URL + HISTORY_ROUTE).mock(side_effect=handler)

    async with TDUClient(BASE_URL) as client:
        await backfill_source(client, store, START,
                              START + int(600 * TICKS_PER_SECOND),
                              window_seconds=600.0)

    assert calls[0] > calls[1] > calls[2], "the window should shrink"


@respx.mock
async def test_probe_finds_the_earliest_bounded_window():
    """!
    @brief Discovery scans the lookback oldest first in contiguous windows and
    returns the first event it finds, not an estimate from sparse probes.
    """
    mock_info()
    end = START + 1200 * TICKS_PER_SECOND
    edge = START + 601 * TICKS_PER_SECOND
    def handler(request):
        """!
        @brief Return one event at `since` when since lies inside the ring (>=
        edge), nothing otherwise.
        """
        since = int(request.url.params["since"])
        until = int(request.url.params["until"])
        rows = [{"Time": edge, "Type": 4, "Number": 1}] if since <= edge < until else []
        return httpx.Response(200, json={"events": rows, "count": len(rows)})
    respx.get(BASE_URL + HISTORY_ROUTE).mock(side_effect=handler)
    async with TDUClient(BASE_URL) as client:
        assert await _probe_earliest(client, end, 1200) == edge


@respx.mock
async def test_probe_returns_none_when_the_ring_is_empty():
    """!
    @brief _probe_earliest returns None when no probe window finds any event.
    """
    mock_info()
    route = respx.get(BASE_URL + HISTORY_ROUTE).mock(
        return_value=httpx.Response(200, json={"events": [], "count": 0})
    )
    async with TDUClient(BASE_URL) as client:
        assert await _probe_earliest(client, START, 600) is None
    assert route.call_count == 2


def test_default_window_is_modest():
    """!
    @brief A wide window makes one request produce more than the TDU's output
    cap allows.
    """
    assert 60.0 <= DEFAULT_WINDOW <= 900.0


@pytest.mark.parametrize("window", [0, -1, float('nan'), float('inf'), 1e-12])
async def test_invalid_window_is_rejected_before_io(window):
    with pytest.raises(ValueError, match="window"):
        await backfill_source(None, None, START, START + 100, window_seconds=window)


@pytest.mark.parametrize("limit", [0, -1])
async def test_invalid_limit_is_rejected_before_io(limit):
    with pytest.raises(ValueError, match="limit"):
        await backfill_source(None, None, START, START + 100, batch_limit=limit)


def test_cli_rejects_invalid_window_before_opening_archive(tmp_path):
    from darpa_spillserver.backfill import main

    database = tmp_path / "must-not-exist.db"
    assert main(["--window", "0", "-d", str(database)]) == 2
    assert not database.exists()
