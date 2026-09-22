"""Tests for the background ingest loop.

The poller is exercised against a stub client rather than a live TDU, so the
tests can drive the exact conditions that matter: an empty archive, an
overlapping re-read, a TDU that fails and recovers, and a TDU with no history
route at all.
"""

import asyncio

import pytest

from darpa_spillserver.config import Config
from darpa_spillserver.novatime import TICKS_PER_SECOND, nova_now
from darpa_spillserver.poller import Poller
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import SpillEvent, SpillStore
from darpa_spillserver.tdu_client import HISTORY_ROUTE, FetchResult, TDUUnavailable

SECOND = TICKS_PER_SECOND

#: Events are anchored to the current time rather than to a fixed timestamp.
#: The poller backfills `ingest.backfill` seconds on a cold start, so events
#: stamped with a literal date fall outside that window as soon as enough wall
#: time has passed -- a test written at 15:53 with a one-hour backfill starts
#: failing at 16:53 and never passes again. Ten minutes back sits comfortably
#: inside the default hour.
ANCHOR_OFFSET = 600 * SECOND


class StubClient:
    """A TDUClient stand-in that records what was asked of it."""

    def __init__(self, events=None, route=HISTORY_ROUTE, error=None,
                 truncated=False):
        self.events = events or []
        self.route = route
        self.error = error
        self.truncated = truncated
        self.calls = []
        self.opened = False
        self.closed = False

    async def open(self):
        self.opened = True

    async def close(self):
        self.closed = True

    async def supports_history(self):
        return self.route == HISTORY_ROUTE

    async def fetch_history(self, since_nova=None, limit=None, **kwargs):
        self.calls.append({"since": since_nova, "limit": limit})
        if self.error is not None:
            raise self.error
        selected = [e for e in self.events
                    if since_nova is None or e.nova_time >= since_nova]
        return FetchResult(events=selected, route=self.route,
                           truncated=self.truncated, raw_count=len(selected))


def make_events(count, start=None, step=SECOND):
    """Build *count* consecutive events, by default ending ten minutes ago."""
    if start is None:
        start = nova_now() - ANCHOR_OFFSET
    return [
        SpillEvent(
            nova_time=start + index * step,
            spill_type=SpillType.ACCEL_ONE_HZ_TCLK,
            signal_code=0x8F,
            event_number=index,
            source="spill_history",
        )
        for index in range(count)
    ]


@pytest.fixture
def config(tmp_path):
    cfg = Config()
    cfg.storage.path = str(tmp_path / "spills.db")
    cfg.ingest.interval = 0.01
    cfg.ingest.overlap = 5.0
    cfg.ingest.backfill = 3600.0
    return cfg


@pytest.fixture
def store(config):
    archive = SpillStore(config.storage.path)
    yield archive
    archive.close()


async def test_single_poll_inserts_events(config, store):
    client = StubClient(events=make_events(10))
    poller = Poller(config, store, client=client)

    inserted = await poller.poll_once()
    assert inserted == 10
    assert store.count() == 10
    assert poller.status.events_inserted == 10
    assert poller.status.consecutive_errors == 0


async def test_cold_start_backfills_rather_than_asking_for_everything(config, store):
    client = StubClient(events=[])
    poller = Poller(config, store, client=client)

    await poller.poll_once()

    since = client.calls[0]["since"]
    expected = nova_now() - int(config.ingest.backfill * TICKS_PER_SECOND)
    # Within a second of the intended backfill window.
    assert abs(since - expected) < TICKS_PER_SECOND


async def test_second_poll_resumes_from_the_watermark_less_overlap(config, store):
    events = make_events(10)
    client = StubClient(events=events)
    poller = Poller(config, store, client=client)

    await poller.poll_once()
    await poller.poll_once()

    newest = events[-1].nova_time
    expected = newest - int(config.ingest.overlap * TICKS_PER_SECOND)
    assert client.calls[1]["since"] == expected


async def test_overlapping_reread_does_not_duplicate(config, store):
    client = StubClient(events=make_events(10))
    poller = Poller(config, store, client=client)

    assert await poller.poll_once() == 10
    assert await poller.poll_once() == 0, "the overlap must be idempotent"
    assert store.count() == 10


async def test_progress_is_counted_in_rows_inserted_not_fetched(config, store):
    """Fetched counts every row seen, including re-reads; inserted counts
    only real progress. The 5 s overlap re-reads the last 6 of 10 one-second
    events, so the two counters must diverge by exactly that much."""
    client = StubClient(events=make_events(10))
    poller = Poller(config, store, client=client)

    await poller.poll_once()
    await poller.poll_once()

    assert poller.status.events_inserted == 10
    assert poller.status.events_fetched == 16
    assert store.count() == 10


async def test_failures_are_recorded_without_stopping(config, store):
    client = StubClient(error=TDUUnavailable("TDU is down"))
    poller = Poller(config, store, client=client)

    with pytest.raises(TDUUnavailable):
        await poller.poll_once()

    poller._record_error(TDUUnavailable("TDU is down"))
    assert poller.status.consecutive_errors == 1
    assert "TDU is down" in poller.status.last_error


async def test_error_counter_resets_after_a_success(config, store):
    client = StubClient(events=make_events(3))
    poller = Poller(config, store, client=client)

    poller._record_error(TDUUnavailable("blip"))
    assert poller.status.consecutive_errors == 1

    await poller.poll_once()
    assert poller.status.consecutive_errors == 0


async def test_degraded_mode_is_flagged(config, store):
    """A TDU with no history route yields a sample, and must say so."""
    client = StubClient(events=make_events(1), route="/tcr_status")
    poller = Poller(config, store, client=client)

    await poller.poll_once()

    assert poller.status.degraded is True
    status = poller.status.as_dict()
    assert "degraded_reason" in status
    assert "6-15 Hz" in status["degraded_reason"]


async def test_healthy_mode_is_not_flagged(config, store):
    client = StubClient(events=make_events(1))
    poller = Poller(config, store, client=client)
    await poller.poll_once()

    assert poller.status.degraded is False
    assert "degraded_reason" not in poller.status.as_dict()


async def test_start_and_stop_run_the_loop(config, store):
    client = StubClient(events=make_events(5))
    poller = Poller(config, store, client=client)

    await poller.start()
    assert poller.status.running is True
    await asyncio.sleep(0.08)
    await poller.stop()

    assert poller.status.running is False
    assert poller.status.polls >= 1
    assert store.count() == 5


async def test_stop_is_safe_when_never_started(config, store):
    poller = Poller(config, store, client=StubClient())
    await poller.stop()
    assert poller.status.running is False


async def test_retention_prunes_old_events(config, store):
    config.storage.retention_days = 1.0
    config.storage.prune_interval = 0.0

    old = nova_now() - int(3 * 86400 * TICKS_PER_SECOND)
    recent = nova_now() - int(60 * TICKS_PER_SECOND)
    store.insert_events(make_events(5, start=old) + make_events(5, start=recent))
    assert store.count() == 10

    poller = Poller(config, store, client=StubClient())
    await poller._maybe_prune()

    assert store.count() == 5
    assert poller.status.pruned_total == 5


async def test_retention_disabled_by_default_keeps_everything(config, store):
    assert config.storage.retention_days == 0.0

    old = nova_now() - int(3650 * 86400 * TICKS_PER_SECOND)
    store.insert_events(make_events(5, start=old))

    poller = Poller(config, store, client=StubClient())
    await poller._maybe_prune()

    assert store.count() == 5, "an archive must not be discarded by default"


async def test_ingest_log_records_each_pass(config, store):
    client = StubClient(events=make_events(3))
    poller = Poller(config, store, client=client)

    await poller.poll_once()
    rows = store.recent_ingests()

    assert len(rows) == 1
    assert rows[0]["inserted"] == 3
    assert rows[0]["source"] == HISTORY_ROUTE


async def test_default_events_sit_inside_the_cold_start_backfill(config, store):
    """Guard against time-dependent flakiness in this file.

    make_events() anchors to the current time for a reason: a fixed timestamp
    drifts out of the backfill window as wall time passes, and every test that
    relies on a cold-start poll then fails -- silently correct code, failing
    tests. This asserts the anchor stays inside the default window.
    """
    events = make_events(10)
    window_start = nova_now() - int(config.ingest.backfill * TICKS_PER_SECOND)

    assert events[0].nova_time >= window_start
    assert events[-1].nova_time <= nova_now()
