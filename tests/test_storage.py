"""Tests for the SQLite event archive."""

import pytest

from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import UNKNOWN_SIGNAL, SpillEvent, SpillStore

BASE = 33778458638680249


@pytest.fixture
def store(tmp_path):
    archive = SpillStore(str(tmp_path / "spills.db"))
    yield archive
    archive.close()


def make_event(offset=0, spill_type=SpillType.BNB_TCLK, signal_code=0x1F, **kwargs):
    return SpillEvent(
        nova_time=BASE + offset,
        spill_type=spill_type,
        signal_code=signal_code,
        event_number=1000 + offset,
        source="test",
        **kwargs
    )


def test_insert_and_count(store):
    assert store.insert_events([make_event(0), make_event(64000000)]) == 2
    assert store.count() == 2


def test_inserting_nothing_is_harmless(store):
    assert store.insert_events([]) == 0


def test_reinserting_the_same_event_is_idempotent(store):
    """Ingest re-reads overlapping windows by design; that must not duplicate."""
    event = make_event(0)
    assert store.insert_events([event]) == 1
    assert store.insert_events([event]) == 0
    assert store.count() == 1


def test_overlapping_batches_insert_only_the_new_rows(store):
    first = [make_event(i * 1000) for i in range(10)]
    overlapping = [make_event(i * 1000) for i in range(5, 15)]

    assert store.insert_events(first) == 10
    assert store.insert_events(overlapping) == 5
    assert store.count() == 15


def test_unknown_signal_sentinel_still_deduplicates(store):
    """A NULL here would defeat the primary key, since SQLite treats NULLs
    as distinct in a unique index."""
    event = make_event(0, signal_code=UNKNOWN_SIGNAL)
    assert store.insert_events([event]) == 1
    assert store.insert_events([event]) == 0


def test_same_time_different_signal_are_distinct_rows(store):
    store.insert_events([
        make_event(0, signal_code=0x1D),
        make_event(0, signal_code=0x1F),
    ])
    assert store.count() == 2


def test_query_range_is_half_open(store):
    store.insert_events([make_event(i) for i in range(10)])

    page = store.query(start_nova=BASE + 2, end_nova=BASE + 5)
    times = [event.nova_time - BASE for event in page.events]
    assert times == [2, 3, 4], "start is inclusive, end exclusive"


def test_adjacent_ranges_tile_without_overlap(store):
    store.insert_events([make_event(i) for i in range(10)])

    first = store.query(start_nova=BASE, end_nova=BASE + 5)
    second = store.query(start_nova=BASE + 5, end_nova=BASE + 10)
    assert first.total + second.total == 10
    assert not (
        {e.nova_time for e in first.events} & {e.nova_time for e in second.events}
    )


def test_query_filters_by_type(store):
    store.insert_events([
        make_event(0, spill_type=SpillType.NUMI, signal_code=0x74),
        make_event(1, spill_type=SpillType.ACCEL_ONE_HZ_TCLK, signal_code=0x8F),
    ])
    page = store.query(spill_types=[SpillType.NUMI])
    assert page.total == 1
    assert page.events[0].spill_type is SpillType.NUMI


def test_query_filters_by_signal_code(store):
    store.insert_events([
        make_event(0, signal_code=0x1D),
        make_event(1, signal_code=0x1F),
    ])
    page = store.query(signal_codes=[0x1F])
    assert page.total == 1
    assert page.events[0].signal_code == 0x1F


def test_query_paging_reports_the_full_total(store):
    store.insert_events([make_event(i) for i in range(50)])

    page = store.query(limit=10)
    assert len(page.events) == 10
    assert page.total == 50
    assert page.truncated is True

    last = store.query(limit=10, offset=40)
    assert last.truncated is False


def test_query_descending_order(store):
    store.insert_events([make_event(i) for i in range(5)])
    page = store.query(descending=True)
    times = [event.nova_time for event in page.events]
    assert times == sorted(times, reverse=True)


def test_iter_query_streams_everything(store):
    store.insert_events([make_event(i) for i in range(2500)])
    streamed = list(store.iter_query(chunk=100))
    assert len(streamed) == 2500
    assert streamed[0].nova_time < streamed[-1].nova_time


def test_latest_and_earliest(store):
    store.insert_events([make_event(i) for i in range(10)])
    assert store.latest().nova_time == BASE + 9
    assert store.earliest().nova_time == BASE


def test_latest_by_type(store):
    store.insert_events([
        make_event(0, spill_type=SpillType.NUMI, signal_code=0x74),
        make_event(5, spill_type=SpillType.ACCEL_ONE_HZ_TCLK, signal_code=0x8F),
    ])
    assert store.latest(SpillType.NUMI).nova_time == BASE


def test_latest_on_an_empty_archive_is_none(store):
    assert store.latest() is None
    assert store.earliest() is None


def test_counts_by_type(store):
    store.insert_events([
        make_event(0, spill_type=SpillType.NUMI, signal_code=0x74),
        make_event(1, spill_type=SpillType.NUMI, signal_code=0x74),
        make_event(2, spill_type=SpillType.ACCEL_ONE_HZ_TCLK, signal_code=0x8F),
    ])
    counts = store.counts_by_type()
    assert counts[SpillType.NUMI] == 2
    assert counts[SpillType.ACCEL_ONE_HZ_TCLK] == 1


def test_prune_removes_only_older_events(store):
    store.insert_events([make_event(i) for i in range(10)])
    removed = store.prune_before(BASE + 5)
    assert removed == 5
    assert store.count() == 5
    assert store.earliest().nova_time == BASE + 5


def test_meta_round_trip(store):
    assert store.get_meta("missing") is None
    assert store.get_meta("missing", "fallback") == "fallback"
    store.set_meta("watermark", "123")
    assert store.get_meta("watermark") == "123"
    store.set_meta("watermark", "456")
    assert store.get_meta("watermark") == "456"


def test_ingest_log(store):
    store.record_ingest("ppc-01", "/spill_history", 100, 101, 50, 10, None)
    store.record_ingest("ppc-01", "/spill_history", 102, 103, 50, 0, "boom")
    rows = store.recent_ingests()
    assert len(rows) == 2
    assert rows[0]["error"] == "boom"


def test_trim_ingest_log(store):
    for index in range(20):
        store.record_ingest("ppc-01", "/spill_history", index, index + 1, 1, 1,
                            None)
    store.trim_ingest_log(keep=5)
    assert len(store.recent_ingests(limit=100)) == 5


def test_signal_helpers_on_event():
    event = make_event(0, signal_code=0x8F)
    assert event.signal_hex == "$8F"
    assert event.signal_name == "one-hertz"

    unknown = make_event(0, signal_code=UNKNOWN_SIGNAL)
    assert unknown.signal_hex is None
    assert unknown.signal_name is None


def test_in_memory_store_shares_one_connection():
    """A per-thread connection to ':memory:' would be a different database."""
    memory = SpillStore(":memory:")
    try:
        memory.insert_events([make_event(0)])
        assert memory.count() == 1
    finally:
        memory.close()


def test_store_creates_parent_directories(tmp_path):
    nested = tmp_path / "a" / "b" / "spills.db"
    archive = SpillStore(str(nested))
    try:
        assert nested.parent.is_dir()
    finally:
        archive.close()
