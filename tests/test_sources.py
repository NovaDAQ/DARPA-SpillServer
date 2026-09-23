"""Tests for recording from several TDUs, and changing them at runtime.

Covers the four layers that carry a source: its configuration, the archive
(including migrating an archive written before sources existed), the ingest
manager that polls each one, and the API that filters by source and lets an
administrator change one.
"""

import asyncio
import sqlite3

import pytest
from fastapi.testclient import TestClient

from darpa_spillserver.app import create_app
from darpa_spillserver.config import Config, ConfigError, load_config, parse_source
from darpa_spillserver.novatime import TICKS_PER_SECOND, nova_now
from darpa_spillserver.poller import IngestManager, SourceError
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import SpillEvent, SpillStore, StoreError
from darpa_spillserver.tdu_client import HISTORY_ROUTE, FetchResult

from conftest import BASE, SECOND, SOURCE_A, SOURCE_B

URL_A = "http://tdu-near-master-ppc-01:8080"
URL_B = "http://tdu-near-master-ppc-02:8080"
URL_C = "http://tdu-near-master-ppc-03:8080"
TOKEN = "0123456789abcdef0123456789abcdef"


# ------------------------------------------------------------ configuration


def test_a_bare_url_is_named_after_its_host():
    source = parse_source("http://tdu-near-master-ppc-02:8080/")
    assert source.name == "tdu-near-master-ppc-02"
    assert source.base_url == URL_B, "the trailing slash is normalised away"


def test_a_source_can_be_given_an_explicit_name():
    source = parse_source("near-02=" + URL_B)
    assert (source.name, source.base_url) == ("near-02", URL_B)


def test_an_equals_sign_in_a_query_string_is_not_a_name():
    source = parse_source("http://tdu:8080/?a=b")
    assert source.name == "tdu"


@pytest.mark.parametrize("spec", ["ftp://tdu", "not a url", "bad name!=" + URL_A])
def test_malformed_sources_are_refused(spec):
    with pytest.raises(ConfigError):
        parse_source(spec)


def test_sources_come_from_repeated_options():
    config = load_config(
        argv=["--tdu-source", URL_A, "--tdu-source", "b=" + URL_B],
        environ={}, search_paths=[],
    )
    assert [s.name for s in config.tdu.resolved_sources()] == [SOURCE_A, "b"]


def test_sources_come_from_a_comma_separated_environment_variable():
    config = load_config(
        argv=[], environ={"DARPA_SPILL_TDU_SOURCES": URL_A + "," + URL_B},
        search_paths=[],
    )
    assert [s.base_url for s in config.tdu.resolved_sources()] == [URL_A, URL_B]


def test_the_single_base_url_form_still_works():
    config = load_config(argv=["--tdu-url", URL_C], environ={}, search_paths=[])
    assert [s.name for s in config.tdu.resolved_sources()] == ["tdu-near-master-ppc-03"]


def test_base_url_and_sources_together_are_refused():
    with pytest.raises(ConfigError, match="both set"):
        load_config(argv=["--tdu-url", URL_A, "--tdu-source", URL_B],
                    environ={}, search_paths=[])


@pytest.mark.parametrize("specs,fragment", [
    (["x=" + URL_A, "x=" + URL_B], "the name 'x' twice"),
    ([URL_A, "other=" + URL_A], "the URL"),
])
def test_duplicate_sources_are_refused(specs, fragment):
    argv = [arg for spec in specs for arg in ("--tdu-source", spec)]
    with pytest.raises(ConfigError, match=fragment):
        load_config(argv=argv, environ={}, search_paths=[])


def test_the_admin_token_is_redacted():
    config = Config()
    config.admin.token = TOKEN
    assert config.to_dict()["admin"]["token"] == "***redacted***"


# ------------------------------------------------------------------ archive


def event(nova_time, source, spill_type=SpillType.ACCEL_ONE_HZ_TCLK, code=0x8F):
    return SpillEvent(nova_time=nova_time, spill_type=spill_type,
                      signal_code=code, source=source, route="spill_history")


def test_the_same_event_from_two_sources_is_kept_twice(store):
    assert store.insert_events([event(BASE, SOURCE_A), event(BASE, SOURCE_B)]) == 2
    assert store.insert_events([event(BASE, SOURCE_A)]) == 0, "still idempotent"
    assert store.counts_by_source() == {SOURCE_A: 1, SOURCE_B: 1}


def test_queries_filter_by_source(populated):
    assert populated.query(sources=[SOURCE_A]).total == 60
    assert populated.query(sources=[SOURCE_B]).total == 5
    assert populated.query(sources=[SOURCE_A, SOURCE_B]).total == 65
    assert populated.query().total == 65
    assert populated.latest(sources=[SOURCE_B]).source == SOURCE_B


def test_source_names_lists_what_the_archive_holds(populated):
    assert populated.source_names() == [SOURCE_A, SOURCE_B]


def test_pruning_one_source_leaves_the_others(store):
    store.insert_events([event(BASE, SOURCE_A), event(BASE, SOURCE_B)])
    assert store.prune_before(BASE + 1, source=SOURCE_A) == 1
    assert store.source_names() == [SOURCE_B]


def test_overrides_are_saved_and_cleared(store):
    store.set_source_override(SOURCE_A, URL_C, False, "tester")
    row = store.source_overrides()[SOURCE_A]
    assert (row["base_url"], row["enabled"], row["updated_by"]) == (URL_C, 0, "tester")
    assert store.clear_source_override(SOURCE_A) is True
    assert store.source_overrides() == {}


def make_v1_archive(path):
    """Write an archive exactly as schema version 1 laid it out."""
    connection = sqlite3.connect(str(path))
    connection.executescript("""
        CREATE TABLE events (
            nova_time INTEGER NOT NULL, spill_type INTEGER NOT NULL,
            signal_code INTEGER NOT NULL DEFAULT -1, event_word INTEGER,
            event_number INTEGER, delta INTEGER, pps_offset INTEGER,
            source TEXT NOT NULL DEFAULT 'unknown', ingested_at INTEGER NOT NULL,
            PRIMARY KEY (nova_time, spill_type, signal_code)
        ) WITHOUT ROWID;
        CREATE INDEX idx_events_time ON events (nova_time);
        CREATE INDEX idx_events_type_time ON events (spill_type, nova_time);
        CREATE INDEX idx_events_signal_time ON events (signal_code, nova_time);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO meta VALUES ('schema_version', '1');
        CREATE TABLE ingest_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, started_at INTEGER NOT NULL,
            finished_at INTEGER, source TEXT, fetched INTEGER DEFAULT 0,
            inserted INTEGER DEFAULT 0, error TEXT
        );
        INSERT INTO ingest_log (started_at, source) VALUES (1, '/tcr_status');
    """)
    connection.executemany(
        "INSERT INTO events VALUES (?, 3, -1, NULL, ?, NULL, NULL, 'tcr_status', 0)",
        [(BASE + index * SECOND, index) for index in range(10)],
    )
    connection.commit()
    connection.close()


def test_a_version_1_archive_is_migrated_in_place(tmp_path):
    path = tmp_path / "old.db"
    make_v1_archive(path)

    store = SpillStore(str(path), legacy_source=SOURCE_A)
    assert store.get_meta("schema_version") == "2"
    assert store.counts_by_source() == {SOURCE_A: 10}
    migrated = store.query().events[0]
    assert migrated.route == "tcr_status", "the old source column held the route"
    assert store.recent_ingests()[0]["route"] == "/tcr_status"

    # Now keyed per source: the same instant from another TDU is a new row.
    assert store.insert_events([event(BASE, SOURCE_B, SpillType(3), -1)]) == 1
    store.close()

    reopened = SpillStore(str(path), legacy_source="ignored-now")
    assert reopened.counts_by_source() == {SOURCE_A: 10, SOURCE_B: 1}
    reopened.close()


def test_an_archive_from_a_newer_version_is_refused(tmp_path):
    path = tmp_path / "future.db"
    SpillStore(str(path)).set_meta("schema_version", "99")
    with pytest.raises(StoreError, match="newer"):
        SpillStore(str(path))


# ------------------------------------------------------------------ ingest


class StubClient:
    """Serves one event per poll, stamped with the URL it was built for."""

    def __init__(self, source, route=HISTORY_ROUTE):
        self.base_url = source.base_url
        self.route = route
        self.polls = 0

    async def open(self):
        pass

    async def close(self):
        pass

    async def supports_history(self):
        return self.route == HISTORY_ROUTE

    async def fetch_history(self, since_nova=None, limit=None, **kwargs):
        self.polls += 1
        stamp = nova_now() - 60 * TICKS_PER_SECOND + self.polls
        return FetchResult(events=[event(stamp, "stub")], route=self.route)


@pytest.fixture
def ingest_config(tmp_path):
    cfg = Config()
    cfg.storage.path = str(tmp_path / "spills.db")
    cfg.tdu.sources = [URL_A, URL_B]
    cfg.ingest.interval = 0.01
    return cfg


@pytest.fixture
def ingest_store(ingest_config):
    archive = SpillStore(ingest_config.storage.path)
    yield archive
    archive.close()


def make_manager(config, store, route=HISTORY_ROUTE):
    clients = {}

    def factory(source):
        clients[source.name] = StubClient(source, route=route)
        return clients[source.name]

    manager = IngestManager(config, store, client_factory=factory)
    return manager, clients


async def test_every_source_is_polled_and_tagged(ingest_config, ingest_store):
    manager, _ = make_manager(ingest_config, ingest_store)
    await manager.start()
    await asyncio.sleep(0.08)
    await manager.stop()

    counts = ingest_store.counts_by_source()
    assert set(counts) == {SOURCE_A, SOURCE_B}, "tagged by poller, not by client"
    assert "stub" not in counts


async def test_disabling_a_source_stops_its_poller(ingest_config, ingest_store):
    manager, _ = make_manager(ingest_config, ingest_store)
    await manager.start()
    await manager.update_source(SOURCE_B, enabled=False, updated_by="tester")
    assert SOURCE_B not in manager.pollers
    assert manager.pollers[SOURCE_A].status.running

    summary = manager.summary()
    assert summary["running"] is True, "every *enabled* source is running"

    await manager.update_source(SOURCE_B, enabled=True)
    assert manager.pollers[SOURCE_B].status.running
    await manager.stop()


async def test_changing_a_url_restarts_that_source_only(ingest_config, ingest_store):
    manager, clients = make_manager(ingest_config, ingest_store)
    await manager.start()
    untouched = manager.pollers[SOURCE_A]

    await manager.update_source(SOURCE_B, base_url=URL_C + "/")
    assert clients[SOURCE_B].base_url == URL_C
    assert manager.pollers[SOURCE_A] is untouched
    assert manager.sources[SOURCE_B].base_url == URL_C
    await manager.stop()


async def test_changes_survive_a_restart_and_reset_clears_them(ingest_config, ingest_store):
    manager, _ = make_manager(ingest_config, ingest_store)
    await manager.update_source(SOURCE_B, base_url=URL_C, enabled=False)

    reloaded, _ = make_manager(ingest_config, ingest_store)
    source = reloaded.sources[SOURCE_B]
    assert (source.base_url, source.enabled) == (URL_C, False)
    assert reloaded.describe()[1]["overridden"] is True

    await reloaded.reset_source(SOURCE_B)
    assert ingest_store.source_overrides() == {}
    assert reloaded.sources[SOURCE_B].base_url == URL_B


async def test_a_change_back_to_the_file_drops_the_override(ingest_config, ingest_store):
    manager, _ = make_manager(ingest_config, ingest_store)
    await manager.update_source(SOURCE_A, enabled=False)
    await manager.update_source(SOURCE_A, enabled=True)
    assert ingest_store.source_overrides() == {}


async def test_bad_changes_are_refused(ingest_config, ingest_store):
    manager, _ = make_manager(ingest_config, ingest_store)
    with pytest.raises(SourceError, match="no source"):
        await manager.update_source("nope", enabled=False)
    with pytest.raises(SourceError, match="http"):
        await manager.update_source(SOURCE_A, base_url="tdu-03")
    with pytest.raises(SourceError, match="already the URL"):
        await manager.update_source(SOURCE_A, base_url=URL_B)


async def test_degradation_is_reported_per_source(ingest_config, ingest_store):
    manager, _ = make_manager(ingest_config, ingest_store, route="/tcr_status")
    await manager.start()
    await asyncio.sleep(0.05)
    summary = manager.summary()
    await manager.stop()

    assert summary["degraded"] is True
    assert summary["degraded_sources"] == [SOURCE_A, SOURCE_B]
    assert "6-15 Hz" in summary["degraded_reason"]


# --------------------------------------------------------------------- API


def test_events_filter_by_source(populated_client):
    rows = populated_client.get(
        "/api/events", params={"source": SOURCE_B}).json()["events"]
    assert len(rows) == 5
    assert {row["source"] for row in rows} == {SOURCE_B}
    assert {row["route"] for row in rows} == {"tcr_status"}


def test_sources_combine_as_a_union(populated_client):
    body = populated_client.get(
        "/api/events", params={"source": SOURCE_A + "," + SOURCE_B}).json()
    assert body["meta"]["total"] == 65
    assert body["meta"]["sources"] == [SOURCE_A, SOURCE_B]


def test_no_source_means_every_source(populated_client):
    meta = populated_client.get("/api/events").json()["meta"]
    assert meta["total"] == 65
    assert meta["sources"] == ["(all sources)"]


def test_an_unknown_source_is_refused_with_the_known_ones(populated_client):
    response = populated_client.get("/api/events", params={"source": "ppc-09"})
    assert response.status_code == 400
    assert SOURCE_A in response.json()["detail"]["hint"]


def test_latest_and_export_take_a_source(populated_client):
    latest = populated_client.get("/api/latest", params={"source": SOURCE_B})
    assert latest.json()["event"]["source"] == SOURCE_B
    csv = populated_client.get("/api/export", params={"source": SOURCE_B}).text
    assert "# sources: " + SOURCE_B in csv
    assert csv.count(SOURCE_B + ",tcr_status") == 5


def test_status_counts_events_per_source(populated_client):
    status = populated_client.get("/api/status").json()
    assert status["archive"]["by_source"] == {SOURCE_A: 60, SOURCE_B: 5}
    assert [s["name"] for s in status["sources"]] == [SOURCE_A, SOURCE_B]


def test_sources_endpoint_describes_each_source(populated_client):
    body = populated_client.get("/api/sources").json()
    first = body["sources"][0]
    assert (first["name"], first["base_url"], first["enabled"]) == (SOURCE_A, URL_A, True)
    assert first["events"] == 60
    assert first["overridden"] is False
    assert body["admin"]["enabled"] is False


def test_changes_are_refused_when_no_admin_token_is_configured(populated_client):
    response = populated_client.patch(
        "/api/sources/" + SOURCE_A, json={"enabled": False},
        headers={"X-Admin-Token": "anything"})
    assert response.status_code == 403
    assert "admin.token_file" in response.json()["detail"]["hint"]


@pytest.fixture
def admin_client(config, populated):
    config.admin.token = TOKEN
    app = create_app(config, store=populated, start_ingest=False)
    with TestClient(app) as test_client:
        yield test_client


def test_changes_need_the_token(admin_client):
    path = "/api/sources/" + SOURCE_A
    assert admin_client.patch(path, json={"enabled": False}).status_code == 401
    wrong = admin_client.patch(path, json={"enabled": False},
                               headers={"X-Admin-Token": "wrong"})
    assert wrong.status_code == 403
    assert admin_client.get("/api/sources").json()["sources"][0]["enabled"] is True


def test_the_token_allows_a_change_and_a_reset(admin_client, populated):
    headers = {"X-Admin-Token": TOKEN}
    assert admin_client.get("/api/admin/check", headers=headers).json()["ok"] is True

    changed = admin_client.patch(
        "/api/sources/" + SOURCE_B, headers=headers,
        json={"enabled": False, "base_url": URL_C}).json()["source"]
    assert (changed["enabled"], changed["base_url"]) == (False, URL_C)
    assert changed["overridden"] is True
    assert changed["configured"]["base_url"] == URL_B
    assert "admin token" in populated.source_overrides()[SOURCE_B]["updated_by"]

    reset = admin_client.post(
        "/api/sources/{}/reset".format(SOURCE_B), headers=headers).json()["source"]
    assert (reset["enabled"], reset["base_url"], reset["overridden"]) == (True, URL_B, False)


def test_bad_changes_are_explained(admin_client):
    headers = {"X-Admin-Token": TOKEN}
    unknown = admin_client.patch("/api/sources/nope", headers=headers,
                                 json={"enabled": False})
    assert unknown.status_code == 404
    empty = admin_client.patch("/api/sources/" + SOURCE_A, headers=headers, json={})
    assert empty.status_code == 400
    bad_url = admin_client.patch("/api/sources/" + SOURCE_A, headers=headers,
                                 json={"base_url": "ppc-03"})
    assert bad_url.status_code == 400


def test_the_admin_token_is_not_reachable_cross_origin(admin_client):
    """CORS must not let another site's page send a write."""
    preflight = admin_client.options(
        "/api/sources/" + SOURCE_A,
        headers={"Origin": "https://evil.example",
                 "Access-Control-Request-Method": "PATCH",
                 "Access-Control-Request-Headers": "x-admin-token"})
    assert "PATCH" not in preflight.headers.get("access-control-allow-methods", "")


def test_query_and_config_pages_list_the_sources(client):
    index = client.get("/").text
    assert 'name="source" value="{}"'.format(SOURCE_B) in index
    page = client.get("/config")
    assert page.status_code == 200
    assert "/api/sources" in page.text


def test_an_unreadable_admin_token_file_stops_startup(config, store, tmp_path):
    config.admin.token_file = str(tmp_path / "missing")
    with pytest.raises(ConfigError, match="admin.token_file"):
        create_app(config, store=store, start_ingest=False)


class HangingClient(StubClient):
    """A TDU that accepts the connection and then never answers."""

    def __init__(self, source):
        super().__init__(source)
        self.cancelled = False

    async def fetch_history(self, since_nova=None, limit=None, **kwargs):
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.cancelled = True
            raise


async def test_disabling_a_source_does_not_wait_for_a_hung_tdu(ingest_config, ingest_store):
    clients = {}

    def factory(source):
        clients[source.name] = HangingClient(source)
        return clients[source.name]

    manager = IngestManager(ingest_config, ingest_store, client_factory=factory)
    await manager.start()
    await asyncio.sleep(0.05)

    started = asyncio.get_running_loop().time()
    await manager.update_source(SOURCE_A, enabled=False)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 1.0, "stop() must not sit out the poller's grace period"
    assert clients[SOURCE_A].cancelled
    await manager.stop()
