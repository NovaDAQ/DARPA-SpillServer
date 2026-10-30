"""!
@file tests/conftest.py
@brief Shared fixtures.

The application is always built with ``start_ingest=False`` so that no test
can reach out to a real TDU; ingest is exercised directly in
``test_poller.py`` against a stubbed client.
"""

import pytest
from fastapi.testclient import TestClient

from darpa_spillserver.app import create_app
from darpa_spillserver.config import Config
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import SpillEvent, SpillStore

#: 2026-09-22 15:53:33 UTC, captured from the live TDU.
BASE = 33778458638680249

#: One second of the NOvA 64 MHz clock.
SECOND = 64000000

#: The two TDUs the populated archive holds events from.
SOURCE_A = "tdu-near-master-ppc-01"
SOURCE_B = "tdu-near-master-ppc-02"


@pytest.fixture
def config(tmp_path):
    """!
    @brief Configuration pointing at a per-test SQLite archive, listing both
    near-detector TDUs as sources, with ingest disabled.
    @param tmp_path pytest per-test temporary directory; holds spills.db.
    @return Config with query limits 100 (default) and 1000 (maximum).
    """
    cfg = Config()
    cfg.storage.path = str(tmp_path / "spills.db")
    cfg.tdu.sources = [
        "http://tdu-near-master-ppc-01:8080",
        "http://tdu-near-master-ppc-02:8080",
    ]
    cfg.ingest.enabled = False
    # Small, fixed limits so paging/limit tests do not depend on the shipped
    # defaults.
    cfg.query.default_limit = 100
    cfg.query.max_limit = 1000
    return cfg


@pytest.fixture
def store(config):
    """!
    @brief Empty SpillStore opened on the configured archive path; closed on
    teardown.
    @param config the config fixture (supplies storage.path).
    @return the open SpillStore.
    """
    archive = SpillStore(config.storage.path)
    yield archive
    archive.close()


@pytest.fixture
def populated(store):
    """!
    @brief An archive holding a minute of 1 Hz events and a few NuMI spills.

    The 1 Hz events come from SOURCE_A and the NuMI spills from SOURCE_B, both
    of which the ``config`` fixture lists as sources.

    The 1 Hz events carry raw signal codes, as a patched TDU would supply;
    the NuMI ones do not, standing in for records ingested through the legacy
    route where the hardware preserved only the decoded type.

    @param store the empty store fixture to fill.
    @return the same store holding 65 events (60 from SOURCE_A, 5 from
    SOURCE_B).
    """
    events = []
    for index in range(60):
        events.append(SpillEvent(
            nova_time=BASE + index * SECOND,
            spill_type=SpillType.ACCEL_ONE_HZ_TCLK,
            signal_code=0x8F,
            # Event word 0x018F = TCLK carrier (bit 8) | $8F.
            event_word=0x018F,
            event_number=1000 + index,
            delta=SECOND,
            source=SOURCE_A,
            route="spill_history",
        ))
    for index in range(5):
        events.append(SpillEvent(
            # NuMI spills every 10 s, offset by 0.5 s so they never coincide
            # with a 1 Hz tick; last one at BASE + 40.5 s.
            nova_time=BASE + index * 10 * SECOND + SECOND // 2,
            spill_type=SpillType.NUMI,
            # -1 is UNKNOWN_SIGNAL: no raw word on the legacy route.
            signal_code=-1,
            event_number=2000 + index,
            source=SOURCE_B,
            route="tcr_status",
        ))
    store.insert_events(events)
    return store


@pytest.fixture
def client(config, store):
    """!
    @brief FastAPI TestClient over an app built on the empty store, with ingest
    not started.
    @param config the config fixture.
    @param store the empty store fixture.
    @return the TestClient (lifespan entered for the test's duration).
    """
    app = create_app(config, store=store, start_ingest=False)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def populated_client(config, populated):
    """!
    @brief FastAPI TestClient over an app built on the populated store, with
    ingest not started.
    @param config the config fixture.
    @param populated the populated store fixture.
    @return the TestClient (lifespan entered for the test's duration).
    """
    app = create_app(config, store=populated, start_ingest=False)
    with TestClient(app) as test_client:
        yield test_client
