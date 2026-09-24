"""Shared fixtures.

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
    cfg = Config()
    cfg.storage.path = str(tmp_path / "spills.db")
    cfg.tdu.sources = [
        "http://tdu-near-master-ppc-01:8080",
        "http://tdu-near-master-ppc-02:8080",
    ]
    cfg.ingest.enabled = False
    cfg.query.default_limit = 100
    cfg.query.max_limit = 1000
    return cfg


@pytest.fixture
def store(config):
    archive = SpillStore(config.storage.path)
    yield archive
    archive.close()


@pytest.fixture
def populated(store):
    """An archive holding a minute of 1 Hz events and a few NuMI spills.

    The 1 Hz events come from SOURCE_A and the NuMI spills from SOURCE_B, both
    of which the ``config`` fixture lists as sources.

    The 1 Hz events carry raw signal codes, as a patched TDU would supply;
    the NuMI ones do not, standing in for records ingested through the legacy
    route where the hardware preserved only the decoded type.
    """
    events = []
    for index in range(60):
        events.append(SpillEvent(
            nova_time=BASE + index * SECOND,
            spill_type=SpillType.ACCEL_ONE_HZ_TCLK,
            signal_code=0x8F,
            event_word=0x018F,
            event_number=1000 + index,
            delta=SECOND,
            source=SOURCE_A,
            route="spill_history",
        ))
    for index in range(5):
        events.append(SpillEvent(
            nova_time=BASE + index * 10 * SECOND + SECOND // 2,
            spill_type=SpillType.NUMI,
            signal_code=-1,
            event_number=2000 + index,
            source=SOURCE_B,
            route="tcr_status",
        ))
    store.insert_events(events)
    return store


@pytest.fixture
def client(config, store):
    app = create_app(config, store=store, start_ingest=False)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def populated_client(config, populated):
    app = create_app(config, store=populated, start_ingest=False)
    with TestClient(app) as test_client:
        yield test_client


#: Admin token the live server accepts, for the client tests that change sources.
ADMIN_TOKEN = "test-admin-token"


@pytest.fixture
def live_server(config, populated):
    """A real uvicorn server on a free loopback port, serving the populated
    archive, for the tests that drive the clients over a socket.

    Yields the base URL. The server runs in a daemon thread so that a test
    that hangs cannot keep the interpreter alive after pytest finishes.
    """
    import socket
    import threading
    import time

    import uvicorn

    config.admin.token = ADMIN_TOKEN
    app = create_app(config, store=populated, start_ingest=False)

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    server = uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=port, log_level="warning", lifespan="on"))
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise RuntimeError("live server did not start")
        time.sleep(0.02)
    yield "http://127.0.0.1:{}".format(port)
    server.should_exit = True
    thread.join(timeout=10)
