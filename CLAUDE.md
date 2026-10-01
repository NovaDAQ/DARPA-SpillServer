# CLAUDE.md

## 1. Project overview

The DARPA Spill Information Server runs on the NOvA DAQ gateway
(`novadaq-near-gateway-01.fnal.gov`). It polls the embedded bottle server
(TDUWeb) on one or more NOvA TDUs, archives the decoded accelerator events
(`$74` NuMI, `$1B` BNB, `$1D`/`$1F` TCLK, `$8F` 1 Hz, …) in SQLite, and serves
them by time range and signal as CSV or JSON, with every timestamp in NOvA
time, UNIX, UTC and GPS. It is production DAQ infrastructure, so correctness
and not silently losing data come before convenience. The specification is
`Design.md`. The HTTP API is consumed by the browser UI, by
`darpa-spill-client` (Python) and by `darpa-spill-client-cpp` (C++), which
are built on the client libraries `darpa_spillserver.client` and
`libdarpa_spill_client`.

## 2. Setup and running

```console
$ ./bootstrap.sh                     # Linux/macOS: venv/, package, and C/C++ build/ if cmake exists
PS> .\bootstrap.ps1                  # Windows 11
$ venv/bin/darpa-spill-server -c config/spillserver.yaml         # foreground
$ ./start-darpa-spillserver.sh -c config/spillserver-near.yaml   # background; run/spillserver.{pid,log}
$ ./stop-darpa-spillserver.sh
$ venv/bin/darpa-spill-client -u http://localhost:8080 events --start -1h --signal '$8f'
$ cmake --preset default && cmake --build build && ctest --test-dir build   # C/C++ only
```

`nova-time-decoder` is a sibling NOvA package, not on PyPI. The bootstrap
scripts install it from `../nova-time-decoder`, or from
`$NOVA_TIME_DECODER_DIR`. On this workstation the checkout is at
`~/Git-Repositories/Norman/nova-time-decoder`. The TDUs
(`http://tdu-near-master-ppc-01:8080`) are reachable only from the DAQ
network.

## 3. Architecture

```
TDU /spill_history ──httpx──▶ poller.IngestManager ──▶ storage.SpillStore (SQLite, WAL)
   (one asyncio task per source)                              │
                                                              ▼
            FastAPI app (app.py) ── api.py /api/* ── formats.py (streamed CSV/JSON)
               │  pages.py: /about /api /sitemap /sitemap.xml (from app.openapi())
               │  web/templates: / (query), /config (sources)
               ▼
   browser · client.py/client_cli.py (Python) · src/cpp (C++ lib + C ABI + CLI)
```

* `signals.py` holds the signal registry and the hardware's decode rules,
  transcribed from `NssSpillInfo::getSpillTypeFromEvent`. `novatime.py`
  parses time expressions and converts between timescales.
* **Threading.** A single asyncio event loop runs under uvicorn. Ingest is an
  asyncio task per source, owned by the app lifespan, not a daemon thread,
  because everything already runs inside one loop. Blocking SQLite calls go
  through `asyncio.to_thread`, and `SpillStore` keeps one connection per
  thread.
* **Clients.** `docs/CLIENT.md` is the contract. The Python and C++ CLIs must
  produce byte-identical stdout, and `tests/test_client_equivalence.py`
  enforces it. Python uses only the standard library for HTTP. The C++ side
  uses Boost.Beast/Asio/JSON/Program_options (Boost 1.75 minimum, so no Boost.URL), yaml-cpp, and optionally
  OpenSSL.
* The web framework is FastAPI with plain JavaScript in the templates. That
  was a deliberate choice, kept at the owner's request; do not migrate it to
  Flask, Litestar, HTMX or Tailwind.
* The database is raw `sqlite3`. SQLAlchemy and Postgres are not used; this
  is a known deviation from the global preferences, left for a later decision.

## 4. Configuration schema

Precedence, highest first: command line > environment > `.env` > YAML >
defaults. Environment variables are `DARPA_SPILL_<SECTION>_<KEY>`; the `.env`
file is `--env-file`, else `$DARPA_SPILL_ENV_FILE`, else `./.env`. Unknown
keys are an error. The full commented file is `config/spillserver.yaml`, and
every key is in `docs/CONFIGURATION.md`.

```yaml
server:  {host: 0.0.0.0, port: 8080, root_path: "", cors_origins: ["*"], ssl_certfile: "", ssl_keyfile: ""}
tdu:     {sources: [http://tdu-near-master-ppc-01:8080], timeout: 20.0, history_timeout: 600.0, retries: 2}
ingest:  {enabled: true, interval: 1.0, batch_limit: 5000, overlap: 5.0, backfill: 3600.0}
storage: {path: ./spills.db, retention_days: 0}      # 0 = keep everything
query:   {timezone: UTC, default_limit: 10000, max_limit: 1000000}
auth:    {enabled: false, provider: oidc, issuer: "", client_id: ""}   # Fermilab SSO hooks
admin:   {token_file: "", allowed_groups: []}        # guards PATCH /api/sources
logging: {level: INFO, file: ""}
```

The client file is `config/spillclient.yaml`, holding a single `client:`
section with `url`, `timeout`, `admin_token`, `admin_token_file`, `ca_file`,
`verify_tls`, `format` and `timezone`. The matching environment variables
are `DARPA_SPILL_CLIENT_<KEY>`.

## 5. Testing

```console
$ venv/bin/python -m pytest -q                        # Python suite
$ ctest --test-dir build --output-on-failure          # CppUnit suite
```

* `tests/conftest.py::live_server` runs a real uvicorn server on a free port
  over a populated archive. The client, C++ CLI and equivalence tests use it.
  The C++ tests skip themselves if `build/darpa-spill-client-cpp` is absent.
* `tests/test_packaging.py` enforces the repository conventions: the
  requirements files mirror `pyproject.toml`, every console script and
  platform script has a man page, the man pages carry the current version,
  there are `.sh` and `.ps1` scripts, and nothing depends on `/proc` or
  `setsid`.
* Keep Python 3.9 compatible, and check with a 3.9 venv:
  `uv venv -p 3.9 /tmp/py39 && VIRTUAL_ENV=/tmp/py39 uv pip install -e ../nova-time-decoder -e '.[test]'`.
* The version is in `src/darpa_spillserver/__init__.py`, `pyproject.toml`,
  `CMakeLists.txt` and every man page's `.TH` line. Tags are
  `vMAJOR.MINOR.PATCH`.
