# DARPA Spill Information Server

Serves **NOvA accelerator event timestamps** collected from one or more TDUs,
as a queryable table over HTTP and from the command line.

The server polls the embedded web server running on each configured NOvA
Timing Distribution Unit, archives the decoded accelerator events they report
tagged with the TDU each came from, and answers questions
like *"every `$74` NuMI spill between 1 July 2026 and today"* or *"the 1 Hz
`$8F` events from 09:15 until 11:34 today"* — as CSV or JSON, with every
timestamp rendered in NOvA base time, UNIX, UTC and GPS at once.

## Quick start

Linux or macOS:

```bash
./bootstrap.sh                        # venv, Python package, and the C/C++ library if CMake is present
source venv/bin/activate
darpa-spill-server -c config/spillserver.yaml
```

Windows 11 (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\bootstrap.ps1
.\venv\Scripts\Activate.ps1
darpa-spill-server -c config\spillserver.yaml
```

To run it in the background instead, detached from the terminal, use
`./start-darpa-spillserver.sh` (same options, e.g.
`./start-darpa-spillserver.sh -c config/spillserver-near.yaml`) and
`./stop-darpa-spillserver.sh`; on Windows, `start-darpa-spillserver.ps1` and
`stop-darpa-spillserver.ps1`. With no `-c`, it uses a local `./spillserver.yaml`
if one exists, and `config/spillserver.yaml` otherwise. The PID file and output
log go in `run/`. [docs/INSTALL.md](docs/INSTALL.md) has the per-platform
details.

Then <http://localhost:8080/> for the query page, <http://localhost:8080/api>
for the API reference, <http://localhost:8080/about> for the version and
dependencies, and <http://localhost:8080/sitemap> for every page and route.

```console
$ curl -s 'http://localhost:8080/api/events?signal=$8f&start=09:15&end=11:34&format=csv'
# DARPA Spill Information Server
# range: 2026-09-22T09:15:00.000000000Z .. 2026-09-22T11:34:00.000000000Z (half-open: start <= t < end)
# selection: $8F
nova_time,utc,utc_string,unix_sec,unix_nsec,gps_seconds,...,signal,signal_name,...
33778582346078940,2026-09-22T16:25:46.157483437Z,2026-Sep-22 16:25:46.157483437500 UTC,...,$8F,one-hertz,...
```

Or without a browser:

```console
$ darpa-spill-query --signal '$74' --start 2026-07-01 --end today
$ darpa-spill-query --signal '$8f' --start 09:15 --end 11:34 -f csv -o spills.csv
$ darpa-spill-query --last
$ darpa-spill-query --source tdu-near-master-ppc-02 --signal '$8f' --start -1h
```

Quote `$74` in a shell — unquoted it expands to nothing. `0x74` and `74` also work.

`darpa-spill-query` reads an archive file directly. From any other host, use
the HTTP client instead. It comes in Python and C++ versions that give
identical output for the same invocation:

```console
$ darpa-spill-client -u http://novadaq-near-gateway-01.fnal.gov:8080 events --start today --signal '$74'
$ darpa-spill-client -f csv export --start 2026-07-01 --signal '$74' -o numi.csv
$ darpa-spill-client --admin-token-file ~/.spill-token source disable tdu-near-master-ppc-02
$ build/darpa-spill-client-cpp -u http://localhost:8080 latest --signal '$8f'
```

Both are built on client libraries — `darpa_spillserver.client` for Python and
`libdarpa_spill_client` for C++ (with a C ABI) — which DAQ code can link
against directly. See [docs/CLIENT.md](docs/CLIENT.md) and
[docs/CPP_LIBRARY.md](docs/CPP_LIBRARY.md).

## Read this before deploying

**The TDU cannot currently supply a complete event history.** Its two event
routes, `/tcr_status` and `/onehz_status`, each return only the single newest
record — and return the *same* record, because `DumpSpillHistory --last`
ignores its own type filters. Accelerator events arrive at roughly 6–15 Hz, so
polling captures a fraction of them. Measured on a live TDU:

```
window                         : 4h 11m, 2026-09-22
hardware events (event_number) : 73,904   (27,996 .. 101,899)
captured                       :  8,700
capture fraction               :    11.8%
```

Separately, the record the hardware stores keeps only a decoded *type*, not the
raw event word — so `$1D` cannot be told from `$1F`, nor `$A9` from `$AD`.

What **is** recorded turns out to be wider than the checked-out source suggests.
In `NovaSpillServer/cxx/src/TCRMonitor.cc` the `memcpy` into shared memory sits
inside `if (evt == 0x041B)`, which would store the TCR reference and nothing
else. The deployed binary evidently differs. A four-hour archive collected on
2026-09-22 contains:

| Signal | Type | Captured | Implied rate |
|---|---|---:|---:|
| `$1D` / `$1F` | `BNB_TCLK` | 7233 | 4.07 /s |
| `$8F` | `ACCEL_ONE_HZ_TCLK` | 1448 | 0.81 /s |
| `$00` | `SUPER_CYCLE` | 10 | 0.01 /s |
| `$1B` | `BNB` | 9 | 0.01 /s |

`$8F` at 0.81 /s is the 1 Hz pulser showing up almost exactly as it should once
the ~12% capture rate is accounted for, so it is genuinely being recorded.

No NuMI signal (`$74`, `$A9`, `$AD`, `$A4`, `$A5`) appeared in that window. That
is **not** evidence they are unrecorded — NuMI beam may simply not have been
running. Whether `$74` reaches shared memory is still unverified, and needs a
window with beam to settle.

The server handles all of this honestly rather than papering over it: it detects
the limitation, reports `ingest.degraded` in `/api/status`, attaches a warning
to every affected response and CSV file, and leaves the `signal` column empty
rather than guessing which of two signals produced a record.

[`contrib/tduweb/`](contrib/tduweb/) contains the upstream changes that fix
this, ordered by risk. The first — adding a `/spill_history` route — is
additive, touches no DAQ process, and removes the event loss entirely.

## Features

- **Time ranges the way people write them** — `2026-07-01`, `Jul 1, 2026`,
  `09:15`, `-2h`, `today`, `nova:337784...`, `gps:1695:259216`. Ranges are
  half-open so adjacent queries tile; a date-only end covers that whole day.
- **Signals in operator notation** — `$74`, `$8f`, `0x74`, or by name.
- **Several TDUs at once** — every event is tagged with its source, and any
  query can be narrowed to one source or several, or cover them all.
- **Sources editable while running** — the `/config` page enables, disables
  and re-points sources without a restart, guarded by an admin token.
- **Every timescale in every row** — NOvA ticks, UNIX, UTC and GPS, so a saved
  table never sends the reader back to the server to convert.
- **CSV and JSON**, both streamed, so a query spanning months does not have to
  fit in memory. CSV carries the query and any caveat as comment lines.
- **Configurable from a YAML file, a `.env` file, the environment, or the
  command line** (highest wins: command line > environment > `.env` > YAML >
  defaults), with unknown keys rejected rather than ignored.
- **Client libraries and CLIs in Python and C/C++**, held to one written
  contract and checked against each other by the test suite.
- **Linux, macOS and Windows 11**: bootstrap, start and stop scripts for each,
  and a CMake build with presets for each.
- **OIDC hooks ready for Fermilab SSO** — off by default, as specified;
  enabling it is a config change, not a code change.
- **Man pages** for every command, script, helper and library.
- **Reference pages served by the server itself**: `/api` (generated from the
  OpenAPI schema), `/about` (version and dependencies), and `/sitemap` and
  `/sitemap.xml`.

## Documentation

| | |
|---|---|
| [docs/INSTALL.md](docs/INSTALL.md) | install, bootstrap, build and run on Linux, macOS and Windows 11 |
| [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) | from checkout to first query, including the Python 3.9+ problem on the gateways |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | every setting, and how the five layers merge |
| [docs/CLIENT.md](docs/CLIENT.md) | the client libraries and `darpa-spill-client`: the contract both languages implement |
| [docs/CPP_LIBRARY.md](docs/CPP_LIBRARY.md) | building, installing and linking the C/C++ library |
| [docs/API.md](docs/API.md) | endpoints, parameters, signals, output columns |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | systemd, reverse proxy, backups, enabling SSO |
| [contrib/tduweb/README.md](contrib/tduweb/README.md) | the TDU-side changes and what each unlocks |

Man pages are in `man/`; read any of them with `man -l man/<page>` or
`man ./man/<page>` on macOS:

| Page | Covers |
|---|---|
| `darpa-spill-server(1)` | the server |
| `darpa-spill-query(1)` | reading an archive file directly |
| `darpa-spill-backfill(1)` | filling the archive from a TDU's ring buffer |
| `darpa-spill-client(1)`, `darpa-spill-client-cpp(1)` | the HTTP clients |
| `darpa-spillserver-bootstrap(1)`, `start-darpa-spillserver(1)`, `stop-darpa-spillserver(1)` | the platform scripts |
| `tdu_webserver(1)`, `tduweb-selftest(1)`, `tduweb-check-python25(1)`, `tduweb-run-as-python25(1)` | the TDU-side helpers in `contrib/tduweb/` |
| `darpa_spillserver(3)` | the Python package, including the client |
| `darpa_spill_client(3)` | the C++ and C client library |

## Requirements

- **Python 3.9+.** The NOvA gateway nodes ship Python 3.6; `bootstrap.sh` finds
  a newer interpreter, including a private one under `~/.local/opt`.
  [GETTING_STARTED](docs/GETTING_STARTED.md#1-get-a-python-39-interpreter) shows
  three ways to get one, one of which needs no root.
- **[nova-time-decoder](https://github.com/normanajn/nova-time-decoder)**, a
  sibling NOvA DAQ package. `bootstrap.sh` installs it from a neighbouring
  checkout if there is one.
- **Network access to the TDU**, which lives on the NOvA DAQ network. The
  gateway nodes are on it.
- **For the C/C++ library only:** CMake 3.16+, a C++17 compiler, Boost 1.75+
  (Beast, Asio, JSON, Program_options), yaml-cpp, and optionally OpenSSL (for
  https) and CppUnit (for tests). The server does not need any of them.

## Layout

```
src/darpa_spillserver/
    signals.py      accelerator signal registry; the hardware's decode rules
    novatime.py     time expressions in, every timescale out
    storage.py      the SQLite archive
    tdu_client.py   async HTTP client for the TDU
    poller.py       the background ingest loop
    formats.py      streaming CSV and JSON
    config.py       defaults, YAML, .env, environment, command line
    auth.py         OIDC hooks; no-op by default
    api.py, app.py  the HTTP surface
    pages.py        /about, /api and /sitemap, generated from the running app
    cli.py          darpa-spill-server
    query_cli.py    darpa-spill-query
    backfill.py     darpa-spill-backfill
    client.py       Python client library for the HTTP API
    client_cli.py   darpa-spill-client
    web/            browser UI (templates, CSS, masthead logo)
src/include/darpa_spill/  C++ (client.hpp) and C (client.h) public headers
src/cpp/            libdarpa_spill_client, and cli/ for darpa-spill-client-cpp
examples/c/         a C program linking the C ABI
CMakeLists.txt, CMakePresets.json, vcpkg.json, cmake/
                    C/C++ build: presets for Linux, macOS and Windows (MSVC + vcpkg)
config/             server and client configurations, and an example .env
contrib/tduweb/     upstream changes needed on the TDU
docs/               guides
man/                man pages
tests/              pytest suite (453 tests); tests/cpp/ holds the CppUnit tests (39)
bootstrap.{sh,ps1}, start-darpa-spillserver.{sh,ps1}, stop-darpa-spillserver.{sh,ps1}
                    setup and background start/stop, for POSIX and Windows
novadaq-logo.png    full-resolution masthead artwork; the served copy under
                    src/darpa_spillserver/web/static/ is scaled from it
```

## Tests

```console
$ python -m pytest -q
453 passed
```

The time conversions are checked against values read live from
`tdu-near-master-ppc-01`, and the signal decode table is transcribed from
`NssSpillInfo::getSpillTypeFromEvent`, so a divergence from the DAQ shows up as
a test failure rather than as a mislabelled event months later.

## Reporting bugs

Open an issue at
<https://github.com/NovaDAQ/DARPA-SpillServer/issues>. The *report a bug* link
in the query page's masthead and footer opens a prefilled issue carrying the
server version and the TDU it was polling, which is what a report usually
lacks.

## Related packages

| | |
|---|---|
| [TDUWeb](https://github.com/NovaDAQ/TDUWeb) | the embedded server on the TDU that this polls |
| [SHM_Utilities](https://github.com/NovaDAQ/SHM_Utilities) | `DumpSpillHistory`, which reads the TDU's shared memory |
| [NovaSpillServer](https://github.com/NovaDAQ/NovaSpillServer) | `TCRMonitor` and the `SpillType` decode rules |
| [TDUControl](https://github.com/NovaDAQ/TDUControl), [TDUUtilities](https://github.com/NovaDAQ/TDUUtilities) | TDU register access and operational scripts |
| [nova-time-decoder](https://github.com/normanajn/nova-time-decoder) | NOvA ↔ UNIX ↔ GPS timestamp conversion |

## Copyright

Copyright 2010-2026 Andrew Norman for Fermi Forward Discovery Group LLC.
All rights reserved.
