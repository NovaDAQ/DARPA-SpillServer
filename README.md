# DARPA Spill Information Server

Serves **NOvA accelerator event timestamps** collected from a TDU, as a
queryable table over HTTP and from the command line.

The server polls the embedded web server running on a NOvA Timing Distribution
Unit, archives the decoded accelerator events it reports, and answers questions
like *"every `$74` NuMI spill between 1 July 2026 and today"* or *"the 1 Hz
`$8F` events from 09:15 until 11:34 today"* — as CSV or JSON, with every
timestamp rendered in NOvA base time, UNIX, UTC and GPS at once.

## Quick start

```bash
./bootstrap.sh
source venv/bin/activate
darpa-spill-server -c config/spillserver.yaml
```

Then <http://localhost:8080/> for the query page, or <http://localhost:8080/docs>
for the generated API reference.

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
```

Quote `$74` in a shell — unquoted it expands to nothing. `0x74` and `74` also work.

## Read this before deploying

**The TDU cannot currently supply a complete event history.** Its two event
routes, `/tcr_status` and `/onehz_status`, each return only the single newest
record — and return the *same* record, because `DumpSpillHistory --last`
ignores its own type filters. Accelerator events arrive at roughly 6–15 Hz, so
polling captures a fraction of them. Measured on a live TDU:

```
captured event numbers: 20230, 20235, 20240, 20245, 20250, 20255, ...
span: 105 hardware events -> 21 captured (20%)
```

Separately, `TCRMonitor` writes to shared memory only inside
`if (evt == 0x041B)`, so **`$74` and `$8F` are decoded and then discarded**, and
the record it stores keeps only a decoded type — `$1D` cannot be told from
`$1F`, nor `$A9` from `$AD`.

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
- **Every timescale in every row** — NOvA ticks, UNIX, UTC and GPS, so a saved
  table never sends the reader back to the server to convert.
- **CSV and JSON**, both streamed, so a query spanning months does not have to
  fit in memory. CSV carries the query and any caveat as comment lines.
- **Configurable from a YAML file, the environment, or the command line**, with
  unknown keys rejected rather than ignored.
- **OIDC hooks ready for Fermilab SSO** — off by default, as specified;
  enabling it is a config change, not a code change.
- **Man pages** for both commands and the library.

## Documentation

| | |
|---|---|
| [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) | from checkout to first query, including the Python 3.9+ problem on the gateways |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | every setting, and how the four layers merge |
| [docs/API.md](docs/API.md) | endpoints, parameters, signals, output columns |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | systemd, reverse proxy, backups, enabling SSO |
| [contrib/tduweb/README.md](contrib/tduweb/README.md) | the TDU-side changes and what each unlocks |

Man pages: `man -l man/darpa-spill-server.1`, `man -l man/darpa-spill-query.1`,
`man -l man/darpa_spillserver.3`.

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

## Layout

```
src/darpa_spillserver/
    signals.py      accelerator signal registry; the hardware's decode rules
    novatime.py     time expressions in, every timescale out
    storage.py      the SQLite archive
    tdu_client.py   async HTTP client for the TDU
    poller.py       the background ingest loop
    formats.py      streaming CSV and JSON
    config.py       defaults, YAML, environment, command line
    auth.py         OIDC hooks; no-op by default
    api.py, app.py  the HTTP surface
    cli.py          darpa-spill-server
    query_cli.py    darpa-spill-query
    web/            browser UI
config/             default and Near Detector configurations
contrib/tduweb/     upstream changes needed on the TDU
docs/               guides
man/                man pages
tests/              237 tests
```

## Tests

```console
$ python -m pytest -q
237 passed
```

The time conversions are checked against values read live from
`tdu-near-master-ppc-01`, and the signal decode table is transcribed from
`NssSpillInfo::getSpillTypeFromEvent`, so a divergence from the DAQ shows up as
a test failure rather than as a mislabelled event months later.

## Related packages

| | |
|---|---|
| [TDUWeb](https://github.com/NovaDAQ/TDUWeb) | the embedded server on the TDU that this polls |
| [SHM_Utilities](https://github.com/NovaDAQ/SHM_Utilities) | `DumpSpillHistory`, which reads the TDU's shared memory |
| [NovaSpillServer](https://github.com/NovaDAQ/NovaSpillServer) | `TCRMonitor` and the `SpillType` decode rules |
| [TDUControl](https://github.com/NovaDAQ/TDUControl), [TDUUtilities](https://github.com/NovaDAQ/TDUUtilities) | TDU register access and operational scripts |
| [nova-time-decoder](https://github.com/normanajn/nova-time-decoder) | NOvA ↔ UNIX ↔ GPS timestamp conversion |

## License

MIT.
