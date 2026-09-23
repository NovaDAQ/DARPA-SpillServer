# API reference

Base path `/api`. The generated OpenAPI reference is served live at `/docs`,
and the raw schema at `/openapi.json`.

Every query endpoint is a `GET`, and the server never modifies the
accelerator data it serves. The only writes are to its own source
configuration (`PATCH /api/sources/{name}` and its reset), and they need the
admin token.

## Time expressions

Wherever a parameter takes a time, all of these work:

| Example | Means |
|---|---|
| `now` | the current instant |
| `today` / `yesterday` | midnight starting that day |
| `-2h`, `-30m`, `-7d`, `-1w` | relative to now |
| `09:15`, `11:34:22` | that clock time today |
| `2026-07-01` | that calendar date |
| `Jul 1, 2026`, `01-Jul-2026`, `07/01/2026` | the same date, other spellings |
| `2026-07-01T09:15:00Z` | ISO 8601; an explicit offset always wins |
| `nova:33778458638680249` | NOvA 64 MHz ticks |
| `unix:1790092413` | UNIX seconds |
| `gps:1025136016`, `gps:1695:259216` | GPS seconds, or week:time-of-week |

Live list: `GET /api/time/help`.

**Ranges are half-open** — `start <= t < end` — so adjacent queries tile
without returning an event twice.

**A date-only `end` covers that whole day.** `start=2026-07-01&end=2026-07-01`
returns everything on 1 July, not nothing.

**Unqualified times use the server's configured zone** (`query.timezone`, UTC
by default). Override per request with `tz=America/Chicago`. A value carrying
its own offset ignores both.

## Signals

Give a signal as `$74`, `74`, `0x74` (any case), or by name (`numi`,
`one-hertz`). Quote `$74` in a shell.

| Hex | Carrier | Name | Decoded type | Meaning |
|---|---|---|---|---|
| `$74` | MIBS | `numi` | `NUMI` | 120 GeV proton extraction into NuMI |
| `$1B` | BNB | `bnb` | `BNB` | parasitic beam inhibit; the TCR reference |
| `$1D` | TCLK | `booster-reset` | `BNB_TCLK` | booster reset for a MiniBooNE cycle |
| `$1F` | TCLK | `booster-extraction` | `BNB_TCLK` | booster extraction sync (BES) |
| `$AD` | TCLK | `numi-mixed-mode` | `NUMI_TCLK` | NuMI reset, mixed-mode ramp |
| `$A9` | TCLK | `numi-tclk` | `NUMI_TCLK` | TCLK reflection of MIBS `$74` |
| `$8F` | TCLK | `one-hertz` | `ACCEL_ONE_HZ_TCLK` | 1 Hz accelerator pulser |
| `$00` | TCLK | `super-cycle` | `SUPER_CYCLE` | super cycle / master clock reset |
| `$A4` | TCLK | `numi-sample-trig` | `NUMI_SAMPLE_TRIG` | NuMI cycle sample trigger |
| `$A5` | TCLK | `numi-reset` | `NUMI_RESET` | NuMI reset for beam |
| `$39` | TCLK | `testbeam-spill` | `TB_SPILL` | test-beam slow extraction |

Live list: `GET /api/signals`.

### Why some rows have an empty `signal`

The TDU stores a **decoded type**, not the raw 16-bit event word, and the
mapping is many-to-one: `$1D` and `$1F` both become `BNB_TCLK`, `$A9` and `$AD`
both become `NUMI_TCLK`. For records ingested without a raw word, the exact
signal is unrecoverable, so the `signal` column is left empty rather than
filled with a guess. Any response whose selection is affected carries a warning
saying which signals cannot be told apart.

Querying by signal matches such records through their decoded type as well, so
a `$74` query still finds NuMI events stored before raw words were available.

## Sources

The server records from one or more TDUs, and tags every event with the name
of the source it came from (`tdu.sources` in the configuration). Every query
endpoint takes `source=`, which is repeatable or comma-separated, and
restricts the result to those sources. Leave it out for all of them.

```console
$ curl -s 'http://localhost:8080/api/events?signal=$8f&start=-1h&source=tdu-near-master-ppc-02'
```

Two TDUs that see the same accelerator event each record it, so an
all-sources query returns it once per source. An unknown source name is a
`400` whose hint lists the known ones. A source that was removed from the
configuration can still be queried by name while its events remain in the
archive.

## Endpoints

### `GET /api/events`

The main query.

| Parameter | Default | Meaning |
|---|---|---|
| `start` | beginning of time | range start, inclusive |
| `end` | now | range end, exclusive |
| `signal` | all | signal such as `$74`; repeatable, or comma-separated |
| `type` | all | decoded type by name or number; repeatable |
| `source` | all | source (TDU) name; repeatable, or comma-separated |
| `format` | `json` | `json` or `csv` |
| `limit` | `query.default_limit` | maximum rows |
| `offset` | `0` | rows to skip |
| `order` | `asc` | `asc` or `desc` by time |
| `tz` | `query.timezone` | zone for times carrying none |
| `columns` | all | comma-separated subset (CSV only) |

Signals and types union together: `signal=$74&signal=$8f` returns both.
Sources union among themselves, and narrow the rest of the selection:
`signal=$8f&source=a&source=b` returns `$8F` events from `a` and from `b`.

`X-Total-Count` carries the number of matching rows regardless of paging.

```console
$ curl -s 'http://localhost:8080/api/events?signal=$8f&start=09:15&end=11:34'
```

```json
{
  "meta": {
    "generated": "2026-09-22T16:25:58.326143+00:00",
    "range": {
      "start": {"nova": 33778..., "utc": "2026-09-22T09:15:00.000000000Z", ...},
      "end":   {"nova": 33778..., "utc": "2026-09-22T11:34:00.000000000Z", ...},
      "note": "half-open: start <= t < end"
    },
    "selection": ["$8F"],
    "sources": ["(all sources)"],
    "timezone": "UTC",
    "total": 8340,
    "returned": 8340,
    "truncated": false
  },
  "events": [
    {
      "nova_time": 33778582346078940,
      "utc": "2026-09-22T16:25:46.157483437Z",
      "utc_string": "2026-Sep-22 16:25:46.157483437500 UTC",
      "unix_sec": 1790094346, "unix_nsec": 157483437,
      "gps_seconds": 1474129564, "gps_nsec": 157483437,
      "gps_week": 2437, "gps_tow": 231964,
      "spill_type": 4, "spill_type_name": "ACCEL_ONE_HZ_TCLK",
      "signal": "$8F", "signal_name": "one-hertz",
      "event_number": 20230, "delta": 4264849, "pps_offset": 10078940,
      "source": "tdu-near-master-ppc-01", "route": "spill_history"
    }
  ]
}
```

With `format=csv` the same metadata is written as `#` comment lines above the
header, so a saved file explains the query that produced it.

### `GET /api/export`

Same filters as `/api/events`, but streams the whole range with no paging,
bounded only by `query.max_export_rows`. Use it for bulk extraction; use
`/api/events` for interactive queries.

### `GET /api/latest`

The newest archived event, optionally narrowed by `signal` and `source`.

### `GET /api/sources`

Every configured source: its name, current `base_url` and `enabled` state,
what the configuration file says (`configured`), whether a runtime change is
in force (`overridden`, with `updated_at` and `updated_by`), its event count,
and its ingest health. Readable without authentication, like `/api/status`.
`admin.enabled` says whether this server accepts changes at all.

### `PATCH /api/sources/{name}`

Change a source while the server runs. Needs the admin token in the
`X-Admin-Token` header, or, with SSO on, a signed-in member of
`admin.allowed_groups`.

```console
$ curl -s -X PATCH -H "X-Admin-Token: $(cat admin_token)" \
       -H 'Content-Type: application/json' \
       -d '{"enabled": false}' \
       http://localhost:8080/api/sources/tdu-near-master-ppc-03

$ curl -s -X PATCH -H "X-Admin-Token: $(cat admin_token)" \
       -H 'Content-Type: application/json' \
       -d '{"base_url": "http://tdu-near-master-ppc-04:8080"}' \
       http://localhost:8080/api/sources/tdu-near-master-ppc-03
```

Either field may be omitted. The change applies immediately: disabling a source
stops its poller, and a new URL starts a fresh poller that resumes from the
source's newest archived event. It is saved in the archive, survives restarts,
and wins over the configuration file. A source keeps its name when its URL
changes, so its history stays in one place.

`401` means no credentials were sent, and `403` means they were wrong or that
changes are disabled on this server. The error body says which. A malformed
URL, or one another source already uses, is a `400`.

### `POST /api/sources/{name}/reset`

Discard runtime changes to a source and return it to the configuration file's
settings. Same credentials as `PATCH`.

### `GET /api/admin/check`

`200` if the caller's credentials would allow a change, otherwise the same
`401`/`403` a change would get. The `/config` page uses this to unlock editing.

### `GET /api/signals`, `GET /api/types`

The registries above, as data. `/api/types` flags each type that maps to more
than one signal.

### `GET /api/time/convert`, `GET /api/time/help`

`?t=<any time expression>` renders one instant in NOvA ticks, UNIX, UTC and
GPS. Useful for checking a query means what you think before running it.

### `GET /api/status`

Configuration in use, archive extent and counts (overall, `by_type` and
`by_source`), and ingest health. Readable without authentication so monitoring
can scrape it; carries no event data and no secrets.

`ingest` summarises every enabled source, and `ingest.sources` gives each
one's own status:

* `ingest.running`: every enabled source is being polled.
* `ingest.degraded`: at least one source has no history route, so its part of
  the archive is a sample rather than a complete record.
  `ingest.degraded_sources` names them.
* `ingest.consecutive_errors`: the worst of any source. When it is non-zero,
  `ingest.last_error` says which source is failing and how.

### `GET /api/health`

Liveness only. Never requires authentication.

## Output columns

| Column | Meaning |
|---|---|
| `nova_time` | NOvA base time, 64 MHz ticks since 01-Jan-2010 UTC |
| `utc` | ISO 8601 UTC, nanosecond precision |
| `utc_string` | the DAQ's own rendering, picosecond precision |
| `unix_sec`, `unix_nsec` | UNIX time |
| `gps` | GPS seconds at full precision, e.g. `1474127631.229378890625` |
| `gps_seconds`, `gps_nsec` | GPS time as whole seconds plus a nanosecond remainder |
| `gps_psec` | the sub-second remainder in picoseconds, exact |
| `gps_week`, `gps_tow` | the same instant as GPS week and whole-second time-of-week |
| `gps_tow_exact` | time-of-week at full precision, e.g. `230031.229378890625` |
| `spill_type`, `spill_type_name` | decoded type, as stored by the hardware |
| `signal`, `signal_name` | raw signal, when preserved; empty otherwise |
| `event_number` | the TDU's sequence counter |
| `delta` | ticks since the previous event |
| `pps_offset` | ticks past the 1 s boundary |
| `source` | name of the TDU the event was read from |
| `route` | which TDU route supplied the record, `spill_history` or `tcr_status` |

Every row carries all four timescales rather than making you choose, because a
saved table that omitted GPS would be useless to the next person who needed it.

### GPS precision

Prefer **`gps`** over `gps_seconds` / `gps_nsec`. A NOvA tick is 15.625 ns, so
a nanosecond field cannot land on a tick boundary and truncates 625 ps of every
one. `gps` is a decimal string good to the picosecond, and `1e12 / 64e6` is
exactly 15625, so every tick maps to a whole number of picoseconds with nothing
lost. Its fractional part is identical to the one in `utc_string`, which is
what the DAQ's own tooling prints.

`gps_nsec` is retained because it is what `nova_time_decoder` reports, and
`gps_psec` carries the same exact remainder as an integer for callers that
would rather not parse a decimal.

## Backfilling from the ring buffer

Ingest only moves forward: `ingest.backfill` applies on a cold start and
nowhere else, so once a source has rows the poller never reaches back past
them. `darpa-spill-backfill` fills that gap.

```bash
darpa-spill-backfill -c config/spillserver.yaml            # everything the ring holds
darpa-spill-backfill --start=-6h --end=now                 # a specific range
darpa-spill-backfill --source tdu-near-master-ppc-01 -n    # dry run, one source
```

Write relative times with an equals sign — `--start=-6h`, not `--start -6h` —
or argparse reads the value as an option.

It walks the range in windows (`--window`, 300 s by default) rather than paging
by count, because `/spill_history` applies its `limit` *after* collecting
everything at or after `since`: a wide `since` with a small `limit` still makes
the TDU produce the rest of the ring. Bounding both ends is what keeps each
request proportional to the window.

Inserts are idempotent, so re-running over a covered range costs time and
changes nothing. It is safe to run against a live archive while the poller
is running.

### What "everything the ring holds" means

Measured on `tdu-near-master-ppc-01`, 2026-09-23: a 32 MB segment of 2,097,152
slots, holding about **120 hours** of events. That is more than the current
`TCRMonitor` run — that process resets its event counter on start but does not
clear the segment, so slots beyond the insert point still hold the previous
run's events. Their timestamps are genuine, and the data is real history worth
keeping; only the `Number` sequence restarts, so **`event_number` is not
continuous across that boundary** and a completeness check spanning it will
under-report.

Budget the time. Cost is dominated by `DumpSpillHistory` formatting each event,
at roughly 5.8 ms apiece plus a second per request, so a full ring is several
hours of TDU work. A 30-minute range took 56 s. Narrow ranges are cheap;
prefer them unless you genuinely want the lot.

## Errors

Failures return a JSON body with an `error` and, where there is one, a `hint`
naming the endpoint that lists valid values:

```json
{"detail": {
  "error": "signal $99 is not decoded by the NOvA DAQ; known signals are $74, $1B, ...",
  "hint": "See /api/signals for every accepted signal."
}}
```

| Status | Cause |
|---|---|
| 400 | a bad time, signal, type, column, timezone, or an over-large `limit` |
| 401 / 403 | authentication required or group not permitted (only when SSO is on) |
| 404 | no such endpoint, or no matching event for `/api/latest` |
| 422 | a parameter failed schema validation, e.g. `format=xml` |
