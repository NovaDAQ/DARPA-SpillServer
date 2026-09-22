# API reference

Base path `/api`. The generated OpenAPI reference is served live at `/docs`,
and the raw schema at `/openapi.json`.

Every endpoint is a `GET`. The server never modifies the accelerator data it
serves, so there is nothing to `POST`.

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

## Endpoints

### `GET /api/events`

The main query.

| Parameter | Default | Meaning |
|---|---|---|
| `start` | beginning of time | range start, inclusive |
| `end` | now | range end, exclusive |
| `signal` | all | signal such as `$74`; repeatable, or comma-separated |
| `type` | all | decoded type by name or number; repeatable |
| `format` | `json` | `json` or `csv` |
| `limit` | `query.default_limit` | maximum rows |
| `offset` | `0` | rows to skip |
| `order` | `asc` | `asc` or `desc` by time |
| `tz` | `query.timezone` | zone for times carrying none |
| `columns` | all | comma-separated subset (CSV only) |

Signals and types union together: `signal=$74&signal=$8f` returns both.

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
      "source": "spill_history"
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

The newest archived event, optionally narrowed by `signal`.

### `GET /api/signals`, `GET /api/types`

The registries above, as data. `/api/types` flags each type that maps to more
than one signal.

### `GET /api/time/convert`, `GET /api/time/help`

`?t=<any time expression>` renders one instant in NOvA ticks, UNIX, UTC and
GPS. Useful for checking a query means what you think before running it.

### `GET /api/status`

Configuration in use, archive extent and counts, and ingest health. Readable
without authentication so monitoring can scrape it; carries no event data and
no secrets.

The `ingest.degraded` flag is the one to watch — true means the TDU has no
history route and the archive is a sample rather than a complete record.

### `GET /api/health`

Liveness only. Never requires authentication.

## Output columns

| Column | Meaning |
|---|---|
| `nova_time` | NOvA base time, 64 MHz ticks since 01-Jan-2010 UTC |
| `utc` | ISO 8601 UTC, nanosecond precision |
| `utc_string` | the DAQ's own rendering, picosecond precision |
| `unix_sec`, `unix_nsec` | UNIX time |
| `gps_seconds`, `gps_nsec` | GPS time, continuous (no leap seconds) |
| `gps_week`, `gps_tow` | the same instant as GPS week and time-of-week |
| `spill_type`, `spill_type_name` | decoded type, as stored by the hardware |
| `signal`, `signal_name` | raw signal, when preserved; empty otherwise |
| `event_number` | the TDU's sequence counter |
| `delta` | ticks since the previous event |
| `pps_offset` | ticks past the 1 s boundary |
| `source` | which TDU route supplied the record |

Every row carries all four timescales rather than making you choose, because a
saved table that omitted GPS would be useless to the next person who needed it.

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
