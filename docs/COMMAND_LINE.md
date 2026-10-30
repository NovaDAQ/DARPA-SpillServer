# Command-line tools

Four commands work with the spill archive and its exports from a terminal:

| Command | Use it to | Row limit | Output |
|---|---|---|---|
| `darpa-spill-query` | answer a question: what happened, when, how many | yes (`--limit`, server `query.max_limit`) | table, CSV or JSON to stdout or `-o FILE` |
| `darpa-spill-dump` | take a copy of a range: a day, a week, the archive | none | CSV and/or JSON files, optionally one per day or hour |
| `darpa-spill-shift` | blur the timestamps in dump files, reversibly, with a secret token | none | `.shifted` / `.restored` copies of the files |
| `darpa-spill-publish` | run the gateway's hourly/daily dump → shift → upload pipeline from cron | none | files under `raw/` and `shifted/`, uploads to FDP, a status page |

`darpa-spill-query` and `darpa-spill-dump` read either the **local SQLite
archive** (on the gateway node) or a **running server** over HTTPS, accept the
same time expressions and the same signal / type / source selection, and
produce the same columns as the web API (see
[API.md](API.md#output-columns)). `darpa-spill-shift` reads no archive: it
works on files `darpa-spill-dump` wrote. `darpa-spill-publish` only drives
the other two, always against a running server (`darpa-spill-dump -s
server.url`); it has no local-archive mode.

Another command, `darpa-spill-backfill`, writes *into* the archive from a
TDU's ring buffer; it is covered in [API.md](API.md). Without `--start`,
it scans contiguous five-minute windows from the oldest end of a bounded
lookback and stops at the first event. `--lookback SECONDS` defaults to 30 days
(up to 8,640 requests for a quiet or recently started ring). Empty gaps do not
end discovery, and network errors abort it. Use an explicit `--start` to skip
these probes or a shorter lookback when the retained interval is known.
History older than the lookback is not discovered automatically.

Every example below was run against the Near Detector gateway in October 2026,
and the output shown is what it printed. Full option lists are in the man
pages: `man -l man/darpa-spill-query.1`, `man -l man/darpa-spill-dump.1`,
`man -l man/darpa-spill-shift.1`, `man -l man/darpa-spill-publish.1`.

Contents:

1. [Setup](#1-setup)
2. [Connecting to the data](#2-connecting-to-the-data)
3. [Writing times](#3-writing-times)
4. [Choosing events](#4-choosing-events)
5. [darpa-spill-query](#5-darpa-spill-query)
6. [darpa-spill-dump](#6-darpa-spill-dump)
7. [Splitting a dump into files by time](#7-splitting-a-dump-into-files-by-time)
8. [Recipes](#8-recipes)
9. [Reading the files](#9-reading-the-files)
10. [Troubleshooting](#10-troubleshooting)
11. [Blurring timestamps reversibly](#11-blurring-timestamps-reversibly)
12. [Publishing on the gateway: darpa-spill-publish](#12-publishing-on-the-gateway-darpa-spill-publish)

---

## 1. Setup

The commands are installed into the project's virtual environment by
`./bootstrap.sh`. Either activate it or call them by path:

```console
$ source venv/bin/activate          # then: darpa-spill-query ...
$ venv/bin/darpa-spill-query ...    # or this, without activating
```

The examples below assume the venv is active.

## 2. Connecting to the data

### Local archive

On the gateway node, with no source option, both commands read the archive
named by the configuration (`storage.path`):

```console
$ darpa-spill-query --last
$ darpa-spill-query -c config/spillserver-near.yaml --last     # a specific config
$ darpa-spill-dump  -d /data/spills/spills.db --start today --csv today.csv   # a specific file
```

Reading the file directly is the fastest path, and needs no server. Opening an
archive applies any pending schema migration, so the file must be writable the
first time after an upgrade.

### A running server

From anywhere that can reach the gateway, pass `-s URL`:

```console
$ darpa-spill-query -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --last
```

The gateway serves HTTPS with a **self-signed certificate**, which fails
verification by default:

```
error: cannot reach https://novadaq-near-gateway-01.fnal.gov:8443/api/latest: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate (_ssl.c:1000)
pass --ca-bundle FILE with the server's certificate, or --insecure to skip verification
```

Two ways round it:

| Option | Effect |
|---|---|
| `-k`, `--insecure` | skip verification entirely |
| `--ca-bundle FILE` | verify against the certificate in `FILE` (PEM) |

`--ca-bundle` is the better habit. Save the gateway's certificate once (and
again whenever it is renewed):

```console
$ openssl s_client -connect novadaq-near-gateway-01.fnal.gov:8443 </dev/null 2>/dev/null \
      | openssl x509 > ~/novadaq-gw.pem
$ darpa-spill-query -s https://novadaq-near-gateway-01.fnal.gov:8443 \
      --ca-bundle ~/novadaq-gw.pem --last
```

If SSO is enabled on the server, `darpa-spill-dump --token TOKEN` sends an
`Authorization: Bearer` ID token.

### Environment variables (darpa-spill-dump)

To avoid repeating the connection options:

```console
$ export DARPA_SPILL_DUMP_SERVER=https://novadaq-near-gateway-01.fnal.gov:8443
$ export DARPA_SPILL_DUMP_CA_BUNDLE=~/novadaq-gw.pem      # or DARPA_SPILL_DUMP_INSECURE=1
$ darpa-spill-dump --start today --csv today.csv
```

| Variable | Same as |
|---|---|
| `DARPA_SPILL_DUMP_SERVER` | `--server` |
| `DARPA_SPILL_DUMP_CA_BUNDLE` | `--ca-bundle` |
| `DARPA_SPILL_DUMP_INSECURE` | `--insecure` (`1`, `true`, `yes`) |
| `DARPA_SPILL_DUMP_TOKEN` | `--token` |
| `DARPA_SPILL_CONFIG`, `DARPA_SPILL_STORAGE_PATH` | `--config`, `--database` |

A command-line option always beats the environment.

## 3. Writing times

`--start` is inclusive and `--end` exclusive: the range is
**`start <= t < end`**, so consecutive ranges never share an event.

| You write | It means |
|---|---|
| `now` | the current instant |
| `today`, `yesterday` | midnight at the start of that day |
| `-2h`, `-30m`, `-7d`, `-1w` | that long before **now** (not before `--start`) |
| `09:15`, `11:34:22` | that clock time today |
| `2026-10-12` | midnight starting that date |
| `Oct 12, 2026` | the same |
| `2026-10-12T06:00`, `2026-10-12T06:00:00Z` | an ISO 8601 instant; a `Z` or offset overrides the zone |
| `nova:33885388992000000` | NOvA 64 MHz ticks |
| `unix:1791763200` | UNIX seconds (fraction allowed) |
| `gps:1475798418`, `gps:2440:86418` | GPS seconds, or GPS week:time-of-week |

Four rules catch people out:

**A date as `--end` covers that whole day.** `--start 2026-10-12 --end
2026-10-12` is all of 12 October; `--end 2026-10-13` would include all of the
13th as well. For an exact bound give a time: `--end 2026-10-13T00:00`.

```console
$ darpa-spill-dump ... --start 2026-10-12 --end 2026-10-12          # 1 day
$ darpa-spill-dump ... --start 2026-10-12 --end 2026-10-13          # 2 days
$ darpa-spill-dump ... --start 2026-10-12 --end 2026-10-13T00:00    # 1 day
$ darpa-spill-dump ... --start yesterday --end yesterday            # yesterday
```

**Write relative times with `=`.** `--start=-6h`, not `--start -6h`; otherwise
the option parser takes `-6h` for an option of its own.

**Times without a zone use the query timezone.** That is `--timezone` if
given, otherwise `query.timezone` from the configuration (local reads) or the
server's configured zone (`-s`), currently UTC on the gateway. Say
`--timezone America/Chicago` for Fermilab local time, or `--timezone UTC` to
be explicit.

**A backwards range is refused**, with the fix spelled out:

```
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --start=2026-10-11 --end=2026-10-10 --csv x.csv
darpa-spill-dump: error: the end of the range (2026-Oct-10 00:00:00.000000000000 UTC) is before its start
(2026-Oct-11 00:00:00.000000000000 UTC); are start and end swapped? A date given as the end includes that
whole day, so start 2026-10-10 with end 2026-10-11 covers both days.
```

## 4. Choosing events

| Option | Selects | Example |
|---|---|---|
| `--signal SIG` | an accelerator signal, by hex code or name | `--signal '$74'`, `--signal 8f`, `--signal numi` |
| `--type TYPE` | a decoded spill type, by name or number | `--type BNB_TCLK`, `--type 4` |
| `--source NAME` | events from one TDU | `--source tdu-near-master-ppc-01` |

Each may be repeated (`--type BNB --type BNB_TCLK`); repeats of one option
are OR-ed, different options AND-ed. `--source` also takes a comma-separated
list. Leaving an option out means "all".

**Quote `$` signals.** In a shell an unquoted `$74` is a variable and expands
to nothing. `'$74'`, `0x74`, `74` and `numi` all mean the same signal.

The signal registry:

```
$ darpa-spill-query --list-signals
HEX    CARRIER  NAME                 TYPE               DESCRIPTION
----------------------------------------------------------------------------------------------------
$74    MIBS     numi                 NUMI               MIBS $74 -- 120 GeV proton extraction from MI into NuMI
$1B    BNB      bnb                  BNB                BNB $1B -- parasitic beam inhibit; the TCR reference signal
$1D    TCLK     booster-reset        BNB_TCLK           TCLK $1D -- booster reset for a MiniBooNE beam cycle; expect $1F about 35 ms later
$1F    TCLK     booster-extraction   BNB_TCLK           TCLK $1F -- booster extraction sync (BES); expect beam about 320 us later
$AD    TCLK     numi-mixed-mode      NUMI_TCLK          TCLK $AD -- NuMI reset for a mixed-mode beamline ramp
$A9    TCLK     numi-tclk            NUMI_TCLK          TCLK $A9 -- TCLK reflection of MIBS $74; the one to use for NuMI
$8F    TCLK     one-hertz            ACCEL_ONE_HZ_TCLK  TCLK $8F -- 1 Hz accelerator pulser
$00    TCLK     super-cycle          SUPER_CYCLE        TCLK $00 -- super cycle and master clock reset
$A4    TCLK     numi-sample-trig     NUMI_SAMPLE_TRIG   TCLK $A4 -- NuMI cycle sample trigger; the reference for $A5
$A5    TCLK     numi-reset           NUMI_RESET         TCLK $A5 -- NuMI reset for beam
$39    TCLK     testbeam-spill       TB_SPILL           TCLK $39 -- start of test-beam slow extraction

Types marked below map to more than one signal, so a stored record
carrying only the type cannot be narrowed to one of them:
  BNB_TCLK           <- $1D or $1F
```

**Signal versus type.** Records ingested without the TDU's raw event word
carry only the decoded type; their `signal` column is empty. A `--signal`
query still finds them by matching their type, so `--signal '$1D'` returns
`$1D` events *and* type-only `BNB_TCLK` records, which may be `$1F`. When the
`signal` column is empty in your results, `--type` says what you are really
selecting. The Near Detector archive is currently all type-only.

What the Near Detector archive holds (totals from `/api/status`; rates
measured over 40 hours on 12-13 October 2026):

| Type | Events in archive | Recent rate, by TDU |
|---|---|---|
| `BNB_TCLK` | 7.3 M | about 4.3 per second, `ppc-01` |
| `BNB` | 6.3 M | 1 Hz each, `ppc-02` and `ppc-03` |
| `ACCEL_ONE_HZ_TCLK` | 2.1 M | 1 Hz, `ppc-01` |
| `SUPER_CYCLE` | 16 | 16 events in total, all on 22-23 September |

There are **no NuMI events** (`$74`, `NUMI`, `NUMI_TCLK`, ...) in the Near
Detector archive at present: a NuMI query is valid and returns zero rows. The
NuMI examples below show the syntax; substitute `--type BNB` or `--signal 8f`
to see data.

## 5. darpa-spill-query

For looking at events. The default output is an aligned table; `-f csv` and
`-f json` are for pipes and files.

### The most recent event

```console
$ darpa-spill-query -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --last --source tdu-near-master-ppc-02
NOVA TIME            UTC                          SIGNAL   TYPE                                 GPS TIME        DELTA  SOURCE
-----------------------------------------------------------------------------------------------------------------------------
33895187648000001    2026-10-13T18:31:44.0000000  -        BNB                   1475951522.000000015625     64000000  tdu-near-master-ppc-02
-----------------------------------------------------------------------------------------------------------------------------
1 event(s)
```

`--last` combines with `--signal`, `--type` and `--source`.

### A short range as a table

```console
$ darpa-spill-query -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --start=-5s --limit 6
note: 6 of 25 matching events shown; raise --limit to see more
NOVA TIME            UTC                          SIGNAL   TYPE                                 GPS TIME        DELTA  SOURCE
-----------------------------------------------------------------------------------------------------------------------------
33895187456000001    2026-10-13T18:31:41.0000000  -        BNB                   1475951519.000000015625     64000000  tdu-near-master-ppc-02
33895187456000001    2026-10-13T18:31:41.0000000  -        BNB                   1475951519.000000015625     64000000  tdu-near-master-ppc-03
33895187456001410    2026-10-13T18:31:41.0000220  -        ACCEL_ONE_HZ_TCLK     1475951519.000022031250     64001407  tdu-near-master-ppc-01
33895187472475944    2026-10-13T18:31:41.2574366  -        BNB_TCLK              1475951519.257436625000     16474534  tdu-near-master-ppc-01
33895187476742080    2026-10-13T18:31:41.3240950  -        BNB_TCLK              1475951519.324095000000      4266136  tdu-near-master-ppc-01
33895187481008203    2026-10-13T18:31:41.3907531  -        BNB_TCLK              1475951519.390753171875      4266123  tdu-near-master-ppc-01
-----------------------------------------------------------------------------------------------------------------------------
6 event(s)
```

When more events match than `--limit` allows, a `note:` on stderr says how
many were left out. `--desc` lists newest first.

### More examples

```console
# NuMI spills today, in Fermilab local time
$ darpa-spill-query --signal '$74' --start today --timezone America/Chicago

# 1 Hz pulser between two clock times, as CSV with chosen columns, to a file
$ darpa-spill-query --signal 8f --start 09:15 --end 11:34 \
      -f csv --columns nova_time,utc,gps,event_number,source -o pulser.csv

# Every BNB event from one TDU in the last ten minutes, as JSON
$ darpa-spill-query --type BNB --source tdu-near-master-ppc-02 --start=-10m -f json

```

Counting events by type and TDU over the last minute, in a pipeline (`grep`
removes the server's `#` comment lines, `--no-header` the column names):

```console
$ darpa-spill-query -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --start=-1m --limit 1000 \
      -f csv --no-header --columns spill_type_name,source | grep -v '^#' | sort | uniq -c
  59 ACCEL_ONE_HZ_TCLK,tdu-near-master-ppc-01
 282 BNB_TCLK,tdu-near-master-ppc-01
  59 BNB,tdu-near-master-ppc-02
  59 BNB,tdu-near-master-ppc-03
```

Avoid doing arithmetic on `nova_time` in `awk`: tick counts are about
3.4 × 10¹⁶, past the 2⁵³ that awk's doubles hold exactly, so differences come
out wrong. Use Python, or `gps` with care (see [§9](#9-reading-the-files)).

The CSV from a server begins with `#` comment lines recording the query:

```
$ darpa-spill-query -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --start=-10s --limit 4 \
      -f csv --columns nova_time,utc,spill_type_name,source,event_number
# DARPA Spill Information Server
# generated: 2026-10-13T18:31:21.494757+00:00
# range: 2026-10-13T18:31:11.488950000Z .. 2026-10-13T18:31:21.488950000Z (half-open: start <= t < end)
# selection: (all signals)
# sources: (all sources)
# timezone for unqualified inputs: UTC
# matching rows: 67
nova_time,utc,spill_type_name,source,event_number
33895185570094556,2026-10-13T18:31:11.532727437Z,BNB_TCLK,tdu-near-master-ppc-01,93938
33895185600000000,2026-10-13T18:31:12.000000000Z,BNB,tdu-near-master-ppc-02,331170
33895185600000001,2026-10-13T18:31:12.000000015Z,BNB,tdu-near-master-ppc-03,331166
33895185600001412,2026-10-13T18:31:12.000022062Z,ACCEL_ONE_HZ_TCLK,tdu-near-master-ppc-01,93940
```

For more rows than one query allows, use `darpa-spill-dump`.

## 6. darpa-spill-dump

For copying a range out. There is no row limit; give at least one of `--csv
FILE` and `--json FILE` (either may be `-` for stdout). Both are written from
one pass over the data, so they always hold the same events.

### One file of each

```console
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k \
      --start 2026-10-12T00:00 --end 2026-10-12T02:00 --timezone UTC \
      --csv r.csv --json r.json --window 1800
darpa-spill-dump: 2026-10-12T00:00:00.000000000Z -> 2026-10-12T02:00:00.000000000Z from https://novadaq-near-gateway-01.fnal.gov:8443: 56,343 events
[ 25.0%] 2026-10-12T00:30:00Z      14,085/56,343 rows       1 req     4,189 rows/s
[ 50.0%] 2026-10-12T01:00:00Z      28,174/56,343 rows       2 req     4,242 rows/s
[ 75.0%] 2026-10-12T01:30:00Z      42,253/56,343 rows       3 req     4,255 rows/s
[100.0%] 2026-10-12T02:00:00Z      56,343/56,343 rows       4 req     4,224 rows/s
darpa-spill-dump: wrote 56,343 events in 13.3 s to r.csv, r.json
```

The first line gives the range and how many events match; progress lines
follow every two seconds; the last line is the total. All of this goes to
stderr (`-q` silences it), so `--csv -` can feed a pipe.

### Selecting and trimming

```console
# The last hour of everything, JSON only
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --start=-1h --json last-hour.json

# One type over a day
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k \
      --start 2026-10-12 --end 2026-10-12 --timezone UTC --type BNB --csv bnb-2026-10-12.csv

# A few columns, no comment lines, straight into another program
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --start=-6h --signal 8f \
      --columns nova_time,utc,gps,source --no-comments --csv - | head
```

`--columns` applies to both files; the full list is in
[API.md](API.md#output-columns). `--no-header` drops the CSV column-name row,
and `--indent N` pretty-prints each JSON event.

### What makes a dump trustworthy

- **Counted and checked.** The matching events are counted before reading
  starts and again as they are written. If the numbers differ (a backfill or
  retention prune touched the range mid-run) the tool warns and exits 3.
  JSON records both in its `summary`.
- **A fixed snapshot.** The range is clipped to the newest event present when
  the dump starts, so events arriving during a long run are not half-included.
- **No partial files.** Output goes to `FILE.part` and is renamed only when
  complete. An error or Ctrl-C deletes the `.part` file.
- **Proportional requests.** The range is read in windows of `--window`
  seconds (default 3600). A window holding more than `--page-size` events
  (default 50,000) is halved and re-read, never paged by offset, so a
  concurrent writer cannot shift a page boundary.

Exit status: `0` complete, `1` runtime failure (unreachable, rejected), `2`
usage error, `3` count changed during the dump, `130` interrupted.

## 7. Splitting a dump into files by time

`--split day` or `--split hour` writes one file per calendar day or hour. A
7-day range gives 7 files; split by hour it gives 168.

### File names

The `--csv` / `--json` path is a template:

| Path given | Files written |
|---|---|
| `week.csv` | `week_2026-10-05.csv`, `week_2026-10-06.csv`, ... |
| `out/nd.json` | `out/nd_2026-10-12T00.json`, `out/nd_2026-10-12T01.json`, ... |
| `'hours/{start}/nd.json'` | `hours/2026-10-12T00/nd.json`, ... (directories are created) |
| `'{year}-{month}/{day}/nd-{date}-{hour}00.csv'` | `2026-10/12/nd-2026-10-12-0000.csv`, `2026-10/12/nd-2026-10-12-0100.csv`, ... |
| `'raw/{mon}/{year}-{mon}-{day}/nd-all-{start}.csv'` | `raw/oct/2026-oct-22/nd-all-2026-10-22.csv`, ... (gateway layout, `--split day`) |

Without a placeholder the label goes before the extension; with one, every
placeholder is replaced. Quote paths containing braces. Labels are
`YYYY-MM-DD` for days and `YYYY-MM-DDTHH` for hours, with the UTC offset
appended outside UTC.

| Placeholder | Hour piece `2026-10-15T14-0500` | Day piece `2026-10-15` |
|---|---|---|
| `{start}` | `2026-10-15T14-0500` | `2026-10-15` |
| `{date}` | `2026-10-15` | `2026-10-15` |
| `{year}` `{month}` `{day}` | `2026` `10` `15` | `2026` `10` `15` |
| `{mon}` | `oct` | `oct` |
| `{hour}` | `14` | `00` |
| `{offset}` | `-0500` (empty in UTC) | empty |

`{hour}` without `{offset}` names the two autumn 01:00 hours alike under a
daylight-saving zone. A path that would send two pieces to one file is refused
before anything is written (exit status 2); add `{offset}` or use `{start}`.
`{mon}` is the lowercase English month abbreviation, `jan` to `dec`, taken
from a fixed table rather than `strftime`, so paths are the same in every
locale. Placeholders without `--split` are a usage error.

### Hourly files

```console
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k \
      --signal 8f --source tdu-near-master-ppc-01 \
      --start 2026-10-13T12:00 --end 2026-10-13T15:00 --timezone UTC \
      --split hour --columns nova_time,utc,gps,event_number,source --csv hz/8f.csv
darpa-spill-dump: 2026-10-13T12:00:00.000000000Z -> 2026-10-13T15:00:00.000000000Z from https://novadaq-near-gateway-01.fnal.gov:8443: 8,269 events, 3 hour file(s)
[ 33.3%] 2026-10-13T13:00:00Z       3,600/8,269 rows       1 req     4,068 rows/s
darpa-spill-dump: 2026-10-13T12       3,600 events  hz/8f_2026-10-13T12.csv
darpa-spill-dump: 2026-10-13T13       3,600 events  hz/8f_2026-10-13T13.csv
darpa-spill-dump: 2026-10-13T14       1,069 events  hz/8f_2026-10-13T14.csv
[100.0%] 2026-10-13T15:00:00Z       8,269/8,269 rows       3 req     3,901 rows/s
darpa-spill-dump: wrote 8,269 events in 2.1 s to hz/8f_2026-10-13T12.csv, hz/8f_2026-10-13T13.csv, hz/8f_2026-10-13T14.csv

$ head -7 hz/8f_2026-10-13T12.csv
# darpa-spill-dump 1.2.0 generated 2026-10-13T18:34:06.357994+00:00
# origin: https://novadaq-near-gateway-01.fnal.gov:8443
# range: 2026-10-13T12:00:00.000000000Z <= t < 2026-10-13T13:00:00.000000000Z
# selection: 8f; sources: tdu-near-master-ppc-01
nova_time,utc,gps,event_number,source
33893683392001409,2026-10-13T12:00:00.000022015Z,1475928018.000022015625,2047762,tdu-near-master-ppc-01
33893683456001408,2026-10-13T12:00:01.000022000Z,1475928019.000022000000,2047769,tdu-near-master-ppc-01
```

A 1 Hz pulser gives 3,600 events an hour, so the 1,069 in the 14:00 file is
worth a look. That file shows `event_number` restarting near 14:00 and no
events from 14:01:42 to 14:43:54, a TCRMonitor restart on that TDU rather
than anything lost in transfer. Per-hour files make gaps like this easy to
spot.

### Daily files in local time

Day boundaries follow `--timezone`. Under `America/Chicago` each file runs
from local midnight to local midnight (05:00Z to 05:00Z in October):

```console
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k \
      --start 2026-10-05 --end 2026-10-11 --timezone America/Chicago \
      --split day --csv week/nd.csv
```

writes `week/nd_2026-10-05.csv` through `week/nd_2026-10-11.csv`. Each file is
complete in itself: its own `#` header with its own range, and for JSON its
own `meta.range`, `meta.piece` and `summary`.

**Daylight saving** is handled in real time. On a changeover day a daily file
covers 23 or 25 hours. Hourly splits give 23 or 25 files that day, and
outside UTC the hour labels carry the UTC offset so the repeated autumn hour
does not overwrite itself: `2026-11-01T01-0500` and `2026-11-01T01-0600`.

### Empty pieces

Every requested historical piece is written, even after the newest archived
event, so completed quiet hours produce empty files. Pieces starting in the
future are omitted. An empty file describes the archive; it does not prove
that ingest covered that interval. The publisher separately checks ingest
coverage before publication. `--skip-empty` drops empty pieces:

```console
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k --type SUPER_CYCLE \
      --start 2026-09-17 --end 2026-10-13 --timezone UTC \
      --split day --skip-empty --json 'sc/{start}/sc.json'
darpa-spill-dump: 2026-09-17T00:00:00.000000000Z -> 2026-10-14T00:00:00.000000000Z from https://novadaq-near-gateway-01.fnal.gov:8443: 16 events, 27 day file(s)
darpa-spill-dump: 2026-09-22          10 events  sc/2026-09-22/sc.json
darpa-spill-dump: 2026-09-23           6 events  sc/2026-09-23/sc.json
[100.0%] 2026-10-13T18:33:19Z          16/16 rows      48 req         6 rows/s
darpa-spill-dump: wrote 16 events in 2.5 s to sc/2026-09-22/sc.json, sc/2026-09-23/sc.json
```

Empty pieces cost one count request each and no reading, so a sparse
selection over a long range is quick.

### When something fails partway

Each piece is renamed into place as soon as it is complete. If a later piece
fails, the finished files are kept and listed, and only the piece in progress
is discarded:

```
darpa-spill-dump: error: cannot reach https://.../api/events: ...
darpa-spill-dump: 4 complete file(s) kept: week/nd_2026-10-05.csv, ...
```

Re-run with `--start` at the first missing day to finish.

## 8. Recipes

**Yesterday, every night, one file per hour** (cron on a machine that can
reach the gateway):

```cron
15 0 * * *  cd /path/to/DARPA-SpillServer && venv/bin/darpa-spill-dump -q \
    -s https://novadaq-near-gateway-01.fnal.gov:8443 --ca-bundle $HOME/novadaq-gw.pem \
    --start yesterday --end yesterday --timezone UTC \
    --split hour --csv '/data/spills/{start}.csv' || echo "spill dump failed: $?"
```

**A month as daily JSON, directly from the archive on the gateway** (fastest):

```console
$ darpa-spill-dump -c config/spillserver-near.yaml \
      --start 2026-09-01 --end 2026-09-30 --timezone UTC \
      --split day --json 'archive/2026-09/{start}.json'
```

**One type, one TDU, the last week, small daily files:**

```console
$ darpa-spill-dump -s https://novadaq-near-gateway-01.fnal.gov:8443 -k \
      --type BNB --source tdu-near-master-ppc-02 --start=-1w \
      --columns nova_time,utc,gps,event_number --split day --csv bnb/ppc02.csv
```

`--start=-1w` is not on a midnight, so the first file is a partial day, as is
the last (it ends at the newest event).

**Size and time.** Against the Near Detector gateway a day is about 630,000
events: roughly 170 MB of CSV and 360 MB of JSON with every column, and about
three minutes (about 3,800 events/s, limited by the server's JSON rendering).
`--columns` shrinks both. Reading the archive file on the gateway is faster
than going through the server.

## 9. Reading the files

**CSV.** The `#` lines at the top record the tool, origin, range and
selection. Most readers skip them if told to:

```python
import pandas as pd
df = pd.read_csv("hz/8f_2026-10-13T12.csv", comment="#")
```

```console
$ grep -v '^#' hz/8f_2026-10-13T12.csv | head
```

Or write files without them using `--no-comments`.

**JSON.** One object, with each event on its own line:

```json
{
"meta": {"generated": "...", "generator": "darpa-spill-dump 1.2.0", "origin": "https://...",
         "range": {"start": {...}, "end": {...}, "note": "half-open: start <= t < end"},
         "selection": ["(all signals)"], "sources": ["(all sources)"], "timezone": "UTC",
         "order": "asc", "columns": [...], "expected": 56343},
"events": [
{"nova_time": 33885388992000001, "utc": "2026-10-12T00:00:00.000000015Z", ..., "source": "tdu-near-master-ppc-02", "route": "spill_history"},
...
],
"summary": {"returned": 56343, "expected": 56343, "complete": true, "requests": 4, "seconds": 13.341}
}
```

`meta` and `events` have the same layout as `/api/events`. Split files add
`meta.piece`: `{"split": "day", "label": "2026-10-12"}`. Check
`summary.complete` before trusting a file.

```python
import json, pandas as pd
doc = json.load(open("r.json"))
assert doc["summary"]["complete"]
df = pd.DataFrame(doc["events"])
```

```console
$ jq '.summary' r.json
$ jq -r '.events[] | [.nova_time, .spill_type_name] | @tsv' r.json | head
```

**Precision.** Use `nova_time` (integer 64 MHz ticks) or the decimal string
`gps` when sub-nanosecond precision matters. `gps_nsec` and `unix_nsec`
truncate the 15.625 ns tick. Loaded as a float, `gps` (about 1.5 × 10⁹ s)
keeps only about 0.25 µs of resolution; read it as a string
(`dtype={"gps": str}`) if you need it all. `nova_time` fits a 64-bit integer
exactly, and pandas reads it as one.

## 10. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `CERTIFICATE_VERIFY_FAILED ... self-signed certificate` | the gateway's certificate; add `-k` or `--ca-bundle FILE` ([§2](#2-connecting-to-the-data)) |
| `error: argument --start: expected one argument` | a relative time written as `--start -6h`; use `--start=-6h` |
| `the end of the range ... is before its start` | start and end swapped; the message gives the corrected pair |
| A "one day" dump holds two days | `--end` was a date, which covers that whole day; use `--end` = `--start`, or `--end ...T00:00` |
| Exit status 141 when piping into `head` | expected: the tool stops quietly when its reader closes the pipe early; any partial dump files are removed |
| `--signal $74` matches everything, or errors | the shell expanded `$74`; quote it or write `74` / `numi` |
| Times are six hours off | unqualified times use the query zone (UTC on the gateway); add `--timezone America/Chicago` |
| `no events from source(s) ...` | a misspelled TDU name; the error lists the ones in the archive |
| `note: N of M matching events shown` | `darpa-spill-query` hit `--limit`; raise it or use `darpa-spill-dump` |
| `--split writes several files; it cannot write to stdout` | `--split` needs real file paths |
| Exit status 3, `the archive changed during the dump` | a backfill or prune ran mid-dump; the files are consistent, re-run if you need the newer state |
| Fewer files than days with `--split` | pieces after the newest archived event are not written; check ingest with `/api/status` |

## 11. Blurring timestamps reversibly

`darpa-spill-shift` shifts every event in a dump file by a small random
offset, so the recipient sees roughly when events happened but not exactly.
Whoever holds the token can undo the shift exactly. The offsets are flat or
truncated Gaussian, at most ±500 ms. The design is summarised in
[TIMESTAMP_SHIFT.md](TIMESTAMP_SHIFT.md) and specified in full, with tests
and plots, in [TimestampShift.nb](TimestampShift.nb).

**1. Make a token and keep it.** It is the only way to undo the shift.

```console
$ darpa-spill-shift token --dist gauss --sigma 20ms -o tok.txt
darpa-spill-shift: wrote Gaussian, sigma 20 ms (1280000 ticks), cut +-60 ms (3840000 ticks) token to tok.txt (fingerprint a8dcfcf5c8a9caa1)
```

`--dist flat --width 50ms` gives uniform offsets in ±50 ms. For the Gaussian,
`--cut` defaults to 3σ. `--seed TEXT` makes the token reproducible from a
passphrase, but then it is only as strong as the passphrase.

**2. Shift.** Each input gets a `.shifted` copy alongside it:

```console
$ darpa-spill-shift apply --token-file tok.txt r.csv r.json
darpa-spill-shift: shifted 56,343 events: r.csv -> r.shifted.csv
darpa-spill-shift: shifted 56,343 events: r.json -> r.shifted.json
```

`nova_time` moves, and every other time column (`utc`, `gps`, `unix_*`, ...)
follows it. `pps_offset` and `delta` are transformed too, since otherwise they
would give the true times away. The file records which token shifted it,
without the secret:

```
# darpa-spill-dump 1.2.0 generated 2026-10-13T15:51:12.469919+00:00
...
# darpa-spill-shift: dts1 shifted, params=dts1:gauss:sigma=1280000:cut=3840000, fingerprint=a8dcfcf5c8a9caa1
nova_time,utc,utc_string,...
```

**3. Restore.** The `.restored` copy is byte-identical to the original:

```console
$ darpa-spill-shift invert --token-file tok.txt r.shifted.csv r.shifted.json
darpa-spill-shift: restored 56,343 events: r.shifted.csv -> r.restored.csv
darpa-spill-shift: restored 56,343 events: r.shifted.json -> r.restored.json
$ cmp r.csv r.restored.csv && echo identical
identical
```

**Split dumps** can be shifted in one call:
`darpa-spill-shift apply --token-file tok.txt week/nd_*.csv`.

**Safety checks:**
- Shifting a file twice is refused.
- Inverting with the wrong token is refused, because it would otherwise give
  wrong times without any sign of failure. `darpa-spill-shift info FILE
  --token-file T` tells you whether a token matches a file.
- The input file is never overwritten.

**What it doesn't hide:**
- **Event order.** `event_number` stays in the file, and it numbers events in
  true order.
- **Predictable events.** The 1 Hz pulser can be snapped back to the second.

Once σ approaches the spacing between events, sorting by time no longer gives
the true order: 0.6% of successive events swap at 20 ms, and 9% at 50 ms. Sort
by `event_number` instead.

## 12. Publishing on the gateway: darpa-spill-publish

`darpa-spill-publish` strings the three commands above together for the
Near Detector gateway, where cron runs it. Each run dumps a range with
`darpa-spill-dump --split hour|day` for each stream (`numi`, `all`,
`booster`), shifts every file with `darpa-spill-shift apply` using the token
of the month the data belongs to, uploads raw and shifted copies to the Fermi
Data Platform with `fdp-put --skip-existing`, and rewrites a status page.
Paths use the `{mon}` placeholder from [section 7](#7-splitting-a-dump-into-files-by-time):

```
/data/DARPA-spilldata/raw/oct/2026-oct-22/nd-numi-2026-10-22T13-0500.csv
/data/DARPA-spilldata/shifted/oct/2026-oct-22/nd-numi-2026-10-22T13-0500.shifted.csv
```

| Subcommand | Does |
|---|---|
| `hourly [--hour TIME]` | the preceding local hour, plus any of the last 24 not complete |
| `daily [--date YYYY-MM-DD]` | the preceding local day (23/24/25 h), plus any of the 3 before it not complete |
| `backfill --start X --end Y` | every whole hour and day in a range (`--no-hourly`, `--no-daily`, `--no-upload`) |
| `upload` | retry every upload not yet done |
| `token [--month YYYY-MM \| --next]` | create a month's shift token if missing; never overwrites |
| `creds` | renew Kerberos (`kinit -R`), refresh the FDP bearer token (`htgettoken`) |
| `status` | re-render the status page |
| `check` | preflight table; exit 1 if anything fails |

```console
$ darpa-spill-publish check                                # is everything in place?
$ darpa-spill-publish -n hourly                            # print what the next hourly run would do
$ darpa-spill-publish -v hourly --hour 2026-11-01T01-0600  # redo the second 01:00 of 1 November
$ darpa-spill-publish -v daily --date 2026-10-21
$ darpa-spill-publish -v backfill --start 2026-09-17 --end now --no-upload
```

Common options (`-c FILE`, `--set SECTION.KEY=VALUE`, `-n`, `-v`, `-q`) go
before or after the subcommand. Settings come from `config/publish.yaml`
(deployed as `~/DARPA/publish/publish.yaml`), `DARPA_SPILL_PUBLISH_*`
variables and `--set`, with the command line winning. Exit status: 0 ok,
1 failure, 2 usage, 3 partial, 75 temporary (ingest not ready, or the job is
already running).

The full reference is `man -l man/darpa-spill-publish.1`; the operator
runbook (weekly `kinit`, token backup, alerts, recovery) is
[GATEWAY_OPERATIONS.md](GATEWAY_OPERATIONS.md).
