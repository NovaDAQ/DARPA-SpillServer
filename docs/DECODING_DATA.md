# Decoding the NOvA Near Detector accelerator-timing data

This note explains how to download the NOvA Near Detector accelerator
event-timing files from the Fermi Data Platform (FDP) and how to restore the
true timestamps in the time-shifted ("blurred") copies.

The current files are **initial test data**. They come from the
DARPA Spill Information Server running on `novadaq-near-gateway-01.fnal.gov`,
which records events from the three Timing Distribution Units (TDUs) of the
NOvA Near Detector.

## 1. What is published, and where

| Area | Path on FDP | Who can read it | Contents |
|---|---|---|---|
| Public, shifted | `/amsc/public/novaend/shifted/<mon>/<YYYY>-<mon>-<DD>/` | anyone, anonymously | timestamps blurred with a Gaussian of σ = 50 µs |
| Public, tokens | `/amsc/public/novaend/tokens/` | anyone, anonymously | the monthly tokens that undo the blur |
| Private, raw | `/amsc/novaend/raw/<mon>/<YYYY>-<mon>-<DD>/` | `novaend` group (bearer token) | the true timestamps |
| Private, tokens | `/amsc/novaend/tokens/` | `novaend` group | the same monthly tokens |

`<mon>` is the lowercase English month (`sep`, `oct`, ...); for example
`/amsc/public/novaend/shifted/oct/2026-oct-22/`. Days and hours are
**America/Chicago** local time.

### File names

Every hour and every day is published in three streams and two formats:

| Stream | Events |
|---|---|
| `numi` | NuMI beam: `$74` (MIBS) and `$A9` (TCLK) — empty when there is no NuMI beam |
| `booster` | Booster: `BNB` (`$1B`) and `BNB_TCLK` (`$1D`, `$1F`) |
| `all` | every accelerator event recorded |

```
nd-<stream>-<label>.<csv|json>              raw (private area)
nd-<stream>-<label>.shifted.<csv|json>      shifted (public area)

label:  2026-10-22T14-0500   one hour, 14:00 <= t < 15:00 local, UTC offset -0500
        2026-10-22           one local day (23 or 25 hours on DST change days)
```

The hourly label carries the UTC offset, so the repeated hour at the autumn
DST change gets two distinct files (`...T01-0500` and `...T01-0600`). The
daily file holds exactly the rows of that day's hourly files.

## 2. Downloading

### Public files (no account needed)

The public area is readable over HTTPS with any client. dCache answers with a
redirect to a storage node, so tell `curl` to follow it (`-L`):

```bash
BASE=https://amsc.fnal.gov:2880/amsc/public/novaend

# list a day's directory (WebDAV PROPFIND)
curl -s -X PROPFIND -H "Depth: 1" "$BASE/shifted/oct/2026-oct-22/" \
  | grep -o '<d:href>[^<]*</d:href>' | sed 's/<[^>]*>//g'

# one shifted file and the token for its month
curl -L -O "$BASE/shifted/oct/2026-oct-22/nd-all-2026-10-22T14-0500.shifted.csv"
curl -L -O "$BASE/tokens/dts1-2026-10.token"
```

If `curl` reports a certificate error, your CA bundle lacks the InCommon IGTF
chain that amsc.fnal.gov uses; add `--capath /etc/grid-security/certificates`
(or another directory holding the IGTF CAs).

### Private files (novaend members)

With the Fermi Data Platform tools and a `novaendread` or `novaendwrite`
token:

```bash
fdp-get /amsc/novaend/raw/oct/2026-oct-22/nd-all-2026-10-22T14-0500.csv .
fdp-get -R /amsc/novaend/raw/oct/2026-oct-22 ./raw-2026-oct-22
```

or with `curl` and a bearer token file:

```bash
curl -L -H "Authorization: Bearer $(cat "$BEARER_TOKEN_FILE")" -O \
  https://amsc.fnal.gov:2880/amsc/novaend/raw/oct/2026-oct-22/nd-all-2026-10-22T14-0500.csv
```

### Checking a download

dCache stores an adler32 checksum for every file. Compare it with your copy:

```bash
fdp-stat /amsc/public/novaend/shifted/oct/2026-oct-22/nd-all-2026-10-22T14-0500.shifted.csv
python3 -c "import sys,zlib; print(format(zlib.adler32(open(sys.argv[1],'rb').read()) & 0xffffffff, '08x'))" \
  nd-all-2026-10-22T14-0500.shifted.csv
```

## 3. The file formats

### CSV

```
# darpa-spill-dump 1.5.0 generated 2026-10-22T21:33:44.387625+00:00
# origin: https://novadaq-near-gateway-01.fnal.gov:8443
# range: 2026-10-22T19:00:00.000000000Z <= t < 2026-10-22T20:00:00.000000000Z
# selection: $74, $A9; sources: (all sources)
# darpa-spill-shift: dts1 shifted, params=dts1:gauss:sigma=3200:cut=9600, fingerprint=402c39c169848160
nova_time,utc,utc_string,unix_sec,unix_nsec,gps,gps_seconds,gps_nsec,gps_psec,gps_week,gps_tow,gps_tow_exact,spill_type,spill_type_name,signal,signal_name,event_number,delta,pps_offset,source,route
...
```

Lines starting with `#` are comments. The `darpa-spill-shift:` line is
present only in shifted files: it names the transform parameters and the
**fingerprint** of the token that was used (a public identifier, not a
secret).

### JSON

```
{
"meta": { "generator": ..., "range": {...}, "columns": [...], "shift": {...} },
"events": [
{ "nova_time": ..., "utc": ..., ... },
...
],
"summary": { "returned": N, ... }
}
```

In shifted files `meta.shift` holds `spec`, `state`, `params` and
`fingerprint`. Each event object has the same fields as a CSV row.

### Columns

| Column | Meaning |
|---|---|
| `nova_time` | event time in NOvA clock ticks: 64 MHz (15.625 ns) since 2010-01-01 00:00:00 UTC |
| `utc`, `utc_string`, `unix_sec`, `unix_nsec` | the same instant as UTC / Unix time |
| `gps*` | the same instant as GPS seconds, week and time of week |
| `spill_type`, `spill_type_name` | decoded accelerator event type (`NUMI`, `BNB`, `BNB_TCLK`, `ACCEL_ONE_HZ_TCLK`, ...) |
| `signal`, `signal_name` | raw accelerator signal code; empty in these files (the TDUs report the type only) |
| `event_number` | the TDU's own event counter; it increases in true time order |
| `delta` | ticks since the previous event of the same TDU (`event_number - 1`) |
| `pps_offset` | ticks since the last whole second (the sub-second phase) |
| `source` | which TDU recorded it: `tdu-near-master-ppc-01`, `-02`, `-03` |
| `route` | how the server read it from the TDU |

The same accelerator event can appear once per TDU; `source` tells them
apart.

## 4. What the shift does

Each event is moved by a pseudorandom offset drawn from a Gaussian of
**σ = 50 µs (3200 ticks), truncated at ±150 µs (9600 ticks)**. The offset is
computed with HMAC-SHA256 from a secret **token** and the event's
`(source, event_number)`, so each event has its own offset and the same event
always gets the same one.

| Column | In the shifted file |
|---|---|
| `nova_time` | true time + offset |
| `utc`, `unix_*`, `gps*` | recomputed from the shifted `nova_time` |
| `pps_offset` | (true phase + offset) mod 1 s |
| `delta` | true gap + this event's offset − previous event's offset |
| everything else | unchanged |

Knowing the token, the shift is undone exactly: the restored file is
**byte-for-byte identical** to the raw file. There is one token per calendar
month of data, `dts1-YYYY-MM.token`; a file is shifted with the token of the
month its events belong to.

Example, three events of `nd-all-2026-10-22T14-0500` (selected columns):

```
shifted:  nova_time          utc_string                              event_number  delta     pps_offset  source
          33945062592001758  2026-Oct-22 19:00:00.000027468750 UTC   1110508       63997378  1758        tdu-near-master-ppc-02
          33945062592001337  2026-Oct-22 19:00:00.000020890625 UTC   1110494       64004994  1337        tdu-near-master-ppc-03
          33945062592001942  2026-Oct-22 19:00:00.000030343750 UTC   1276199       64001441  1942        tdu-near-master-ppc-01

restored: 33945062592000000  2026-Oct-22 19:00:00.000000000000 UTC   1110508       64000000  0           tdu-near-master-ppc-02
          33945062592000001  2026-Oct-22 19:00:00.000000015625 UTC   1110494       64000000  1           tdu-near-master-ppc-03
          33945062592001411  2026-Oct-22 19:00:00.000022046875 UTC   1276199       64001407  1411        tdu-near-master-ppc-01
```

The first two events (the 1 Hz pulse on ppc-02 and ppc-03) sit exactly on the
second, `delta` = 64000000 ticks = 1 s; in the shifted file they are moved by
+27.5 µs and +20.9 µs.

## 5. Decoding with darpa-spill-shift

### Install

Python 3.9 or newer. The package depends on `nova-time-decoder`, which is
not on PyPI, so install it from GitHub first:

```bash
python3 -m venv spill && . spill/bin/activate
pip install "git+https://github.com/normanajn/nova-time-decoder.git"
pip install "git+https://github.com/NovaDAQ/DARPA-SpillServer.git@v1.5.2"
darpa-spill-shift --version        # darpa-spill-shift 1.5.2
```

### Which token does a file need?

```bash
darpa-spill-shift info nd-all-2026-10-22T14-0500.shifted.csv
darpa-spill-shift info nd-all-2026-10-22T14-0500.shifted.csv --token-file dts1-2026-10.token
```

The first prints the file's parameters and token fingerprint; the second
also says `this token matches the file` (exit status 0) or that it does not
(exit status 1). `darpa-spill-shift info dts1-2026-10.token` prints a token's
own fingerprint.

| Data month | Token | Fingerprint |
|---|---|---|
| 2026-09 | `dts1-2026-09.token` | `3fb8ef94c7159669` |
| 2026-10 | `dts1-2026-10.token` | `402c39c169848160` |
| 2026-11 | `dts1-2026-11.token` | `f96a4f776f4b14b1` |

### Restore one file

```bash
darpa-spill-shift invert --token-file dts1-2026-10.token nd-all-2026-10-22T14-0500.shifted.csv
# writes nd-all-2026-10-22T14-0500.restored.csv

darpa-spill-shift invert --token-file dts1-2026-10.token \
    -o nd-all-2026-10-22T14-0500.csv nd-all-2026-10-22T14-0500.shifted.json
# JSON works the same way; -o names the output (one input only; '-' = stdout)
```

`.shifted` in the name is replaced by `.restored`; the input is never
overwritten. The restored file has the `darpa-spill-shift:` comment (CSV) or
`meta.shift` (JSON) removed and is identical to the raw file.

### Restore a whole day or month

```bash
# every shifted file of one day (CSV and JSON)
darpa-spill-shift invert --token-file dts1-2026-10.token 2026-oct-22/*.shifted.*

# a mixed set: pick each file's token from its month
for f in */*.shifted.csv; do
    month=$(echo "$f" | grep -o '20[0-9][0-9]-[0-1][0-9]' | head -1)
    darpa-spill-shift invert -q --token-file "dts1-$month.token" "$f"
done
```

### Safety checks

`darpa-spill-shift invert` refuses, and writes nothing, if

- the token's fingerprint differs from the one recorded in the file (a wrong
  token would silently give wrong times), e.g.
  `... was shifted with token fingerprint 402c39c169848160 ..., but this token is 3fb8ef94c7159669 ...`;
- the file has no shift marker (it is not a shifted file);
- the output would overwrite the input.

Exit status: 0 success, 1 a file could not be processed, 2 bad token or
usage. `--force` skips the marker and fingerprint checks; do not use it to
"make a wrong token work".

### Check a round trip (novaend members)

```bash
darpa-spill-shift invert --token-file dts1-2026-10.token nd-all-2026-10-22T14-0500.shifted.csv
cmp nd-all-2026-10-22T14-0500.restored.csv nd-all-2026-10-22T14-0500.csv && echo identical
```

## 6. Using the shifted data without decoding

The shifted times are within ±150 µs of the truth (σ = 50 µs), which is
enough for anything that does not need sub-millisecond timing. Points to keep
in mind:

- Events can swap order in shifted time when they are closer together than
  about 100 µs. Sort by `source` and `event_number`, which follow true order,
  rather than by `nova_time`.
- `delta` and `pps_offset` are shifted consistently with `nova_time`, so they
  cannot be used to recover the true times.
- Periodic signals with a known phase (the 1 Hz pulse fires on the second)
  can be snapped back to their true times by anyone; the shift does not hide
  them.

## 7. More information

- Source code, manual pages and the design of the shift:
  <https://github.com/NovaDAQ/DARPA-SpillServer>
  - `docs/TIMESTAMP_SHIFT.md` — the dts1 transform and its verification
  - `docs/TimestampShift.nb` — the full specification (Mathematica)
  - `man/darpa-spill-shift.1`, `man/darpa-spill-dump.1`
- Live status of the publishing pipeline:
  <https://novadaq-near-gateway-01.fnal.gov:8443/status/> (Fermilab network)
- Contact: Andrew Norman, anorman@fnal.gov
