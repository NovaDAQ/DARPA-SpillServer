# Gateway operations: spill data publishing on novadaq-near-gateway-01

This is the operator runbook for the publishing pipeline on
`novadaq-near-gateway-01.fnal.gov`. Every hour and every day, cron:

1. dumps the archive behind `darpa-spill-server` into CSV and JSON files,
2. blurs their timestamps with `darpa-spill-shift`,
3. uploads the raw files and the shifted files to the Fermi Data Platform (FDP),
4. and writes a status page.

The tool is `darpa-spill-publish`; its full reference is
`man -l man/darpa-spill-publish.1`. The design and the reasons behind it
(decisions D1–D18) are in
[deployment/GATEWAY_DEPLOYMENT_PLAN.md](deployment/GATEWAY_DEPLOYMENT_PLAN.md).

Everything below runs as **`novadaq`** on the gateway unless it says *on the
Mac*. The commands assume the package's venv is on `PATH`:

```bash
ssh novadaq@novadaq-near-gateway-01.fnal.gov
source ~/DARPA/DARPA-SpillServer/venv/bin/activate
```

Contents:

1. [At a glance](#1-at-a-glance)
2. [Layout](#2-layout)
3. [Schedule](#3-schedule)
4. [Status page](#4-status-page)
5. [Weekly: renew the Kerberos ticket](#5-weekly-renew-the-kerberos-ticket)
6. [Monthly: back up the shift token](#6-monthly-back-up-the-shift-token)
7. [Alerts: what they mean, what to do](#7-alerts-what-they-mean-what-to-do)
8. [Re-running a failed hour or day](#8-re-running-a-failed-hour-or-day)
9. [Backfill](#9-backfill)
10. [Restoring (inverting) a shifted file](#10-restoring-inverting-a-shifted-file)
11. [Recovery](#11-recovery)
12. [Looking inside the state database](#12-looking-inside-the-state-database)

---

## 1. At a glance

| What | Where / command |
|---|---|
| Status page | <https://novadaq-near-gateway-01.fnal.gov:8443/status/> (and `status.json` beside it) |
| Preflight | `darpa-spill-publish check` |
| What would the next hourly run do? | `darpa-spill-publish -n hourly` |
| Human action, about weekly | `kinit -r 7d -c FILE:/home/novadaq/anorman-fdp/tokens/krb5cc_anorman anorman@FNAL.GOV` |
| Human action, monthly | copy `dts1-YYYY-MM.token` to the Mac |
| Alerts go to | `anorman@fnal.gov`, subject `[darpa-spill-publish] ...` |
| Configuration | `~/DARPA/publish/publish.yaml` (example with comments: `config/publish.yaml`) |
| Exit status | 0 ok, 1 failure, 2 usage/config, 3 partial, 75 temporary (ingest not ready, job already running, or the shared dump lock busy) |

## 2. Layout

### On the gateway

```
~/DARPA/DARPA-SpillServer/          checkout, venv/, tls/cert.pem, start.sh, stop.sh, run/
~/DARPA/publish/publish.yaml        deployed publish configuration
~/anorman-fdp/                      FDP tools area
    env.sh                          sourced before fdp-put and htgettoken
    certificates/                   CA directory for amsc.fnal.gov (D12)
    .env                            FERMI_USER / FERMI_PASSWORD, mode 0600
    tokens/                         mode 0700
        krb5cc_anorman              Kerberos ticket cache cron uses
        fdp_bt_u46550_amsc_novaendwrite   FDP bearer token (written by 'creds')
        fermi_sso.cookies           SSO cookie jar (SSO fallback only)
        dts1-2026-09.token          one shift token per data month, mode 0600
        dts1-2026-10.token
/data/DARPA-spilldata/
    raw/oct/2026-oct-22/            nd-<stream>-<label>.csv / .json
    shifted/oct/2026-oct-22/        nd-<stream>-<label>.shifted.csv / .json
    publish/state/publish.db        SQLite state; *.lock
    publish/status/                 index.html, status.json  (served at /status/)
    publish/logs/                   cron output
```

The `publish/` directories are `paths.state_dir`, `paths.status_dir` and
`paths.log_dir` in `~/DARPA/publish/publish.yaml` (also the built-in
defaults). `~/DARPA/publish/bin/` holds the cron wrapper `run-publish.sh`,
`server-watchdog.sh` and `logrotate.conf`, installed from `deploy/gateway/`. The server's `server.status_dir` must point at the same
directory as `paths.status_dir`, or `/status/` shows a stale page or 404.

**Streams** (D5): `numi` = `$74` and `$A9`; `all` = every event; `booster` =
types `BNB` and `BNB_TCLK`. Empty files are kept, so an hour without NuMI beam
is an empty `numi` file, not a missing one.

**Labels** (D2, D4), in America/Chicago:

| Kind | Label | Example file |
|---|---|---|
| hourly | local hour + UTC offset | `raw/oct/2026-oct-22/nd-numi-2026-10-22T13-0500.csv` |
| daily | local date | `raw/oct/2026-oct-22/nd-numi-2026-10-22.csv` |
| shifted | `.shifted` before the extension | `shifted/oct/2026-oct-22/nd-numi-2026-10-22T13-0500.shifted.csv` |

On the autumn changeover (1 November 2026) the hour 01:00–02:00 happens twice
and gives two files, `...T01-0500` and `...T01-0600`; that day's daily file
covers 25 hours. The spring day (14 March 2027) has 23 hours and no
`T02` file. The directory is the local date of the piece's start.

### On FDP

| Local | Remote (D9) |
|---|---|
| `/data/DARPA-spilldata/raw/oct/2026-oct-22/` | `/amsc/novaend/raw/oct/2026-oct-22/` |
| `/data/DARPA-spilldata/shifted/oct/2026-oct-22/` | `/amsc/public/novaend/shifted/oct/2026-oct-22/` |

The `novaendwrite` role can write only under `/amsc/novaend` and
`/amsc/public/novaend`. dCache forbids overwriting, and the pipeline never
passes `--replace`: a file, once uploaded, is final. The older manual uploads
under `/amsc/novaend/{numi,booster,all-accelerator}-timing/` are not touched.

**Upload verification.** `fdp-put`'s exit status alone never marks a file
uploaded. A PUT that fails part-way (seen live: `PUT ...: SSL connect error`
from a bad pool node, `fdp-put` exit 12) leaves an empty namespace entry on
dCache (`fdp-stat` shows `size: -1`, no checksum), and the retry's
`--skip-existing` skips it and exits 0. So after every `fdp-put` batch
(unless it failed with exit 4 or 125) the publisher runs
`fdp-stat --json` on the same files, inside `env.sh` with the same
`--token-file`/`--ca-path`/`fdp.extra_args` (one call per batch; one per
file if that fails; exit 5 = not found), and per file:

| On FDP | Result |
|---|---|
| size and adler32 equal the local file's | recorded uploaded, error cleared |
| not there | stays pending, error noted; the next run sends it |
| `size -1` (left by a failed PUT) | removed with `fdp-rm -f`; stays pending with `removed incomplete remote copy (size X, adler32 Y); will retry`; the next run sends it |
| complete, but size/adler32 differ | never removed (it may be the published copy, D9/D10); stays pending, error recorded and `uploaded file differs on FDP` mailed. Compare the copies and `fdp-rm -f` the remote one by hand if it is wrong |

The local adler32 is computed once (1 MiB chunks, 8 lowercase hex digits)
and cached in the `raw_adler32`/`shifted_adler32` columns of the state
database. The pending count on the status page follows the verified state.
A dry run (`-n`) prints the `fdp-stat` command but runs neither it nor
`fdp-rm`.

To look at FDP by hand:

```bash
source ~/anorman-fdp/env.sh
fdp-ls /amsc/novaend/raw/oct/2026-oct-22
fdp-stat --checksum /amsc/novaend/raw/oct/2026-oct-22/nd-all-2026-10-22T13-0500.csv
fdp-token show
```

## 3. Schedule

From the deployed crontab (`deploy/gateway/crontab.fragment`, installed with
`deploy/gateway/install.sh --install-cron`; `crontab -l` shows what is
installed). Times are the gateway's local time. Each line runs
`~/DARPA/publish/bin/run-publish.sh JOB`, which sets `PATH` and the locale and
calls the venv's `darpa-spill-publish -c ~/DARPA/publish/publish.yaml`; each
job appends to its own log, `/data/DARPA-spilldata/publish/logs/JOB.log`.

| When | Job | Does |
|---|---|---|
| HH:03, HH:33 | `creds` | `kinit -R` if the ticket expires within 12 h; `htgettoken` refreshes the bearer token if it has under 1 h left |
| HH:10 | `hourly` | the preceding hour, plus any of the last 24 hours not complete; waits up to 5 min for ingest |
| 00:30 | `daily` | the preceding day (the authoritative complete copy, D10), plus any of the 3 days before it not complete |
| 09:00 on the 25th | `token --next` | creates next month's shift token (alert: back it up) |
| HH:07, :22, :37, :52 | `upload` | retries every file not yet uploaded |
| every 5 min | `status` | re-renders the status page (credential ages, disk, ingest lag) |
| every 5 min, @reboot | `server-watchdog.sh` | restarts `darpa-spill-server` if `/api/health` fails |

Each job holds its own lock; a second copy of the same job exits 75 at once,
quietly. A manual run of the same job while cron's is running therefore exits
75 and does nothing; wait and repeat. Hourly files are never replaced: data that arrives after HH:10 is in
the daily file.

## 4. Status page

<https://novadaq-near-gateway-01.fnal.gov:8443/status/> — refreshes every
60 s. The browser will warn about the server's self-signed certificate. From
a shell:

```bash
curl -s --cacert ~/DARPA/DARPA-SpillServer/tls/cert.pem \
    https://novadaq-near-gateway-01.fnal.gov:8443/status/status.json | python3 -m json.tool | less
```

It shows the last run of each job (result, duration), credentials (Kerberos
expiry and renew-until, bearer token expiry, Vault token age), ingest
watermark and lag per source, disk use, totals, pending uploads, the last 20
errors, and two grids: the last 48 hours and the last 14 days, one column per
stream. Cell states:

| Cell | Meaning |
|---|---|
| `missing` | nothing recorded yet |
| `partial` | some formats dumped, not all |
| `dumped` | raw written, not yet shifted |
| `shifted` | shifted copy written, nothing uploaded |
| `raw-up` | raw copy on FDP, shifted not yet |
| `public` | both copies on FDP: done |
| `error` | the file's `last_error` is set; see the errors table |

A healthy page is all `public` except the current hour, with every job `ok`.
The page is public, like `/static`; nothing secret is written to it.

## 5. Weekly: renew the Kerberos ticket

cron cannot see a forwarded ticket, so the pipeline uses a fixed cache that
`creds` renews with `kinit -R`. A renewable ticket lasts at most 7 days;
before then a person must make a new one. You are alerted a day ahead
(*Kerberos ticket must be re-created by ...*), and the status page shows the
renew-until time.

On the gateway, as novadaq:

```bash
kinit -r 7d -c FILE:/home/novadaq/anorman-fdp/tokens/krb5cc_anorman anorman@FNAL.GOV
# (type the anorman@FNAL.GOV password)
klist -c FILE:/home/novadaq/anorman-fdp/tokens/krb5cc_anorman    # check "renew until"
darpa-spill-publish -v creds                                      # refresh the bearer token now
```

If uploads failed while the ticket was gone, they are retried by the next
`upload` run (every 15 min), or at once with `darpa-spill-publish -v upload`.

## 6. Monthly: back up the shift token

Each month of data is shifted with its own `dts1` token. **The token is the
only way to restore that month's shifted files**: if it is lost, those files
can never be inverted. The token for month M is created on the 25th of M−1
(or on first use, if missing); either way an alert *new shift token created -
back it up* is sent.

On the Mac:

```bash
mkdir -p -m 700 ~/Credentials/DARPA-shift-tokens
scp novadaq@novadaq-near-gateway-01.fnal.gov:anorman-fdp/tokens/dts1-2026-11.token ~/Credentials/DARPA-shift-tokens/
chmod 600 ~/Credentials/DARPA-shift-tokens/dts1-2026-11.token
```

i.e. in general
`scp novadaq@novadaq-near-gateway-01.fnal.gov:anorman-fdp/tokens/dts1-YYYY-MM.token ~/Credentials/DARPA-shift-tokens/`
then `chmod 600`. Check the copy is the same token by comparing fingerprints
(the fingerprint is public; the secret is not printed):

```bash
# on the Mac
darpa-spill-shift info --token-file ~/Credentials/DARPA-shift-tokens/dts1-2026-11.token
# on the gateway
darpa-spill-shift info --token-file ~/anorman-fdp/tokens/dts1-2026-11.token
```

Tokens are never overwritten by the pipeline. Do not delete, regenerate or
edit one by hand: a different token for a month that already has shifted
files makes that month's hourly and daily copies inconsistent. The token is
chosen by the month of the **data**, so the 23:00 hour of 31 October,
processed after midnight, still uses `dts1-2026-10.token`.

Creating a token by hand (e.g. ahead of a backfill; it is never overwritten
if it exists):

```bash
darpa-spill-publish token --month 2026-09
darpa-spill-publish token --next
```

## 7. Alerts: what they mean, what to do

A condition is mailed at once, then again after an hour
(`alerts.min_interval`), two hours, four, ... at most once a day
(`alerts.max_interval`) while it lasts; the back-off resets when it clears,
so a new occurrence is mailed at once. `upload failed: fdp-put exit 4` is
not mailed while `kinit needed` or `bearer token refresh failed` is out. On
a full disk alerts still go out (rate-limited by a stamp file in `/tmp`).
The same lines are in the logs under `paths.log_dir` and in `runs` in the
state database. Running the tool off the gateway with the example config
mails for real; add `--set alerts.enabled=false` there.

| Subject `[darpa-spill-publish] ...` | Meaning | What to do |
|---|---|---|
| `kinit needed` | No valid Kerberos ticket in the cache, or it has expired past its renewable lifetime. The body has the exact command. | [Section 5](#5-weekly-renew-the-kerberos-ticket). |
| `Kerberos ticket must be re-created by <time>` | Renewable lifetime ends within 24 h. | Section 5, before that time. |
| `Kerberos renewal failed` | `kinit -R` failed. | Section 5. |
| `bearer token refresh failed` | `htgettoken` could not get a token (Kerberos gone, Vault down, SSO account wrong). After a failure in SSO password mode that mode is held off for 12 h (`credentials.sso_backoff_hours`) or until `.env` changes, so a wrong password is not replayed every 30 min. | `klist -c FILE:...krb5cc_anorman`; if expired, section 5. Then `darpa-spill-publish -v creds` and read the error. If Kerberos is fine but Vault refuses it, fill in the non-MFA account in `~/anorman-fdp/.env` (`FERMI_USER`, `FERMI_PASSWORD`, mode 0600) so the SSO fallback is used. |
| `upload failed: fdp-put exit 4 (authentication)` | The bearer token was rejected. Upload stopped for this run. | As above (`creds -v`), then `darpa-spill-publish -v upload`. |
| `upload failed: fdp-put exit 7 (network/TLS)` | Network or certificate problem reaching amsc.fnal.gov. Upload stopped for this run. | [TLS / CA, section 11](#tls-and-ca-problems). |
| `upload failed: fdp-put exit 9 (checksum mismatch)` | The copy on FDP does not match the local file. | Retried automatically; if it persists, compare with `fdp-stat --checksum` (section 12) and report to FDP. |
| `upload failed: fdp-put exit 6 (exists)` | A destination file exists and was not skipped. | Compare checksums (section 12). The pipeline never replaces; if the remote copy is wrong, remove it by hand with the FDP tools and run `upload`. |
| `ingest not ready` | A source has not been read up to the end of the hour/day after 5 min (neither its newest event nor its last complete poll reached it), or an enabled source's ingest is not running; those pieces were skipped (exit 75). A quiet beam is *not* a cause: polls without events still count. | Usually transient (TDU slow, server restarted): the next hourly run catches up the last 24 h. If it repeats, check the server and the TDU (section 11); disable a TDU that is gone for good on `/config`. |
| `hourly <label> not published; backfill needed` (or `daily`) | A piece left the catch-up window (24 h / 3 days) without being completed; no run will retry it. | Fix the cause, then run the `backfill` command in the body (section 9). |
| `<job> has been running for <time>` | A job found its previous run still holding its lock after `alerts.stale_job_hours` (3 h): hung. Every later run of it exits 75. | `ps -fp <PID>` from the body; kill it if hung. Subprocesses have timeouts (`tools.dump_timeout`, `fdp.timeout`, ...), so this should be rare. |
| `state database unwritable (disk full?)` | `publish.db` cannot be written. Nothing is processed. | [Disk full, section 11](#disk-full). |
| `upload failed: fdp-put exit 12 (partial), again` | Uploads to one directory were partial on two runs in a row: some file fails every time. | `darpa-spill-publish -v upload` and read `fdp-put`'s output; see exit 6/9 above. |
| `uploaded file differs on FDP` | A copy recorded as uploaded has a different size or adler32 on FDP (found by `upload --verify`, or a race). It is **not** removed: dCache forbids overwriting and the pipeline never uses `--replace`. | Compare (section 12). If the remote copy is wrong, remove it with the `fdp-rm -f` command in the body, then `darpa-spill-publish -v upload --verify --since <date>`: the copy is then missing, goes back to pending, and is sent again. |
| `cannot remove an incomplete upload on FDP` | `fdp-rm -f` failed on an empty entry left by a failed PUT. Until it is gone `--skip-existing` skips the file. | Retried every upload run. If it persists: `source ~/anorman-fdp/env.sh; fdp-rm -f <path from the body>`, then `darpa-spill-publish -v upload`. |
| `upload failed: fdp-put exit 125 (env script unreadable)` | `fdp.env_script` is missing or unreadable. | Check `~/anorman-fdp/env.sh`. |
| `darpa-spill-dump failed` | The dump exited with an error (server down, timeout, disk full). | Read the body; fix the cause; the next run retries (section 8). |
| `new shift token created - back it up` | A `dts1` token was made for a data month. | [Section 6](#6-monthly-back-up-the-shift-token), now. |
| `cannot create shift token` | `darpa-spill-shift token` failed (directory permissions, disk). Files of that month are not shifted. | Check `~/anorman-fdp/tokens` (mode 0700, writable); `darpa-spill-publish -v token --month YYYY-MM`. |
| `darpa-spill-shift failed` | A raw file could not be shifted. | Read the body; the next hourly run retries anything dumped but not shifted. |
| `disk NN% full` | The file system holding `/data/DARPA-spilldata` passed 85 %. | [Disk full, section 11](#disk-full). |

A single partial upload (exit 12) is not alerted: the `fdp-stat` that
follows marks the files that arrived intact, removes the empty entries the
failed PUTs left (those rows show `removed incomplete remote copy ...` and
`error` on the status page), and the next `upload` run sends them again. Two
in a row for one directory are mailed.

Test the mail path: `echo test | mailx -s "[darpa-spill-publish] test alert" anorman@fnal.gov`.

## 8. Re-running a failed hour or day

A piece is *complete* when every stream and format is dumped and shifted.
Incomplete pieces are retried automatically: `hourly` looks back 24 hours,
`daily` 3 days, and `upload` retries every upload. To redo something older,
or now:

```bash
# one hour (local time, or the label from the file name / status page)
darpa-spill-publish -v hourly --hour 2026-10-22T13
darpa-spill-publish -v hourly --hour 2026-11-01T01-0600       # the second 01:00 on 1 Nov

# only that hour, without the 24-hour catch-up
darpa-spill-publish -v --set hourly.catchup_hours=1 hourly --hour 2026-10-22T13

# one day (and the 3 before it, if incomplete); only that day:
darpa-spill-publish -v daily --date 2026-10-21
darpa-spill-publish -v --set daily.catchup_days=0 daily --date 2026-10-21

# uploads only
darpa-spill-publish -v upload

# re-check files already recorded uploaded (size + adler32 via fdp-stat),
# repair empty entries left by failed PUTs, then upload what is pending;
# dry run first (prints the fdp-stat commands, runs nothing)
darpa-spill-publish -n upload --verify --since 2026-10-21
darpa-spill-publish -v upload --verify --since 2026-10-21

# see first what it would do (reads the ingest gate once; does not wait)
darpa-spill-publish -n hourly --hour 2026-10-22T13
```

Only what is missing is redone: a recorded raw file is never dumped again,
and if only the JSON of a piece is missing, only the JSON is dumped.

**Replacing a file that was written but is wrong** is deliberately manual,
and only possible if it has **not** been uploaded (FDP forbids overwriting).
Check with section 12 that `raw_uploaded_at` and `shifted_uploaded_at` are
empty, then remove the local files and the row, and re-run:

```bash
F=/data/DARPA-spilldata/raw/oct/2026-oct-22/nd-all-2026-10-22T13-0500.csv
rm -f "$F" /data/DARPA-spilldata/shifted/oct/2026-oct-22/nd-all-2026-10-22T13-0500.shifted.csv
python3 -c 'import sqlite3,sys; d=sqlite3.connect(sys.argv[1]); d.execute("DELETE FROM files WHERE raw_path=? AND raw_uploaded_at IS NULL AND shifted_uploaded_at IS NULL", (sys.argv[2],)); d.commit()' \
    /data/DARPA-spilldata/publish/state/publish.db "$F"
darpa-spill-publish -v hourly --hour 2026-10-22T13
```

Do this only while no job is running: `pgrep -fa darpa-spill-publish` should
print nothing. (The `*.lock` files in `state/` always exist; a lock is held,
not created.)

## 9. Backfill

The archive starts on 2026-09-17. `backfill` processes every **whole** hour
and day in the range (partial ones at either end are dropped; the end is
clamped to now), with one `darpa-spill-dump` call per stream per kind:

```bash
# everything, dumped and shifted only; long, so detach it
nohup darpa-spill-publish -v backfill --start 2026-09-17 --end now --no-upload \
    > /data/DARPA-spilldata/publish/logs/backfill.log 2>&1 &

# then upload (the 'upload' cron job, four times an hour, would also get there)
darpa-spill-publish -v upload

# only daily files for September; only hourly files for one week
darpa-spill-publish -v backfill --start 2026-09-17 --end 2026-09-30 --no-hourly
darpa-spill-publish -v backfill --start 2026-10-01 --end 2026-10-07 --no-daily
```

Sizes for the full range: about 860 hours × 3 streams × 2 formats, ~160 GB
locally and ~80 GB to FDP. Re-running a backfill is safe: complete pieces
are skipped.

`backfill`, `hourly` and `daily` share `state/dump.lock` around their dump
and shift stage, so they never write the same piece at once: while a long
backfill holds it, the cron runs wait up to 30 minutes and then exit 75
(result `busy`), and the catch-up windows pick their hours up afterwards. A
backfill longer than the catch-up window can therefore leave recent hours to
an `expired ... backfill needed` alert; running it before the cron lines are
installed, or with them commented out, avoids that. Uploads are serialised
by `upload.lock` in any case.

Make sure the shift tokens for every month in the range exist and are backed
up first (`darpa-spill-publish token --month 2026-09` etc.; section 6).

## 10. Restoring (inverting) a shifted file

You need the token of the file's **data month** (the month in its label).
Restore on the gateway, or on the Mac with the backed-up token:

```bash
darpa-spill-shift info --token-file ~/anorman-fdp/tokens/dts1-2026-10.token \
    /data/DARPA-spilldata/shifted/oct/2026-oct-22/nd-all-2026-10-22T13-0500.shifted.csv
                                     # says whether this token matches the file
darpa-spill-shift invert --token-file ~/anorman-fdp/tokens/dts1-2026-10.token \
    /data/DARPA-spilldata/shifted/oct/2026-oct-22/nd-all-2026-10-22T13-0500.shifted.csv \
    -o /tmp/nd-all-2026-10-22T13-0500.restored.csv
cmp /tmp/nd-all-2026-10-22T13-0500.restored.csv \
    /data/DARPA-spilldata/raw/oct/2026-oct-22/nd-all-2026-10-22T13-0500.csv && echo identical
```

Without `-o`, the output is `NAME.restored.EXT` beside the input. Inverting
with the wrong token is refused. On the Mac, after copying the shifted file
over with `scp`:

```bash
darpa-spill-shift invert --token-file ~/Credentials/DARPA-shift-tokens/dts1-2026-10.token \
    nd-all-2026-10-22T13-0500.shifted.csv
```

(As of 2026-10-22, downloads from FDP fail at the dCache pool's TLS
handshake; take shifted files from the gateway's `shifted/` tree until FDP
fixes it.)

## 11. Recovery

Start with `darpa-spill-publish check`: one line per check, `ok` or `FAIL`.

```
server        ok    https://novadaq-near-gateway-01.fnal.gov:8443/api/health: ok
ca bundle     ok    /home/novadaq/DARPA/DARPA-SpillServer/tls/cert.pem
data root     ok    /data/DARPA-spilldata
state dir     ok    /data/DARPA-spilldata/publish/state
status dir    ok    /data/DARPA-spilldata/publish/status
token dir     ok    /home/novadaq/anorman-fdp/tokens mode 0700
...
```

### Server down

The watchdog cron job restarts `darpa-spill-server` within 5 minutes of
`/api/health` failing. By hand:

```bash
cd ~/DARPA/DARPA-SpillServer
./stop.sh
./start.sh                # uses ./spillserver.yaml; waits for /api/health
tail -50 run/spillserver.log
curl -s --cacert tls/cert.pem https://novadaq-near-gateway-01.fnal.gov:8443/api/health
```

Use the FQDN, not `localhost` (which hangs on this host, D18), and
`/api/sources`, not `/api/status` (which takes minutes):

```bash
curl -s --cacert tls/cert.pem https://novadaq-near-gateway-01.fnal.gov:8443/api/sources | python3 -m json.tool
```

While the server is down, hourly runs fail (`darpa-spill-dump failed`) or
exit 75. Ingest backfills up to an hour of TDU history after a restart; once
the watermarks pass, the next hourly run catches up the last 24 hours and
the next daily run the last 3 days. For a longer outage, run a backfill over
the gap (section 9). Events the TDU no longer holds are lost; the files for
those hours will be thin, not missing.

### TLS and CA problems

**Publisher → server.** `darpa-spill-dump` and the ingest check verify the
server with its own certificate, `server.ca_bundle` =
`~/DARPA/DARPA-SpillServer/tls/cert.pem`. If the server's certificate is
re-issued in place, nothing changes; if it moves, update `server.ca_bundle`
in `~/DARPA/publish/publish.yaml`. `check` shows a TLS error under `server`.
`--set server.insecure=true` skips verification for a one-off run; do not
leave it on.

**Publisher → FDP (amsc.fnal.gov:2880)** fails with `fdp-put` exit 7. This
happened on 2026-10-21 when AMSC's certificate was re-issued under a root not
in the host bundles (D12). Find the issuer and test the CA directory:

```bash
openssl s_client -connect amsc.fnal.gov:2880 -CApath ~/anorman-fdp/certificates \
    -showcerts </dev/null 2>&1 | grep -E 'issuer=|Verify return code'
```

Add the missing CA's PEM to `~/anorman-fdp/certificates/`, check its
fingerprint (`openssl x509 -noout -fingerprint -sha256 -in FILE.pem`) against
the CA's published value, rehash, and test:

```bash
c_rehash ~/anorman-fdp/certificates        # or: openssl rehash ~/anorman-fdp/certificates
source ~/anorman-fdp/env.sh && fdp-ls /amsc/novaend
darpa-spill-publish -v upload
```

### Token expired

- **Kerberos** (`kinit needed`): section 5.
- **FDP bearer token** (`fdp-put` exit 4, `bearer token refresh failed`):
  `darpa-spill-publish -v creds`. It runs, inside `env.sh`,
  `htgettoken -a htvaultprod.fnal.gov -i amsc -r novaendwrite --minsecs=3600 --nooidc -o ~/anorman-fdp/tokens/fdp_bt_u46550_amsc_novaendwrite`
  (Vault re-authenticates with the Kerberos ticket). With a filled-in
  `~/anorman-fdp/.env`, `--nooidc` becomes `--ssoapprove=password` and Vault
  can fall back to SSO. Then `source ~/anorman-fdp/env.sh && fdp-token show`
  and `darpa-spill-publish -v upload`.
- **Vault token** (`/tmp/vt_u46550`, age on the status page): renewed by
  `htgettoken` as above. If `/tmp` was cleaned, the next `creds` run makes a
  new one from Kerberos.
- **Shift token** never expires; a *missing* one is created automatically
  for a month with no shifted files yet. If a month that already has shifted
  files lost its token, restore it from the Mac copy (section 6) with
  `scp` in the other direction and `chmod 600`; do not let the pipeline
  create a new one (stop cron first if needed).

### Disk full

Alert `disk NN% full` at 85 % (D16). All local copies are kept; at about
4.6 GB/day there are months of headroom from 766 GB free, so check first what
grew:

```bash
df -h /data
du -sh /data/DARPA-spilldata/{raw,shifted,publish}
```

Files that are on FDP in both copies can be removed or compressed locally;
the database still records them, so they are not dumped or uploaded again.
List them (section 12) with
`WHERE raw_uploaded_at IS NOT NULL AND shifted_uploaded_at IS NOT NULL`.
Never remove a file whose upload is pending, or a shift token.

If the disk filled during a run, `darpa-spill-dump` left no partial file (it
writes `.part` and renames when complete): free space, then re-run as in
section 8. Logs are rotated by the user logrotate job
(`deploy/gateway/logrotate.conf`); `run/spillserver.log` is on `/home`, not
`/data`.

### State database lost or damaged

`publish.db` can be rebuilt: raw files found on disk are adopted on the
next run, re-recorded and shifted again with the same token (which gives the
same shifted file), and their uploads are re-tried with `--skip-existing`,
which skips what is already on FDP. Move the damaged
database aside rather than deleting it, then run `darpa-spill-publish -v
backfill` over the affected range.

## 12. Looking inside the state database

`/data/DARPA-spilldata/publish/state/publish.db` (SQLite, tables `files`,
`runs`, `alerts`). Read-only queries with the stdlib:

```bash
DB=/data/DARPA-spilldata/publish/state/publish.db
q() { python3 -c 'import sqlite3,sys; d=sqlite3.connect("file:"+sys.argv[1]+"?mode=ro", uri=True)
for r in d.execute(sys.argv[2]): print(*r, sep=" | ")' "$DB" "$1"; }

q "SELECT job, started_at, result FROM runs ORDER BY id DESC LIMIT 10"
q "SELECT raw_path, last_error FROM files WHERE last_error IS NOT NULL ORDER BY updated_at DESC LIMIT 20"
q "SELECT count(*) FROM files WHERE (dumped_at IS NOT NULL AND raw_uploaded_at IS NULL) OR (shifted_at IS NOT NULL AND shifted_uploaded_at IS NULL)"
q "SELECT stream, fmt, rows, bytes, dumped_at, shifted_at, raw_uploaded_at, shifted_uploaded_at FROM files WHERE label = '2026-10-22T13-0500'"
q "SELECT key, last_sent, count FROM alerts"
```

`upload --verify` (section 8) does this comparison for every uploaded file;
the cached local value is in `raw_adler32`/`shifted_adler32`:

```bash
q "SELECT raw_path, raw_adler32, shifted_adler32 FROM files WHERE label = '2026-10-22T13-0500'"
```

To compare a local file with its FDP copy by hand (adler32, as
`fdp-stat --checksum` reports it):

```bash
python3 -c 'import sys,zlib; a=1
with open(sys.argv[1],"rb") as f:
    for b in iter(lambda: f.read(1<<20), b""): a=zlib.adler32(b,a)
print("%08x" % a)' /data/DARPA-spilldata/raw/oct/2026-oct-22/nd-all-2026-10-22T13-0500.csv
source ~/anorman-fdp/env.sh
fdp-stat --checksum /amsc/novaend/raw/oct/2026-oct-22/nd-all-2026-10-22T13-0500.csv
```
