# Gateway deployment plan: DARPA spill data publishing on novadaq-near-gateway-01

Status tracker and design record for the deployment described in
[`novagateway-deployment.md`](../../novagateway-deployment.md).
Started 2026-10-22. Update the **Status** column as work lands
(`todo` → `doing` → `done`, or `blocked: <reason>`, `user: <action>`).

## 1. Goal

On `novadaq-near-gateway-01.fnal.gov`, as user `novadaq`:

1. Every hour, dump the preceding local hour; every day, dump the preceding
   local day, for three event streams, in CSV and JSON.
2. Shift every file's timestamps with `darpa-spill-shift` (Gaussian,
   σ = 50 µs), using one token per month of data.
3. Upload raw files to `/amsc/novaend` and shifted files to the public area
   on the Fermi Data Platform (FDP), keeping the directory layout.
4. Publish a status web page for the latest transfers.
5. Backfill everything from the start of the archive (2026-09-17).

## 2. Decisions (with reasons)

| # | Decision | Reason / source |
|---|---|---|
| D1 | Data root `/data/DARPA-spilldata` (configurable) | Spec's `/data/DARPA-spill-data` does not exist and `/data` is root-owned; `DARPA-spilldata` exists, owned by novadaq. User choice. |
| D2 | Hour/day boundaries and directory dates in **America/Chicago** | User choice. Matches the spec's "13:05 → 12:00–13:00" example. DST: 23/25-hour days; the repeated fall-back hour is its own file, labelled with its UTC offset. |
| D3 | Layout `raw/<mon>/<YYYY>-<mon>-<DD>/` and `shifted/<mon>/<YYYY>-<mon>-<DD>/`, e.g. `raw/oct/2026-oct-22/` | User choice. `<mon>` = lowercase English abbreviation, independent of locale. Year in the day directory avoids collisions across runs. |
| D4 | File names `nd-<stream>-<label>.<ext>`; hourly label `2026-10-22T13-0500`, daily label `2026-10-22`; shifted copy `nd-<stream>-<label>.shifted.<ext>` | Hourly and daily files share the day directory and are told apart by label. The offset makes the fall-back hour unique. |
| D5 | Streams: `numi` = `--signal '$74' --signal '$A9'`; `all` = no filter; `booster` = `--type BNB --type BNB_TCLK` | User choices. Archive stores type only (`signal_code = -1`), so filters are by spill type. $A9 (NUMI_TCLK) has 0 events so far; `numi` is empty when there is no NuMI beam, and those empty files are kept so gaps are explicit. |
| D6 | Both CSV and JSON, written from one fetch | User choice; one fetch guarantees identical content. |
| D7 | Shift: `dts1` Gaussian σ = 50 µs (3200 ticks), cut 3σ = 150 µs; token `tokens/dts1-YYYY-MM.token`, chosen by **the month of the data**, not the month the job runs | User choice (σ). Data-month selection keeps every event under exactly one token, so hourly and daily copies shift identically and cannot be averaged against each other. |
| D8 | Tokens kept in `~/anorman-fdp/tokens` (0700, files 0600), created ahead for the next month on the 25th and lazily if missing; backed up to the Mac `~/Credentials/DARPA-shift-tokens/` | Spec + user choice. A lost token makes that month irreversible. |
| D9 | Upload with `fdp-put --skip-existing`; raw → `/amsc/novaend/raw/...`; shifted → `/amsc/public/novaend/shifted/...` | The novaendwrite token's scopes are `storage.modify:/amsc/novaend` and `storage.modify:/amsc/public/novaend` only. dCache forbids overwrite; never `--replace` automatically. |
| D10 | Late data: hourly runs at HH:10 after an ingest-watermark check; daily at 00:30 is the authoritative complete copy; hourly files are never replaced | User choice. Ingest backfills up to 3600 s late. |
| D11 | Credentials: renewable Kerberos TGT in a fixed cache, renewed by `kinit -R`; Vault re-auth by Kerberos, falling back to SSO password approval (`htgettoken --ssoapprove=password`, non-MFA account from `.env`); bearer token written to `tokens/` | User choices. Cron cannot see forwarded tickets; `/run/user/46550` vanishes without linger. libfdp never passes `--ssoapprove`, so a refresh step runs htgettoken itself. Human action: `kinit -r 7d` about weekly (alerted a day ahead). |
| D12 | AMSC TLS: user CA directory `~/anorman-fdp/certificates` with `InCommon RSA IGTF Server CA 4` and `emSign Root TLS CA - G1`, via `FDP_TRANSFER_CA_PATH` | User choice. amsc.fnal.gov:2880's certificate was reissued on 2026-10-21 with a root that is not in either CA bundle on the host; all FDP storage calls fail with exit 7 until this is fixed. |
| D13 | Status page served by darpa-spill-server at `:8443/status/` from a new `server.status_dir` setting (release 1.5.0) | User choice. No httpd/nginx, no root, only 8443 known to be reachable. Generated files stay outside the git checkout. |
| D14 | Orchestration in a new `darpa-spill-publish` command in this package (Python, YAML config + env + CLI), driven by cron; fdp-* tools run through `anorman-fdp/env.sh` | Matches the package and CLAUDE.md conventions. cron because systemd user units need linger (root). |
| D15 | Alerts by email to anorman@fnal.gov via `mailx` (postfix → smtp.fnal.gov), rate-limited per condition | User choice. |
| D16 | Keep all local files for now; alert at 85% of `/data` | User choice. About 4.6 GB/day uncompressed for all copies; 766 GB free. |
| D17 | Adjacent ops: `@reboot` + 5-min watchdog cron for darpa-spill-server; user-level logrotate for `run/spillserver.log` (990 MB) and pipeline logs; chmod 600 `.env` and credkeys | User approved. The pipeline depends on the server, which has no supervision. |
| D18 | Use the FQDN `https://novadaq-near-gateway-01.fnal.gov:8443` with `--ca-bundle` the server's own `tls/cert.pem` | `localhost` hangs (IPv6 vs IPv4 bind). `/api/status` takes over 2 minutes: health checks use `/api/sources` (watermarks, ~3 s) and `/api/health`. |

## 3. Architecture

```
cron (novadaq)
 ├─ 3,33  darpa-spill-publish creds     kinit -R; htgettoken (Kerberos → SSO) → tokens/bt_amsc_novaendwrite
 ├─ 10 *  darpa-spill-publish hourly    check watermarks → dump 3 streams × {csv,json} → shift → upload → status
 ├─ 30 0  darpa-spill-publish daily     same, for the preceding local day
 ├─ 0 9 25 darpa-spill-publish token --next   create next month's shift token
 ├─ 7,22,37,52 darpa-spill-publish upload  retry anything not yet uploaded
 ├─ */5   darpa-spill-publish status    re-render status page (credential ages, disk, ingest lag)
 ├─ */5   server-watchdog.sh            restart darpa-spill-server if /api/health fails
 ├─ @reboot server-watchdog.sh
 └─ daily logrotate -s state conf       spillserver.log, publish logs
```

- **Data flow**: darpa-spill-server (8443) → `darpa-spill-dump` (HTTP) → `raw/` →
  `darpa-spill-shift apply` → `shifted/` → `fdp-put` → FDP. Every file's state
  (dumped, rows, bytes, shifted, uploaded raw/public, last error) is a row of
  the SQLite database `{state_dir}/publish.db` (stdlib `sqlite3`, rollback
  journal): table `files` (one row per raw file), `runs` (job, start, finish,
  result, JSON detail), `alerts` (rate limit and back-off per condition) and
  `kv` (SSO back-off, consecutive partial uploads).
- **Status page**: `status/index.html` + `status/status.json`, written by the
  publisher, served at `https://novadaq-near-gateway-01.fnal.gov:8443/status/`.
- **Locking**: `fcntl.flock` per job type; hourly, daily and backfill also
  share `dump.lock` around dump and shift (darpa-spill-dump writes each
  piece to a fixed `FILE.part`), and the upload stage holds `upload.lock` so
  hourly/daily/retry never upload concurrently.
- **Config**: `config/publish.yaml` (example in the repo; the deployed copy in
  `~/DARPA/publish/publish.yaml`), overridden by `DARPA_SPILL_PUBLISH_*`
  environment variables, `.env`, and command-line options (precedence:
  command line > environment > .env > config).

## 4. Work breakdown and status

### WP1. Code and release 1.5.0 (repo, workers)

| ID | Task | Status |
|---|---|---|
| 1.1 | `darpa-spill-dump`: add `{mon}` path placeholder (lowercase month abbreviation, locale-independent); tests; man page and COMMAND_LINE.md | done |
| 1.2 | `darpa-spill-server`: `server.status_dir` (config, `DARPA_SPILL_STATUS_DIR`, CLI) mounted at `/status/` (`html=True`), absent → not mounted; tests; CONFIGURATION.md and man page | done |
| 1.3 | `darpa-spill-publish` command: `hourly`, `daily`, `backfill`, `upload`, `token`, `creds`, `status`, `check`; YAML/env/CLI config; manifest; locks; alerts; exit codes | done |
| 1.4 | Tests for 1.3: paths and labels across DST, data-month token choice, manifest, upload command construction against a fake `fdp-put`, status rendering, alert rate limit, watermark check | done |
| 1.5 | Gateway files in `deploy/gateway/`: `publish.yaml`, `crontab.fragment`, `logrotate.conf`, `server-watchdog.sh`, `install.sh`, README | done |
| 1.6 | Docs: man page `darpa-spill-publish.1`, `docs/GATEWAY_OPERATIONS.md` (runbook: weekly kinit, token backup, recovery), README and docs table | done |
| 1.7 | Independent review of 1.1–1.6; full test suite green; release 1.5.0 (version strings, tag, push). Review done and fixes applied (log 2026-10-22). Release step: bump `__version__`, `pyproject.toml` and the `.TH` line of every `man/*.1` together (`man/darpa-spill-publish.1` already says 1.5.0, the rest 1.4.0) | done: v1.5.0 (2026-10-22), v1.5.1 (2026-10-23: verified uploads, future-event guard) |

### WP2. Gateway preparation (needs the user in places)

| ID | Task | Who | Status |
|---|---|---|---|
| 2.1 | Build `~/anorman-fdp/certificates` (IGTF CA 4 + emSign Root TLS CA - G1), show fingerprints, `c_rehash`; confirm `fdp-ls /amsc` works | Claude, user confirms fingerprints | done: IGTF 1.146 PEMs + 148 system roots, rehashed; fingerprints confirmed; `fdp-ls /amsc` rc 0 |
| 2.2 | Close the `vim .env` session (PID 1293589) | user | done |
| 2.3 | Write `~/anorman-fdp/.env` with `FERMI_USER=CHANGEME`, `FERMI_PASSWORD=CHANGEME` (0600); chmod 600 credkeys | Claude after 2.2 | done |
| 2.4 | Fill in `.env` with the non-MFA SSO account | user | user: fill in when convenient (Kerberos route works without it) |
| 2.5 | Install Fermi-SSO-AccessLib into the FDP venv (gives htgettoken `--ssoapprove`) | Claude | done: requests_fermi_sso 0.3.0 |
| 2.6 | `kinit -r 7d -c FILE:/home/novadaq/anorman-fdp/tokens/krb5cc_anorman anorman@FNAL.GOV` on the gateway | user | done: renew until 2026-10-29 15:33 |
| 2.7 | Test the credential chain (`darpa-spill-publish creds -v`), `fdp-token show`; check or create `/amsc/novaend/raw`, `/amsc/public/novaend/shifted` with a probe file, then remove it | Claude | done (manual): htgettoken `--nooidc` → bearer in tokens/; probe put to both areas rc 0, adler32 verified, removed. Re-test with `creds` after deploy |
| 2.8 | Send a test alert mail | Claude, user confirms receipt | done: received |

### WP3. Deploy 1.5.0 on the gateway

| ID | Task | Status |
|---|---|---|
| 3.1 | `git pull` v1.5.0 in `~/DARPA/DARPA-SpillServer`, `pip install -e .`, run tests there | done: v1.5.1 deployed; 526 tests passed on the gateway at 1.5.0 |
| 3.2 | Install `~/DARPA/publish/` (config, state, status, logs) via `deploy/gateway/install.sh` | done |
| 3.3 | Add `server.status_dir` to `spillserver.yaml`; `stop.sh` / `start.sh` (short ingest gap, covered by the 3600 s backfill) | done: /status/ live |
| 3.4 | Create shift tokens for 2026-09, 2026-10, 2026-11; back them up to the Mac | done: SHA-256 of the Mac copies match |

### WP4. Validation (before cron)

| ID | Task | Status |
|---|---|---|
| 4.1 | One manual `hourly` run: files, row counts vs `/api/events`, shifted files invert byte-identically | done: 2026-10-22T14; counts = /api/events; 6/6 shifted files invert byte-identically |
| 4.2 | One manual `daily` run for 2026-10-21; daily rows = sum of that day's hourly rows | done: 2026-10-21 daily = Σ 24 hourly for all, booster, numi in csv and json (708,313 / 589,952 / 31,961 rows) |
| 4.3 | Upload check: `fdp-stat --checksum` matches local adler32 for raw and shifted | done: fdp-stat size + adler32 verification built into upload (1.5.1); upload --verify 12/12 ok |
| 4.4 | Status page loads at `:8443/status/` and shows the runs and credential state | done |

### WP5. Backfill and go-live

| ID | Task | Status |
|---|---|---|
| 5.1 | Backfill hourly + daily from 2026-09-17 to the last complete hour (about 860 hours × 3 streams × 2 formats, ~160 GB local, ~80 GB raw + shifted upload) | paused (user, 2026-10-23 16:40): published 09-17..09-26, 10-21..; 09-27 partial (raw numi/all hourly only); 09-28..10-20 not published. Resume: backfill-history.sh 2026-09-27 2026-10-20 |
| 5.2 | Install the crontab fragment (alec's `grabSpills.sh` line kept) | done 2026-10-23 10:25 (backup crontab.backup-20261023-102540) |
| 5.3 | Observe 24 h: every hour's files present, uploaded, status page green | todo |
| 5.4 | Final artifacts: this tracker updated, test matrix, runbook | todo |

## 5. Test matrix

| Area | Test | Where | Result |
|---|---|---|---|
| `{mon}` placeholder | all 12 months, locale `C` and others | `tests/test_dump.py` | pass |
| status mount | served when set, 404 when unset, no directory listing | `tests/test_api.py` | pass |
| path layout | normal hour, 23 h and 25 h days, fall-back hour twice, month/year rollover | `tests/test_publish.py` (`test_paths_*`, `test_spring_forward_*`, `test_fall_back_*`, `test_month_and_year_rollover`) | pass |
| token month | 23:00 on the 31st processed after midnight uses the old month's token; never overwritten; concurrent creation | `test_token_is_chosen_by_data_month_not_run_month`, `test_existing_token_is_never_overwritten`, `test_a_token_created_by_a_concurrent_run_is_used`, `tests/test_shift.py::test_token_created_concurrently_is_never_replaced` | pass |
| upload | `fdp-put` exit 0/4/6/7/9/12/125 handling; `--skip-existing`; retry | `test_upload_auth_or_network_failure_then_retry[4,7]`, `test_upload_exists_or_checksum_failure_then_retry[6,9]`, `test_partial_upload_*`, `test_a_batch_partial_twice_is_alerted`, `test_env_script_*` | pass |
| upload verification | uploaded only after `fdp-stat` size + adler32 match; zombie (`size -1`) after a partial removed with `fdp-rm -f` and re-sent; `--skip-existing` over a zombie not marked; missing remote stays pending (per-path stat fallback); recorded copy never removed; `upload --verify [--since]` repairs zombies/missing, reports mismatches; dry run runs no stat/rm; adler32 formatting; schema migration | `test_partial_upload_zombie_is_removed_and_retried`, `test_skip_existing_over_a_zombie_is_not_marked_uploaded`, `test_a_missing_remote_file_stays_pending`, `test_a_good_remote_file_is_uploaded_only_after_the_stat_matches`, `test_a_mismatch_on_a_copy_already_recorded_uploaded_is_never_removed`, `test_upload_verify_*`, `test_adler32_formatting_and_stat_parsing`, `test_old_state_database_gains_the_checksum_columns` | pass |
| watermark gate | stalled source → no dump, alert; quiet beam passes on `polled_through_nova`; source not running blocks; dry run does not wait | `test_stalled_ingest_blocks_the_dump`, `test_quiet_beam_passes_the_gate_on_polled_through`, `test_a_source_that_never_polled_or_is_stopped_blocks`, `test_dry_run_reads_ingest_once_and_does_not_wait`, `tests/test_poller.py::test_a_quiet_tdu_is_still_polled_through` | pass |
| dump without `/api/status` | remote dump with `--timezone` makes no `/api/status` request | `tests/test_dump.py::test_remote_dump_with_timezone_never_asks_for_status` | pass |
| locks, timeouts | shared dump lock; busy job → 75; stale job alert; timeout kills the process group | `test_dumping_jobs_share_the_dump_lock`, `test_a_busy_job_exits_75`, `test_a_hung_dump_is_alerted_and_the_job_reported_stale`, `test_a_timeout_kills_the_whole_process_group` | pass |
| alerts | rate limit, back-off, clearing, full-disk fallback, SSO back-off | `test_alert_*`, `test_alerts_still_go_when_the_state_database_fails`, `test_an_unwritable_state_database_is_mailed`, `test_sso_password_mode_backs_off_after_a_failure` | pass |
| full suite | `venv/bin/python -m pytest -q` | macOS, Python 3.12 (534 passed); `tests/test_publish.py` on 3.9.6 (80 passed) | 534 passed |
| end-to-end | live hourly + daily on the gateway | WP4 | — |
| inversion | shifted gateway files invert byte-identically | WP4.1 | — |

## 6. Risks and open items

- **Credential expiry** (Kerberos renewable limit about 7 days; Vault refresh
  token lifetime unknown). Mitigation: status page shows expiries; email a
  day ahead.
- **SSO page changes** break `--ssoapprove=password`. Kerberos is tried first.
- **`/amsc/public/novaend` may not exist or may not be creatable** by the
  role; FDP admins may need to create it (checked in 2.7).
- **$A9 absent** from the archive; `numi` files contain only $74.
- **Disk**: about 5 months of headroom without pruning or compression.
- Logs and the 6 GB `spills.db` are on NFS `/home`; dumps go through HTTP,
  not direct SQLite reads.

## 7. Log

| Date | Event |
|---|---|
| 2026-10-22 | Survey of the gateway (read-only); decisions D1–D18 made with the user; plan written. |
| 2026-10-22 | WP2: CA dir installed (AMSC TLS fixed); .env placeholders (0600); credkeys 0600; Fermi-SSO-AccessLib in FDP venv; Kerberos cache seeded; bearer token minted via Kerberos/vault; probe uploads to /amsc/novaend and /amsc/public/novaend OK; test mail sent. |
| 2026-10-22 | WP1.7 review fixes: ingest gate also accepts the poller's new `polled_through_nova` (quiet beam no longer blocks); `darpa-spill-dump -s` no longer calls `/api/status` when `--timezone` is given; shared `dump.lock`; subprocess timeouts with process-group kill; stale-job, expired-piece, repeated-partial and disk-full alerts; alert back-off; SSO password back-off; atomic token creation; watchdog lock fd no longer inherited by the server. 526 tests pass (3.12, 3.9.6). |
| 2026-10-22 | Found: existing manual uploads under /amsc/novaend/{numi,booster,all-accelerator}-timing/ — left untouched; pipeline uses raw/ and shifted/ alongside (user). Downloads (GET) fail at the dCache pool TLS handshake (amscstor2305:21769), from both Mac and gateway; uploads unaffected — FDP-side issue to report. |
| 2026-10-22 | Bug (live, darpa-spill-publish 1.5.0): `fdp-put` failed single files with `PUT ...: SSL connect error` (bad dCache pool node), exit 12, leaving zombie namespace entries (`fdp-stat`: `size -1`, no checksum); the next `upload` ran `fdp-put --skip-existing`, which skipped them and exited 0, so 5 broken remote files were recorded uploaded. Fix: a file is recorded uploaded only after `fdp-stat --json` shows its local size and adler32 (local adler32 cached in new `raw_adler32`/`shifted_adler32` columns); missing copies stay pending; incomplete/differing copies not yet recorded are removed with `fdp-rm -f` and re-sent next run; recorded copies are never removed (error + `uploaded file differs on FDP` alert); new `upload --verify [--since YYYY-MM-DD]` re-checks recorded uploads, removes zombies and resets them (and missing copies) to pending, then uploads, to repair the 5 files on the gateway. `check` also probes `fdp-stat` and `fdp-rm`. 534 tests pass. |
| 2026-10-22 | Power outage took the TDUs down 16:16 CDT. On return ppc-01 reported one event at INT64_MAX ticks (year 6576, type FAKE); stored, it became ppc-01's resume point and stopped its ingest. |
| 2026-10-23 | ppc-01 gap 2026-10-22T21:15Z..2026-10-23T14:39Z recovered from the TDU ring with darpa-spill-backfill (221,841 events); 21:15Z..~02:00Z is the outage itself. 1.5.1: poller drops events more than ingest.max_future (1 day) ahead and the resume point ignores them; upload verification narrowed so only size -1 leftovers are ever removed. |
| 2026-10-23 | Crontab installed. Upload verify repaired the 5 zombie files (12/12 ok). Daily validation for 2026-10-21 passed. History backfill 2026-09-17..2026-10-20 started (backfill-history.sh). |
| 2026-10-23 12:03 | First cron hourly (11:10) caught up 2026-10-23 00:00–11:00: 264 steps, 0 errors, 132 files verified on FDP. Backfill 09-17..09-19 done (~22 min/day); 0 file errors; /data 14 GB used. |
| 2026-10-23 15:07 | Backfill days grew to ~50 min (> the 30 min hourly lock wait); the 14:10 hourly gave up (busy). backfill-history.sh now yields to queued cron hourly/daily runs; driver restarted from 2026-09-25 (09-17..09-24 done, 0 errors). ETA ~09:00 CDT 10-24. |
| 2026-10-23 16:12 | 15:10 hourly also gave up (the 09-25 day held the lock to 15:47). 1.5.2: dump-lock wait is now `locks.dump_wait`; gateway set to 3900 s (> one backfill day). Deployed; applies from the 17:10 run. Today 13:00+ hours are queued in the catch-up window. |
| 2026-10-23 16:40 | Backfill stopped at the user's request (driver, run and dump killed by PID; no .part left). 16:10 hourly then caught up 13:00–15:00. 1,992 files raw-uploaded, 1,896 public; 0 errors. |
| 2026-10-23 17:24 | Shift tokens dts1-2026-09/10/11 uploaded to /amsc/novaend/tokens/ (private area) at the user's request; size and adler32 verified. Later months are not uploaded automatically. |
| 2026-10-23 17:26 | At the user's explicit request (consequence confirmed), shift tokens dts1-2026-09/10/11 also published to /amsc/public/novaend/tokens/; verified. The public shifted data for those months is therefore reversible by anyone. |
| 2026-10-23 17:30 | Data-user guide docs/DECODING_DATA.md written; every example run from a clean venv (install from GitHub, anonymous public download, info, invert CSV+JSON byte-identical to raw, wrong-token refusal). Uploaded as DECODING.md to /amsc/novaend and /amsc/public/novaend (adler32 60c2227d). |
| 2026-10-23 22:28 | 17:10 hourly partial: booster dump of 16:00 hit the 120 s read timeout on /api/events (dumps slow that hour); 18:10 run filled it. Hourly 18:10–22:10 all ok; today 00:00–21:00 published; 0 file errors. Monitoring from the Mac lost connectivity ~19:10–22:00 (gateway unaffected). |
