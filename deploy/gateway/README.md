# Gateway deployment files (novadaq-near-gateway-01)

Files that install and schedule `darpa-spill-publish` and supervise
`darpa-spill-server` on `novadaq-near-gateway-01.fnal.gov`, running as
`novadaq` without root. The design record and status tracker is
[`docs/deployment/GATEWAY_DEPLOYMENT_PLAN.md`](../../docs/deployment/GATEWAY_DEPLOYMENT_PLAN.md)
(WP1.5, WP3, WP5.2); decisions are cited below as D1–D18.

## Layout on the gateway

| Path | What |
|---|---|
| `/home/novadaq/DARPA/DARPA-SpillServer` | repository checkout and `venv/` (darpa-spill-server, -dump, -shift, -publish) |
| `/home/novadaq/DARPA/publish/publish.yaml` | deployed config (copied once, then edited in place) |
| `/home/novadaq/DARPA/publish/bin/` | installed copies of `run-publish.sh`, `server-watchdog.sh`, `logrotate.conf` |
| `/home/novadaq/DARPA/publish/logrotate.state` | logrotate's state file (user-level rotation) |
| `/home/novadaq/DARPA/publish/crontab.backup-<timestamp>` | crontab saved before each `--install-cron` |
| `/home/novadaq/DARPA/publish/watchdog.disabled` | if present, the watchdog does nothing |
| `/data/DARPA-spilldata/{raw,shifted}/` | dumped and shifted files (D1, D3) |
| `/data/DARPA-spilldata/publish/state/` | `publish.db`, job locks, watchdog lock and alert stamps |
| `/data/DARPA-spilldata/publish/status/` | `index.html`, `status.json` (served at `:8443/status/`, D13) |
| `/data/DARPA-spilldata/publish/logs/` | one log per cron job |
| `/home/novadaq/anorman-fdp/` | FDP tools (`env.sh`), `certificates/`, `tokens/` (0700), `.env` (0600) |

## Files

| File | Installed as | Purpose |
|---|---|---|
| `publish.yaml` | `~/DARPA/publish/publish.yaml` (only if absent) | Gateway config. Same values as `config/publish.yaml` (which documents every key); header describes the gateway layout. Never overwritten by the installer; a re-install shows a diff if the repository copy has changed. |
| `run-publish.sh` | `~/DARPA/publish/bin/` | cron wrapper: sets `PATH` (venv first), `HOME`, `LANG=C.UTF-8`, `umask 022`, `cd`s to `~/DARPA/publish`, then `exec venv/bin/darpa-spill-publish -c ~/DARPA/publish/publish.yaml "$@"`. Exit status is darpa-spill-publish's (0, 1, 2, 3, 75). |
| `server-watchdog.sh` | `~/DARPA/publish/bin/` | Every 5 min and at boot: `GET /api/health` with `--cacert tls/cert.pem --max-time 20`; on two failures 10 s apart and no live server process (PID file or `pgrep`), runs the repo's `start.sh`. If the process is alive but unhealthy it only logs and mails (no kill, to avoid restart loops on a slow server). `flock` against overlap; alerts rate-limited to one per hour per condition. |
| `logrotate.conf` | `~/DARPA/publish/bin/` | `run/spillserver.log`: daily, 14 kept, `maxsize 200M`; publish logs: weekly, 8 kept. `copytruncate` for both, since the server and cron hold the files open. |
| `crontab.fragment` | merged into novadaq's crontab | Schedule (below). Absolute paths, `>> LOG 2>&1`, no `%`. Appended to the existing crontab, which keeps alec's `grabSpills.sh` line. |
| `install.sh` | run from the checkout | Idempotent installer (below). |

### Schedule (`crontab.fragment`, plan section 3)

| When | Job | Log |
|---|---|---|
| `3,33 * * * *` | `creds` (Kerberos renew, bearer token refresh), shortly before the hourly run | `creds.log` |
| `10 * * * *` | `hourly`: preceding local hour plus catch-up, after the ingest-watermark gate (D10) | `hourly.log` |
| `30 0 * * *` | `daily`: preceding local day | `daily.log` |
| `0 9 25 * *` | `token --next`: next month's shift token (D7, D8) | `token.log` |
| `7,22,37,52 * * * *` | `upload`: retry pending uploads, away from `:10` | `upload.log` |
| `*/5 * * * *` | `status -q`: re-render the status page | `status.log` |
| `*/5 * * * *`, `@reboot` | `server-watchdog.sh` (`--boot-delay 90` at boot) | `watchdog.log` |
| `45 4 * * *` | `logrotate -s ~/DARPA/publish/logrotate.state logrotate.conf` | `logrotate.log` |

Times are host local time (America/Chicago). Jobs that overlap are safe:
each `darpa-spill-publish` job takes its own lock (busy: exit 75, quiet), and
uploads share `upload.lock`. The `@reboot` line logs to `/data`; if `/data`
is not yet mounted when cron starts, that one run is lost and the next
5-minute run starts the server.

## Install order

Prerequisites (WP2 in the plan, already done on the gateway): CA directory
`~/anorman-fdp/certificates`, `~/anorman-fdp/.env` (0600), a renewable
Kerberos ticket in `FILE:/home/novadaq/anorman-fdp/tokens/krb5cc_anorman`.

1. Update the checkout and the venv (release 1.5.0 or later):

   ```bash
   cd /home/novadaq/DARPA/DARPA-SpillServer
   git pull --ff-only
   venv/bin/pip install -e .
   venv/bin/python -m pytest -q
   ```

2. Preview, then install the files (no crontab yet):

   ```bash
   deploy/gateway/install.sh --dry-run
   deploy/gateway/install.sh
   ```

   This creates the directories, copies `publish.yaml` if absent, installs
   the scripts, tightens `tokens/` to 0700 and `.env` to 0600 if needed, and
   prints the merged crontab without installing it.

3. Point darpa-spill-server at the status directory (D13): add
   `status_dir: /data/DARPA-spilldata/publish/status` under `server:` in the
   server's config, then `./stop.sh && ./start.sh` (the ingest gap is
   covered by the server's 3600 s backfill).

4. Check and validate by hand (plan WP3.4, WP4):

   ```bash
   ~/DARPA/publish/bin/run-publish.sh check
   ~/DARPA/publish/bin/run-publish.sh creds -v
   ~/DARPA/publish/bin/run-publish.sh token --month 2026-10 -v   # back tokens up off-host
   ~/DARPA/publish/bin/run-publish.sh hourly -v
   ~/DARPA/publish/bin/server-watchdog.sh -v
   /usr/sbin/logrotate -d -s ~/DARPA/publish/logrotate.state ~/DARPA/publish/bin/logrotate.conf
   ```

5. Backfill (WP5.1), e.g. in `screen`/`tmux`:

   ```bash
   ~/DARPA/publish/bin/run-publish.sh backfill --start 2026-09-17 --end now -v \
       >> /data/DARPA-spilldata/publish/logs/backfill.log 2>&1
   ```

6. Install the crontab (WP5.2):

   ```bash
   deploy/gateway/install.sh --install-cron
   crontab -l | grep grabSpills.sh      # alec's line must still be there
   ```

   The installer saves the current crontab to
   `~/DARPA/publish/crontab.backup-<timestamp>`, appends only the fragment
   lines not already present (a job whose command is present with another
   schedule is left as it is), and refuses to install if `grabSpills.sh`
   would be lost. Undo with `crontab ~/DARPA/publish/crontab.backup-<timestamp>`.

   Manual alternative:

   ```bash
   crontab -l > ~/DARPA/publish/crontab.old
   cat ~/DARPA/publish/crontab.old deploy/gateway/crontab.fragment | crontab -
   ```

Re-run `deploy/gateway/install.sh` after each `git pull`; it updates the
scripts in `bin/` and leaves `publish.yaml` and the crontab alone unless asked.

## Operations notes

- **Pause the watchdog** while stopping the server on purpose:
  `touch ~/DARPA/publish/watchdog.disabled`; remove it afterwards.
- **Overrides** for one run: `run-publish.sh hourly --set fdp.enabled=false -v`,
  or `-n` for a dry run. The wrapper honours `SPILL_REPO`, `PUBLISH_HOME`
  and `PUBLISH_CONFIG`; the watchdog honours the `WATCHDOG_*` variables in
  its header.
- **Help**: `server-watchdog.sh --help`, `install.sh --help`,
  `run-publish.sh --help` (darpa-spill-publish's own help).
