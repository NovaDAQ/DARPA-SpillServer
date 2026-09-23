# Configuration

Everything is settable from a YAML file or the command line, as Design.md
requires. Four layers merge, each overriding the one before:

1. built-in defaults
2. a YAML file
3. environment variables prefixed `DARPA_SPILL_`
4. command-line options

Check what a given invocation actually ends up with — it contacts nothing, so
it is safe against a production config:

```console
$ darpa-spill-server -c config/spillserver.yaml --print-config
```

## Finding the file

`--config FILE` (or `DARPA_SPILL_CONFIG`) is used if given, and a path that
does not exist is an error rather than a silent fallback. Otherwise the first
of these that exists is used:

1. `./spillserver.yaml`
2. `~/.config/darpa-spillserver/spillserver.yaml`
3. `/etc/darpa-spillserver/spillserver.yaml`

## Environment variables

`DARPA_SPILL_<SECTION>_<KEY>`, upper case:

```console
$ export DARPA_SPILL_TDU_SOURCES=http://tdu-far-master-ppc-01:8080,http://tdu-far-master-ppc-02:8080
$ export DARPA_SPILL_STORAGE_PATH=/var/lib/darpa-spillserver/spills.db
$ export DARPA_SPILL_QUERY_TIMEZONE=America/Chicago
```

Lists are comma-separated; booleans accept `true/false`, `yes/no`, `on/off`,
`1/0`.

## Unknown keys are errors

A misspelled key stops startup:

```console
$ darpa-spill-server -c bad.yaml
error: unknown setting 'retention_day' in section 'storage' of bad.yaml
(expected one of: ingest_log_keep, path, prune_interval, retention_days, timeout)
```

This is deliberate. A silently ignored `retention_day` is noticed only after
the archive has already been pruned — or has grown without bound.

## Settings

### `server`

| Key | Default | Notes |
|---|---|---|
| `host` | `0.0.0.0` | bind `127.0.0.1` when a reverse proxy fronts it |
| `port` | `8080` | |
| `root_path` | `""` | mount prefix behind a proxy, e.g. `/spills` |
| `cors_origins` | `["*"]` | narrow this once authentication is on |
| `ssl_certfile` | `""` | PEM certificate, intermediates appended; setting it serves HTTPS |
| `ssl_keyfile` | `""` | PEM private key; required whenever `ssl_certfile` is set |

CLI: `--host`, `--port`, `--root-path`, `--cors-origin` (repeatable),
`--ssl-certfile`, `--ssl-keyfile`.

Setting one of the two TLS files without the other is a configuration error.
Both files are opened at startup, so a missing or unreadable key stops the
server with a message naming it. `--print-config` does not open them, so it
works for users who can't read the key.

### `tdu`

| Key | Default | Notes |
|---|---|---|
| `sources` | `[]` | TDUs to record from, each `URL` or `NAME=URL`; see below |
| `base_url` | `""` | a single TDU, the pre-`sources` form; cannot be combined with `sources` |
| `timeout` | `10.0` | per-request seconds, for the quick routes |
| `history_timeout` | `600.0` | seconds for a bulk history read |
| `retries` | `2` | retries per failed request |
| `retry_backoff` | `0.5` | seconds before the first retry; doubles each time |

CLI: `--tdu-source` (repeatable), `--tdu-url`, `--tdu-timeout`, `--tdu-retries`.

With neither `sources` nor `base_url` set, the server records from
`http://tdu-near-master-ppc-01:8080`.

Each source is polled independently, and every event it supplies is tagged
with the source's **name**. Queries select by that name (`source=` on the API,
`--source` on `darpa-spill-query`), and leaving it out returns every source. A
bare URL is named after its host:

```yaml
tdu:
  sources:
    - http://tdu-near-master-ppc-01:8080          # named tdu-near-master-ppc-01
    - near-02=http://tdu-near-master-ppc-02:8080  # named near-02
```

Names are 1–64 letters, digits, `.`, `_` or `-`. The same name or URL listed
twice is an error. A name is permanent once events are archived under it, and
renaming a source starts a new history. To move a source to a replacement TDU,
keep the name and change the URL.

Sources can be enabled, disabled or given a new URL while the server runs, from
the `/config` page or `PATCH /api/sources/{name}`. Changes are saved in the
archive, survive restarts, and take precedence over this file until the source
is reset. The server logs a warning at startup for each such change. Adding or
removing a source is done here, followed by a restart.

### `ingest`

| Key | Default | Notes |
|---|---|---|
| `enabled` | `true` | `false` serves the existing archive read-only |
| `interval` | `1.0` | seconds between polls — see below |
| `batch_limit` | `5000` | maximum records per poll |
| `overlap` | `5.0` | seconds of archived time re-read each poll |
| `backfill` | `3600.0` | seconds requested on a cold start |
| `max_consecutive_errors` | `0` | `0` never gives up |

CLI: `--no-ingest`, `--interval`, `--batch-limit`, `--overlap`, `--backfill`.

**`interval` means different things depending on the TDU.** With the
`/spill_history` route installed it only bounds how stale the archive can be,
because each poll retrieves a whole window. Without it, the TDU can return just
its single newest event per poll, so `interval` bounds how much is *lost* —
measured live, a 1 s interval captures about 20% of events. Lowering it helps
only marginally and hammers the TDU's PowerPC. Install the route instead; see
[`contrib/tduweb/README.md`](../contrib/tduweb/README.md).

**`overlap` costs nothing.** Inserts are idempotent, keyed on
`(nova_time, spill_type, signal_code)`, so re-reading closes the window that a
slow poll, a restart, or clock skew would leave empty.

### `storage`

| Key | Default | Notes |
|---|---|---|
| `path` | `./spills.db` | SQLite file; parent directories are created |
| `timeout` | `15.0` | seconds to wait for a write lock |
| `retention_days` | `0` | `0` keeps everything |
| `prune_interval` | `3600.0` | seconds between retention sweeps |
| `ingest_log_keep` | `1000` | ingest-log rows retained |

CLI: `--database`, `--retention-days`.

Retention is off by default on purpose: the archive is small — a Near Detector
TDU produces tens of megabytes a year — and deleting it cannot be undone.

The database runs in WAL mode, so the ingest writer and API readers never block
each other. Back it up with `sqlite3 spills.db ".backup out.db"` rather than
copying the file while the server is running.

### `query`

| Key | Default | Notes |
|---|---|---|
| `timezone` | `UTC` | zone for client inputs carrying none |
| `default_limit` | `10000` | rows when the client does not say |
| `max_limit` | `1000000` | ceiling on `limit` |
| `max_export_rows` | `5000000` | ceiling on `/api/export` |

CLI: `--timezone`, `--default-limit`, `--max-limit`.

`timezone` decides what `09:15` means. UTC by default so an unconfigured server
never shifts a result silently; set `America/Chicago` where operators speak
Fermilab local time. Clients override per request with `tz=`, and an input with
an explicit offset ignores both.

### `auth`

Disabled by default, as Design.md specifies. See
[DEPLOYMENT.md](DEPLOYMENT.md#enabling-fermilab-single-sign-on) for the full
procedure.

| Key | Default | Notes |
|---|---|---|
| `enabled` | `false` | |
| `provider` | `oidc` | |
| `issuer` | `""` | e.g. `https://pingprod.fnal.gov/idp` |
| `client_id` | `""` | |
| `client_secret` | `""` | prefer `client_secret_file` |
| `client_secret_file` | `""` | path to a file holding the secret |
| `redirect_url` | `""` | must match what is registered with the provider |
| `scopes` | `[openid, profile, email]` | |
| `allowed_groups` | `[]` | empty admits any authenticated account |
| `session_secret` | `""` | `openssl rand -hex 32` |
| `session_cookie` | `darpa_spill_session` | |
| `session_max_age` | `28800` | seconds |

CLI: `--auth`, `--no-auth`, `--oidc-issuer`, `--oidc-client-id`,
`--oidc-client-secret-file`, `--oidc-redirect-url`.

Enabling auth without its settings fails at startup, naming what is missing —
the server will not come up unprotected after being asked for SSO.

### `admin`

Who may change sources at runtime. By default nobody can, and `/config` is
read-only.

| Key | Default | Notes |
|---|---|---|
| `token_file` | `""` | file holding the admin token; `openssl rand -hex 32` |
| `token` | `""` | the token itself; prefer `token_file` |
| `allowed_groups` | `[]` | with `auth.enabled`, signed-in members may edit without the token |

CLI: `--admin-token-file`.

The token is sent in the `X-Admin-Token` header, and the `/config` page asks
for it. It is never put in a cookie, so a page on another site cannot get a
browser to send it. An unreadable `token_file` stops the server at startup.

### `logging`

| Key | Default | Notes |
|---|---|---|
| `level` | `INFO` | |
| `file` | `""` | also write here |
| `format` | `%(asctime)s %(levelname)-8s %(name)s: %(message)s` | |

CLI: `--log-level`, `--log-file`, `-v`.

A log file that cannot be opened produces a warning on stderr and console-only
logging, rather than refusing to serve.

## Worked example

`config/spillserver-near.yaml`, for the Near Detector gateway:

```yaml
server:
  host: 0.0.0.0
  port: 8080

tdu:
  sources:
    - http://tdu-near-master-ppc-01:8080
    - http://tdu-near-master-ppc-02:8080
    - http://tdu-near-master-ppc-03:8080

ingest:
  enabled: true
  interval: 1.0
  backfill: 86400.0

storage:
  path: /var/lib/darpa-spillserver/spills-near.db
  retention_days: 0

query:
  timezone: America/Chicago

admin:
  token_file: /etc/darpa-spillserver/admin_token

logging:
  level: INFO
  file: /var/log/darpa-spillserver/server.log
```

Override one setting for a one-off run without editing the file:

```console
$ darpa-spill-server -c config/spillserver-near.yaml --port 9090 --no-ingest
```
