# Deployment

Running the server as a service on `novadaq-near-gateway-01.fnal.gov`, and
turning on Fermilab single sign-on.

## Before anything else: the TDU side

Deployed against an unmodified TDU, the server works but captures only about
**20% of accelerator events**, and cannot see `$74` or `$8F` at all. That is a
limitation of the TDU's current routes, not of this server.

Read [`contrib/tduweb/README.md`](../contrib/tduweb/README.md) first and decide
which of the upstream changes to deploy. Change 1 (`/spill_history`) is
additive, low risk, and removes the event loss entirely.

## Layout

| Path | Contents |
|---|---|
| `/opt/darpa-spillserver` | the checkout and its `venv` |
| `/etc/darpa-spillserver/spillserver.yaml` | configuration |
| `/var/lib/darpa-spillserver/` | the SQLite archive |
| `/var/log/darpa-spillserver/` | log files |

```console
$ sudo mkdir -p /opt/darpa-spillserver /etc/darpa-spillserver \
                /var/lib/darpa-spillserver /var/log/darpa-spillserver
$ sudo useradd --system --home /var/lib/darpa-spillserver --shell /sbin/nologin spillsrv
$ sudo chown -R spillsrv:spillsrv /var/lib/darpa-spillserver /var/log/darpa-spillserver
```

Install the checkout and bootstrap it as that user, then:

```console
$ sudo cp config/spillserver-near.yaml /etc/darpa-spillserver/spillserver.yaml
$ sudo chown root:spillsrv /etc/darpa-spillserver/spillserver.yaml
$ sudo chmod 640 /etc/darpa-spillserver/spillserver.yaml
```

That configuration records from all three Near Detector master TDUs and reads
an admin token from `/etc/darpa-spillserver/admin_token`. The token lets the
`/config` page enable, disable and re-point sources while the server runs.
Create it:

```console
$ openssl rand -hex 32 | sudo tee /etc/darpa-spillserver/admin_token >/dev/null
$ sudo chown root:spillsrv /etc/darpa-spillserver/admin_token
$ sudo chmod 640 /etc/darpa-spillserver/admin_token
```

Share it only with people who should change what is recorded. To keep
`/config` read-only, remove `admin.token_file` from the configuration.

## Upgrading an archive from before multiple sources

Version 1.2 changes the archive's layout so that each event records which TDU
it came from. The first time a 1.2 server (or `darpa-spill-query`) opens an
older archive, it migrates it in place in a single transaction. Every existing
event is tagged with the name of the **first** source in the configuration, so
list the TDU the old server was polling first. The server logs how many events
it tagged.

Stop the old server before upgrading, because it cannot write to a migrated
archive. Take a backup first:

```console
$ sudo systemctl stop darpa-spillserver
$ sqlite3 /var/lib/darpa-spillserver/spills-near.db \
      ".backup /var/lib/darpa-spillserver/spills-near-pre-1.2.db"
$ # update the checkout and the configuration, then
$ sudo systemctl start darpa-spillserver
```

The migration copies the events table once, which takes a few seconds per
million events.

## systemd unit

`/etc/systemd/system/darpa-spillserver.service`:

```ini
[Unit]
Description=DARPA Spill Information Server
Documentation=https://github.com/NovaDAQ/DARPA-SpillServer
After=network-online.target
Wants=network-online.target

[Service]
Type=exec
User=spillsrv
Group=spillsrv
WorkingDirectory=/opt/darpa-spillserver
ExecStart=/opt/darpa-spillserver/venv/bin/darpa-spill-server \
          --config /etc/darpa-spillserver/spillserver.yaml
Restart=on-failure
RestartSec=5s

# The server reads one TDU over HTTP and writes one database. Nothing else
# needs to be reachable from it.
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/darpa-spillserver /var/log/darpa-spillserver
ProtectKernelTunables=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_INET AF_INET6 AF_UNIX

[Install]
WantedBy=multi-user.target
```

```console
$ sudo systemctl daemon-reload
$ sudo systemctl enable --now darpa-spillserver
$ systemctl status darpa-spillserver
$ journalctl -u darpa-spillserver -f
```

## Verifying

```console
$ curl -s localhost:8080/api/health
{"status":"ok","time":"2026-09-22T16:25:46.157483+00:00"}

$ curl -s localhost:8080/api/status | python3 -m json.tool | head -30
```

Three things in `/api/status` are worth watching. Each summarises every
enabled source, and `ingest.sources` breaks them down per source:

* `ingest.running`: a poller is alive for every enabled source.
* `ingest.degraded`: **true means part of the archive is a sample, not a
  history.** `ingest.degraded_sources` says which sources.
* `ingest.consecutive_errors`: non-zero means a TDU is unreachable or
  failing. `ingest.last_error` names the source and says how.

The `/config` page shows the same thing per source, and refreshes every few
seconds.

A simple check for monitoring:

```console
$ curl -sf localhost:8080/api/health >/dev/null || echo "spill server down"
```

## Serving HTTPS

There are two ways to serve HTTPS. The server can terminate TLS itself, or a
reverse proxy in front of it can (see the next section). If there's no proxy,
let the server do it. Point it at the host certificate and key:

```yaml
server:
  host: 0.0.0.0
  port: 8443
  ssl_certfile: /etc/darpa-spillserver/tls/cert.pem   # intermediates appended
  ssl_keyfile: /etc/darpa-spillserver/tls/key.pem
```

```console
$ sudo mkdir -p /etc/darpa-spillserver/tls
$ sudo cp host.crt /etc/darpa-spillserver/tls/cert.pem
$ sudo cp host.key /etc/darpa-spillserver/tls/key.pem
$ sudo chown root:spillsrv /etc/darpa-spillserver/tls/*.pem
$ sudo chmod 644 /etc/darpa-spillserver/tls/cert.pem
$ sudo chmod 640 /etc/darpa-spillserver/tls/key.pem
$ curl -s https://novadaq-near-gateway-01.fnal.gov:8443/api/health
```

The systemd unit above needs no change, because `ProtectSystem=strict` still
leaves `/etc` readable. Ports below 1024, including 443, need root. To serve
on 443 as `spillsrv`, add `AmbientCapabilities=CAP_NET_BIND_SERVICE` to the
unit, or keep 8443 and let a proxy take 443.

To try it out without a real certificate, generate a self-signed one. Browsers
will warn about it, and clients need `curl -k`:

```console
$ openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=$(hostname -f)" \
      -keyout key.pem -out cert.pem
```

When the server terminates TLS itself, the SSO callback is
`https://<host>:<port>/auth/callback`, e.g.
`https://novadaq-near-gateway-01.fnal.gov:8443/auth/callback`.

## Behind a reverse proxy

Set `server.root_path` to the mount prefix so generated URLs and the API docs
resolve correctly:

```yaml
server:
  host: 127.0.0.1
  port: 8080
  root_path: /spills
```

```nginx
location /spills/ {
    proxy_pass         http://127.0.0.1:8080/;
    proxy_set_header   Host              $host;
    proxy_set_header   X-Real-IP         $remote_addr;
    proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header   X-Forwarded-Proto $scheme;
}
```

## Backups

The archive is a single SQLite file in WAL mode. Use SQLite's own backup rather
than copying the file while the server runs:

```console
$ sqlite3 /var/lib/darpa-spillserver/spills-near.db \
      ".backup /backup/spills-$(date +%F).db"
```

It is small — a Near Detector TDU produces tens of megabytes a year — so
`retention_days: 0` (keep everything) is the recommended setting.

## Enabling Fermilab single sign-on

The server ships with authentication off, as Design.md specifies, and every
route already declares its dependency on the authenticator. Turning SSO on is a
configuration change and a restart; no route is rewritten, and none can be
forgotten.

**1. Install the OIDC extra.**

```console
$ /opt/darpa-spillserver/venv/bin/pip install -e '/opt/darpa-spillserver[oidc]'
```

**2. Register the service** with the Fermilab identity provider (PingFederate,
issuer typically `https://pingprod.fnal.gov/idp`). You need a client id, a
client secret, and a registered redirect URI matching `redirect_url` exactly:

```
https://novadaq-near-gateway-01.fnal.gov/auth/callback
```

**3. Store the secrets outside the config file.**

```console
$ printf '%s' 'THE-CLIENT-SECRET' | sudo tee /etc/darpa-spillserver/client_secret >/dev/null
$ openssl rand -hex 32 | sudo tee /etc/darpa-spillserver/session_secret >/dev/null
$ sudo chown root:spillsrv /etc/darpa-spillserver/client_secret /etc/darpa-spillserver/session_secret
$ sudo chmod 640 /etc/darpa-spillserver/client_secret /etc/darpa-spillserver/session_secret
```

**4. Configure.**

```yaml
auth:
  enabled: true
  provider: oidc
  issuer: https://pingprod.fnal.gov/idp
  client_id: darpa-spillserver
  client_secret_file: /etc/darpa-spillserver/client_secret
  redirect_url: https://novadaq-near-gateway-01.fnal.gov/auth/callback
  scopes: [openid, profile, email]
  allowed_groups: []      # empty admits any authenticated account
  session_secret: "<contents of session_secret>"
```

`session_secret` is read from the config rather than a file; put it there via
your configuration management, or set `DARPA_SPILL_AUTH_SESSION_SECRET` in a
systemd drop-in with `EnvironmentFile=`.

**5. Serve over HTTPS.** The session cookie is marked `secure` only when the
request scheme is HTTPS. Terminating TLS at a proxy is fine as long as
`X-Forwarded-Proto` is passed through.

**6. Restart and check.**

```console
$ sudo systemctl restart darpa-spillserver
$ curl -s localhost:8080/api/status | python3 -c 'import json,sys; print(json.load(sys.stdin)["auth"])'
{'enabled': True, 'provider': 'oidc', 'issuer': 'https://pingprod.fnal.gov/idp', ...}

$ curl -s -o /dev/null -w '%{http_code}\n' localhost:8080/api/events
401
```

Misconfigured SSO stops the server at startup with a message naming what is
missing. That is deliberate: a deployment that asked for authentication must
not come up without it.

### What stays public

`/api/health` and `/api/status` remain readable without credentials so
monitoring can scrape them. Neither returns event data or secrets. To close
them too, restrict at the proxy.

### Scripted clients

Browsers use the session cookie from `/auth/login`. Scripts pass an ID token:

```console
$ curl -H "Authorization: Bearer $ID_TOKEN" \
       'https://novadaq-near-gateway-01.fnal.gov/api/events?signal=$74&start=-1h'
```

Tokens are validated against the provider's JWKS, checking issuer and audience.

## Troubleshooting

| Symptom | Cause |
|---|---|
| `ingest.degraded` is true | a TDU in `ingest.degraded_sources` has no `/spill_history` route; see `contrib/tduweb/` |
| `consecutive_errors` climbing | a TDU is unreachable; `ingest.last_error` names it; `curl http://tdu-.../tcr_running` |
| A source ignores the YAML | a runtime change from `/config` is in force; the startup log says so; reset it there |
| `/config` is read-only | no `admin.token_file` configured, or the token was not entered |
| Startup: `... newer than this server's` | the archive was written by a later version; upgrade this one |
| Archive not growing | `TCRMonitor` not running on the TDU — `curl http://tdu-.../tcr_running` |
| `09:15` returns the wrong window | `query.timezone` is UTC; set `America/Chicago` or pass `tz=` |
| `signal` column empty | the hardware stored only the decoded type; see `contrib/tduweb/` change 4 |
| Startup: `unknown setting ...` | a typo in the YAML; the message lists the valid keys |
| Startup: `auth.enabled is true but ...` | SSO settings incomplete; the message names them |
