#!/usr/bin/env bash
#
# start.sh -- start the DARPA Spill Information Server in the background.
#
# For running the server from a checkout without systemd, e.g. on a gateway
# node where you have no root. A service install should use the systemd unit
# in docs/DEPLOYMENT.md instead.
#
# The server is detached from the terminal, so it survives logging out. Its
# PID is written to run/spillserver.pid and its output to run/spillserver.log.
# The script waits for /api/health to answer before reporting success, so a
# bad config or a port already in use is reported here rather than found out
# later. Stop it again with ./stop.sh.
#
# Usage:
#   ./start.sh                                   # ./spillserver.yaml if present,
#                                                # else config/spillserver.yaml
#   ./start.sh -c config/spillserver-near.yaml   # another config file
#   ./start.sh --port 8081                       # extra options are passed
#                                                # to darpa-spill-server
# Environment:
#   SPILL_CONFIG     config file (default: as above)
#   SPILL_PIDFILE    PID file    (default: run/spillserver.pid)
#   SPILL_LOGFILE    output log  (default: run/spillserver.log)
#   SPILL_WAIT       seconds to wait for the health check (default: 30)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

SERVER="$SCRIPT_DIR/venv/bin/darpa-spill-server"
VPY="$SCRIPT_DIR/venv/bin/python"
PIDFILE="${SPILL_PIDFILE:-$SCRIPT_DIR/run/spillserver.pid}"
LOGFILE="${SPILL_LOGFILE:-$SCRIPT_DIR/run/spillserver.log}"
WAIT="${SPILL_WAIT:-30}"

# A -c/--config on the command line wins over SPILL_CONFIG. Without either,
# prefer the untracked local override, as the server's own search does.
if [ -f spillserver.yaml ]; then
    DEFAULT_CONFIG=spillserver.yaml
else
    DEFAULT_CONFIG=config/spillserver.yaml
fi
CONFIG="${SPILL_CONFIG:-$DEFAULT_CONFIG}"
ARGS=()
while [ $# -gt 0 ]; do
    case "$1" in
        -c|--config) CONFIG="$2"; shift 2 ;;
        --config=*)  CONFIG="${1#--config=}"; shift ;;
        *)           ARGS+=("$1"); shift ;;
    esac
done

if [ ! -x "$SERVER" ]; then
    echo "error: $SERVER not found; run ./bootstrap.sh first" >&2
    exit 1
fi
if [ ! -f "$CONFIG" ]; then
    echo "error: config file $CONFIG not found" >&2
    exit 1
fi

# True if $1 is a live darpa-spill-server process. Checking the command line,
# not just that the PID exists, keeps a stale PID file from matching whatever
# unrelated process has since been given the same PID.
is_server() {
    [ -r "/proc/$1/cmdline" ] && tr '\0' ' ' <"/proc/$1/cmdline" | grep -q darpa-spill-server
}

if [ -f "$PIDFILE" ]; then
    pid="$(cat "$PIDFILE")"
    if [ -n "$pid" ] && is_server "$pid"; then
        echo "darpa-spill-server is already running (PID $pid)"
        exit 0
    fi
    echo ">> Removing stale PID file $PIDFILE"
    rm -f "$PIDFILE"
fi

# Ask the server itself where it will listen, so the health check follows the
# config file, the environment and any --host/--port given here. This contacts
# nothing, and fails fast on a config error.
if ! merged="$("$SERVER" -c "$CONFIG" ${ARGS[@]+"${ARGS[@]}"} --print-config)"; then
    echo "error: configuration rejected; nothing started" >&2
    exit 2
fi
read -r SCHEME HOST PORT < <("$VPY" -c '
import json, sys
s = json.load(sys.stdin)["server"]
host = s["host"]
# A wildcard bind is reachable on loopback.
print("https" if s["ssl_certfile"] else "http",
      "127.0.0.1" if host in ("0.0.0.0", "", "::") else host, s["port"])
' <<<"$merged")
URL="$SCHEME://$HOST:$PORT/api/health"

mkdir -p "$(dirname "$PIDFILE")" "$(dirname "$LOGFILE")"

echo ">> Starting darpa-spill-server with $CONFIG"
# Remember where this run's output begins, so a failure shows only that.
LOGSTART=$(( $(wc -l <"$LOGFILE" 2>/dev/null || echo 0) + 1 ))
{
    echo ""
    echo "===== $(date '+%Y-%m-%d %H:%M:%S %Z') start.sh: -c $CONFIG ${ARGS[*]-} ====="
} >>"$LOGFILE"

# setsid puts the server in its own session, so a hangup on this terminal
# does not reach it.
setsid "$SERVER" -c "$CONFIG" ${ARGS[@]+"${ARGS[@]}"} \
    </dev/null >>"$LOGFILE" 2>&1 &
pid=$!
echo "$pid" >"$PIDFILE"

for _ in $(seq "$WAIT"); do
    if ! kill -0 "$pid" 2>/dev/null; then
        rm -f "$PIDFILE"
        echo "error: darpa-spill-server exited during startup; its output ($LOGFILE):" >&2
        tail -n +"$LOGSTART" "$LOGFILE" | tail -n 20 >&2
        exit 1
    fi
    # -k: the certificate names the host, not 127.0.0.1, and this only
    # asks whether the server is up.
    if curl -sfk -o /dev/null --max-time 2 "$URL"; then
        echo ">> darpa-spill-server is up (PID $pid), health check at $URL"
        echo ">> Log: $LOGFILE"
        exit 0
    fi
    sleep 1
done

echo "warning: darpa-spill-server (PID $pid) is running but $URL did not answer" >&2
echo "         within ${WAIT}s; check $LOGFILE" >&2
exit 1
