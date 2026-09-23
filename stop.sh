#!/usr/bin/env bash
#
# stop.sh -- stop a DARPA Spill Information Server started by ./start.sh.
#
# Sends SIGTERM, which lets uvicorn finish in-flight requests and the poller
# close the SQLite archive cleanly, then waits for the process to exit. Only
# if it is still running after the grace period is it killed with SIGKILL;
# the archive is in WAL mode, so even that loses at most the last poll.
#
# Usage:
#   ./stop.sh
#
# Environment:
#   SPILL_PIDFILE    PID file written by start.sh (default: run/spillserver.pid)
#   SPILL_GRACE      seconds to wait after SIGTERM (default: 15)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

PIDFILE="${SPILL_PIDFILE:-$SCRIPT_DIR/run/spillserver.pid}"
GRACE="${SPILL_GRACE:-15}"

# Same check as start.sh: never signal a process that merely inherited the PID.
is_server() {
    [ -r "/proc/$1/cmdline" ] && tr '\0' ' ' <"/proc/$1/cmdline" | grep -q darpa-spill-server
}

if [ ! -f "$PIDFILE" ]; then
    echo "darpa-spill-server is not running (no PID file at $PIDFILE)"
    exit 0
fi

pid="$(cat "$PIDFILE")"
if [ -z "$pid" ] || ! is_server "$pid"; then
    echo "darpa-spill-server is not running; removing stale PID file $PIDFILE"
    rm -f "$PIDFILE"
    exit 0
fi

echo ">> Stopping darpa-spill-server (PID $pid)"
kill -TERM "$pid"

for _ in $(seq "$GRACE"); do
    if ! is_server "$pid"; then
        rm -f "$PIDFILE"
        echo ">> Stopped"
        exit 0
    fi
    sleep 1
done

echo "warning: still running after ${GRACE}s; sending SIGKILL" >&2
kill -KILL "$pid" 2>/dev/null || true
sleep 1
if is_server "$pid"; then
    echo "error: PID $pid survived SIGKILL" >&2
    exit 1
fi
rm -f "$PIDFILE"
echo ">> Killed"
