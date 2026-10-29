#!/usr/bin/env bash
## @file stop.sh
## @brief Stop a darpa-spill-server started by start.sh: SIGTERM, then
##        SIGKILL after a grace period.
## @details
##   Usage:   ./stop.sh
##   Env:     SPILL_PIDFILE  PID file written by start.sh
##            SPILL_GRACE    seconds between SIGTERM and SIGKILL (default 15)
##   Exit:    0 stopped, or was not running (stale PID file removed);
##            1 the process survived SIGKILL.
##   Linux-oriented: liveness is read from /proc; on a host without /proc
##   every PID looks dead and the PID file is just removed.
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
## @fn is_server()
## @brief Whether a PID is a live darpa-spill-server process.
## @param $1 PID to test.
## @return 0 if /proc/$1/cmdline mentions darpa-spill-server, non-zero
##         otherwise.
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
