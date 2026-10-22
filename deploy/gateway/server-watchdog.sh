#!/usr/bin/env bash
#
# server-watchdog.sh -- restart darpa-spill-server on the gateway if it is down.
#
# Run from cron every 5 minutes and at @reboot (plan D17). The server has no
# supervisor, and darpa-spill-publish depends on it.
#
#   1. GET $WATCHDOG_URL (/api/health), verified with the server's own
#      tls/cert.pem. If it answers, exit quietly.
#   2. Otherwise wait 10 s and ask again, so a single slow answer during a
#      busy ingest poll does not count.
#   3. Still failing:
#      - no live darpa-spill-server behind run/spillserver.pid: run the
#        repository's start.sh (which removes a stale PID file, starts the
#        server detached and waits for its health check). Log the outcome;
#        mail an alert if start.sh fails.
#      - the PID is a live darpa-spill-server: log and mail an alert, but do
#        NOT kill it. A server that is slow (a long /api/status, a large
#        dump) would otherwise be restarted in a loop; a person decides.
#
# A flock on $WATCHDOG_STATE_DIR/server-watchdog.lock keeps runs from
# overlapping. Alerts for the same condition are mailed at most once per
# $WATCHDOG_ALERT_MINUTES. Creating $PUBLISH_HOME/watchdog.disabled pauses
# the watchdog, e.g. while the server is stopped on purpose:
#
#   touch /home/novadaq/DARPA/publish/watchdog.disabled   # pause
#   rm    /home/novadaq/DARPA/publish/watchdog.disabled   # resume
#
# Usage:
#   server-watchdog.sh [-v] [--boot-delay SECONDS]
#     -v                    also log when the server is healthy
#     --boot-delay SECONDS  sleep first (for @reboot, so /data, the network
#                           and the NFS home are up)
#
# Environment (defaults in brackets):
#   SPILL_REPO              [/home/novadaq/DARPA/DARPA-SpillServer]
#   PUBLISH_HOME            [/home/novadaq/DARPA/publish]
#   WATCHDOG_URL            [https://novadaq-near-gateway-01.fnal.gov:8443/api/health]
#   WATCHDOG_CACERT         [$SPILL_REPO/tls/cert.pem]
#   WATCHDOG_STATE_DIR      [/data/DARPA-spilldata/publish/state]
#   WATCHDOG_EMAIL          [anorman@fnal.gov]   (space-separated)
#   WATCHDOG_MAIL_COMMAND   [mailx]
#   WATCHDOG_ALERT_MINUTES  [60]
#   SPILL_CONFIG, SPILL_PIDFILE, SPILL_LOGFILE   passed through to start.sh
#
# Exit status: 0 healthy or restarted, 1 restart failed or server unhealthy.

set -euo pipefail

SPILL_REPO="${SPILL_REPO:-/home/novadaq/DARPA/DARPA-SpillServer}"
PUBLISH_HOME="${PUBLISH_HOME:-/home/novadaq/DARPA/publish}"
URL="${WATCHDOG_URL:-https://novadaq-near-gateway-01.fnal.gov:8443/api/health}"
CACERT="${WATCHDOG_CACERT:-$SPILL_REPO/tls/cert.pem}"
STATE_DIR="${WATCHDOG_STATE_DIR:-/data/DARPA-spilldata/publish/state}"
EMAIL="${WATCHDOG_EMAIL:-anorman@fnal.gov}"
MAIL_COMMAND="${WATCHDOG_MAIL_COMMAND:-mailx}"
ALERT_MINUTES="${WATCHDOG_ALERT_MINUTES:-60}"
PIDFILE="${SPILL_PIDFILE:-$SPILL_REPO/run/spillserver.pid}"

export PATH="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export LANG=C.UTF-8

VERBOSE=0
BOOT_DELAY=0
while [ $# -gt 0 ]; do
    case "$1" in
        -v|--verbose)   VERBOSE=1; shift ;;
        --boot-delay)   BOOT_DELAY="$2"; shift 2 ;;
        --boot-delay=*) BOOT_DELAY="${1#--boot-delay=}"; shift ;;
        -h|--help)      sed -n '2,/^$/s/^# \{0,1\}//p' "$0"; exit 0 ;;
        *)              echo "error: unknown option $1" >&2; exit 2 ;;
    esac
done

log() {
    echo "$(date '+%Y-%m-%dT%H:%M:%S%z') server-watchdog $*"
}

# Mail $1 (subject) with body $2, at most once per ALERT_MINUTES for the key
# $3. A failing mailer is logged, never fatal.
alert() {
    local stamp="$STATE_DIR/server-watchdog.alert-$3"
    if [ -f "$stamp" ] && [ -n "$(find "$stamp" -mmin "-$ALERT_MINUTES" 2>/dev/null)" ]; then
        log "alert '$1' suppressed (sent within ${ALERT_MINUTES} min)"
        return 0
    fi
    # shellcheck disable=SC2086  # EMAIL is a space-separated list
    if printf '%s\n' "$2" | "$MAIL_COMMAND" -s "[darpa-spill-publish] $1" $EMAIL; then
        touch "$stamp" 2>/dev/null || true
        log "alert mailed: $1"
    else
        log "error: could not mail alert '$1' with $MAIL_COMMAND"
    fi
}

healthy() {
    curl -sf -o /dev/null --max-time 20 --cacert "$CACERT" "$URL"
}

# Same check as start.sh / stop.sh: the PID must be a darpa-spill-server,
# not whatever process later reused the number.
is_server() {
    [ -r "/proc/$1/cmdline" ] && tr '\0' ' ' <"/proc/$1/cmdline" | grep -q darpa-spill-server
}

if [ "$BOOT_DELAY" -gt 0 ]; then
    sleep "$BOOT_DELAY"
fi

if [ -e "$PUBLISH_HOME/watchdog.disabled" ]; then
    [ "$VERBOSE" -eq 1 ] && log "paused ($PUBLISH_HOME/watchdog.disabled exists)"
    exit 0
fi

# /data can be missing early at boot; locking in /tmp is still better than
# not locking at all.
if ! mkdir -p "$STATE_DIR" 2>/dev/null || [ ! -w "$STATE_DIR" ]; then
    STATE_DIR="${TMPDIR:-/tmp}"
fi
# fd 9 is opened without close-on-exec, so every child inherits it; start.sh
# below is run with 9>&- or the detached server would hold the lock for its
# whole lifetime and silence every later run.
exec 9>"$STATE_DIR/server-watchdog.lock"
if ! flock -n 9; then
    # Logged always: a lock that stays busy means the watchdog is not running.
    log "another watchdog run holds $STATE_DIR/server-watchdog.lock; exiting"
    exit 0
fi

if healthy; then
    [ "$VERBOSE" -eq 1 ] && log "ok: $URL"
    exit 0
fi
sleep 10
if healthy; then
    log "health check failed once, then answered; no action"
    exit 0
fi

pid=""
if [ -f "$PIDFILE" ]; then
    pid="$(cat "$PIDFILE" 2>/dev/null || true)"
fi

# A server started by hand without start.sh has no PID file; starting a
# second one would only fail on the port, so treat it as alive too.
if [ -z "$pid" ] || ! is_server "$pid"; then
    other="$(pgrep -u "$(id -u)" -f 'bin/darpa-spill-server' | head -n 1 || true)"
    if [ -n "$other" ]; then
        pid="$other"
    fi
fi

if [ -n "$pid" ] && is_server "$pid"; then
    log "warning: $URL failed twice but darpa-spill-server PID $pid is alive; not restarting"
    alert "darpa-spill-server unhealthy (PID $pid alive)" \
"$URL did not answer twice, 10 s apart (curl --max-time 20), on $(hostname).
darpa-spill-server PID $pid is still running, so the watchdog has not
restarted it. Check $SPILL_REPO/run/spillserver.log, then if needed:

  cd $SPILL_REPO && ./stop.sh && ./start.sh" unhealthy
    exit 1
fi

log "darpa-spill-server is down (${pid:+stale PID $pid, }health check failed twice); running start.sh"
if output="$(cd "$SPILL_REPO" && ./start.sh 9>&- 2>&1)"; then
    printf '%s\n' "$output" | sed 's/^/    /'
    log "restarted darpa-spill-server"
    alert "darpa-spill-server restarted" \
"The watchdog on $(hostname) found darpa-spill-server down and restarted it.
start.sh output:

$output" restarted
    exit 0
fi
printf '%s\n' "$output" | sed 's/^/    /'
log "error: start.sh failed"
alert "darpa-spill-server restart FAILED" \
"The watchdog on $(hostname) found darpa-spill-server down; start.sh failed.
start.sh output:

$output" restart-failed
exit 1
