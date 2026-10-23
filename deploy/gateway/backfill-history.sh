#!/usr/bin/env bash
#
# backfill-history.sh -- publish a historical range one local day at a time.
#
# darpa-spill-publish backfill holds the shared dump lock for a whole run, so
# one call over weeks would keep the cron hourly job waiting for hours.  This
# walks the range a day per call, releasing the lock in between, and retries a
# day whose run found the lock busy (exit 75).
#
# Usage (on the gateway, as novadaq; run it under nohup, it takes hours):
#   backfill-history.sh FIRST_DAY LAST_DAY [extra backfill options]
#   nohup ~/DARPA/publish/bin/backfill-history.sh 2026-09-17 2026-10-22 \
#       >> /data/DARPA-spilldata/publish/logs/backfill.log 2>&1 &
#
# Days are local (America/Chicago) dates, both inclusive.  Each day is
#   run-publish.sh backfill --start DAY --end DAY [extra options]
# (a date-only end includes that day).  Days already complete are skipped by
# darpa-spill-publish itself, so the script can be re-run after a failure.
# Exit status: 0 if every day succeeded, 1 otherwise.

set -uo pipefail

if [ $# -lt 2 ]; then
    echo "usage: $(basename "$0") FIRST_DAY LAST_DAY [backfill options]" >&2
    exit 2
fi
FIRST="$1"
LAST="$2"
shift 2

RUN="${RUN_PUBLISH:-/home/novadaq/DARPA/publish/bin/run-publish.sh}"
RETRIES="${BACKFILL_BUSY_RETRIES:-6}"

failed=0
day="$FIRST"
while [ "$(date -d "$day" +%s)" -le "$(date -d "$LAST" +%s)" ]; do
    echo "== $(date '+%F %T') backfill $day"
    attempt=0
    while :; do
        "$RUN" backfill --start "$day" --end "$day" "$@"
        rc=$?
        if [ "$rc" -ne 75 ] || [ "$attempt" -ge "$RETRIES" ]; then
            break
        fi
        attempt=$((attempt + 1))
        echo "   dump lock busy; retry $attempt of $RETRIES in 60 s"
        sleep 60
    done
    echo "   $day exit $rc"
    case "$rc" in
        0) ;;
        *) failed=$((failed + 1)) ;;
    esac
    day="$(date -d "$day 12:00 next day" +%F)"
done
echo "== $(date '+%F %T') done; $failed day(s) did not finish cleanly"
[ "$failed" -eq 0 ]
