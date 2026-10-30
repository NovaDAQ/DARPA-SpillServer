#!/usr/bin/env bash
## @file deploy/gateway/backfill-history.sh
## @brief Publish a historical date range one local day per
##        darpa-spill-publish backfill call, yielding to cron jobs.
## @details
##   Each day is dumped, shifted (dts1 blur, the month's token) and uploaded
##   by run-publish.sh backfill.  This is also the re-publish step after
##   contrib/logbackfill/unpublish_pieces.py has withdrawn pieces.
##   Usage:   backfill-history.sh FIRST_DAY LAST_DAY [backfill options]
##            backfill-history.sh 2026-09-17 2026-10-22
##   Args:    FIRST_DAY, LAST_DAY  YYYY-MM-DD local dates, inclusive.
##            Further arguments are passed to darpa-spill-publish backfill.
##   Env:     RUN_PUBLISH            path of run-publish.sh
##            BACKFILL_BUSY_RETRIES  retries of a day that exits 75 (6)
##            PUBLISH_STATE_DIR      where the job lock files live
##            BACKFILL_YIELD_MAX     max seconds to wait for cron jobs (3600)
##   Exit:    0 every day succeeded; 1 at least one day failed; 2 usage.
##   Requires GNU date (date -d) and flock.
#
# backfill-history.sh -- publish a historical range one local day at a time.
#
# darpa-spill-publish backfill holds the shared dump lock for a whole run, so
# one call over weeks would keep the cron hourly job waiting for hours.  This
# walks the range a day per call, releasing the lock in between; before each
# day it waits for a cron hourly or daily run that is queued for the lock, and
# it retries a day whose run found the lock busy (exit 75).
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

# No -e: a failed day must not end the walk; failures are counted instead.
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

STATE="${PUBLISH_STATE_DIR:-/data/DARPA-spilldata/publish/state}"
YIELD_MAX="${BACKFILL_YIELD_MAX:-3600}"

# Let a waiting cron hourly or daily run take the dump lock first.  Those
# jobs hold {state_dir}/<job>.lock while they wait (up to 30 min) for the
# dump lock, and a backfill day takes longer than that, so without this they
# keep giving up ("busy") for as long as the backfill runs.
## @fn yield_to_cron()
## @brief Wait while a cron hourly or daily publish job holds its job lock.
## @details A held lock is detected by failing to take it non-blocking
##          (flock -n FILE true takes and immediately releases it).  The
##          wait budget BACKFILL_YIELD_MAX is shared across both jobs.
## @return always 0; after the budget is spent it proceeds anyway.
yield_to_cron() {
    local waited=0 job
    for job in hourly daily; do
        while [ -f "$STATE/$job.lock" ] && ! flock -n "$STATE/$job.lock" true; do
            if [ "$waited" -eq 0 ]; then
                echo "   waiting for the cron $job run to finish"
            fi
            if [ "$waited" -ge "$YIELD_MAX" ]; then
                echo "   $job still running after $YIELD_MAX s; going ahead"
                return
            fi
            sleep 15
            waited=$((waited + 15))
        done
    done
}

# Compare as epoch seconds; GNU date parses YYYY-MM-DD as local midnight.
failed=0
day="$FIRST"
while [ "$(date -d "$day" +%s)" -le "$(date -d "$LAST" +%s)" ]; do
    yield_to_cron
    echo "== $(date '+%F %T') backfill $day"
    attempt=0
    while :; do
        "$RUN" backfill --start "$day" --end "$day" "$@"
        rc=$?
        if [ "$rc" -ne 75 ] || [ "$attempt" -ge "$RETRIES" ]; then
            break
        fi
        attempt=$((attempt + 1))
        echo "   another backfill or the dump lock is busy; retry $attempt of $RETRIES in 60 s"
        sleep 60
        yield_to_cron
    done
    echo "   $day exit $rc"
    case "$rc" in
        0) ;;
        *) failed=$((failed + 1)) ;;
    esac
    # Step from noon, not midnight, so the date arithmetic stays clear of
    # the local DST transition hours (America/Chicago).
    day="$(date -d "$day 12:00 next day" +%F)"
done
echo "== $(date '+%F %T') done; $failed day(s) did not finish cleanly"
[ "$failed" -eq 0 ]
