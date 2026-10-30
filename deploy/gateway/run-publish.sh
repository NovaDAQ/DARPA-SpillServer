#!/usr/bin/env bash
## @file deploy/gateway/run-publish.sh
## @brief cron wrapper: run the venv's darpa-spill-publish with the deployed
##        config in a known environment.
## @details
##   Installed to $PUBLISH_HOME/bin by install.sh.  Every publish stage
##   (dump -> shift -> upload -> status) runs inside the exec'd
##   darpa-spill-publish; this script only fixes PATH, HOME, locale, umask
##   and cwd.  The shift stage produces the timestamp-blurred ("shifted",
##   transform dts1) copy of every raw dump; upload sends raw files to
##   fdp.raw_dest (group-private) and shifted files to fdp.shifted_dest
##   (public), both set in publish.yaml.
##   Usage:   run-publish.sh <subcommand> [options]
##            run-publish.sh hourly -v
##            run-publish.sh backfill --start 2026-10-01 --end 2026-10-01
##   Env:     SPILL_REPO, PUBLISH_HOME, PUBLISH_CONFIG (see below)
##   Exit:    2 no subcommand; 1 darpa-spill-publish not installed;
##            otherwise darpa-spill-publish's own status (0 ok, 1 failure,
##            2 usage/config, 3 partial, 75 temporary, e.g. lock busy or
##            ingest not ready).
#
# run-publish.sh -- cron wrapper for darpa-spill-publish on the gateway.
#
# cron starts jobs with a bare environment (PATH=/usr/bin:/bin, no LANG) in
# the home directory. This sets a known PATH, HOME and locale, moves to a
# directory that always exists, and runs the venv's darpa-spill-publish with
# the deployed config, so each crontab line stays short:
#
#   10 * * * * /home/novadaq/DARPA/publish/bin/run-publish.sh hourly >> LOG 2>&1
#
# Usage:
#   run-publish.sh <subcommand> [options]    e.g. run-publish.sh hourly -v
#                                                 run-publish.sh check
# Everything is passed to darpa-spill-publish after -c CONFIG; a later -c on
# the command line wins. The exit status is darpa-spill-publish's.
#
# Environment:
#   SPILL_REPO       repository checkout (default /home/novadaq/DARPA/DARPA-SpillServer)
#   PUBLISH_HOME     deployed config dir (default /home/novadaq/DARPA/publish)
#   PUBLISH_CONFIG   config file         (default $PUBLISH_HOME/publish.yaml)

set -euo pipefail

SPILL_REPO="${SPILL_REPO:-/home/novadaq/DARPA/DARPA-SpillServer}"
PUBLISH_HOME="${PUBLISH_HOME:-/home/novadaq/DARPA/publish}"
PUBLISH_CONFIG="${PUBLISH_CONFIG:-$PUBLISH_HOME/publish.yaml}"
PUBLISH="$SPILL_REPO/venv/bin/darpa-spill-publish"

# Version matching matters most for darpa-spill-shift: the blur must be
# computed by the same dts1 implementation the publisher was installed
# with, so a re-shift of a piece reproduces the offsets already published.
# The venv first, so darpa-spill-dump and darpa-spill-shift are the ones
# installed with darpa-spill-publish; the system dirs give klist, kinit,
# mailx and curl. fdp-put and htgettoken come from fdp.env_script.
export PATH="$SPILL_REPO/venv/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export HOME="${HOME:-/home/novadaq}"
export LANG=C.UTF-8
unset LC_ALL

# Under 022 the local raw dumps (true timestamps) are world-readable on the
# gateway; their access control is on FDP, where raw goes to the group-only
# destination and only the blurred copies to the public one.
# Files written here (dumps, status page) are readable by others; secrets
# (tokens) are created 0600 in a 0700 directory by the tools themselves.
umask 022

if [ $# -eq 0 ]; then
    echo "usage: $(basename "$0") <subcommand> [options]" >&2
    exit 2
fi
if [ ! -x "$PUBLISH" ]; then
    echo "error: $PUBLISH not found; run 'pip install -e .' in $SPILL_REPO" >&2
    exit 1
fi

# cwd is PUBLISH_HOME, or / if that is unavailable (e.g. NFS home not yet
# mounted), so the job never starts in a directory that may vanish.
cd "$PUBLISH_HOME" 2>/dev/null || cd /

# exec: no wrapper shell remains, so signals and the exit status are
# darpa-spill-publish's own.
exec "$PUBLISH" -c "$PUBLISH_CONFIG" "$@"
