#!/usr/bin/env bash
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

# The venv first, so darpa-spill-dump and darpa-spill-shift are the ones
# installed with darpa-spill-publish; the system dirs give klist, kinit,
# mailx and curl. fdp-put and htgettoken come from fdp.env_script.
export PATH="$SPILL_REPO/venv/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export HOME="${HOME:-/home/novadaq}"
export LANG=C.UTF-8
unset LC_ALL

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

cd "$PUBLISH_HOME" 2>/dev/null || cd /

exec "$PUBLISH" -c "$PUBLISH_CONFIG" "$@"
