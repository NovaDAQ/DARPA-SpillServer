#!/usr/bin/env bash
#
# install.sh -- install the darpa-spill-publish deployment on the gateway.
#
# Run ON novadaq-near-gateway-01 as novadaq, from the repository checkout,
# after 'pip install -e .' has put darpa-spill-publish in the venv:
#
#   cd /home/novadaq/DARPA/DARPA-SpillServer
#   deploy/gateway/install.sh --dry-run        # show what would be done
#   deploy/gateway/install.sh                  # install; print the crontab
#   deploy/gateway/install.sh --install-cron   # also merge and install it
#
# Idempotent; safe to re-run after every 'git pull'. It
#   - creates $PUBLISH_HOME (/home/novadaq/DARPA/publish) and its bin/;
#   - copies publish.yaml there only if absent (never overwrites; shows a
#     diff against the repository copy if they differ);
#   - creates /data/DARPA-spilldata/publish/{state,status,logs};
#   - checks the FDP token directory is mode 0700 (tightens it if not) and
#     that the FDP .env file is not readable by others;
#   - installs run-publish.sh, server-watchdog.sh (0755) and logrotate.conf
#     (0644) into $PUBLISH_HOME/bin, replacing older copies;
#   - prints the crontab that results from merging crontab.fragment into the
#     current one. With --install-cron it backs the current crontab up to
#     $PUBLISH_HOME/crontab.backup-<timestamp> and installs the merge.
#
# The merge keeps every existing line (alec's grabSpills.sh in particular)
# and appends only fragment lines not already there. A job line counts as
# present when the same command already appears with any schedule, so a
# schedule edited by hand is left alone rather than duplicated.
#
# Options:
#   -n, --dry-run       print the actions, change nothing
#   --install-cron      install the merged crontab (after a backup)
#   --any-user          do not insist on running as novadaq
#   -h, --help          this text
#
# Environment:
#   PUBLISH_HOME   [/home/novadaq/DARPA/publish]
#   DATA_ROOT      [/data/DARPA-spilldata]
#   FDP_HOME       [/home/novadaq/anorman-fdp]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$SCRIPT_DIR/../.." && pwd)"

PUBLISH_HOME="${PUBLISH_HOME:-/home/novadaq/DARPA/publish}"
DATA_ROOT="${DATA_ROOT:-/data/DARPA-spilldata}"
FDP_HOME="${FDP_HOME:-/home/novadaq/anorman-fdp}"
BIN="$PUBLISH_HOME/bin"
FRAGMENT="$SCRIPT_DIR/crontab.fragment"

DRY_RUN=0
INSTALL_CRON=0
ANY_USER=0
while [ $# -gt 0 ]; do
    case "$1" in
        -n|--dry-run)   DRY_RUN=1; shift ;;
        --install-cron) INSTALL_CRON=1; shift ;;
        --any-user)     ANY_USER=1; shift ;;
        -h|--help)      sed -n '2,/^$/s/^# \{0,1\}//p' "$0"; exit 0 ;;
        *)              echo "error: unknown option $1 (see --help)" >&2; exit 2 ;;
    esac
done

WARNINGS=0
say()  { echo ">> $*"; }
warn() { echo "warning: $*" >&2; WARNINGS=$((WARNINGS + 1)); }

# Run "$@", or only print it under --dry-run.
run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "   [dry-run] $*"
    else
        "$@"
    fi
}

if [ "$ANY_USER" -eq 0 ] && [ "$(id -un)" != novadaq ]; then
    echo "error: run this as novadaq on the gateway (or pass --any-user)" >&2
    exit 1
fi
for f in publish.yaml run-publish.sh server-watchdog.sh backfill-history.sh logrotate.conf crontab.fragment; do
    if [ ! -f "$SCRIPT_DIR/$f" ]; then
        echo "error: $SCRIPT_DIR/$f missing; run from a complete checkout" >&2
        exit 1
    fi
done

# --- directories -----------------------------------------------------------

for d in "$PUBLISH_HOME" "$BIN" \
         "$DATA_ROOT/publish/state" "$DATA_ROOT/publish/status" \
         "$DATA_ROOT/publish/logs"; do
    if [ -d "$d" ]; then
        say "exists: $d"
    else
        say "create: $d"
        run mkdir -p "$d"
    fi
done

# --- config: copy once, never overwrite -----------------------------------

if [ -f "$PUBLISH_HOME/publish.yaml" ]; then
    say "keep:   $PUBLISH_HOME/publish.yaml (never overwritten)"
    if ! cmp -s "$SCRIPT_DIR/publish.yaml" "$PUBLISH_HOME/publish.yaml"; then
        say "        it differs from the repository copy:"
        diff -u "$PUBLISH_HOME/publish.yaml" "$SCRIPT_DIR/publish.yaml" \
            | sed 's/^/        /' || true
    fi
else
    say "copy:   publish.yaml -> $PUBLISH_HOME/publish.yaml"
    run install -m 0644 "$SCRIPT_DIR/publish.yaml" "$PUBLISH_HOME/publish.yaml"
fi

# --- scripts: always the repository version ---------------------------------

install_file() {   # mode name
    local src="$SCRIPT_DIR/$2" dst="$BIN/$2"
    if [ -f "$dst" ] && cmp -s "$src" "$dst"; then
        say "same:   $dst"
        # Still fix the mode, e.g. a group-writable logrotate.conf.
        run chmod "$1" "$dst"
    else
        say "install: $2 -> $dst ($1)"
        run install -m "$1" "$src" "$dst"
    fi
}
install_file 0755 run-publish.sh
install_file 0755 server-watchdog.sh
install_file 0755 backfill-history.sh
install_file 0644 logrotate.conf

# --- credentials area -------------------------------------------------------

mode_of() { stat -c '%a' "$1" 2>/dev/null || stat -f '%Lp' "$1"; }

TOKENS="$FDP_HOME/tokens"
if [ -d "$TOKENS" ]; then
    m="$(mode_of "$TOKENS")"
    if [ "$m" = 700 ]; then
        say "ok:     $TOKENS mode 0700"
    else
        warn "$TOKENS is mode 0$m; tightening to 0700 (shift and bearer tokens live there)"
        run chmod 0700 "$TOKENS"
    fi
else
    warn "$TOKENS does not exist; darpa-spill-publish creates it 0700 on first use"
fi
if [ -f "$FDP_HOME/.env" ]; then
    m="$(mode_of "$FDP_HOME/.env")"
    if [ "$m" = 600 ]; then
        say "ok:     $FDP_HOME/.env mode 0600"
    else
        warn "$FDP_HOME/.env is mode 0$m; tightening to 0600 (SSO password)"
        run chmod 0600 "$FDP_HOME/.env"
    fi
fi

# --- prerequisites (reported, not fixed) ----------------------------------

if [ ! -x "$REPO/venv/bin/darpa-spill-publish" ]; then
    warn "$REPO/venv/bin/darpa-spill-publish missing: run 'venv/bin/pip install -e .' in $REPO"
fi
if [ ! -x /usr/sbin/logrotate ]; then
    warn "/usr/sbin/logrotate not found; fix its path in the crontab"
fi
for c in flock curl crontab mailx; do
    command -v "$c" >/dev/null 2>&1 || warn "$c not found in PATH"
done

# --- crontab ----------------------------------------------------------------

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
crontab -l >"$TMP/current" 2>/dev/null || : >"$TMP/current"

# Append fragment lines that are not already present. Job lines are keyed by
# their command (the schedule stripped), comments by the whole line; blank
# lines are dropped. The number of job lines added goes to $TMP/added.
awk -v added="$TMP/added" '
    function key(line,   f, n, i, k) {
        if (line ~ /^[ \t]*#/) return line
        n = split(line, f, /[ \t]+/)
        i = (f[1] ~ /^@/) ? 2 : 6
        k = f[i]; for (i++; i <= n; i++) k = k " " f[i]
        return k
    }
    NR == FNR { if ($0 !~ /^[ \t]*$/) have[key($0)] = 1; print; next }
    /^[ \t]*$/ { next }
    !(key($0) in have) {
        if (!started) { print ""; started = 1 }
        print
        if ($0 !~ /^[ \t]*#/) jobs++
    }
    END { print jobs + 0 > added }
' "$TMP/current" "$FRAGMENT" >"$TMP/merged"
ADDED="$(cat "$TMP/added")"

if grep -q grabSpills.sh "$TMP/current" && ! grep -q grabSpills.sh "$TMP/merged"; then
    echo "error: merged crontab lost the grabSpills.sh line; not installing" >&2
    exit 1
fi

if [ "$ADDED" -eq 0 ]; then
    say "crontab: every job in crontab.fragment is already installed"
    if [ "$INSTALL_CRON" -eq 1 ]; then
        say "crontab: nothing to install"
    fi
elif [ "$INSTALL_CRON" -eq 0 ]; then
    say "crontab: $ADDED job line(s) to add; merged crontab (NOT installed):"
    echo "------------------------------------------------------------------------"
    cat "$TMP/merged"
    echo "------------------------------------------------------------------------"
    say "install it with: $0 --install-cron"
else
    BACKUP="$PUBLISH_HOME/crontab.backup-$(date '+%Y%m%d-%H%M%S')"
    say "crontab: backup -> $BACKUP"
    run cp "$TMP/current" "$BACKUP"
    say "crontab: installing with $ADDED new job line(s)"
    if [ "$DRY_RUN" -eq 1 ]; then
        echo "   [dry-run] crontab $TMP/merged, containing:"
        sed 's/^/   | /' "$TMP/merged"
    else
        crontab "$TMP/merged"
        if grep -q grabSpills.sh "$BACKUP" && ! crontab -l | grep -q grabSpills.sh; then
            echo "error: grabSpills.sh missing after install; restore with: crontab $BACKUP" >&2
            exit 1
        fi
        say "crontab: installed; restore the old one with: crontab $BACKUP"
    fi
fi

# --- next steps -------------------------------------------------------------

cat <<EOF

Next steps:
  $BIN/run-publish.sh check            # preflight; all rows should be OK
  $BIN/run-publish.sh token --next -v  # shift tokens; back them up off-host
  $BIN/server-watchdog.sh -v           # should log "ok"
  /usr/sbin/logrotate -d -s $PUBLISH_HOME/logrotate.state $BIN/logrotate.conf
EOF
if [ "$DRY_RUN" -eq 1 ]; then
    echo "(dry run: nothing was changed)"
fi
if [ "$WARNINGS" -gt 0 ]; then
    echo "$WARNINGS warning(s) above." >&2
fi
