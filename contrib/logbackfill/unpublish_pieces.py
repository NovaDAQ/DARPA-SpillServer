#!/usr/bin/env python3
"""!
@file contrib/logbackfill/unpublish_pieces.py
@brief Withdraw published pieces so darpa-spill-publish can publish them again.

Withdraw published pieces so darpa-spill-publish can publish them again.

Copyright 2010-2026 Andrew Norman for Fermi Forward Discovery Group LLC.
All rights reserved.

A one-off companion to ``backfill_from_nsstduapp_logs.py``.  Once rows are
added to the archive for hours that were already published, the published
files are short.  ``darpa-spill-publish`` never replaces a file that has
been uploaded --- dCache forbids overwriting, and a recorded piece counts as
complete --- so this does by hand, for a list of pieces, what
GATEWAY_OPERATIONS.md section 8 describes for one file:

1. ``fdp-rm -f`` the raw and shifted copies on FDP;
2. remove the local raw and shifted files;
3. delete their rows from the publish state database.

A ``darpa-spill-publish backfill`` (or ``backfill-history.sh``) over the same
range then dumps, shifts and uploads them afresh.

FDP is done first: if it fails, nothing local is touched and the script can
simply be run again.  It refuses to run while any darpa-spill-publish job is
running, as section 8 requires, and holds the publisher dump and upload locks
throughout withdrawal so scheduled jobs cannot begin conflicting work.

PIECES is a file with one piece per line, ``STREAM KIND LABEL`` (e.g.
``all hourly 2026-10-21T00-0500`` or ``numi daily 2026-10-21``); anything
after the label is ignored.  Both formats of each piece are withdrawn.

Usage::

    python contrib/logbackfill/unpublish_pieces.py -n changed.txt   # show
    python contrib/logbackfill/unpublish_pieces.py changed.txt

**Raw versus shifted (blurred) copies.**  Every published piece exists
twice: the *raw* file with true timestamps, uploaded to the group-only
``--raw-dest`` (``/amsc/novaend/raw``), and the *shifted* file, the raw file
passed through ``darpa-spill-shift apply`` (transform dts1: each event's
time moved by a keyed pseudorandom offset, Gaussian sigma 50 us), uploaded
to the anonymously readable ``--shifted-dest``
(``/amsc/public/novaend/shifted``).  Both destinations mirror the local
``{data_root}/raw/...`` and ``{data_root}/shifted/...`` trees.  The shifted
copy is derived from the raw one, so once the raw piece is short the shifted
piece is short too, and both must be withdrawn together; withdrawing only
the raw copy would leave a public blurred file that disagrees with its
eventual raw replacement.

Re-shifting the regenerated raw file does not weaken the blur.  The offset
of an event is a function of the month's dts1 token and the event's
``(source, event_number)`` only.  Rows the TDU supplied are never altered by
the log backfill, so the new shifted file carries the *same* offsets for
events that were already public, and no one ends up holding two
independently blurred copies of one event to average against each other
(plan D7).  The caveat is a log-backfilled row that was published and then
*renumbered* by a later backfill run: its offset changes, and the withdrawn
and republished blurred copies of it differ.  The monthly tokens are not
touched here.

A shifted copy may be absent (``shifted_path`` NULL: the piece was dumped
but never shifted) or present locally but never uploaded; both cases are
handled by skipping what does not exist.

Exit status: 0 success (or dry run); 1 a publish job is running, or FDP
failed (nothing local changed).
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import fcntl
import json
import os
import sqlite3
import subprocess
import sys
from typing import List, Sequence

## bash -c program run around every FDP tool: source the FDP environment
## script (which puts fdp-stat/fdp-rm on PATH), then exec the tool with the
## remaining arguments.  Exit 125 marks "env script unreadable" distinctly
## from any status the FDP tools return.
ENV_WRAPPER = ('[ -r "$ENV_SCRIPT" ] || { echo "cannot read env script '
               '$ENV_SCRIPT" >&2; exit 125; }; '
               'source "$ENV_SCRIPT" >/dev/null; exec "$@"')

## fdp-rm paths per call (bounds the command line and the blast radius of
## one failed call).
BATCH = 50


def remote(local: str, data_root: str, area: str, dest: str) -> str:
    """!@brief The FDP path of *local*: the same layout under *dest*.

    Raw and shifted files are mapped independently: a raw file relative to
    ``{data_root}/raw`` lands under the raw destination, a shifted file
    relative to ``{data_root}/shifted`` under the public shifted one.

    @param local      absolute local path of the file.
    @param data_root  publish data root, e.g. /data/DARPA-spilldata.
    @param area       "raw" or "shifted": the subtree *local* must be in.
    @param dest       FDP directory that mirrors that subtree.
    @return the FDP path, with "/" separators.
    @throws ValueError  *local* is not under ``data_root/area``.
    """
    rel = os.path.relpath(local, os.path.join(data_root, area))
    if rel.startswith(".."):
        raise ValueError("{} is not under {}/{}".format(local, data_root, area))
    return dest.rstrip("/") + "/" + rel.replace(os.sep, "/")


def publish_jobs() -> List[str]:
    """!@brief PIDs of running darpa-spill-publish processes.

    Matched on an argument that *is* the program, not ``pgrep -f``, which
    also matches any shell whose command line merely mentions the name.
    Only argv[0] and argv[1] are examined, which covers both a direct
    exec and ``python .../darpa-spill-publish``.

    Linux-only: reads /proc.

    @return list of PID strings, excluding this process.
    @throws FileNotFoundError  /proc does not exist (non-Linux host).
    """
    pids = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit() or int(pid) == os.getpid():
            continue
        try:
            with open("/proc/{}/cmdline".format(pid), "rb") as handle:
                args = handle.read().split(b"\0")
        except OSError:
            continue
        if any(os.path.basename(arg) == b"darpa-spill-publish" for arg in args[:2]):
            pids.append(pid)
    return pids


@contextmanager
def publisher_locks(state_db: str):
    """Exclude dump/shift and upload jobs throughout the withdrawal."""
    held = []
    try:
        # Match the publisher's lock files; acquire in this order everywhere
        # a withdrawal needs both. No publisher holds upload while taking dump.
        for name in ("dump", "upload"):
            path = os.path.join(os.path.dirname(state_db) or ".", name + ".lock")
            handle = open(path, "a+")
            held.append(handle)
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise RuntimeError("publisher {} lock is busy; try again later".format(name))
        yield
    finally:
        for handle in reversed(held):
            handle.close()


def main(argv: Sequence[str] = None) -> int:
    """!@brief Withdraw every piece listed in the PIECES file.

    Order is FDP first, then local files, then state rows, so that an FDP
    failure leaves the local state intact and the run can simply be
    repeated.

    @param argv  command-line arguments (default: sys.argv[1:]).
    @return process exit status: 0 success or dry run; 1 publish job
            running or FDP failure.
    @throws ValueError        a state row's path is outside its data area.
    @throws sqlite3.Error     the state database cannot be read or written.
    @throws OSError           PIECES unreadable, or a local file cannot be
                              removed for a reason other than absence.
    """
    parser = argparse.ArgumentParser(
        description="Withdraw published pieces so they can be republished.")
    parser.add_argument("pieces", metavar="PIECES",
                        help="file of 'STREAM KIND LABEL' lines")
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="print what would be removed; change nothing")
    parser.add_argument("--state-db",
                        default="/data/DARPA-spilldata/publish/state/publish.db")
    parser.add_argument("--data-root", default="/data/DARPA-spilldata")
    # Raw (true times) is group-private; shifted (dts1-blurred) is public.
    # The defaults match fdp.raw_dest / fdp.shifted_dest in
    # deploy/gateway/publish.yaml and must stay in step with it.
    parser.add_argument("--raw-dest", default="/amsc/novaend/raw")
    parser.add_argument("--shifted-dest", default="/amsc/public/novaend/shifted")
    parser.add_argument("--env-script", default="~/anorman-fdp/env.sh")
    parser.add_argument("--token-file",
                        default="~/anorman-fdp/tokens/fdp_bt_u46550_amsc_novaendwrite")
    parser.add_argument("--ca-path", default="~/anorman-fdp/certificates")
    args = parser.parse_args(argv)

    # A concurrent publish could re-upload a piece between the fdp-rm and the
    # state-row delete, or record a piece this run is about to remove, so a
    # real withdrawal holds the publisher locks for its whole duration.
    if args.dry_run:
        return _withdraw(args)
    try:
        with publisher_locks(args.state_db):
            return _withdraw(args)
    except (OSError, RuntimeError) as exc:
        print("error: {}".format(exc), file=sys.stderr)
        return 1


def _withdraw(args) -> int:
    """!@brief Remove the listed pieces from FDP, local disk and state.

    @param args parsed command line from main().
    @return 0 on success, 1 on an error reported to stderr.
    """
    running = publish_jobs()
    if running and not args.dry_run:
        print("error: darpa-spill-publish is running (pid {}); try again when "
              "it is not".format(", ".join(running)), file=sys.stderr)
        return 1

    pieces = []
    with open(args.pieces) as handle:
        for line in handle:
            fields = line.split()
            # Lines that are not a recognisable piece (headers, blanks) are
            # ignored silently.
            if len(fields) >= 3 and fields[1] in ("hourly", "daily"):
                pieces.append(tuple(fields[:3]))

    # One state row per format (CSV, JSON) of a piece; each row names the
    # raw file and, once shifted, its blurred counterpart.
    db = sqlite3.connect(args.state_db, timeout=60)
    rows = []
    for stream, kind, label in pieces:
        found = db.execute(
            "SELECT raw_path, shifted_path FROM files WHERE stream = ? AND "
            "kind = ? AND label = ?", (stream, kind, label)).fetchall()
        if not found:
            print("warning: no state rows for {} {} {}".format(stream, kind, label),
                  file=sys.stderr)
        rows.extend(found)

    remotes: List[str] = []
    locals_: List[str] = []
    for raw_path, shifted_path in rows:
        # Raw copy -> private raw destination.
        remotes.append(remote(raw_path, args.data_root, "raw", args.raw_dest))
        locals_.append(raw_path)
        # Shifted copy -> public shifted destination.  NULL when the piece
        # was never shifted; then there is nothing blurred to withdraw.
        if shifted_path:
            remotes.append(remote(shifted_path, args.data_root, "shifted",
                                  args.shifted_dest))
            locals_.append(shifted_path)
    print("{} piece(s), {} state row(s), {} file(s) on FDP and locally".format(
        len(pieces), len(rows), len(remotes)))

    if args.dry_run:
        for path in remotes:
            print("  fdp-rm -f " + path)
        for path in locals_:
            print("  rm " + path)
        return 0

    # The FDP tools take credentials from both the environment and options;
    # both are set so either convention finds the novaendwrite bearer token.
    env = dict(os.environ, ENV_SCRIPT=os.path.expanduser(args.env_script))
    token = os.path.expanduser(args.token_file)
    env["FDP_TOKEN_DIR"] = os.path.dirname(token)
    env["FDP_TRANSFER_CA_PATH"] = os.path.expanduser(args.ca_path)
    options = ["--token-file", token, "--ca-path", os.path.expanduser(args.ca_path)]

    def existing(paths: List[str]):
        """!@brief The subset of *paths* on FDP, or None if fdp-stat failed.

        fdp-stat lists only what exists and exits 5 if anything is missing.

        @param paths  FDP paths to query.
        @return list of paths fdp-stat reports as present (as it spells
                them), or None on any other exit status or unparsable output.
        """
        stat = subprocess.run(
            ["bash", "-c", ENV_WRAPPER, "bash", "fdp-stat", "--json"] + options + paths,
            env=env, capture_output=True, text=True)
        try:
            found = json.loads(stat.stdout) if stat.returncode in (0, 5) else None
        except ValueError:
            found = None
        return None if found is None else [entry["path"] for entry in found]

    def fail(message: str) -> int:
        """!@brief Report an FDP failure before anything local was touched.

        @param message  error text.
        @return 1, the exit status for main to return.
        """
        print("error: " + message, file=sys.stderr)
        print("nothing local was changed; re-run once FDP is fixed",
              file=sys.stderr)
        return 1

    for i in range(0, len(remotes), BATCH):
        batch = remotes[i:i + BATCH]
        # fdp-rm stops at the first path that does not exist --- a shifted
        # copy never made, or a file an interrupted run already removed ---
        # so it is given only the paths fdp-stat finds.
        present = existing(batch)
        if present is None:
            return fail("fdp-stat failed on batch starting " + batch[0])
        if present:
            proc = subprocess.run(
                ["bash", "-c", ENV_WRAPPER, "bash", "fdp-rm", "-f"] + options + present,
                env=env, capture_output=True, text=True)
            # Confirm by re-stat rather than trusting fdp-rm's exit status:
            # local files and state are only removed once FDP is empty.
            left = existing(present)
            if proc.returncode != 0 or left is None or left:
                return fail("fdp-rm exit {} on batch starting {}; still there: "
                            "{}\n{}".format(proc.returncode, batch[0], left,
                                            proc.stderr.strip()))
        print("  removed {} file(s); {} of {} checked on FDP".format(
            len(present), i + len(batch), len(remotes)))

    # FDP is clean; now the local raw and shifted files.  Already-missing
    # files (an earlier interrupted run) are fine.
    for path in locals_:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    # raw_path is the primary key; deleting the row also forgets the shifted
    # copy's state (shifted_path, shifted_uploaded_at), so the next backfill
    # dumps, re-shifts with the same monthly token, and re-uploads both.
    with db:
        for raw_path, _ in rows:
            db.execute("DELETE FROM files WHERE raw_path = ?", (raw_path,))
    db.close()
    print("removed {} local file(s) and {} state row(s); now run "
          "backfill-history.sh over the same days".format(len(locals_), len(rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
