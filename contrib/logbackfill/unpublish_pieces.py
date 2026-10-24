#!/usr/bin/env python3
"""Withdraw published pieces so darpa-spill-publish can publish them again.

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
running, as section 8 requires.

PIECES is a file with one piece per line, ``STREAM KIND LABEL`` (e.g.
``all hourly 2026-10-21T00-0500`` or ``numi daily 2026-10-21``); anything
after the label is ignored.  Both formats of each piece are withdrawn.

Usage::

    python contrib/logbackfill/unpublish_pieces.py -n changed.txt   # show
    python contrib/logbackfill/unpublish_pieces.py changed.txt
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import subprocess
import sys
from typing import List, Sequence

ENV_WRAPPER = ('[ -r "$ENV_SCRIPT" ] || { echo "cannot read env script '
               '$ENV_SCRIPT" >&2; exit 125; }; '
               'source "$ENV_SCRIPT" >/dev/null; exec "$@"')

#: fdp-rm paths per call.
BATCH = 50


def remote(local: str, data_root: str, area: str, dest: str) -> str:
    """The FDP path of *local*: the same layout under *dest*."""
    rel = os.path.relpath(local, os.path.join(data_root, area))
    if rel.startswith(".."):
        raise ValueError("{} is not under {}/{}".format(local, data_root, area))
    return dest.rstrip("/") + "/" + rel.replace(os.sep, "/")


def publish_jobs() -> List[str]:
    """PIDs of running darpa-spill-publish processes.

    Matched on an argument that *is* the program, not ``pgrep -f``, which
    also matches any shell whose command line merely mentions the name.
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


def main(argv: Sequence[str] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Withdraw published pieces so they can be republished.")
    parser.add_argument("pieces", metavar="PIECES",
                        help="file of 'STREAM KIND LABEL' lines")
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="print what would be removed; change nothing")
    parser.add_argument("--state-db",
                        default="/data/DARPA-spilldata/publish/state/publish.db")
    parser.add_argument("--data-root", default="/data/DARPA-spilldata")
    parser.add_argument("--raw-dest", default="/amsc/novaend/raw")
    parser.add_argument("--shifted-dest", default="/amsc/public/novaend/shifted")
    parser.add_argument("--env-script", default="~/anorman-fdp/env.sh")
    parser.add_argument("--token-file",
                        default="~/anorman-fdp/tokens/fdp_bt_u46550_amsc_novaendwrite")
    parser.add_argument("--ca-path", default="~/anorman-fdp/certificates")
    args = parser.parse_args(argv)

    running = publish_jobs()
    if running and not args.dry_run:
        print("error: darpa-spill-publish is running (pid {}); try again when "
              "it is not".format(", ".join(running)), file=sys.stderr)
        return 1

    pieces = []
    with open(args.pieces) as handle:
        for line in handle:
            fields = line.split()
            if len(fields) >= 3 and fields[1] in ("hourly", "daily"):
                pieces.append(tuple(fields[:3]))

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
        remotes.append(remote(raw_path, args.data_root, "raw", args.raw_dest))
        locals_.append(raw_path)
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

    env = dict(os.environ, ENV_SCRIPT=os.path.expanduser(args.env_script))
    token = os.path.expanduser(args.token_file)
    env["FDP_TOKEN_DIR"] = os.path.dirname(token)
    env["FDP_TRANSFER_CA_PATH"] = os.path.expanduser(args.ca_path)
    options = ["--token-file", token, "--ca-path", os.path.expanduser(args.ca_path)]

    def existing(paths: List[str]):
        """The subset of *paths* on FDP, or None if fdp-stat failed.

        fdp-stat lists only what exists and exits 5 if anything is missing.
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
            left = existing(present)
            if proc.returncode != 0 or left is None or left:
                return fail("fdp-rm exit {} on batch starting {}; still there: "
                            "{}\n{}".format(proc.returncode, batch[0], left,
                                            proc.stderr.strip()))
        print("  removed {} file(s); {} of {} checked on FDP".format(
            len(present), i + len(batch), len(remotes)))

    for path in locals_:
        try:
            os.remove(path)
        except FileNotFoundError:
            pass
    with db:
        for raw_path, _ in rows:
            db.execute("DELETE FROM files WHERE raw_path = ?", (raw_path,))
    db.close()
    print("removed {} local file(s) and {} state row(s); now run "
          "backfill-history.sh over the same days".format(len(locals_), len(rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
