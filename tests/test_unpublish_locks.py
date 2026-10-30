"""Withdrawal shares locks with scheduled publishers; no FDP calls needed."""
import importlib.util
from pathlib import Path
import subprocess
import sys

import pytest


spec = importlib.util.spec_from_file_location(
    "unpublish_pieces", Path(__file__).resolve().parents[1]
    / "contrib/logbackfill/unpublish_pieces.py")
withdrawal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(withdrawal)


def probe(path):
    code = """
import fcntl, sys
with open(sys.argv[1], 'a+') as handle:
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        sys.exit(75)
"""
    return subprocess.run([sys.executable, "-c", code, str(path)]).returncode


def test_withdrawal_holds_both_locks_and_releases_them(tmp_path):
    with withdrawal.publisher_locks(str(tmp_path / "publish.db")):
        assert probe(tmp_path / "dump.lock") == 75
        assert probe(tmp_path / "upload.lock") == 75
    assert probe(tmp_path / "dump.lock") == 0
    assert probe(tmp_path / "upload.lock") == 0


@pytest.mark.parametrize("busy", ["dump", "upload"])
def test_busy_lock_prevents_withdrawal_and_releases_partial_locks(tmp_path, monkeypatch, busy):
    import fcntl

    def must_not_withdraw(args):
        pytest.fail("withdrawal ran without exclusive access")

    monkeypatch.setattr(withdrawal, "_withdraw", must_not_withdraw)
    with open(tmp_path / (busy + ".lock"), "a+") as held:
        fcntl.flock(held, fcntl.LOCK_EX)
        assert withdrawal.main(["--state-db", str(tmp_path / "publish.db"),
                                str(tmp_path / "pieces")]) == 1
    assert probe(tmp_path / "dump.lock") == 0
    assert probe(tmp_path / "upload.lock") == 0
