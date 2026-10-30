"""Exercise the real cron merge without installing jobs or touching credentials."""
from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize("existing", ["", "# Existing jobs\n0 1 * * * /bin/grabSpills.sh\n"])
def test_cron_merge_adds_jobs_once_and_keeps_existing(existing, tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = (root / "deploy/gateway/install.sh").read_text()
    # Run the actual embedded awk, avoiding the installer's deployment actions.
    program = script.split('awk -v added="$TMP/added" \'\n', 1)[1].split(
        '\n\' "$TMP/current"', 1)[0]
    fragment = root / "deploy/gateway/crontab.fragment"
    current = tmp_path / "current"
    added = tmp_path / "added"
    current.write_text(existing)

    def merge():
        return subprocess.run(["awk", "-v", "added=" + str(added), program,
                               str(current), str(fragment)],
                              check=True, capture_output=True, text=True).stdout

    merged = merge()
    jobs = [line for line in fragment.read_text().splitlines()
            if line.strip() and not line.startswith("#")]
    assert int(added.read_text()) == len(jobs)
    assert merged.startswith(existing)
    assert all(merged.count(job) == 1 for job in jobs)
    current.write_text(merged)
    assert merge() == merged
    assert int(added.read_text()) == 0
