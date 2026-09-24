"""``darpa-spill-client`` and ``darpa-spill-client-cpp`` agree byte for byte.

docs/CLIENT.md promises that the same invocation gives identical standard
output and exit status from either program. This runs both against one live
server and compares them. The only differences allowed are the values that
change between two requests a moment apart: generation timestamps, the health
check's clock, and uptime. Those are masked before comparing.

Skipped when the C++ program has not been built.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import ADMIN_TOKEN, BASE, SECOND, SOURCE_A, SOURCE_B

ROOT = Path(__file__).resolve().parent.parent


def _cpp_client():
    explicit = os.environ.get("DARPA_SPILL_CPP_CLIENT")
    candidates = [Path(explicit)] if explicit else []
    for name in ("darpa-spill-client-cpp", "darpa-spill-client-cpp.exe"):
        candidates += [ROOT / "build" / name, ROOT / "build" / "Release" / name]
        candidates += sorted((ROOT / "build").glob("*/" + name))
    return next((c for c in candidates if c.is_file()), None)


CPP = _cpp_client()
pytestmark = pytest.mark.skipif(CPP is None, reason="darpa-spill-client-cpp not built")

#: Values that legitimately differ between two requests.
_VOLATILE = [
    (re.compile(r'"generated":\s*"[^"]*"'), '"generated": "*"'),
    (re.compile(r"(# generated: ).*"), r"\1*"),
    (re.compile(r'"time":\s*"[^"]*"'), '"time": "*"'),
    (re.compile(r"^time: .*$", re.M), "time: *"),
    (re.compile(r'"uptime_seconds":\s*[0-9.]+'), '"uptime_seconds": *'),
    # A source change is stamped in whole seconds; the two runs may straddle one.
    (re.compile(r'"updated_at":\s*[0-9.]+'), '"updated_at": *'),
    (re.compile(r"^updated_at: [0-9.]+$", re.M), "updated_at: *"),
    (re.compile(r"^uptime_seconds: .*$", re.M), "uptime_seconds: *"),
]


def _mask(text):
    for pattern, replacement in _VOLATILE:
        text = pattern.sub(replacement, text)
    return text


def _run(program, url, argv, cwd):
    env = dict(os.environ)
    for name in list(env):
        if name.startswith("DARPA_SPILL_"):
            del env[name]
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(program + ["-u", url] + argv, capture_output=True,
                            cwd=str(cwd), env=env, timeout=60)
    return result.returncode, _mask(result.stdout.decode("utf-8"))


START = "nova:{}".format(BASE)
END = "nova:{}".format(BASE + 120 * SECOND)
WINDOW = ["--start", START, "--end", END]

INVOCATIONS = [
    ["health"],
    ["-f", "json", "health"],
    ["status"],
    ["-f", "json", "status"],
    ["sources"],
    ["-f", "csv", "sources"],
    ["-f", "json", "sources"],
    ["signals"],
    ["-f", "csv", "signals"],
    ["types"],
    ["-f", "csv", "types"],
    ["-f", "json", "types"],
    ["convert", "2026-07-01T09:15:00Z"],
    ["convert", "09:15", ],
    ["--tz", "America/Chicago", "convert", "2026-07-01 09:15"],
    ["-f", "json", "convert", "gps:1466899218"],
    ["time-help"],
    ["latest"],
    ["latest", "--signal", "$74"],
    ["latest", "--source", SOURCE_A, "--source", SOURCE_B],
    ["-f", "json", "latest", "--signal", "$8f"],
    ["events"] + WINDOW,
    ["events"] + WINDOW + ["--signal", "$8f", "--limit", "5", "--offset", "3", "--desc"],
    ["events"] + WINDOW + ["--type", "NUMI", "--columns", "nova_time,utc,gps,signal"],
    ["events"] + WINDOW + ["--source", SOURCE_B, "--signal", "numi"],
    ["-f", "csv", "events"] + WINDOW + ["--signal", "$8F", "--signal", "$74"],
    ["-f", "json", "events"] + WINDOW + ["--limit", "4"],
    ["export"] + WINDOW + ["--signal", "$8f"],
    ["-f", "json", "export"] + WINDOW + ["--source", SOURCE_B],
    ["export"] + WINDOW + ["--columns", "event_number,source"],
    # Errors: stdout is empty and the status must agree.
    ["events", "--signal", "bogus"],
    ["events", "--limit", "0"],
    ["latest", "--source", "no-such-tdu"],
    ["admin-check"],
    ["source", "disable", SOURCE_A],
]

ADMIN_SEQUENCE = [
    ["admin-check"],
    ["source", "disable", SOURCE_B],
    ["-f", "json", "source", "enable", SOURCE_B],
    ["source", "set-url", SOURCE_B, "http://tdu-near-master-ppc-04:8080"],
    ["source", "reset", SOURCE_B],
    ["source", "reset", "no-such-tdu"],
]


def _ids(invocations):
    return [" ".join(argv) for argv in invocations]


@pytest.fixture
def programs(tmp_path):
    """Both programs, run from an empty directory so that neither picks up a
    config/spillclient.yaml or .env from the checkout."""
    python = [sys.executable, "-m", "darpa_spillserver.client_cli"]
    return python, [str(CPP)], tmp_path


@pytest.mark.parametrize("argv", INVOCATIONS, ids=_ids(INVOCATIONS))
def test_same_output(live_server, programs, argv):
    python, cpp, cwd = programs
    expected = _run(python, live_server, argv, cwd)
    actual = _run(cpp, live_server, argv, cwd)
    assert actual == expected


def test_same_output_for_admin_sequence(live_server, programs):
    """Source changes are stateful, so each program runs the sequence on the
    same server in turn and every step is compared."""
    python, cpp, cwd = programs
    token = ["--admin-token", ADMIN_TOKEN]
    for argv in ADMIN_SEQUENCE:
        assert _run(cpp, live_server, token + argv, cwd) == \
            _run(python, live_server, token + argv, cwd), argv
        # Put the source back so that both see the same starting state next.
        _run(python, live_server, token + ["source", "reset", SOURCE_B], cwd)


def test_same_print_config(tmp_path):
    config = tmp_path / "client.yaml"
    config.write_text("client:\n  url: https://example:8443\n  timeout: 2.5\n"
                      "  format: csv\n  timezone: America/Chicago\n")
    dotenv = tmp_path / "x.env"
    dotenv.write_text("DARPA_SPILL_CLIENT_VERIFY_TLS=off\nDARPA_SPILL_CLIENT_TIMEOUT=4\n")
    argv = ["-c", str(config), "--env-file", str(dotenv), "--admin-token", "t",
            "--print-config"]
    python = [sys.executable, "-m", "darpa_spillserver.client_cli"]
    env = {k: v for k, v in os.environ.items() if not k.startswith("DARPA_SPILL_")}
    env["PYTHONPATH"] = str(ROOT / "src")
    out = [subprocess.run(p + argv, capture_output=True, cwd=str(tmp_path), env=env)
           for p in (python, [str(CPP)])]
    assert out[0].returncode == out[1].returncode == 0
    assert out[1].stdout == out[0].stdout
    assert b"timeout: 4.0\n" in out[0].stdout and b"verify_tls: false\n" in out[0].stdout
