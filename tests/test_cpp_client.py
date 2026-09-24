"""Drive the C++ ``darpa-spill-client-cpp`` against a live server.

The binary comes from the CMake build (docs/CPP_LIBRARY.md). It is looked
for at ``$DARPA_SPILL_CPP_CLIENT``, then ``build/darpa-spill-client-cpp``, then
``build/*/darpa-spill-client-cpp``; every test here is skipped when none is
found, so a Python-only checkout still passes.

Each run happens in an empty directory with every ``DARPA_SPILL_*`` variable
removed and ``HOME`` pointed there too, so that no configuration file or
``.env`` on the machine running the tests can change the result.
"""

import json
import os
import socket
import subprocess
from pathlib import Path

import pytest

from conftest import ADMIN_TOKEN, SOURCE_A, SOURCE_B

ROOT = Path(__file__).resolve().parent.parent


def _find(name):
    explicit = os.environ.get("DARPA_SPILL_CPP_CLIENT") if name == "darpa-spill-client-cpp" else None
    candidates = [Path(explicit)] if explicit else []
    for suffix in ("", ".exe"):
        candidates.append(ROOT / "build" / (name + suffix))
        candidates.extend(sorted((ROOT / "build").glob("*/" + name + suffix)))
        candidates.extend(sorted((ROOT / "build").glob("*/*/" + name + suffix)))
    for candidate in candidates:
        if candidate.is_file() and os.access(str(candidate), os.X_OK):
            return candidate
    return None


BINARY = _find("darpa-spill-client-cpp")
EXAMPLE = _find("example_c_latest")

pytestmark = pytest.mark.skipif(
    BINARY is None, reason="darpa-spill-client-cpp is not built (see docs/CPP_LIBRARY.md)")


@pytest.fixture
def run(tmp_path):
    """Run the client in an isolated directory and environment."""
    base_env = {k: v for k, v in os.environ.items() if not k.startswith("DARPA_SPILL_")}
    base_env["HOME"] = str(tmp_path)

    def invoke(*args, env=None, check=None):
        environment = dict(base_env)
        environment.update(env or {})
        result = subprocess.run(
            [str(BINARY)] + [str(a) for a in args], cwd=str(tmp_path), env=environment,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
        result.out = result.stdout.decode("utf-8")
        result.err = result.stderr.decode("utf-8")
        if check is not None:
            assert result.returncode == check, (result.returncode, result.out, result.err)
        return result

    return invoke


@pytest.fixture
def api(live_server, run):
    """Run the client against the live server."""
    def invoke(*args, **kwargs):
        return run("-u", live_server, *args, **kwargs)
    return invoke


def _table(text):
    return [line.split() for line in text.splitlines()]


# ------------------------------------------------------------ command line


def test_version(run):
    result = run("--version", check=0)
    assert result.out == "darpa-spill-client-cpp 1.3.0\n"


def test_help(run):
    result = run("--help", check=0)
    assert "COMMAND" in result.out and "export" in result.out
    assert "--start" in run("events", "--help", check=0).out


def test_missing_command_is_a_usage_error(run):
    result = run(check=2)
    assert result.out == ""
    assert "error: a COMMAND is required" in result.err


@pytest.mark.parametrize("args", [
    ["frobnicate"],
    ["-f", "xml", "health"],
    ["convert"],
    ["source"],
    ["source", "explode", "x"],
    ["source", "set-url", "x"],
    ["health", "extra"],
    ["events", "--limit", "many"],
    ["--no-such-option", "health"],
])
def test_usage_errors(run, args):
    result = run(*args, check=2)
    assert result.out == ""
    assert "error" in result.err


def test_print_config_defaults(run):
    assert run("--print-config", check=0).out == (
        "client:\n"
        "  url: http://localhost:8080\n"
        "  timeout: 30.0\n"
        "  admin_token: ''\n"
        "  admin_token_file: ''\n"
        "  ca_file: ''\n"
        "  verify_tls: true\n"
        "  format: table\n"
        "  timezone: ''\n")


def test_print_config_layers(run, tmp_path):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "spillclient.yaml").write_text(
        "client:\n  url: http://yaml:1\n  timeout: 2\n  format: csv\n  timezone: UTC\n")
    (tmp_path / ".env").write_text(
        "# local overrides\nexport DARPA_SPILL_CLIENT_TIMEOUT=2.5\n"
        "DARPA_SPILL_CLIENT_FORMAT='json'\nDARPA_SPILL_CLIENT_ADMIN_TOKEN=abc\n")
    result = run("-k", "--tz", "America/Chicago", "--print-config",
                 env={"DARPA_SPILL_CLIENT_FORMAT": "table"}, check=0)
    assert result.out == (
        "client:\n"
        "  url: http://yaml:1\n"
        "  timeout: 2.5\n"
        "  admin_token: '***'\n"
        "  admin_token_file: ''\n"
        "  ca_file: ''\n"
        "  verify_tls: false\n"
        "  format: table\n"
        "  timezone: America/Chicago\n")


@pytest.mark.parametrize("setup, args", [
    ("unknown-key", []),
    ("other-section", []),
    (None, ["-c", "absent.yaml"]),
    (None, ["--env-file", "absent.env"]),
    (None, ["-t", "0"]),
    (None, ["-u", "localhost:8080"]),
])
def test_configuration_errors(run, tmp_path, setup, args):
    if setup:
        (tmp_path / "config").mkdir()
        (tmp_path / "config" / "spillclient.yaml").write_text(
            "client:\n  urll: x\n" if setup == "unknown-key"
            else "client:\n  url: http://x\nserver:\n  port: 1\n")
    result = run(*(args + ["health"]), check=2)
    assert result.err.startswith("error: ")


def test_connection_refused(run):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    result = run("-u", "http://127.0.0.1:{}".format(port), "-t", "5", "health", check=3)
    assert result.out == ""
    assert result.err.startswith("error: cannot reach http://127.0.0.1:{}: ".format(port))


def test_url_from_environment(live_server, run):
    result = run("health", env={"DARPA_SPILL_CLIENT_URL": live_server}, check=0)
    assert "status: ok" in result.out


# --------------------------------------------------------------- read routes


def test_health(api):
    lines = api("health", check=0).out.splitlines()
    assert lines[0] == "status: ok"
    assert lines[1].startswith("time: ")
    raw = api("-f", "json", "health", check=0).out
    assert raw.endswith("\n") and json.loads(raw)["status"] == "ok"


def test_status_flattens(api):
    out = api("status", check=0).out
    assert "archive.events: 65\n" in out
    assert "sources.0.name: {}\n".format(SOURCE_A) in out
    assert "sources.1.enabled: yes\n" in out
    assert "archive.retention_days: -\n" in out


def test_sources_table_and_csv(api):
    rows = _table(api("sources", check=0).out)
    assert rows[0] == ["name", "base_url", "enabled", "overridden", "events"]
    assert rows[1] == [SOURCE_A, "http://tdu-near-master-ppc-01:8080", "yes", "no", "60"]
    assert rows[2] == [SOURCE_B, "http://tdu-near-master-ppc-02:8080", "yes", "no", "5"]
    csv_out = api("-f", "csv", "sources", check=0).out
    assert csv_out.splitlines()[0] == "name,base_url,enabled,overridden,events"


def test_table_columns_are_aligned(api):
    lines = api("signals", check=0).out.splitlines()
    header = lines[0]
    assert header.startswith("hex  ")
    start = header.index("name")
    assert all(line[start - 2:start] == "  " for line in lines[1:])
    assert all(line == line.rstrip() for line in lines)


def test_signals_and_types(api):
    signals = api("signals", check=0).out
    assert any(line.startswith("$8F ") for line in signals.splitlines())
    types_csv = api("-f", "csv", "types", check=0).out.splitlines()
    assert types_csv[0] == "value,name,ambiguous,signals"
    # A type with several signals has a comma in its cell, so it is quoted.
    assert any(line.count('"') == 2 for line in types_csv[1:])


def test_convert(api):
    out = api("convert", "nova:33778458638680249", check=0).out
    assert "nova: 33778458638680249\n" in out
    assert "utc_string: " in out and "gps_week: " in out
    local = api("--tz", "America/Chicago", "convert", "2026-09-22T10:53:33", check=0).out
    utc = api("convert", "2026-09-22T15:53:33Z", check=0).out
    assert local.splitlines()[0] == utc.splitlines()[0]


def test_time_help(api):
    out = api("time-help", check=0).out
    assert "default_timezone: UTC\n" in out
    assert "forms.0.example: now\n" in out


def test_latest(api):
    out = api("latest", check=0).out
    assert "source: {}\n".format(SOURCE_A) in out
    assert "signal: $8F\n" in out
    other = api("latest", "--source", SOURCE_B, check=0).out
    assert "spill_type_name: NUMI\n" in other


# -------------------------------------------------------------------- events


def test_events_table(api):
    lines = api("events", check=0).out.splitlines()
    assert lines[0].split() == ["utc_string", "gps_week", "gps_tow_exact", "signal",
                                "spill_type_name", "event_number", "source"]
    assert len(lines) == 1 + 65


def test_events_json_and_csv(api):
    page = json.loads(api("-f", "json", "events", "--limit", "10", check=0).out)
    assert page["meta"]["total"] == 65 and len(page["events"]) == 10
    body = api("-f", "csv", "events", "--signal", "$8F", "--desc", "--limit", "5",
               "--columns", "event_number,signal", check=0).out
    lines = body.splitlines()
    assert lines[0].startswith("# DARPA Spill Information Server")
    data = [line for line in lines if not line.startswith("#")]
    assert data[0] == "event_number,signal"
    assert data[1:] == ["{},$8F".format(1059 - i) for i in range(5)]


def test_events_selection(api):
    lines = api("events", "--source", SOURCE_B, "--offset", "1", check=0).out.splitlines()
    assert len(lines) == 1 + 4
    assert all(line.split()[-1] == SOURCE_B for line in lines[1:])
    empty = api("events", "--start", "nova:1", "--end", "nova:2", check=0).out
    assert empty.splitlines() == [empty.splitlines()[0]]


def test_events_http_errors(api):
    result = api("events", "--signal", "zz", check=1)
    assert result.out == ""
    assert result.err.startswith("error: HTTP 400: ")
    assert "\nhint: " in result.err
    invalid = api("events", "--limit", "0", check=1)
    assert invalid.err.startswith("error: HTTP 422: query.limit: ")


def test_export(api, tmp_path):
    body = api("export", check=0).out
    data = [line for line in body.splitlines() if not line.startswith("#")]
    assert len(data) == 1 + 65
    target = tmp_path / "out.csv"
    assert api("export", "--signal", "numi", "-o", str(target), check=0).out == ""
    assert len([l for l in target.read_text().splitlines() if not l.startswith("#")]) == 1 + 5
    exported = json.loads(api("-f", "json", "export", "--source", SOURCE_A, check=0).out)
    assert len(exported["events"]) == 60


def test_export_error_status(api):
    result = api("export", "--type", "nonsense", check=1)
    assert result.err.startswith("error: HTTP 400: ")


# --------------------------------------------------------------------- admin


def test_admin_check(api, tmp_path):
    denied = api("admin-check", check=1)
    assert denied.err.startswith("error: HTTP 401: ")
    assert "ok: yes\n" in api("--admin-token", ADMIN_TOKEN, "admin-check", check=0).out
    token = tmp_path / "token"
    token.write_text(ADMIN_TOKEN + "\n")
    assert api("--admin-token-file", str(token), "admin-check", check=0).out.startswith("ok: yes\n")
    wrong = api("--admin-token", "nope", "admin-check", check=1)
    assert wrong.err.startswith("error: HTTP 403: ")


def test_source_changes(api):
    admin = ["--admin-token", ADMIN_TOKEN]
    out = api(*admin, "source", "disable", SOURCE_B, check=0).out
    assert out.startswith("name: {}\n".format(SOURCE_B))
    assert "enabled: no\n" in out and "overridden: yes\n" in out
    assert "enabled: yes\n" in api(*admin, "source", "enable", SOURCE_B, check=0).out

    moved = json.loads(api(*admin, "-f", "json", "source", "set-url", SOURCE_B,
                           "http://tdu-near-master-ppc-04:8080", check=0).out)
    assert moved["source"]["base_url"] == "http://tdu-near-master-ppc-04:8080"

    reset = api(*admin, "source", "reset", SOURCE_B, check=0).out
    assert "base_url: http://tdu-near-master-ppc-02:8080\n" in reset
    assert "overridden: no\n" in reset

    missing = api(*admin, "source", "enable", "no-such-tdu", check=1)
    assert missing.err.startswith("error: HTTP 404: ")
    assert api("source", "enable", SOURCE_B, check=1).err.startswith("error: HTTP 401: ")


# ------------------------------------------------------------------ C example


@pytest.mark.skipif(EXAMPLE is None, reason="example_c_latest is not built")
def test_c_example(live_server):
    result = subprocess.run([str(EXAMPLE), live_server, "$8F"], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=60)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["event"]["signal"] == "$8F"
    bad = subprocess.run([str(EXAMPLE), live_server, "zz"], stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, timeout=60)
    assert bad.returncode == 1 and b"HTTP 400" in bad.stderr
