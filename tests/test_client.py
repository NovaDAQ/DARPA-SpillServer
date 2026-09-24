"""The Python API client and ``darpa-spill-client``, against a live server.

These go over a real socket (the ``live_server`` fixture) rather than
FastAPI's in-process test client, because what is under test is the client's
own HTTP handling: encoding, streaming, error bodies and exit status.
"""

import io
import json

import pytest

from conftest import ADMIN_TOKEN, SOURCE_A, SOURCE_B
from darpa_spillserver.client import (
    ClientConfig,
    Selection,
    SpillClient,
    SpillConnectionError,
    SpillHTTPError,
    encode_query,
    load_client_config,
)
from darpa_spillserver.client_cli import cell, flatten, main, render_csv, render_table
from darpa_spillserver.config import ConfigError


def run(*argv):
    """Run the CLI in-process; return (exit status, stdout, stderr)."""
    out, err = io.BytesIO(), io.StringIO()
    status = main(list(argv), out=out, err=err)
    return status, out.getvalue().decode("utf-8"), err.getvalue()


def free_port():
    import socket
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


# ---------------------------------------------------------------- encoding


def test_encode_query_is_rfc3986_and_ordered():
    assert encode_query([("signal", "$74"), ("start", "Jul 1, 2026"),
                         ("t", "a/b~c_d.e-f")]) == (
        "signal=%2474&start=Jul%201%2C%202026&t=a%2Fb~c_d.e-f")


def test_selection_parameter_order():
    selection = Selection(start="s", end="e", signals=["$74", "$8F"],
                          types=["NUMI"], sources=["x"])
    assert selection.params() == [
        ("start", "s"), ("end", "e"), ("signal", "$74"), ("signal", "$8F"),
        ("type", "NUMI"), ("source", "x")]
    assert SpillClient.events_params(selection, "csv", 10, 5, True,
                                     [("tz", "UTC")], "nova_time")[-6:] == [
        ("format", "csv"), ("limit", "10"), ("offset", "5"),
        ("order", "desc"), ("tz", "UTC"), ("columns", "nova_time")]


# --------------------------------------------------------------- rendering


@pytest.mark.parametrize("value, text", [
    (None, "-"), (True, "yes"), (False, "no"), (7, "7"), (-3, "-3"),
    (2.5, "2.5"), (2.0, "2"), (0.1234567, "0.123457"), (1e-9, "0"),
    ("$74", "$74"), (["$AD", "$A9"], "$AD,$A9"), ([], ""),
])
def test_cell(value, text):
    assert cell(value) == text


def test_flatten():
    assert flatten({"a": 1, "b": {"c": [True, {"d": None}], "e": {}, "f": []}}) == [
        "a: 1", "b.c.0: yes", "b.c.1.d: -", "b.e: {}", "b.f: []"]
    assert flatten({}) == []


def test_render_table_and_csv():
    assert render_table(["a", "bb"], [["xxx", ""], ["y", "z"]]) == (
        "a    bb\nxxx\ny    z\n")
    assert render_csv(["a", "b"], [['x,y', 'say "hi"'], ["p\nq", "r"]]) == (
        'a,b\n"x,y","say ""hi"""\n"p\nq",r\n')


# ------------------------------------------------------------ configuration


def test_client_config_precedence(tmp_path):
    yaml_file = tmp_path / "spillclient.yaml"
    yaml_file.write_text("client:\n  url: http://from-yaml:1\n  timeout: 5\n"
                         "  format: csv\n  timezone: America/Chicago\n")
    dotenv = tmp_path / ".env"
    dotenv.write_text("DARPA_SPILL_CLIENT_URL=http://from-dotenv:2\n"
                      "DARPA_SPILL_CLIENT_TIMEOUT=6\n"
                      "DARPA_SPILL_CLIENT_VERIFY_TLS=no\n")
    config = load_client_config(
        config_file=str(yaml_file), env_file=str(dotenv),
        environ={"DARPA_SPILL_CLIENT_URL": "http://from-env:3"},
        overrides={"timeout": 7.0, "format": None})
    assert config.url == "http://from-env:3"     # environment > .env > YAML
    assert config.timeout == 7.0                  # command line > everything
    assert config.verify_tls is False             # .env > default
    assert config.format == "csv"                 # YAML > default; None ignored
    assert config.timezone == "America/Chicago"
    assert config.source == str(yaml_file)


def test_client_config_search_and_defaults(tmp_path):
    config = load_client_config(environ={}, search_paths=[], dotenv_path=None)
    assert config == ClientConfig()
    found = tmp_path / "found.yaml"
    found.write_text("client:\n  url: https://found:9\n")
    config = load_client_config(environ={}, search_paths=[str(tmp_path / "no"),
                                                         str(found)], dotenv_path=None)
    assert config.url == "https://found:9"
    config = load_client_config(environ={"DARPA_SPILL_CLIENT_CONFIG": str(found)},
                                search_paths=[], dotenv_path=None)
    assert config.url == "https://found:9"


@pytest.mark.parametrize("text, message", [
    ("client:\n  bogus: 1\n", "unknown client setting 'bogus'"),
    ("server:\n  port: 1\n", "single 'client:' section"),
    ("client:\n  timeout: soon\n", "timeout must be a number"),
    ("client:\n  verify_tls: maybe\n", "verify_tls must be a boolean"),
    ("client:\n  url: ftp://x\n", "url must start with http"),
    ("client:\n  format: xml\n", "format must be table, json or csv"),
])
def test_client_config_errors(tmp_path, text, message):
    path = tmp_path / "bad.yaml"
    path.write_text(text)
    with pytest.raises(ConfigError, match=message):
        load_client_config(config_file=str(path), environ={}, dotenv_path=None)


def test_admin_token_file(tmp_path):
    token = tmp_path / "token"
    token.write_text("s3cret\nignored\n")
    assert ClientConfig(admin_token_file=str(token)).resolved_token() == "s3cret"
    assert ClientConfig(admin_token="direct",
                        admin_token_file=str(token)).resolved_token() == "direct"
    with pytest.raises(ConfigError, match="cannot read"):
        ClientConfig(admin_token_file=str(tmp_path / "absent")).resolved_token()


def test_print_config_hides_the_token():
    status, out, _ = run("-u", "http://h:1", "--admin-token", "abc", "-t", "2.5",
                         "--print-config")
    assert status == 0
    assert out == ("client:\n  url: http://h:1\n  timeout: 2.5\n"
                   "  admin_token: '***'\n  admin_token_file: ''\n  ca_file: ''\n"
                   "  verify_tls: true\n  format: table\n  timezone: ''\n")


# ----------------------------------------------------------------- library


def test_library_reference_calls(live_server):
    client = SpillClient(live_server)
    assert client.health()["status"] == "ok"
    assert client.status()["archive"]["events"] == 65
    assert {s["name"] for s in client.sources()["sources"]} == {SOURCE_A, SOURCE_B}
    assert any(s["hex"] == "$74" for s in client.signals()["signals"])
    assert any(t["name"] == "NUMI" for t in client.types()["types"])
    assert client.time_convert("unix:1782864000")["utc"].startswith("2026-07-01T00:00:00")
    assert client.time_help()["forms"]


def test_library_events_and_latest(live_server):
    client = SpillClient(live_server)
    page = client.events(Selection(signals=["$8f"]), limit=10)
    assert page["meta"]["total"] == 60
    assert len(page["events"]) == 10
    assert {e["signal"] for e in page["events"]} == {"$8F"}

    newest = client.events(Selection(sources=[SOURCE_B]), limit=1, descending=True)
    assert newest["events"][0]["event_number"] == 2004

    text = client.events_csv(Selection(types=["NUMI"]), columns="event_number")
    rows = [line for line in text.splitlines() if not line.startswith("#")]
    assert rows == ["event_number", "2000", "2001", "2002", "2003", "2004"]

    assert client.latest(signal="$74")["event"]["event_number"] == 2004


def test_library_export_streams(live_server):
    client = SpillClient(live_server)
    sink = io.BytesIO()
    written = client.export(Selection(signals=["$8F"]), sink, columns="event_number")
    body = sink.getvalue().decode()
    assert written == len(sink.getvalue())
    assert body.splitlines()[-1] == "1059"
    sink = io.BytesIO()
    client.export(Selection(sources=[SOURCE_B]), sink, fmt="json")
    assert len(json.loads(sink.getvalue())["events"]) == 5


def test_library_http_error_carries_hint(live_server):
    with pytest.raises(SpillHTTPError) as caught:
        SpillClient(live_server).events(Selection(signals=["bogus"]))
    assert caught.value.status == 400
    assert "not a signal" in caught.value.message
    assert "/api/signals" in caught.value.hint


def test_library_validation_error_is_readable(live_server):
    with pytest.raises(SpillHTTPError) as caught:
        SpillClient(live_server).events(limit=0)
    assert caught.value.status == 422
    assert caught.value.message.startswith("query.limit:")


def test_library_admin(live_server):
    anonymous = SpillClient(live_server)
    with pytest.raises(SpillHTTPError) as caught:
        anonymous.update_source(SOURCE_B, enabled=False)
    assert caught.value.status == 401

    admin = SpillClient(live_server, admin_token=ADMIN_TOKEN)
    assert admin.admin_check()["ok"] is True
    assert admin.update_source(SOURCE_B, enabled=False)["source"]["enabled"] is False
    moved = admin.update_source(SOURCE_B, base_url="http://tdu-near-master-ppc-04:8080")
    assert moved["source"]["base_url"] == "http://tdu-near-master-ppc-04:8080"
    reset = admin.reset_source(SOURCE_B)["source"]
    assert reset["enabled"] is True and reset["overridden"] is False


def test_library_connection_error():
    with pytest.raises(SpillConnectionError, match="cannot reach"):
        SpillClient("http://127.0.0.1:{}".format(free_port()), timeout=2).health()


# --------------------------------------------------------------------- CLI


def test_cli_tables(live_server):
    status, out, _ = run("-u", live_server, "sources")
    assert status == 0
    lines = out.splitlines()
    assert lines[0].split() == ["name", "base_url", "enabled", "overridden", "events"]
    assert lines[1].split() == [SOURCE_A, "http://tdu-near-master-ppc-01:8080",
                                "yes", "no", "60"]

    status, out, _ = run("-u", live_server, "-f", "csv", "types")
    assert out.splitlines()[0] == "value,name,ambiguous,signals"
    assert '2,NUMI_TCLK,yes,"$AD,$A9"' in out.splitlines()


def test_cli_events_table_and_formats(live_server):
    status, out, _ = run("-u", live_server, "events", "--signal", "$74", "--limit", "2")
    assert status == 0
    lines = out.splitlines()
    assert lines[0].split()[:3] == ["utc_string", "gps_week", "gps_tow_exact"]
    assert len(lines) == 3

    status, out, _ = run("-u", live_server, "-f", "json", "events", "--limit", "1")
    assert json.loads(out)["meta"]["returned"] == 1

    status, out, _ = run("-u", live_server, "-f", "csv", "events", "--limit", "1",
                         "--columns", "nova_time,signal")
    assert out.startswith("# DARPA Spill Information Server\n")
    assert "nova_time,signal\n" in out


def test_cli_key_value(live_server):
    status, out, _ = run("-u", live_server, "latest", "--source", SOURCE_A)
    assert status == 0
    assert "event_number: 1059\n" in out
    assert "signal: $8F\n" in out
    status, out, _ = run("-u", live_server, "convert", "gps:1466899218")
    assert "utc: 2026-07-01T00:00:00.000000000Z\n" in out


def test_cli_export_to_file(live_server, tmp_path):
    target = tmp_path / "out.csv"
    status, out, _ = run("-u", live_server, "export", "--source", SOURCE_B,
                         "--columns", "event_number", "-o", str(target))
    assert status == 0 and out == ""
    rows = [l for l in target.read_text().splitlines() if not l.startswith("#")]
    assert rows == ["event_number", "2000", "2001", "2002", "2003", "2004"]


def test_cli_source_commands(live_server):
    token = ("--admin-token", ADMIN_TOKEN)
    status, out, _ = run("-u", live_server, *token, "source", "disable", SOURCE_A)
    assert status == 0 and "enabled: no\n" in out
    status, out, _ = run("-u", live_server, *token, "source", "set-url", SOURCE_A,
                         "http://elsewhere:8080")
    assert "base_url: http://elsewhere:8080\n" in out
    status, out, _ = run("-u", live_server, *token, "source", "reset", SOURCE_A)
    assert "overridden: no\n" in out
    status, out, _ = run("-u", live_server, *token, "admin-check")
    assert status == 0 and "ok: yes\n" in out


def test_cli_exit_status(live_server):
    status, _, err = run("-u", live_server, "events", "--signal", "bogus")
    assert status == 1
    assert err.startswith("error: HTTP 400: ")
    assert "\nhint: " in err

    status, _, err = run("-u", live_server, "source", "enable", SOURCE_A)
    assert status == 1 and "HTTP 401" in err

    status, _, err = run("-u", "http://127.0.0.1:{}".format(free_port()), "health")
    assert status == 3 and err.startswith("error: cannot reach")

    status, _, err = run("-u", "ftp://nowhere", "health")
    assert status == 2

    status, _, err = run("-u", live_server)
    assert status == 2 and "a COMMAND is required" in err

    with pytest.raises(SystemExit) as caught:
        run("-u", live_server, "no-such-command")
    assert caught.value.code == 2
