"""Tests for darpa-spill-query's server mode TLS options."""

import httpx

from darpa_spillserver import query_cli


class _Response:
    status_code = 200
    text = '{"event": null}\n'

    def json(self):
        return {"event": None}


def _capture(monkeypatch):
    seen = {}

    def fake_get(url, params=None, timeout=None, verify=True):
        seen["url"], seen["verify"] = url, verify
        return _Response()

    monkeypatch.setattr(httpx, "get", fake_get)
    return seen


def test_verifies_by_default(monkeypatch, capsys):
    seen = _capture(monkeypatch)
    assert query_cli.main(["-s", "https://gw:8443", "--last"]) == 0
    assert seen == {"url": "https://gw:8443/api/latest", "verify": True}


def test_ca_bundle_and_insecure(monkeypatch, capsys):
    seen = _capture(monkeypatch)
    query_cli.main(["-s", "https://gw:8443", "--ca-bundle", "gw.pem", "--last"])
    assert seen["verify"] == "gw.pem"
    query_cli.main(["-s", "https://gw:8443", "-k", "--last"])
    assert seen["verify"] is False
    query_cli.main(["-s", "https://gw:8443", "--insecure", "--ca-bundle",
                    "gw.pem", "--last"])
    assert seen["verify"] is False


def test_certificate_failure_suggests_options(monkeypatch, capsys):
    def failing_get(url, **kwargs):
        raise httpx.ConnectError(
            "[SSL: CERTIFICATE_VERIFY_FAILED] self-signed certificate")

    monkeypatch.setattr(httpx, "get", failing_get)
    assert query_cli.main(["-s", "https://gw:8443", "--last"]) == 1
    err = capsys.readouterr().err
    assert "--ca-bundle" in err and "--insecure" in err


def test_server_table_is_rendered_not_raw_json(populated_client, monkeypatch,
                                                capsys):
    """--server with the default format prints the same table as a local
    query, not the server's JSON."""
    def via_app(url, params=None, timeout=None, verify=True):
        return populated_client.get(url.replace("https://gw:8443", ""),
                                    params=params)

    monkeypatch.setattr(httpx, "get", via_app)
    assert query_cli.main(["-s", "https://gw:8443", "--type", "numi",
                           "--start", "2026-09-22"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("NOVA TIME")
    assert "5 event(s)" in out
    assert "NUMI" in out and '"meta"' not in out

    assert query_cli.main(["-s", "https://gw:8443", "--last"]) == 0
    out = capsys.readouterr().out
    assert out.startswith("NOVA TIME") and "1 event(s)" in out

    assert query_cli.main(["-s", "https://gw:8443", "--last", "-f", "json"]) == 0
    assert capsys.readouterr().out.startswith('{"event"')


def test_server_csv_no_header(populated_client, monkeypatch, capsys):
    def via_app(url, params=None, timeout=None, verify=True):
        return populated_client.get(url.replace("https://gw:8443", ""),
                                    params=params)

    monkeypatch.setattr(httpx, "get", via_app)
    args = ["-s", "https://gw:8443", "--type", "numi", "--start", "2026-09-22",
            "-f", "csv", "--columns", "nova_time,source"]
    assert query_cli.main(args) == 0
    data = [l for l in capsys.readouterr().out.splitlines()
            if not l.startswith("#")]
    assert data[0] == "nova_time,source" and len(data) == 6

    assert query_cli.main(args + ["--no-header"]) == 0
    data = [l for l in capsys.readouterr().out.splitlines()
            if not l.startswith("#")]
    assert len(data) == 5 and data[0].split(",")[1] == "tdu-near-master-ppc-02"


def test_broken_pipe_exits_quietly(monkeypatch):
    def closed(*args, **kwargs):
        raise BrokenPipeError()

    monkeypatch.setattr(query_cli, "_main", closed)
    monkeypatch.setattr(query_cli.os, "dup2", lambda *a: None)
    assert query_cli.main(["--last"]) == 141
