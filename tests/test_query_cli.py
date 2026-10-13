"""Tests for darpa-spill-query's server mode TLS options."""

import httpx

from darpa_spillserver import query_cli


class _Response:
    status_code = 200
    text = '{"event": null}\n'


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
