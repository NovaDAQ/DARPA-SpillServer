"""Tests for the bulk export tool, ``darpa-spill-dump``."""

import csv
import json

import pytest

from darpa_spillserver import dump
from darpa_spillserver.dump import (
    CsvSink,
    DumpError,
    JsonSink,
    LocalReader,
    RemoteReader,
    dump_range,
    main,
)
from darpa_spillserver.formats import COLUMNS

from conftest import BASE, SECOND, SOURCE_A, SOURCE_B

#: The range covering every event in the ``populated`` fixture.
RANGE = ["--start", "nova:{}".format(BASE), "--end",
         "nova:{}".format(BASE + 61 * SECOND)]


def read_csv(path):
    with open(path) as handle:
        return list(csv.DictReader(l for l in handle if not l.startswith("#")))


def read_json(path):
    with open(path) as handle:
        return json.load(handle)


def keys(rows):
    return [(int(r["nova_time"]), r["source"]) for r in rows]


# ------------------------------------------------------------- local mode


def test_local_dump_writes_both_files(populated, tmp_path):
    out_csv, out_json = tmp_path / "d.csv", tmp_path / "d.json"
    code = main(["-d", populated.path, *RANGE, "-q",
                 "--csv", str(out_csv), "--json", str(out_json)], environ={})
    assert code == 0

    rows = read_csv(out_csv)
    doc = read_json(out_json)
    assert len(rows) == 65
    assert len(doc["events"]) == 65
    assert keys(rows) == keys(doc["events"])
    assert keys(rows) == sorted(keys(rows))
    assert doc["summary"]["complete"] is True
    assert doc["summary"]["returned"] == doc["meta"]["expected"] == 65
    assert list(doc["events"][0]) == list(COLUMNS)


def test_csv_carries_provenance_comments(populated, tmp_path):
    out = tmp_path / "d.csv"
    main(["-d", populated.path, *RANGE, "-q", "--csv", str(out)], environ={})
    lines = out.read_text().splitlines()
    assert lines[0].startswith("# darpa-spill-dump")
    assert any(l.startswith("# origin: sqlite:") for l in lines)

    main(["-d", populated.path, *RANGE, "-q", "--no-comments", "--no-header",
          "--csv", str(out)], environ={})
    assert out.read_text().startswith(str(BASE))


def test_windows_tile_without_gaps_or_repeats(populated, tmp_path):
    out = tmp_path / "d.json"
    # A 7 s window does not divide the minute evenly and lands boundaries on
    # event times, which is where an off-by-one would show.
    main(["-d", populated.path, *RANGE, "-q", "--window", "7",
          "--json", str(out)], environ={})
    events = read_json(out)["events"]
    assert len(events) == 65
    assert len(set(keys(events))) == 65


def test_dense_window_is_halved_not_truncated(populated, tmp_path):
    out = tmp_path / "d.json"
    code = main(["-d", populated.path, *RANGE, "-q", "--page-size", "4",
                 "--json", str(out)], environ={})
    assert code == 0
    doc = read_json(out)
    assert len(doc["events"]) == 65
    assert doc["summary"]["requests"] > 16


def test_selection_and_columns(populated, tmp_path):
    out = tmp_path / "d.csv"
    main(["-d", populated.path, *RANGE, "-q", "--type", "numi",
          "--columns", "nova_time,spill_type_name,source", "--no-comments",
          "--csv", str(out)], environ={})
    rows = read_csv(out)
    assert len(rows) == 5
    assert list(rows[0]) == ["nova_time", "spill_type_name", "source"]
    assert {r["source"] for r in rows} == {SOURCE_B}

    main(["-d", populated.path, *RANGE, "-q", "--source", SOURCE_A,
          "--signal", "$8f", "--csv", str(out)], environ={})
    assert len(read_csv(out)) == 60


def test_range_outside_archive_is_empty(populated, tmp_path):
    out = tmp_path / "d.json"
    code = main(["-d", populated.path, "-q", "--start", "2011-01-01",
                 "--end", "2011-01-02", "--json", str(out)], environ={})
    assert code == 0
    doc = read_json(out)
    assert doc["events"] == []
    assert doc["summary"]["complete"] is True


def test_usage_errors(populated, tmp_path):
    assert main(["-d", populated.path, *RANGE], environ={}) == 2
    assert main(["-d", populated.path, *RANGE, "--csv", "-", "--json", "-"],
                environ={}) == 2
    assert main(["-d", populated.path, *RANGE, "--columns", "bogus",
                 "--csv", str(tmp_path / "x")], environ={}) == 2
    assert main(["-d", populated.path, *RANGE, "--source", "nope",
                 "--csv", str(tmp_path / "x")], environ={}) == 2
    assert main(["-d", str(tmp_path / "missing.db"),
                 "--csv", str(tmp_path / "x")], environ={}) == 2
    assert not (tmp_path / "x").exists()


def test_stdout_sink(populated, capsys):
    main(["-d", populated.path, *RANGE, "-q", "--no-comments",
          "--type", "numi", "--csv", "-"], environ={})
    out = capsys.readouterr().out
    assert out.splitlines()[0] == ",".join(COLUMNS)
    assert len(out.splitlines()) == 6


# ------------------------------------------------------------ failure path


class _FailingReader:
    description = "failing"

    def __init__(self, fail_after):
        self.calls = 0
        self.fail_after = fail_after

    def fetch(self, start, end, limit):
        self.calls += 1
        if self.calls > self.fail_after:
            raise DumpError("boom")
        return [{"nova_time": start, "source": "x"}], 1


def test_failure_leaves_no_files(tmp_path):
    path_csv, path_json = str(tmp_path / "d.csv"), str(tmp_path / "d.json")
    sinks = [CsvSink(path_csv, COLUMNS), JsonSink(path_json, COLUMNS, {})]
    with pytest.raises(DumpError):
        dump_range(_FailingReader(2), sinks, 0, 10 * SECOND, window_seconds=1)
    for sink in sinks:
        sink.abort()
    assert list(tmp_path.iterdir()) == []


def test_one_tick_overflow_is_an_error():
    class Crowded:
        def fetch(self, start, end, limit):
            return [{}] * limit, limit + 1

    with pytest.raises(DumpError, match="share timestamp"):
        dump_range(Crowded(), [], 0, SECOND, page_size=2)


# ------------------------------------------------------------ remote mode


def _remote(client, **kwargs):
    kwargs.setdefault("signals", [])
    kwargs.setdefault("types", [])
    kwargs.setdefault("sources", [])
    return RemoteReader("", client=client, **kwargs)


def test_remote_matches_local(populated, populated_client, tmp_path):
    remote = _remote(populated_client)
    local = LocalReader(populated, None, None, None)
    start, end = BASE, BASE + 61 * SECOND

    remote_csv, local_csv = str(tmp_path / "r.csv"), str(tmp_path / "l.csv")
    dump_range(remote, [CsvSink(remote_csv, COLUMNS)], start, end,
               window_seconds=5, page_size=3)
    dump_range(local, [CsvSink(local_csv, COLUMNS)], start, end,
               window_seconds=5, page_size=3)
    # Sinks commit on close; do it by hand as main() would.
    assert len(read_csv(remote_csv + ".part")) == 65
    with open(remote_csv + ".part") as a, open(local_csv + ".part") as b:
        assert a.read() == b.read()


def test_remote_extent_and_limits(populated_client):
    remote = _remote(populated_client)
    first, last = remote.extent()
    assert first == BASE
    assert remote.max_limit() == 1000
    assert remote.server_timezone()


def test_remote_selection_is_forwarded(populated_client):
    remote = _remote(populated_client, types=["numi"])
    rows, total = remote.fetch(BASE, BASE + 61 * SECOND, 100)
    assert total == 5
    assert {r["spill_type_name"] for r in rows} == {"NUMI"}


def test_remote_error_is_readable(populated_client):
    remote = _remote(populated_client, signals=["zz"])
    with pytest.raises(DumpError, match="is not a signal"):
        remote.fetch(BASE, BASE + SECOND, 10)


def test_server_from_environment(monkeypatch, populated_client, tmp_path):
    seen = {}

    def fake_remote(base_url, *args, **kwargs):
        seen["url"] = base_url
        seen["verify"] = kwargs["verify"]
        seen["token"] = kwargs["token"]
        reader = _remote(populated_client)
        reader.close = lambda: None  # the fixture owns the client
        return reader

    monkeypatch.setattr(dump, "RemoteReader", fake_remote)
    env = {dump.ENV_SERVER: "https://example:8443", dump.ENV_INSECURE: "1",
           dump.ENV_TOKEN: "tok"}
    out = tmp_path / "d.json"
    code = main([*RANGE, "-q", "--json", str(out)], environ=env)
    assert code == 0
    assert seen == {"url": "https://example:8443", "verify": False,
                    "token": "tok"}
    assert len(read_json(out)["events"]) == 65

    # The command line outranks the environment.
    main([*RANGE, "-q", "--server", "https://cli:1", "--ca-bundle", "ca.pem",
          "--json", str(out)], environ={dump.ENV_SERVER: "https://env:2"})
    assert seen["url"] == "https://cli:1"
    assert seen["verify"] == "ca.pem"
