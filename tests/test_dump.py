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


# ------------------------------------------------------------------ split


from datetime import datetime, timezone  # noqa: E402

from darpa_spillserver.dump import segment_path, split_range  # noqa: E402
from darpa_spillserver.novatime import (  # noqa: E402
    datetime_to_nova,
    get_timezone,
    nova_to_datetime,
)
from darpa_spillserver.storage import SpillEvent  # noqa: E402
from darpa_spillserver.signals import SpillType  # noqa: E402


def utc(*args):
    return datetime_to_nova(datetime(*args, tzinfo=timezone.utc))


def test_split_days_tile_the_range():
    start, end = utc(2026, 10, 12, 6), utc(2026, 10, 15, 3)
    pieces = split_range(start, end, "day", get_timezone("UTC"))
    assert [p[2] for p in pieces] == ["2026-10-12", "2026-10-13",
                                      "2026-10-14", "2026-10-15"]
    assert pieces[0][0] == start and pieces[-1][1] == end
    assert all(a[1] == b[0] for a, b in zip(pieces, pieces[1:]))
    assert pieces[1][:2] == (utc(2026, 10, 13), utc(2026, 10, 14))


def test_split_hours_and_unsplit():
    start, end = utc(2026, 10, 12, 0, 30), utc(2026, 10, 12, 3)
    pieces = split_range(start, end, "hour", get_timezone("UTC"))
    assert [p[2] for p in pieces] == ["2026-10-12T00", "2026-10-12T01",
                                      "2026-10-12T02"]
    assert split_range(start, end, None, None) == [(start, end, None)]


def test_split_days_follow_local_midnight():
    chicago = get_timezone("America/Chicago")
    pieces = split_range(utc(2026, 10, 12, 5), utc(2026, 10, 14, 5),
                         "day", chicago)
    assert [p[2] for p in pieces] == ["2026-10-12", "2026-10-13"]
    assert pieces[1][0] == utc(2026, 10, 13, 5)


def test_split_across_daylight_saving():
    chicago = get_timezone("America/Chicago")
    # 2026-11-01: clocks fall back at 02:00 CDT; the day is 25 hours.
    days = split_range(utc(2026, 11, 1, 5), utc(2026, 11, 2, 6), "day", chicago)
    assert len(days) == 1 and days[0][2] == "2026-11-01"
    hours = split_range(utc(2026, 11, 1, 5), utc(2026, 11, 2, 6), "hour",
                        chicago)
    labels = [p[2] for p in hours]
    assert len(hours) == 25 and len(set(labels)) == 25
    assert "2026-11-01T01-0500" in labels and "2026-11-01T01-0600" in labels
    # 2026-03-08: spring forward; 23 hours, no 02:00.
    hours = split_range(utc(2026, 3, 8, 6), utc(2026, 3, 9, 5), "hour",
                        chicago)
    assert len(hours) == 23
    assert not any(p[2].startswith("2026-03-08T02") for p in hours)


def test_segment_path():
    assert segment_path("week.csv", "2026-10-12") == "week_2026-10-12.csv"
    assert segment_path("out/{start}/s.json", "2026-10-12") == \
        "out/2026-10-12/s.json"
    assert segment_path("week.csv", None) == "week.csv"


def test_segment_path_fields():
    template = "{year}-{month}/{day}/nd-{date}-{hour}00{offset}.csv"
    assert segment_path(template, "2026-10-15T14-0500") == \
        "2026-10/15/nd-2026-10-15-1400-0500.csv"
    assert segment_path(template, "2026-10-15T14") == \
        "2026-10/15/nd-2026-10-15-1400.csv"
    # A day piece starts at local midnight.
    assert segment_path(template, "2026-10-15") == \
        "2026-10/15/nd-2026-10-15-0000.csv"
    # Any one field selects replacement; unknown braces are left alone.
    assert segment_path("{date}/{x}.csv", "2026-10-15") == \
        "2026-10-15/{x}.csv"
    with pytest.raises(ValueError):
        segment_path("{date}.csv", "not-a-label")


def test_split_by_hour_into_month_and_day_directories(two_days, tmp_path):
    template = str(tmp_path / "{year}-{month}" / "{day}" /
                   "neardet-signals-all-{date}-{hour}00.csv")
    code = main(["-d", two_days.path, "-q", "--timezone", "UTC",
                 "--start", "2026-10-12T00:00", "--end", "2026-10-12T12:00",
                 "--split", "hour", "--skip-empty", "--csv", template],
                environ={})
    assert code == 0
    assert sorted(str(f.relative_to(tmp_path))
                  for f in tmp_path.rglob("*.csv")) == [
        "2026-10/12/neardet-signals-all-2026-10-12-0000.csv",
        "2026-10/12/neardet-signals-all-2026-10-12-0600.csv"]


def test_split_path_collision_is_refused(tmp_path, capsys):
    from darpa_spillserver.storage import SpillStore
    store = SpillStore(str(tmp_path / "fall.db"))
    # One event in each of the two 01:00 hours of 2026-11-01 in Chicago.
    store.insert_events([
        SpillEvent(nova_time=utc(2026, 11, 1, hour, 30),
                   spill_type=SpillType.ACCEL_ONE_HZ_TCLK, signal_code=0x8F,
                   event_number=i, source=SOURCE_A, route="spill_history")
        for i, hour in enumerate((6, 7))
    ])
    out = tmp_path / "out"
    args = ["-d", store.path, "-q", "--timezone", "America/Chicago",
            "--start", "2026-11-01T06:00Z", "--end", "2026-11-01T08:00Z",
            "--split", "hour"]
    code = main(args + ["--csv", str(out / "{date}-{hour}.csv")], environ={})
    assert code == 2
    assert "2026-11-01T01-0500 and 2026-11-01T01-0600" in \
        capsys.readouterr().err
    assert not out.exists()
    assert main(args + ["--csv", str(out / "{date}-{hour}{offset}.csv")],
                environ={}) == 0
    assert sorted(f.name for f in out.iterdir()) == [
        "2026-11-01-01-0500.csv", "2026-11-01-01-0600.csv"]
    store.close()


@pytest.fixture
def two_days(tmp_path):
    """Events every 6 h across 2026-10-12 and -13 UTC, all one source."""
    from darpa_spillserver.storage import SpillStore
    store = SpillStore(str(tmp_path / "two.db"))
    store.insert_events([
        SpillEvent(nova_time=utc(2026, 10, 12) + i * 6 * 3600 * SECOND,
                   spill_type=SpillType.ACCEL_ONE_HZ_TCLK, signal_code=0x8F,
                   event_number=i, source=SOURCE_A, route="spill_history")
        for i in range(8)
    ])
    yield store
    store.close()


def test_split_by_day_writes_one_file_per_day(two_days, tmp_path):
    out = tmp_path / "out"
    code = main(["-d", two_days.path, "-q", "--timezone", "UTC",
                 "--start", "2026-10-12", "--end", "2026-10-13",
                 "--split", "day", "--csv", str(out / "s.csv"),
                 "--json", str(out / "{start}" / "s.json")], environ={})
    assert code == 0
    assert len(read_csv(out / "s_2026-10-12.csv")) == 4
    assert len(read_csv(out / "s_2026-10-13.csv")) == 4
    doc = read_json(out / "2026-10-13" / "s.json")
    assert doc["meta"]["piece"] == {"split": "day", "label": "2026-10-13"}
    assert doc["meta"]["range"]["start"]["utc"].startswith("2026-10-13T00:00")
    assert doc["summary"]["complete"] is True
    assert not list(out.rglob("*.part"))


def test_split_by_hour_and_skip_empty(two_days, tmp_path):
    out = tmp_path / "h.csv"
    args = ["-d", two_days.path, "-q", "--timezone", "UTC",
            "--start", "2026-10-12T00:00", "--end", "2026-10-12T12:00",
            "--split", "hour", "--csv", str(out)]
    assert main(args, environ={}) == 0
    assert len(list(tmp_path.glob("h_*.csv"))) == 12
    assert len(read_csv(tmp_path / "h_2026-10-12T06.csv")) == 1
    assert len(read_csv(tmp_path / "h_2026-10-12T07.csv")) == 0

    for f in tmp_path.glob("h_*.csv"):
        f.unlink()
    assert main(args + ["--skip-empty"], environ={}) == 0
    assert sorted(f.name for f in tmp_path.glob("h_*.csv")) == [
        "h_2026-10-12T00.csv", "h_2026-10-12T06.csv"]


def test_split_skips_pieces_after_the_archive(two_days, tmp_path):
    main(["-d", two_days.path, "-q", "--timezone", "UTC",
          "--start", "2026-10-12", "--end", "2026-10-20",
          "--split", "day", "--json", str(tmp_path / "d.json")], environ={})
    assert sorted(f.name for f in tmp_path.glob("d_*.json")) == [
        "d_2026-10-12.json", "d_2026-10-13.json"]


def test_split_usage_errors(two_days, tmp_path):
    assert main(["-d", two_days.path, "--split", "day", "--csv", "-"],
                environ={}) == 2
    assert main(["-d", two_days.path, "--skip-empty",
                 "--csv", str(tmp_path / "x.csv")], environ={}) == 2
    assert main(["-d", two_days.path,
                 "--csv", str(tmp_path / "{date}.csv")], environ={}) == 2


def test_failed_piece_keeps_completed_ones(two_days, tmp_path, monkeypatch):
    real = LocalReader.fetch

    def flaky(self, start, end, limit):
        if limit > 1 and nova_to_datetime(start).day == 13:
            raise DumpError("boom")
        return real(self, start, end, limit)

    monkeypatch.setattr(LocalReader, "fetch", flaky)
    code = main(["-d", two_days.path, "-q", "--timezone", "UTC",
                 "--start", "2026-10-12", "--end", "2026-10-13",
                 "--split", "day", "--csv", str(tmp_path / "s.csv")],
                environ={})
    assert code == 1
    assert sorted(f.name for f in tmp_path.glob("s_*")) == ["s_2026-10-12.csv"]


def test_empty_pieces_are_not_walked(two_days, tmp_path, monkeypatch):
    calls = []
    real = LocalReader.fetch

    def counting(self, start, end, limit):
        calls.append(limit)
        return real(self, start, end, limit)

    monkeypatch.setattr(LocalReader, "fetch", counting)
    main(["-d", two_days.path, "-q", "--timezone", "UTC", "--type", "numi",
          "--start", "2026-10-12", "--end", "2026-10-13", "--split", "hour",
          "--csv", str(tmp_path / "n.csv")], environ={})
    # Only the count requests (limit 1): the whole-range count and one per
    # piece. No window is read.
    assert set(calls) == {1}
    # 00:00 on the 12th to 18:00 on the 13th, the last archived event;
    # later hours have not been recorded yet and are not written.
    assert len(list(tmp_path.glob("n_*.csv"))) == 43


def test_broken_stdout_pipe_exits_quietly(populated, tmp_path, monkeypatch,
                                          capsys):
    class ClosedPipe:
        def write(self, text):
            raise BrokenPipeError()

        def flush(self):
            pass

    monkeypatch.setattr("sys.stdout", ClosedPipe())
    monkeypatch.setattr(dump, "_quiet_stdout", lambda: None)
    code = main(["-d", populated.path, *RANGE, "-q", "--csv", "-",
                 "--json", str(tmp_path / "d.json")], environ={})
    assert code == 141
    assert list(tmp_path.glob("d.json*")) == []
    assert "Traceback" not in capsys.readouterr().err
