"""!
@file tests/test_dump.py
@brief Tests for the bulk export tool, ``darpa-spill-dump``.
"""

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
         # Last 1 Hz event is at +59 s; +61 s leaves margin for the half-open
         # end.
         "nova:{}".format(BASE + 61 * SECOND)]


def read_csv(path):
    """!
    @brief Read a dump CSV, skipping '#' provenance comments.
    @param path file to read.
    @return list of row dicts.
    """
    with open(path) as handle:
        return list(csv.DictReader(l for l in handle if not l.startswith("#")))


def read_json(path):
    """!
    @brief Load a dump JSON document.
    @param path file to read.
    @return the parsed document.
    """
    with open(path) as handle:
        return json.load(handle)


def keys(rows):
    """!
    @brief Identity of each row for ordering/duplication checks.
    @param rows row dicts from CSV or JSON.
    @return list of (nova_time, source) tuples.
    """
    return [(int(r["nova_time"]), r["source"]) for r in rows]


# ------------------------------------------------------------- local mode


def test_local_dump_writes_both_files(populated, tmp_path):
    """!
    @brief A local dump writes CSV and JSON with the same 65 rows in time
    order, a complete summary, and JSON keys in COLUMNS order.
    """
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
    """!
    @brief CSV starts with a provenance preamble naming the origin;
    --no-comments --no-header starts directly with data.
    """
    out = tmp_path / "d.csv"
    main(["-d", populated.path, *RANGE, "-q", "--csv", str(out)], environ={})
    lines = out.read_text().splitlines()
    assert lines[0].startswith("# darpa-spill-dump")
    assert any(l.startswith("# origin: sqlite:") for l in lines)

    main(["-d", populated.path, *RANGE, "-q", "--no-comments", "--no-header",
          "--csv", str(out)], environ={})
    assert out.read_text().startswith(str(BASE))


def test_windows_tile_without_gaps_or_repeats(populated, tmp_path):
    """!
    @brief Windowed reads return every event exactly once.
    """
    out = tmp_path / "d.json"
    # A 7 s window does not divide the minute evenly and lands boundaries on
    # event times, which is where an off-by-one would show.
    main(["-d", populated.path, *RANGE, "-q", "--window", "7",
          "--json", str(out)], environ={})
    events = read_json(out)["events"]
    assert len(events) == 65
    assert len(set(keys(events))) == 65


def test_dense_window_is_halved_not_truncated(populated, tmp_path):
    """!
    @brief A window holding more than --page-size events is split and re-read,
    never truncated.
    """
    out = tmp_path / "d.json"
    code = main(["-d", populated.path, *RANGE, "-q", "--page-size", "4",
                 "--json", str(out)], environ={})
    assert code == 0
    doc = read_json(out)
    assert len(doc["events"]) == 65
    # 65 events / 4 per page needs at least 17 reads.
    assert doc["summary"]["requests"] > 16


def test_selection_and_columns(populated, tmp_path):
    """!
    @brief --type/--source/--signal select rows and --columns selects and
    orders fields.
    """
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
    """!
    @brief A range with no events writes an empty, complete document and exits
    0.
    """
    out = tmp_path / "d.json"
    code = main(["-d", populated.path, "-q", "--start", "2011-01-01",
                 "--end", "2011-01-02", "--json", str(out)], environ={})
    assert code == 0
    doc = read_json(out)
    assert doc["events"] == []
    assert doc["summary"]["complete"] is True


def test_usage_errors(populated, tmp_path):
    """!
    @brief Usage errors exit 2 without creating output: no sink, two sinks on
    stdout, bad column, unknown source, missing archive.
    """
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
    """!
    @brief '--csv -' writes to stdout: header plus the 5 NuMI rows.
    """
    main(["-d", populated.path, *RANGE, "-q", "--no-comments",
          "--type", "numi", "--csv", "-"], environ={})
    out = capsys.readouterr().out
    assert out.splitlines()[0] == ",".join(COLUMNS)
    assert len(out.splitlines()) == 6


# ------------------------------------------------------------ failure path


class _FailingReader:
    """!
    @brief Reader that returns one row per window, then raises DumpError.
    """
    description = "failing"

    def __init__(self, fail_after):
        """!
        @brief Set the failure point.
        @param fail_after number of successful fetch() calls before raising.
        """
        self.calls = 0
        self.fail_after = fail_after

    def fetch(self, start, end, limit):
        """!
        @brief Return one row, or raise once fail_after calls have been served.
        @return (rows, total) with a single row.
        """
        self.calls += 1
        if self.calls > self.fail_after:
            raise DumpError("boom")
        return [{"nova_time": start, "source": "x"}], 1


def test_failure_leaves_no_files(tmp_path):
    """!
    @brief A failure mid-dump followed by abort() leaves no partial files
    behind.
    """
    path_csv, path_json = str(tmp_path / "d.csv"), str(tmp_path / "d.json")
    sinks = [CsvSink(path_csv, COLUMNS), JsonSink(path_json, COLUMNS, {})]
    with pytest.raises(DumpError):
        # Ten 1 s windows; the third read raises.
        dump_range(_FailingReader(2), sinks, 0, 10 * SECOND, window_seconds=1)
    for sink in sinks:
        sink.abort()
    assert list(tmp_path.iterdir()) == []


def test_one_tick_overflow_is_an_error():
    """!
    @brief More events than page_size sharing one timestamp cannot be split
    further and raise DumpError.
    """
    class Crowded:
        """!
        @brief Reader whose every window reports more matches than the page
        size.
        """
        def fetch(self, start, end, limit):
            """!
            @brief Report one match more than fits.
            @return limit empty rows and a total of limit + 1.
            """
            return [{}] * limit, limit + 1

    with pytest.raises(DumpError, match="share timestamp"):
        dump_range(Crowded(), [], 0, SECOND, page_size=2)


# ------------------------------------------------------------ remote mode


def _remote(client, **kwargs):
    """!
    @brief Build a RemoteReader over an in-process client with empty selections
    by default.
    @param client object with get(url, params) (TestClient or _Recording).
    @param kwargs RemoteReader options overriding the defaults.
    @return the RemoteReader.
    """
    kwargs.setdefault("signals", [])
    kwargs.setdefault("types", [])
    kwargs.setdefault("sources", [])
    return RemoteReader("", client=client, **kwargs)


def test_remote_matches_local(populated, populated_client, tmp_path):
    """!
    @brief Remote (HTTP) and local (SQLite) readers produce byte-identical CSV
    for the same range, window and page size.
    """
    remote = _remote(populated_client)
    local = LocalReader(populated, None, None, None)
    start, end = BASE, BASE + 61 * SECOND

    remote_csv, local_csv = str(tmp_path / "r.csv"), str(tmp_path / "l.csv")
    remote_sink, local_sink = CsvSink(remote_csv, COLUMNS), CsvSink(local_csv, COLUMNS)
    try:
        dump_range(remote, [remote_sink], start, end,
                   window_seconds=5, page_size=3)
        dump_range(local, [local_sink], start, end,
                   window_seconds=5, page_size=3)
        # Sinks commit on close, as main() does before reading output files.
        remote_sink.close({})
        local_sink.close({})
        assert len(read_csv(remote_csv)) == 65
        with open(remote_csv) as a, open(local_csv) as b:
            assert a.read() == b.read()
    finally:
        remote_sink.abort()
        local_sink.abort()


class _Recording:
    """!
    @brief Wrap a test client and record the paths requested.
    """

    def __init__(self, client):
        """!
        @brief Wrap *client* with an empty path log.
        @param client the wrapped test client.
        """
        self.client = client
        self.paths = []

    def get(self, url, params=None):
        """!
        @brief Record the path, then forward the request.
        @return the wrapped client's response.
        """
        self.paths.append(url.split("?")[0])
        return self.client.get(url, params=params)

    def close(self):
        """!
        @brief No-op; the fixture owns the client.
        """
        pass


def test_remote_extent_and_limits(populated_client):
    """!
    @brief RemoteReader.extent() asks only for the newest event; max_limit()
    probes the server ceiling; only server_timezone() touches /api/status.
    """
    recorder = _Recording(populated_client)
    remote = _remote(recorder)
    first, last = remote.extent()
    # Only the newest event is cheap to ask a server for.
    assert first is None and last is not None and last >= BASE
    # 1000 = query.max_limit in the conftest config.
    assert remote.max_limit() == 1000          # probed, above the ceiling
    assert remote.max_limit(500) is None       # within it
    assert "/api/status" not in recorder.paths
    assert remote.server_timezone()
    assert recorder.paths[-1] == "/api/status"


def test_remote_dump_with_timezone_never_asks_for_status(
        monkeypatch, populated_client, tmp_path):
    """!
    @brief With --timezone given, a remote dump never requests /api/status.
    """
    # /api/status counts the whole archive: minutes on the gateway (D18).
    recorder = _Recording(populated_client)
    monkeypatch.setattr(dump, "RemoteReader",
                        lambda *a, **k: _remote(recorder))
    out = tmp_path / "d.csv"
    code = main([*RANGE, "-q", "-s", "https://example:8443",
                 "--timezone", "UTC", "--csv", str(out)], environ={})
    assert code == 0
    assert len(read_csv(out)) == 65
    assert "/api/status" not in recorder.paths
    assert {"/api/latest", "/api/events"} <= set(recorder.paths)


def test_remote_extent_of_an_empty_archive(config, store):
    """!
    @brief extent() of an empty remote archive is (None, None).
    """
    from fastapi.testclient import TestClient
    from darpa_spillserver.app import create_app
    app = create_app(config, store=store, start_ingest=False)
    with TestClient(app) as client:
        assert _remote(client).extent() == (None, None)


def test_remote_selection_is_forwarded(populated_client):
    """!
    @brief Type selection is passed to the server.
    """
    remote = _remote(populated_client, types=["numi"])
    rows, total = remote.fetch(BASE, BASE + 61 * SECOND, 100)
    assert total == 5
    assert {r["spill_type_name"] for r in rows} == {"NUMI"}


def test_remote_error_is_readable(populated_client):
    """!
    @brief A server 400 surfaces as DumpError carrying the server's message.
    """
    remote = _remote(populated_client, signals=["zz"])
    with pytest.raises(DumpError, match="is not a signal"):
        remote.fetch(BASE, BASE + SECOND, 10)


def test_server_from_environment(monkeypatch, populated_client, tmp_path):
    """!
    @brief Server URL, insecure flag and token come from the environment;
    command-line options override them.
    """
    seen = {}

    def fake_remote(base_url, *args, **kwargs):
        """!
        @brief Record the RemoteReader arguments and return a reader over the
        in-process client.
        """
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
    """!
    @brief Convert a UTC calendar instant to ticks.
    @return NOvA ticks for a UTC datetime built from *args.
    """
    return datetime_to_nova(datetime(*args, tzinfo=timezone.utc))


def test_split_days_tile_the_range():
    """!
    @brief Day pieces are labelled by date, clip to the requested range at both
    ends and tile without gaps.
    """
    start, end = utc(2026, 10, 12, 6), utc(2026, 10, 15, 3)
    pieces = split_range(start, end, "day", get_timezone("UTC"))
    assert [p[2] for p in pieces] == ["2026-10-12", "2026-10-13",
                                      "2026-10-14", "2026-10-15"]
    assert pieces[0][0] == start and pieces[-1][1] == end
    assert all(a[1] == b[0] for a, b in zip(pieces, pieces[1:]))
    assert pieces[1][:2] == (utc(2026, 10, 13), utc(2026, 10, 14))


def test_split_hours_and_unsplit():
    """!
    @brief Hour pieces are labelled YYYY-MM-DDTHH; no split yields the whole
    range unlabelled.
    """
    start, end = utc(2026, 10, 12, 0, 30), utc(2026, 10, 12, 3)
    pieces = split_range(start, end, "hour", get_timezone("UTC"))
    assert [p[2] for p in pieces] == ["2026-10-12T00", "2026-10-12T01",
                                      "2026-10-12T02"]
    assert split_range(start, end, None, None) == [(start, end, None)]


def test_split_days_follow_local_midnight():
    """!
    @brief Day boundaries are local midnight in the given zone.
    """
    chicago = get_timezone("America/Chicago")
    # 05:00Z = 00:00 CDT (UTC-5).
    pieces = split_range(utc(2026, 10, 12, 5), utc(2026, 10, 14, 5),
                         "day", chicago)
    assert [p[2] for p in pieces] == ["2026-10-12", "2026-10-13"]
    assert pieces[1][0] == utc(2026, 10, 13, 5)


def test_split_across_daylight_saving():
    """!
    @brief Local-day splitting yields a 25 h day at fall-back and a 23 h day at
    spring-forward; the repeated hour is disambiguated by UTC offset.
    """
    chicago = get_timezone("America/Chicago")
    # 2026-11-01: clocks fall back at 02:00 CDT; the day is 25 hours.
    # 05:00Z = 00:00 CDT on 11-01; 06:00Z = 00:00 CST on 11-02.
    days = split_range(utc(2026, 11, 1, 5), utc(2026, 11, 2, 6), "day", chicago)
    assert len(days) == 1 and days[0][2] == "2026-11-01"
    hours = split_range(utc(2026, 11, 1, 5), utc(2026, 11, 2, 6), "hour",
                        chicago)
    labels = [p[2] for p in hours]
    assert len(hours) == 25 and len(set(labels)) == 25
    assert "2026-11-01T01-0500" in labels and "2026-11-01T01-0600" in labels
    # 2026-03-08: spring forward; 23 hours, no 02:00.
    # 06:00Z = 00:00 CST on 03-08; 05:00Z = 00:00 CDT on 03-09.
    hours = split_range(utc(2026, 3, 8, 6), utc(2026, 3, 9, 5), "hour",
                        chicago)
    assert len(hours) == 23
    assert not any(p[2].startswith("2026-03-08T02") for p in hours)


def test_segment_path():
    """!
    @brief segment_path appends _label before the extension, or substitutes
    {start} when present; no label leaves the path alone.
    """
    assert segment_path("week.csv", "2026-10-12") == "week_2026-10-12.csv"
    assert segment_path("out/{start}/s.json", "2026-10-12") == \
        "out/2026-10-12/s.json"
    assert segment_path("week.csv", None) == "week.csv"


def test_segment_path_fields():
    """!
    @brief Template fields {year} {month} {day} {date} {hour} {offset} are
    filled from the label; unknown braces survive; a bad label raises.
    """
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


def test_label_fields_mon_all_months():
    """!
    @brief {mon} is the lower-case English month abbreviation for every month
    and label form; invalid months raise.
    """
    from darpa_spillserver.dump import MONTH_ABBREVIATIONS, label_fields
    expected = ["jan", "feb", "mar", "apr", "may", "jun",
                "jul", "aug", "sep", "oct", "nov", "dec"]
    assert list(MONTH_ABBREVIATIONS) == expected
    for number, name in enumerate(expected, 1):
        day = "2026-{:02d}-01".format(number)
        assert label_fields(day)["mon"] == name
        assert label_fields(day + "T23-0600")["mon"] == name
        assert label_fields(day + "T05")["mon"] == name
    with pytest.raises(ValueError):
        label_fields("2026-13-01")
    with pytest.raises(ValueError):
        label_fields("2026-00-01")


def test_segment_path_mon_gateway_layout():
    """!
    @brief The gateway's raw/{mon}/... layout renders for day and hour labels;
    {mon} and {month} coexist.
    """
    template = "raw/{mon}/{year}-{mon}-{day}/nd-all-{start}.csv"
    assert segment_path(template, "2026-10-22") == \
        "raw/oct/2026-oct-22/nd-all-2026-10-22.csv"
    assert segment_path(template, "2026-10-22T14-0500") == \
        "raw/oct/2026-oct-22/nd-all-2026-10-22T14-0500.csv"
    assert segment_path(template, "2026-01-05T00") == \
        "raw/jan/2026-jan-05/nd-all-2026-01-05T00.csv"
    # {mon} alone selects replacement, and does not disturb {month}.
    assert segment_path("{mon}.csv", "2026-05-01") == "may.csv"
    assert segment_path("{month}-{mon}.csv", "2026-05-01") == "05-may.csv"


def test_mon_is_locale_independent():
    """!
    @brief {mon} does not change with LC_TIME (locales missing on the host are
    skipped).
    """
    import locale
    saved = locale.setlocale(locale.LC_TIME)
    try:
        for name in ("C", "de_DE.UTF-8", "fr_FR.UTF-8", "es_ES.UTF-8"):
            try:
                locale.setlocale(locale.LC_TIME, name)
            except locale.Error:
                continue
            assert segment_path("{mon}", "2026-12-01") == "dec"
            assert segment_path("{mon}", "2026-05-01") == "may"
            assert segment_path("{mon}", "2026-03-01") == "mar"
    finally:
        locale.setlocale(locale.LC_TIME, saved)


def test_split_by_day_into_mon_directories(two_days, tmp_path):
    """!
    @brief An end-to-end day split writes into {mon} directories.
    """
    template = str(tmp_path / "raw" / "{mon}" / "{year}-{mon}-{day}" /
                   "nd-all-{start}.csv")
    code = main(["-d", two_days.path, "-q", "--timezone", "UTC",
                 "--start", "2026-10-12", "--end", "2026-10-12",
                 "--split", "day", "--csv", template],
                environ={})
    assert code == 0
    assert sorted(str(f.relative_to(tmp_path))
                  for f in tmp_path.rglob("*.csv")) == [
        "raw/oct/2026-oct-12/nd-all-2026-10-12.csv"]


def test_split_by_hour_into_month_and_day_directories(two_days, tmp_path):
    """!
    @brief An end-to-end hourly split with --skip-empty writes only the hours
    holding events, under {year}-{month}/{day}.
    """
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


def test_split_path_collision_is_refused(tmp_path, capsys, monkeypatch):
    """!
    @brief When the path template cannot tell the two fall-back 01:00 hours
    apart the dump exits 2 naming both and writes nothing; adding {offset}
    resolves it.
    """
    class Clock(dump.datetime):
        """!@brief Pins "now" after the fall-back night so both hours are past."""

        @classmethod
        def now(cls, tz=None):
            """!@brief 2026-11-02 00:00 in *tz*."""
            return cls(2026, 11, 2, tzinfo=tz)
    monkeypatch.setattr(dump, "datetime", Clock)
    from darpa_spillserver.storage import SpillStore
    store = SpillStore(str(tmp_path / "fall.db"))
    # One event in each of the two 01:00 hours of 2026-11-01 in Chicago.
    store.insert_events([
        # 06:30Z = 01:30 CDT, 07:30Z = 01:30 CST.
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
    """!
    @brief Events every 6 h across 2026-10-12 and -13 UTC, all one source.
    @param tmp_path pytest per-test temporary directory.
    @return SpillStore with 8 events at 00/06/12/18 UTC on 2026-10-12/13.
    """
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
    """!
    @brief --split day writes one CSV (suffix) and one JSON ({start} directory)
    per day, each complete, with piece metadata and no .part leftovers.
    """
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
    """!
    @brief Hourly split writes every hour (empty ones too); --skip-empty writes
    only hours holding events.
    """
    out = tmp_path / "h.csv"
    args = ["-d", two_days.path, "-q", "--timezone", "UTC",
            "--start", "2026-10-12T00:00", "--end", "2026-10-12T12:00",
            "--split", "hour", "--csv", str(out)]
    assert main(args, environ={}) == 0
    # 00:00..12:00 is 12 hourly pieces; events at 00:00 and 06:00 only.
    assert len(list(tmp_path.glob("h_*.csv"))) == 12
    assert len(read_csv(tmp_path / "h_2026-10-12T06.csv")) == 1
    assert len(read_csv(tmp_path / "h_2026-10-12T07.csv")) == 0

    for f in tmp_path.glob("h_*.csv"):
        f.unlink()
    assert main(args + ["--skip-empty"], environ={}) == 0
    assert sorted(f.name for f in tmp_path.glob("h_*.csv")) == [
        "h_2026-10-12T00.csv", "h_2026-10-12T06.csv"]


def test_split_includes_quiet_pieces_after_the_archive(two_days, tmp_path):
    """!
    @brief Completed pieces after the newest archived event are still written,
    empty, so quiet historical intervals are not silently missing.
    """
    main(["-d", two_days.path, "-q", "--timezone", "UTC",
          "--start", "2026-10-12", "--end", "2026-10-20",
          "--split", "day", "--json", str(tmp_path / "d.json")], environ={})
    assert sorted(f.name for f in tmp_path.glob("d_*.json")) == [
        "d_2026-10-{:02d}.json".format(day) for day in range(12, 21)]


def test_split_usage_errors(two_days, tmp_path):
    """!
    @brief --split to stdout, --skip-empty without --split, and a template
    without --split exit 2.
    """
    assert main(["-d", two_days.path, "--split", "day", "--csv", "-"],
                environ={}) == 2
    assert main(["-d", two_days.path, "--skip-empty",
                 "--csv", str(tmp_path / "x.csv")], environ={}) == 2
    assert main(["-d", two_days.path,
                 "--csv", str(tmp_path / "{date}.csv")], environ={}) == 2


def test_failed_piece_keeps_completed_ones(two_days, tmp_path, monkeypatch):
    """!
    @brief A failure in one piece exits 1 but keeps the pieces already
    completed.
    """
    real = LocalReader.fetch

    def flaky(self, start, end, limit):
        """!
        @brief Fail data reads (limit > 1) for the 13th; pass count requests
        and the 12th through.
        """
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
    """!
    @brief Pieces with no matching events are skipped after a count; no data
    window is read.
    """
    calls = []
    real = LocalReader.fetch

    def counting(self, start, end, limit):
        """!
        @brief Record each fetch's limit, then delegate.
        """
        calls.append(limit)
        return real(self, start, end, limit)

    monkeypatch.setattr(LocalReader, "fetch", counting)
    main(["-d", two_days.path, "-q", "--timezone", "UTC", "--type", "numi",
          "--start", "2026-10-12", "--end", "2026-10-13", "--split", "hour",
          "--csv", str(tmp_path / "n.csv")], environ={})
    # Only the count requests (limit 1): the whole-range count and one per
    # piece. No window is read.
    assert set(calls) == {1}
    # Historical quiet hours after the last archived event are still emitted.
    assert len(list(tmp_path.glob("n_*.csv"))) == 48


def test_broken_stdout_pipe_exits_quietly(populated, tmp_path, monkeypatch,
                                          capsys):
    """!
    @brief A closed stdout exits 141 with no traceback, and the JSON sink's
    partial file is removed.
    """
    class ClosedPipe:
        """!
        @brief stdout replacement whose writes raise BrokenPipeError.
        """
        def write(self, text):
            """!
            @brief Raise BrokenPipeError.
            """
            raise BrokenPipeError()

        def flush(self):
            """!
            @brief No-op.
            """
            pass

    monkeypatch.setattr("sys.stdout", ClosedPipe())
    monkeypatch.setattr(dump, "_quiet_stdout", lambda: None)
    code = main(["-d", populated.path, *RANGE, "-q", "--csv", "-",
                 "--json", str(tmp_path / "d.json")], environ={})
    assert code == 141
    assert list(tmp_path.glob("d.json*")) == []
    assert "Traceback" not in capsys.readouterr().err
