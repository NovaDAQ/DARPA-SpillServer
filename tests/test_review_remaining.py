"""Regression cases from the non-OIDC October repository review."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from darpa_spillserver import dump, query_cli, shift
from darpa_spillserver.backfill import _probe_earliest
from darpa_spillserver.formats import iter_json
from darpa_spillserver.novatime import convert, parse_instant, NovaTimeError
from darpa_spillserver.signals import SpillType
from darpa_spillserver.storage import SpillEvent
from darpa_spillserver.tdu_client import TDUError
from conftest import BASE, SECOND

ROOT = Path(__file__).resolve().parents[1]

@pytest.fixture
def selections(store):
    rows = [(1, SpillType.BNB_TCLK, 0x1D),
            (2, SpillType.BNB_TCLK, -1),
            (3, SpillType.BNB_TCLK, 0x1F),
            (4, SpillType.NUMI, 0xFE),
            (5, SpillType.ACCEL_ONE_HZ_TCLK, 0x8F)]
    store.insert_events([SpillEvent(nova_time=BASE+i, spill_type=t,
                                   signal_code=c, source='test') for i,t,c in rows])
    return store


def test_latest_exact_signal_and_unknown_fallback(client, selections):
    assert client.get('/api/latest', params={'signal': '0x1D'}).json()['event']['nova_time'] == BASE+2
    assert client.get('/api/latest', params={'signal': '0x1F'}).json()['event']['nova_time'] == BASE+3


@pytest.mark.parametrize('route', ['/api/events', '/api/export'])
def test_mixed_selection_unions_explicit_types(client, selections, route):
    response = client.get(route, params={'signal': '0x1D', 'type': 'numi', 'format': 'json'})
    assert response.status_code == 200
    assert [e['nova_time'] for e in response.json()['events']] == [BASE+1, BASE+2, BASE+4]


@pytest.mark.parametrize('remote', [False, True])
def test_cli_last_honors_union_range_and_format(client, selections, monkeypatch, capsys, remote):
    def get(url, **kwargs):
        return client.get('/api/events', params=kwargs['params'])
    monkeypatch.setattr(httpx, 'get', get)
    target = ['-s', 'https://test'] if remote else ['-d', str(selections.path)]
    assert query_cli.main(target + ['--last', '--signal', '0x1D', '--type', 'numi',
             '--end', 'nova:'+str(BASE+4), '-f', 'json']) == 0
    doc = json.loads(capsys.readouterr().out)
    row = doc['event'] if remote else doc['events'][0]
    assert row['nova_time'] == BASE+2
    assert query_cli.main(target + ['--last', '--signal', '0x1D', '--type', 'numi', '-f', 'csv']) == 0
    assert str(BASE+4) in capsys.readouterr().out


def test_dump_mixed_selection(selections, tmp_path):
    out = tmp_path/'union.json'
    assert dump.main(['-d', str(selections.path), '-q', '--signal', '0x1D', '--type', 'numi',
                      '--json', str(out)], environ={}) == 0
    assert [e['nova_time'] for e in json.loads(out.read_text())['events']] == [BASE+1,BASE+2,BASE+4]


@pytest.mark.parametrize('fmt', ['csv', 'json'])
@pytest.mark.parametrize('cap', [64, 65, 66])
def test_export_ceiling_is_explicit(populated_client, config, cap, fmt):
    config.query.max_export_rows = cap
    response = populated_client.get('/api/export', params={'format': fmt})
    assert response.status_code == (400 if cap == 64 else 200)
    if cap == 64:
        assert 'exceeding' in response.text
    elif fmt == 'json':
        assert len(response.json()['events']) == 65
    else:
        assert len([l for l in response.text.splitlines() if l and not l.startswith('#')]) == 66


@pytest.mark.parametrize('ticks', [33894147920934449, BASE, BASE+1, BASE+2, BASE+3])
def test_gps_exact_roundtrip(ticks):
    converted = convert(ticks)
    assert parse_instant('gps:'+converted.gps) == ticks
    assert parse_instant('gps:{}:{}'.format(converted.gps_week, converted.gps_tow_exact)) == ticks


@pytest.mark.parametrize('token', ['NaN', 'Infinity', '-1', '1:NaN'])
def test_invalid_gps_values(token):
    with pytest.raises(NovaTimeError):
        parse_instant('gps:'+token)


@pytest.mark.parametrize('offsets', [[], [1199], [301,1199]])
async def test_discovery_covers_recent_and_gapped_history(offsets):
    calls = []
    async def fetch_history(since_nova, until_nova, limit):
        calls.append((since_nova, until_nova))
        return SimpleNamespace(route='/spill_history', events=[SimpleNamespace(nova_time=BASE+i*SECOND)
              for i in offsets if since_nova <= BASE+i*SECOND < until_nova])
    result = await _probe_earliest(SimpleNamespace(fetch_history=fetch_history), BASE+1200*SECOND, 1200)
    assert result == (BASE+min(offsets)*SECOND if offsets else None)
    assert len(calls) <= 4
    assert calls[0][0] == BASE
    assert all(a[1] == b[0] for a,b in zip(calls,calls[1:]))


async def test_discovery_propagates_failure():
    async def fetch_history(**kwargs):
        raise TDUError('unreachable')
    with pytest.raises(TDUError, match='unreachable'):
        await _probe_earliest(SimpleNamespace(fetch_history=fetch_history), BASE, 300)


def test_json_ticks_survive_browser_number_precision():
    event = SpillEvent(nova_time=33894147920934449, spill_type=SpillType.NUMI, source='test')
    doc = json.loads(''.join(iter_json([event], {})))
    assert doc['events'][0]['nova_time_exact'] == '33894147920934449'
    template = (ROOT/'src/darpa_spillserver/web/templates/index.html').read_text()
    assert 'cell(tr, event.nova_time_exact)' in template


@pytest.mark.parametrize('tail', ['', '\n', ']',' ] garbage', '],\n"summary": {"returned": 2}\n}', ',\n]\n}', '\n{}\n]\n}'])
def test_shift_rejects_incomplete_json_without_replacing_output(tmp_path, tail):
    vectors=json.loads((ROOT/'docs/shift/dts1_test_vectors.json').read_text())
    token=shift.Token.parse(vectors['tokens']['flat'])
    src=tmp_path/'in.json'; out=tmp_path/'out.json'
    row=json.dumps({'nova_time': BASE, 'source':'test', 'event_number':1})
    src.write_text('{\n"meta": {},\n"events": [\n'+row+tail)
    out.write_text('original')
    with pytest.raises((shift.ShiftError, ValueError)):
        shift.shift_file(str(src), str(out), token, 1)
    assert out.read_text() == 'original'
    assert not list(tmp_path.glob('*.part'))


def test_shift_updates_exact_ticks():
    vectors=json.loads((ROOT/'docs/shift/dts1_test_vectors.json').read_text())
    offsets=shift.OffsetFunction(shift.Token.parse(vectors['tokens']['flat']))
    row={'nova_time':BASE, 'nova_time_exact':str(BASE), 'source':'test', 'event_number':1}
    shift.transform_row(row, offsets, 1, text=False)
    assert row['nova_time_exact'] == str(row['nova_time'])
    shift.transform_row(row, offsets, -1, text=False)
    assert row['nova_time_exact'] == str(BASE)


@pytest.mark.parametrize('numbers,expected', [([2,1,3],100), ([None,1,3],None), ([2,2,3],None), ([2,3,3],None)])
def test_log_delta_uses_unique_hardware_predecessor(numbers, expected):
    spec=importlib.util.spec_from_file_location('logbackfill', ROOT/'contrib/logbackfill/backfill_from_nsstduapp_logs.py')
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    rows=module.to_events([(100,3),(101,1),(200,4)], numbers, 'test', [4], 150, 300)
    assert len(rows) == 1
    assert rows[0].delta == expected


@pytest.mark.parametrize('fraction,extra', [('0000000078125', 0), ('0000000234375', 2), ('9999999921875', SECOND)])
def test_gps_half_tick_rounding(fraction, extra):
    whole = convert(BASE).gps_seconds
    base = parse_instant('gps:'+str(whole))
    assert parse_instant('gps:{}.{}'.format(whole, fraction)) == base+extra


def test_latest_does_not_count_entire_archive(client, selections):
    statements=[]
    selections.connection.set_trace_callback(statements.append)
    try:
        assert client.get('/api/latest').status_code == 200
    finally:
        selections.connection.set_trace_callback(None)
    assert not any('COUNT(*)' in s for s in statements)


def test_split_excludes_future_intervals(selections, tmp_path, monkeypatch):
    class Clock(dump.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 10, 13, 0, 0, tzinfo=tz)
    monkeypatch.setattr(dump, 'datetime', Clock)
    assert dump.main(['-d', str(selections.path), '-q', '--timezone','UTC',
        '--start', '2026-10-12', '--end', '2026-10-14', '--split','day',
        '--json', str(tmp_path/'quiet.json')], environ={}) == 0
    assert [p.name for p in tmp_path.glob('quiet_*.json')] == ['quiet_2026-10-12.json']
    assert json.loads((tmp_path/'quiet_2026-10-12.json').read_text())['events'] == []


@pytest.mark.parametrize('tail', ['', '"summary": {"returned": 1}}', '"summary": {}'])
def test_shift_rejects_malformed_empty_document(tmp_path, tail):
    vectors=json.loads((ROOT/'docs/shift/dts1_test_vectors.json').read_text())
    token=shift.Token.parse(vectors['tokens']['flat'])
    src=tmp_path/'empty.json'; out=tmp_path/'out.json'
    src.write_text('{\n"meta": {},\n"events": [],\n'+tail)
    with pytest.raises((shift.ShiftError, ValueError)):
        shift.shift_file(str(src), str(out), token, 1)
    assert not out.exists()


@pytest.mark.parametrize('fmt', ['json', 'csv'])
def test_export_growth_beyond_cap_fails_stream(populated_client, populated, config, monkeypatch, fmt):
    config.query.max_export_rows = 65
    real=populated.iter_query
    def growing(**kwargs):
        yield from real(**kwargs)
        yield SpillEvent(nova_time=BASE+100*SECOND, spill_type=SpillType.NUMI, source='test')
    monkeypatch.setattr(populated, 'iter_query', growing)
    with pytest.raises(RuntimeError, match='archive grew'):
        populated_client.get('/api/export', params={'format':fmt})
