"""TDU embedded web server, with a bulk spill-history route.

This is a drop-in replacement for ``TDUWeb/server/tdu_webserver.py``.  It keeps
every existing route byte-for-byte compatible and adds one new route,
``/spill_history``, which is what the DARPA Spill Information Server needs in
order to build a real event history.

Why a new route is necessary
----------------------------
The existing ``/tcr_status`` and ``/onehz_status`` routes both run
``DumpSpillHistory --last``, which returns exactly one event --- the newest in
the shared-memory ring.  Two consequences follow, and both are easy to verify
on a live TDU:

1. ``--last`` in ``DumpSpillHistory.cc`` indexes ``data_entries - 1`` directly
   and never applies the ``--tcr`` / ``--onehertz`` / ``--numi`` filters, so
   **the two routes return the same record**.
2. Accelerator events are recorded at roughly 6--15 Hz.  A client polling once
   a second therefore observes under a tenth of them, and no polling rate that
   is kind to the TDU's PowerPC can close that gap.

``/spill_history`` returns a *window* of events in one request, so a client
polls at whatever rate it likes and still misses nothing.

Compatibility
-------------
This file runs on the TDU's stock Python 2.6/2.7 as well as Python 3: it uses
no f-strings, no ``pathlib``, and no third-party modules beyond the vendored
``bottle.py`` already present in ``TDUWeb/server/``.

It also works with an **unpatched** ``DumpSpillHistory``.  That utility's bulk
JSON is malformed --- it emits ``printf("{\\nEvents: [\\n")``, an unquoted key
--- so this server parses the utility's CSV output instead, which has no such
problem.  Applying ``patches/0001-DumpSpillHistory-json-and-time-window.patch``
makes the utility filter by time itself, which is faster on a full ring, but
is not required.

Usage
-----
Install as ``TDUWeb/server/tdu_webserver.py`` on the TDU and restart the
service.  See README.md in this directory for the deployment steps.
"""

import re
import subprocess
import sys

from bottle import route, run, request, response

#: Shared-memory segment identifier the DAQ uses for accelerator events.
MEMORY_SEGMENT = 'BEAM'

#: Path to the SHM_Utilities dump tool.
DUMP_SPILL_HISTORY = 'DumpSpillHistory'

#: Hard ceiling on rows returned in one request, so a client cannot ask the
#: TDU's PowerPC to serialise its entire ring buffer at once.
MAX_LIMIT = 20000

#: Default rows returned when the client does not say.
DEFAULT_LIMIT = 5000

#: One second of the NOvA 64 MHz clock, in ticks.
ONE_SECOND = 64000000

#: Matches one CSV row of DumpSpillHistory output:
#:     "   3,    10733, 0x0780f1e2c3d4e5f6,  33778458638680249    4266769"
#: A stray leading comma appears on every row after the first, because
#: print_event() writes its separator before the row rather than after it.
_CSV_ROW = re.compile(
    r'^\s*,?\s*(?P<type>\d+)\s*,'
    r'\s*(?P<number>\d+)\s*,'
    r'\s*0x(?P<hextime>[0-9a-fA-F]+)\s*,'
    r'\s*(?P<time>\d+)\s+'
    r'(?P<delta>\d+)'
)


def _allow_cors():
    response.set_header('Access-Control-Allow-Origin', '*')


#: Text DumpSpillHistory prints to stdout when it cannot reach the segment.
#: It is reported as an error rather than parsed as an empty result.
_SHM_FAILURE_MARKERS = (
    'Failed to attach',
    'Unable to stat shared memory',
    'Error finding last event',
)


def _run(command):
    """Run *command* and return its stdout as text.

    The exit status is deliberately ignored, matching the original
    tdu_webserver.py. It is not a success indicator for these tools:
    DumpSpillHistory ends ``return 1;`` on the normal path (see
    SHM_Utilities/cxx/src/DumpSpillHistory.cc lines 420 and 472), so treating
    a non-zero status as failure would reject every successful call. Real
    failures are detected from the output instead, by _check_output below.

    subprocess.Popen is used rather than check_output so that this file keeps
    working on the Python 2.6 that some TDUs still carry.
    """
    process = subprocess.Popen(command, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
    out, err = process.communicate()
    if not isinstance(out, str):
        out = out.decode('utf-8', 'replace')
    if not isinstance(err, str):
        err = err.decode('utf-8', 'replace')

    # Nothing on stdout and something on stderr is the one unambiguous
    # failure: the tool produced no result and said why.
    if not out.strip() and err.strip():
        raise RuntimeError('%s produced no output: %s' % (
            ' '.join(command), err.strip()))
    return out


def _check_output(text, command):
    """Raise if *text* is a shared-memory failure message rather than data."""
    for marker in _SHM_FAILURE_MARKERS:
        if marker in text:
            raise RuntimeError('%s could not read shared memory: %s' % (
                ' '.join(command), text.strip().splitlines()[0]))
    return text


def _int_param(name, default, minimum=None, maximum=None):
    """Read an integer query parameter, clamped to a range."""
    raw = request.query.get(name)
    if raw is None or raw == '':
        return default
    try:
        value = int(raw, 0)
    except ValueError:
        raise ValueError('%s must be an integer, got %r' % (name, raw))
    if minimum is not None and value < minimum:
        value = minimum
    if maximum is not None and value > maximum:
        value = maximum
    return value


def _parse_history(text):
    """Parse DumpSpillHistory CSV output into a list of dicts."""
    events = []
    for line in text.splitlines():
        if not line.strip():
            continue
        # Skip the header and its rule.
        if line.startswith('Type,') or line.startswith('==='):
            continue
        match = _CSV_ROW.match(line)
        if match is None:
            continue
        time_ticks = int(match.group('time'))
        events.append({
            'Type': int(match.group('type')),
            'Number': int(match.group('number')),
            'Time': time_ticks,
            'Delta': int(match.group('delta')),
            'Offset': time_ticks % ONE_SECOND,
        })
    return events


def _json_response(payload):
    """Serialise *payload* as JSON without depending on bottle's encoder."""
    import json
    _allow_cors()
    response.content_type = 'application/json'
    return json.dumps(payload)


# ---------------------------------------------------------------------------
# Existing routes, unchanged in behaviour.
# ---------------------------------------------------------------------------

@route('/tcr_status')
def tcr_status():
    _allow_cors()
    return _run([DUMP_SPILL_HISTORY, '-m', MEMORY_SEGMENT,
                 '--last', '--tcr', '--json'])


@route('/onehz_status')
def onehz_status():
    _allow_cors()
    return _run([DUMP_SPILL_HISTORY, '-m', MEMORY_SEGMENT,
                 '--last', '--onehertz', '--json'])


@route('/tdu_status')
def tdu_status():
    _allow_cors()
    return _run(['tduRegDump'])


@route('/tdu_sync')
@route('/tdu_init')
def tdu_init():
    _allow_cors()
    return _run(['tduControl', 'set', '0x0', '0x0420'])


@route('/tdu_scrub')
def tdu_scrub():
    _allow_cors()
    return _run(['tduControl', 'set', '0x0009', '0x0004'])


@route('/tdu_errors')
def tdu_errors():
    _allow_cors()
    return _run(['tduRegDump', '-w', '-a', '0x0014'])


@route('/tdu_errors2')
def tdu_errors2():
    _allow_cors()
    return _run(['tduRegDump', '-w', '-a', '0x000e'])


@route('/tcr_start')
def tcr_start():
    _allow_cors()
    subprocess.Popen(['TCRMonitor', '-D', '0', '-S', '0'],
                     stdout=subprocess.PIPE)
    return 'TCRMonitor Started'


@route('/tcr_running')
def tcr_running():
    _allow_cors()
    state = subprocess.call(['pidof', 'TCRMonitor'])
    if state == 0:
        return '{ "State": "Running"}'
    return '{ "State": "Defunct"}'


# ---------------------------------------------------------------------------
# New: bulk spill history.
# ---------------------------------------------------------------------------

@route('/spill_history')
def spill_history():
    """Return a window of accelerator events as JSON.

    Query parameters, all optional:

    ``since``
        Lower bound in NOvA ticks, inclusive.  Accepts ``0x``-prefixed hex.
    ``until``
        Upper bound in NOvA ticks, exclusive.
    ``limit``
        Maximum rows to return (default %d, capped at %d).  When more rows
        match than are returned, ``truncated`` is true and the client should
        ask again with ``since`` set past the last row it received.
    ``types``
        Comma-separated decoded spill-type numbers to keep, e.g. ``0,4``.

    The response is::

        {"events": [{"Type":..,"Number":..,"Time":..,"Delta":..,"Offset":..}],
         "count": N, "truncated": false, "limit": L}

    Rows are ordered oldest first, which is the order the ring holds them.
    """
    try:
        since = _int_param('since', None, minimum=0)
        until = _int_param('until', None, minimum=0)
        limit = _int_param('limit', DEFAULT_LIMIT, minimum=1, maximum=MAX_LIMIT)
    except ValueError as exc:
        response.status = 400
        return _json_response({'error': str(exc)})

    types_param = request.query.get('types')
    keep_types = None
    if types_param:
        try:
            keep_types = set(int(t) for t in types_param.split(',') if t.strip())
        except ValueError:
            response.status = 400
            return _json_response(
                {'error': 'types must be comma-separated integers, got %r'
                          % types_param})

    # Every selection flag is passed so the utility emits all stored events;
    # filtering happens here, where it is cheap to change.
    command = [DUMP_SPILL_HISTORY, '-m', MEMORY_SEGMENT,
               '--booster', '--numi', '--onehertz', '--tcr']
    try:
        raw = _check_output(_run(command), command)
    except (RuntimeError, OSError) as exc:
        response.status = 500
        return _json_response({'error': str(exc)})

    events = _parse_history(raw)

    if since is not None:
        events = [e for e in events if e['Time'] >= since]
    if until is not None:
        events = [e for e in events if e['Time'] < until]
    if keep_types is not None:
        events = [e for e in events if e['Type'] in keep_types]

    events.sort(key=lambda e: e['Time'])

    truncated = len(events) > limit
    if truncated:
        events = events[:limit]

    return _json_response({
        'events': events,
        'count': len(events),
        'truncated': truncated,
        'limit': limit,
    })


spill_history.__doc__ = spill_history.__doc__ % (DEFAULT_LIMIT, MAX_LIMIT)


@route('/spill_history_info')
def spill_history_info():
    """Report what this server supports, for client capability probing."""
    return _json_response({
        'spill_history': True,
        'max_limit': MAX_LIMIT,
        'default_limit': DEFAULT_LIMIT,
        'memory_segment': MEMORY_SEGMENT,
        'raw_event_word': False,
        'note': ('Records carry the decoded spill type only. The shared-memory '
                 'layout does not preserve the raw 16-bit event word, so $1D '
                 'cannot be distinguished from $1F, nor $A9 from $AD. See '
                 'patches/0002 for the change that would add it.'),
    })


if __name__ == '__main__':
    port = 8080
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    run(host='0.0.0.0', port=port, debug=True)
else:
    run(host='0.0.0.0', port=8080, debug=True)
