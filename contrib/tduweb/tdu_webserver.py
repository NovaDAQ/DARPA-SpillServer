"""!
@file contrib/tduweb/tdu_webserver.py
@brief TDU embedded web server, with a bulk spill-history route.

TDU embedded web server, with a bulk spill-history route.

.. note::
   This is a **copy**.  The file that gets deployed lives at
   ``server/tdu_webserver.py`` on the ``Darpa-Modifications`` branch of
   https://github.com/NovaDAQ/TDUWeb and that copy is authoritative.  It is
   mirrored here so this repository explains, in one place, what the TDU needs
   in order to serve a real event history.  Change it there, then re-sync.

This is a drop-in replacement for ``TDUWeb/server/tdu_webserver.py``.  Every
existing route issues the same command as before, and one new route is added:
``/spill_history``, which is what the DARPA Spill Information Server needs in
order to build a real event history.

There is one intentional behavioural change, in ``/tcr_start`` --- it no longer
blocks forever waiting for a process that never exits.  See that route.

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
This file runs on the TDU's stock Python 2.5 as well as Python 3.  That floor
is lower than it looks: 2.5 predates ``except X as e`` (it needs ``except X, e``),
``str.format``, and the ``json`` module, so this file uses ``%`` formatting
throughout and carries its own small JSON encoder.  bottle's ``json_dumps``
is not usable: without ``simplejson`` installed it is a stub that raises
``ImportError``.  Nothing is needed beyond the vendored ``bottle.py`` already
present in ``TDUWeb/server/``.

It also works with an **unpatched** ``DumpSpillHistory``.  That utility's bulk
JSON is malformed --- it emits ``printf("{\\nEvents: [\\n")``, an unquoted key
--- so this server parses the utility's CSV output instead, which has no such
problem.  Applying the DumpSpillHistory patch in the DARPA-SpillServer repository
(``contrib/tduweb/patches/0001-DumpSpillHistory-json-and-time-window.patch``)
makes the utility filter by time itself, which is faster on a full ring, but
is not required.

Usage
-----
Install on the TDU in place of the previous ``tdu_webserver.py`` and restart
the service.  See ``README.md`` beside this file, and ``server/README-DARPA.md``
on the TDUWeb ``Darpa-Modifications`` branch, for deployment and rollback.

It listens on ``0.0.0.0:8080`` by default, as the previous server did.  To use
another port, pass it as the first argument or set ``TDU_WEB_PORT``;
``TDU_WEB_HOST`` sets the address::

    python2.5 ./tdu_webserver.py 8081
    TDU_WEB_PORT=8081 python2.5 ./tdu_webserver.py
"""

import os
import re
import signal
import subprocess
import sys
import tempfile
import time

from bottle import route, run, request, response

## Where to listen.  Override with the TDU_WEB_HOST / TDU_WEB_PORT
## environment variables, or by passing the port as the first argument.
DEFAULT_HOST = '0.0.0.0'
## Default TCP port; the same 8080 the stock TDUWeb server used.
DEFAULT_PORT = 8080

## Shared-memory segment identifier the DAQ uses for accelerator events.
MEMORY_SEGMENT = 'BEAM'

## Path to the SHM_Utilities dump tool.
DUMP_SPILL_HISTORY = 'DumpSpillHistory'

## Hard ceiling on rows returned in one request, so a client cannot ask the
## TDU's PowerPC to serialise its entire ring buffer at once.
MAX_LIMIT = 20000

## Default rows returned when the client does not say.
DEFAULT_LIMIT = 5000

## One second of the NOvA 64 MHz clock, in ticks.
ONE_SECOND = 64000000

## Decoded SpillType values for the NuMI signals. kNuMI is 0, which is also
## what an unused ring slot decodes to -- see the note in spill_history().
NUMI_TYPE = 0
## Decoded SpillType value of the NuMI TCLK signal; also needs --numi.
NUMI_TCLK_TYPE = 2

## Seconds a helper command may run before it is killed.  bottle's default
## server is single-threaded, so a command that never returns takes down every
## route, not just the one that invoked it.  Nothing here may block without a
## bound.
COMMAND_TIMEOUT = 20.0

## Bytes of command output to keep.  A full-ring dump on the TDU's PowerPC can
## be very large, and reading it all would exhaust memory before it finished.
MAX_OUTPUT_BYTES = 8 * 1024 * 1024

## Matches one CSV row of DumpSpillHistory output:
##     "   3,    10733, 0x0780f1e2c3d4e5f6,  33778458638680249    4266769"
## A stray leading comma appears on every row after the first, because
## print_event() writes its separator before the row rather than after it.
## Groups: type (decoded SpillType), number (TDU event number), hextime (the
## same time in hex, unused), time (NOvA ticks), delta (ticks since the
## previous hardware event).  Note the last two are space-, not comma-,
## separated in the tool's output.
_CSV_ROW = re.compile(
    r'^\s*,?\s*(?P<type>\d+)\s*,'
    r'\s*(?P<number>\d+)\s*,'
    r'\s*0x(?P<hextime>[0-9a-fA-F]+)\s*,'
    r'\s*(?P<time>\d+)\s+'
    r'(?P<delta>\d+)'
)


## @brief Allow any origin to read the response (browser dashboards on
##        other hosts poll these routes directly).
## @return None; sets the Access-Control-Allow-Origin header on the current
##         bottle response.
def _allow_cors():
    response.set_header('Access-Control-Allow-Origin', '*')


## Text DumpSpillHistory prints to stdout when it cannot reach the segment.
## It is reported as an error rather than parsed as an empty result.
_SHM_FAILURE_MARKERS = (
    'Failed to attach',
    'Unable to stat shared memory',
    'Error finding last event',
)


def _run(command, timeout=COMMAND_TIMEOUT):
    """!@brief Run *command* and return its stdout as text, never blocking forever.

    Two properties matter more than they look, because bottle's default server
    is single-threaded: a command that hangs takes down every route, and output
    large enough to fill a pipe buffer hangs the command itself.

    So stdout goes to a temporary file rather than a pipe -- there is no buffer
    to fill -- and the process is polled against a deadline and killed if it
    passes it. Python 2.5's subprocess has no timeout argument, so the wait is
    written out by hand.

    The exit status is deliberately ignored, matching the original
    tdu_webserver.py. It is not a success indicator for these tools:
    DumpSpillHistory ends ``return 1;`` on the normal path (see
    SHM_Utilities/cxx/src/DumpSpillHistory.cc lines 420 and 472), so treating a
    non-zero status as failure would reject every successful call. Real
    failures are detected from the output instead, by _check_output below.

    @param command  argv list, executed directly (no shell).
    @param timeout  seconds before the process is killed; default
                    COMMAND_TIMEOUT.
    @return stdout as a text string (decoded UTF-8 with replacement on
            Python 3), at most MAX_OUTPUT_BYTES long.
    @throws RuntimeError  the command timed out, produced more than
                          MAX_OUTPUT_BYTES, or wrote nothing to stdout but
                          something to stderr.
    @throws OSError       the executable could not be started (from Popen).
    """
    out_file = tempfile.TemporaryFile()
    err_file = tempfile.TemporaryFile()
    try:
        process = subprocess.Popen(command, stdout=out_file, stderr=err_file)

        # Busy-poll at 20 ms: coarse enough to cost nothing on the PowerPC,
        # fine enough that a fast command returns without visible latency.
        deadline = time.time() + timeout
        while process.poll() is None:
            if time.time() >= deadline:
                _terminate(process)
                raise RuntimeError(
                    '%s did not finish within %.0f s and was killed. A full-ring '
                    'dump can take far longer than this; narrow the request with '
                    'since/until or types.' % (' '.join(command), timeout))
            time.sleep(0.02)

        out_file.seek(0)
        out = out_file.read(MAX_OUTPUT_BYTES)
        # One more byte after the cap means the output was longer than the
        # cap; this detects overflow without reading the rest into memory.
        truncated = len(out_file.read(1)) > 0
        err_file.seek(0)
        err = err_file.read(64 * 1024)
    finally:
        out_file.close()
        err_file.close()

    if not isinstance(out, str):
        out = out.decode('utf-8', 'replace')
    if not isinstance(err, str):
        err = err.decode('utf-8', 'replace')

    if truncated:
        raise RuntimeError(
            '%s produced more than %d bytes of output. That normally means the '
            'whole shared-memory segment is being dumped; narrow the request '
            'with since/until or types.' % (' '.join(command), MAX_OUTPUT_BYTES))

    # Nothing on stdout and something on stderr is the one unambiguous
    # failure: the tool produced no result and said why.
    if not out.strip() and err.strip():
        raise RuntimeError('%s produced no output: %s' % (
            ' '.join(command), err.strip()))
    return out


def _terminate(process):
    """!@brief End *process*, politely then not.

    SIGTERM first, then SIGKILL, each followed by up to 0.5 s (25 x 20 ms)
    of polling so the child is reaped and does not linger as a zombie.
    os.kill is used rather than Popen.terminate/kill, which are 2.6+.

    @param process  a subprocess.Popen instance.
    @return None.  Returns early once the process has exited, or when
            os.kill fails because the PID is already gone.
    """
    for signal_number in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.kill(process.pid, signal_number)
        except OSError:
            return
        for _ in range(25):
            if process.poll() is not None:
                return
            time.sleep(0.02)


def _check_output(text, command):
    """!@brief Raise if *text* is a shared-memory failure message rather than data.

    Needed because DumpSpillHistory reports these failures on stdout and its
    exit status is meaningless (see _run).

    @param text     stdout of a DumpSpillHistory call.
    @param command  the argv that produced it, for the error message.
    @return *text* unchanged when no failure marker is present.
    @throws RuntimeError  one of _SHM_FAILURE_MARKERS appears in *text*.
    """
    for marker in _SHM_FAILURE_MARKERS:
        if marker in text:
            raise RuntimeError('%s could not read shared memory: %s' % (
                ' '.join(command), text.strip().splitlines()[0]))
    return text


def _int_param(name, default, minimum=None, maximum=None):
    """!@brief Read an integer query parameter, clamped to a range.

    @param name     query-string key.
    @param default  value returned when the key is absent or empty.
    @param minimum  lower clamp, or None for none.
    @param maximum  upper clamp, or None for none.
    @return the parsed, clamped integer, or *default*.
    @throws ValueError  the value is not an integer literal.
    """
    raw = request.query.get(name)
    if raw is None or raw == '':
        return default
    try:
        # Base 0 accepts decimal and 0x-prefixed hex, since NOvA times are
        # often quoted in hex.  It also accepts 0o/0b and, on Python 2,
        # a leading-zero literal as octal.
        value = int(raw, 0)
    except ValueError:
        raise ValueError('%s must be an integer, got %r' % (name, raw))
    if minimum is not None and value < minimum:
        value = minimum
    if maximum is not None and value > maximum:
        value = maximum
    return value


def _parse_history(text):
    """!@brief Parse DumpSpillHistory CSV output into a list of dicts.

    Lines that do not match _CSV_ROW (headers, rules, diagnostics) are
    skipped silently rather than raising, so a cosmetic change in the tool's
    output degrades to fewer rows instead of a 500.

    @param text  stdout of DumpSpillHistory without --json.
    @return list of dicts with keys Type, Number, Time, Delta (ints) and
            Offset (Time modulo one second, i.e. the PPS phase), in input
            order.  Zeroed ring slots are omitted.
    """
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
        # A zeroed ring slot parses as a type-0 event at time 0. It is not an
        # event, it is unused space, and it must never reach a client.
        if time_ticks == 0:
            continue
        events.append({
            'Type': int(match.group('type')),
            'Number': int(match.group('number')),
            'Time': time_ticks,
            'Delta': int(match.group('delta')),
            'Offset': time_ticks % ONE_SECOND,
        })
    return events


# ---------------------------------------------------------------------------
# JSON encoding.
#
# Python 2.5 has no json module -- it arrived in 2.6 -- and the TDU has no
# simplejson either.  bottle's json_dumps falls back to a stub that raises
# ImportError("JSON support requires Python 2.6 or simplejson."), so it cannot
# be used here.  Rather than add a dependency to a machine that is deliberately
# minimal, the few shapes this server emits are serialised directly.
#
# The encoder is used on every interpreter, not only on 2.5, so that the output
# is identical wherever this runs and the code path is exercised by every test
# rather than only in production.
# ---------------------------------------------------------------------------

# _TEXT_TYPE / _BYTE_TYPE / _INTEGER_TYPES:
# Text and byte types, named without relying on anything newer than 2.5.
# `bytes` cannot be used directly: it is a 2.6 addition (an alias for str), so
# referring to it raises NameError on the TDU while working fine on 2.6+.
try:
    _TEXT_TYPE = unicode          # noqa: F821  (Python 2)
    _BYTE_TYPE = str
except NameError:                 # pragma: no cover  (Python 3)
    _TEXT_TYPE = str
    _BYTE_TYPE = bytes        # py25-ok

try:
    _INTEGER_TYPES = (int, long)  # noqa: F821  (Python 2)
except NameError:                 # pragma: no cover  (Python 3)
    _INTEGER_TYPES = (int,)

## Characters with a short JSON escape.  '/' is legal unescaped and is left
## alone; other control characters fall through to \\uXXXX in _json_string.
_JSON_STRING_ESCAPES = {
    '"':  '\\"',
    '\\': '\\\\',
    '\b': '\\b',
    '\f': '\\f',
    '\n': '\\n',
    '\r': '\\r',
    '\t': '\\t',
}


def _json_text(value):
    """!@brief Return *value* as a text string, on either Python 2 or 3.

    @param value  text, bytes (decoded as UTF-8 with replacement), or any
                  object (converted with the text type's constructor).
    @return a unicode (Py2) / str (Py3) object.
    """
    if isinstance(value, _TEXT_TYPE):
        return value
    if isinstance(value, _BYTE_TYPE):
        return value.decode('utf-8', 'replace')
    return _TEXT_TYPE(value)


def _json_string(value):
    """!@brief Encode a string as a JSON string literal, escaped to plain ASCII.

    Everything outside printable ASCII becomes a \\uXXXX escape, so the body
    is valid JSON whatever encoding the client assumes.

    @param value  string-like value; see _json_text for coercion.
    @return the quoted, escaped JSON literal as a string.
    """
    parts = ['"']
    for character in _json_text(value):
        if character in _JSON_STRING_ESCAPES:
            parts.append(_JSON_STRING_ESCAPES[character])
            continue
        code = ord(character)
        if code < 0x20 or code > 0x7E:
            if code > 0xFFFF:
                # Encode astral characters as a UTF-16 surrogate pair, which
                # is what the JSON specification requires.
                code = code - 0x10000
                parts.append('\\u%04x' % (0xD800 + (code >> 10)))
                parts.append('\\u%04x' % (0xDC00 + (code & 0x3FF)))
            else:
                parts.append('\\u%04x' % code)
        else:
            parts.append(character)
    parts.append('"')
    return ''.join(parts)


def _json_encode(value):
    """!@brief Serialise *value* as JSON.

    Handles the shapes this server produces: None, booleans, integers,
    floats, strings, lists, tuples and dicts.  Anything else is rendered as
    its string form rather than raising, so a diagnostic response can never
    fail to serialise.

    @param value  the object to encode; recursed into for lists, tuples and
                  dicts.  Dict keys are always emitted as strings.
    @return the JSON text.  Dict key order is the interpreter's iteration
            order, so it is not stable on Python 2.
    """
    if value is None:
        return 'null'
    if value is True:
        return 'true'
    if value is False:
        return 'false'
    # bool is a subclass of int, so it must be tested before this.
    if isinstance(value, _INTEGER_TYPES):
        return str(value)
    if isinstance(value, float):
        # repr, not str: Python 2's str() rounds to 12 significant digits.
        # NaN/Infinity would come out as nan/inf, which is not JSON; no
        # route emits a float today.
        return repr(value)
    if isinstance(value, (list, tuple)):
        return '[' + ', '.join([_json_encode(item) for item in value]) + ']'
    if isinstance(value, dict):
        pairs = []
        for key in value:
            pairs.append('%s: %s' % (_json_string(key), _json_encode(value[key])))
        return '{' + ', '.join(pairs) + '}'
    return _json_string(value)


def _json_response(payload):
    """!@brief Serialise *payload* as JSON and set the response headers.

    @param payload  the object to encode (normally a dict).
    @return the JSON body; bottle sends a returned string as-is.
    """
    _allow_cors()
    response.content_type = 'application/json'
    return _json_encode(payload)


# ---------------------------------------------------------------------------
# Existing routes. Every one issues the same command as before; see
# README-DARPA.md for the single behavioural difference, in /tcr_start.
# ---------------------------------------------------------------------------

## @brief GET /tcr_status: newest event in the ring, as DumpSpillHistory's
##        JSON.
## @details Despite --tcr, --last ignores the type filter, so this returns
##          the same record as /onehz_status (see the module docstring).
## @return DumpSpillHistory --last --json stdout, passed through unparsed.
## @throws RuntimeError/OSError from _run, surfacing as a bottle 500.
@route('/tcr_status')
def tcr_status():
    _allow_cors()
    return _run([DUMP_SPILL_HISTORY, '-m', MEMORY_SEGMENT,
                 '--last', '--tcr', '--json'])


## @brief GET /onehz_status: nominally the newest 1 Hz event; in practice
##        identical to /tcr_status (--last ignores --onehertz).
## @return DumpSpillHistory --last --json stdout, passed through unparsed.
## @throws RuntimeError/OSError from _run, surfacing as a bottle 500.
@route('/onehz_status')
def onehz_status():
    _allow_cors()
    return _run([DUMP_SPILL_HISTORY, '-m', MEMORY_SEGMENT,
                 '--last', '--onehertz', '--json'])


## @brief GET /tdu_status: full TDU register dump.
## @return tduRegDump stdout as plain text.
## @throws RuntimeError/OSError from _run, surfacing as a bottle 500.
@route('/tdu_status')
def tdu_status():
    _allow_cors()
    return _run(['tduRegDump'])


## @brief GET /tdu_init and /tdu_sync (aliases): write 0x0420 to TDU
##        register 0x0, the init/sync command of the stock server.
## @details State-changing on a GET, unchanged from the original server.
## @return tduControl stdout.
## @throws RuntimeError/OSError from _run, surfacing as a bottle 500.
@route('/tdu_sync')
@route('/tdu_init')
def tdu_init():
    _allow_cors()
    return _run(['tduControl', 'set', '0x0', '0x0420'])


## @brief GET /tdu_scrub: write 0x0004 to TDU register 0x0009 (scrub
##        command, as in the stock server).
## @return tduControl stdout.
## @throws RuntimeError/OSError from _run, surfacing as a bottle 500.
@route('/tdu_scrub')
def tdu_scrub():
    _allow_cors()
    return _run(['tduControl', 'set', '0x0009', '0x0004'])


## @brief GET /tdu_errors: read the TDU error register at 0x0014.
## @return tduRegDump -w -a 0x0014 stdout.
## @throws RuntimeError/OSError from _run, surfacing as a bottle 500.
@route('/tdu_errors')
def tdu_errors():
    _allow_cors()
    return _run(['tduRegDump', '-w', '-a', '0x0014'])


## @brief GET /tdu_errors2: read the second TDU error register at 0x000e.
## @return tduRegDump -w -a 0x000e stdout.
## @throws RuntimeError/OSError from _run, surfacing as a bottle 500.
@route('/tdu_errors2')
def tdu_errors2():
    _allow_cors()
    return _run(['tduRegDump', '-w', '-a', '0x000e'])


## @brief GET /tcr_start: launch TCRMonitor detached-in-practice and return
##        immediately.
## @return the fixed string 'TCRMonitor Started'.
## @throws OSError  TCRMonitor is not on PATH (from Popen).
@route('/tcr_start')
def tcr_start():
    """!Start TCRMonitor in the background.

    The previous version called .communicate() on this process. With -D 0
    TCRMonitor does not daemonize (daemon_flag = 0 in TCRMonitor.cc), so it
    never exits, and .communicate() blocked forever -- hanging the request and,
    with bottle's single-threaded default server, every later request too.
    The process is launched and left alone here; the route already returned a
    fixed string rather than its output, so nothing is lost.
    """
    _allow_cors()
    # The pipe is never read.  That is harmless only while TCRMonitor writes
    # less than a pipe buffer to stdout; more and it would block on write.
    subprocess.Popen(['TCRMonitor', '-D', '0', '-S', '0'],
                     stdout=subprocess.PIPE)
    return 'TCRMonitor Started'


## @brief GET /tcr_running: whether a TCRMonitor process exists.
## @return the literal JSON text {"State": "Running"} if pidof finds one,
##         {"State": "Defunct"} otherwise (hand-written, as in the stock
##         server).
## @throws OSError  pidof is not on PATH.
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

## @brief GET /spill_history: a time window of accelerator events as JSON.
## @details The Python docstring of this function is NOT Doxygen-formatted
##          on purpose: it is a runtime %-format template, filled with
##          (DEFAULT_LIMIT, MAX_LIMIT) right after the definition and served
##          as user-facing help.  Adding markup or a literal percent sign to
##          it would break that format operation at import time.
##          Query parameters: since, until (NOvA ticks, half-open
##          [since, until)), limit (clamped to 1..MAX_LIMIT), types
##          (comma-separated decoded SpillType numbers).
## @return JSON object {events, count, truncated, limit}; HTTP 400 with
##         {error} on a malformed parameter; HTTP 500 with {error} when
##         DumpSpillHistory fails, times out, overflows, or cannot attach
##         to shared memory.
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
    except ValueError, exc:
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

    # --numi is NOT passed by default, and that is not an oversight.
    #
    # DumpSpillHistory decides what to print by switching on evt_type, and
    # kNuMI is the first member of the SpillType enum, so its value is 0. An
    # unused slot in the shared-memory ring is all zeros, which therefore
    # matches `case kNuMI` -- and with --numi set, every empty slot in the
    # whole segment is printed, each one paying for a Boost date conversion.
    # On the TDU's PowerPC that does not finish in any useful time, and with
    # bottle's single-threaded server it takes down every other route with it.
    #
    # So NuMI is requested only when the caller actually asks for it by type,
    # and even then _run's timeout and output cap bound the damage.
    command = [DUMP_SPILL_HISTORY, '-m', MEMORY_SEGMENT,
               '--booster', '--onehertz', '--tcr']
    if keep_types is not None and (NUMI_TYPE in keep_types
                                   or NUMI_TCLK_TYPE in keep_types):
        command.append('--numi')
    try:
        raw = _check_output(_run(command), command)
    except (RuntimeError, OSError), exc:
        response.status = 500
        return _json_response({'error': str(exc)})

    events = _parse_history(raw)

    # Filtering is done here, after a full dump, because the stock
    # DumpSpillHistory has no time-window option; the optional patch in
    # contrib/tduweb/patches adds one but this server does not rely on it.
    if since is not None:
        events = [e for e in events if e['Time'] >= since]
    if until is not None:
        events = [e for e in events if e['Time'] < until]
    if keep_types is not None:
        events = [e for e in events if e['Type'] in keep_types]

    # Dump order is not relied on to be time order (the ring is circular),
    # so sort before truncating: truncation must drop the newest rows, or a
    # client resuming with since = last Time received would skip events.
    # list.sort(key=) is available from 2.4.
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


# Fill the limits into the docstring so the help text cannot drift from the
# constants.  This is why that docstring must stay a plain %-template.
spill_history.__doc__ = spill_history.__doc__ % (DEFAULT_LIMIT, MAX_LIMIT)


@route('/spill_history_info')
def spill_history_info():
    """!@brief GET /spill_history_info: report what this server supports,
    for client capability probing.

    Costs the TDU nothing, unlike probing /spill_history?limit=1, which
    walks the whole ring.  A stock TDUWeb server answers 404 here
    (darpa_spillserver.tdu_client.TDUClient.supports_history relies on that).

    @return JSON object: spill_history (true), max_limit, default_limit,
            memory_segment, raw_event_word (false) and an explanatory note.
    """
    return _json_response({
        'spill_history': True,
        'max_limit': MAX_LIMIT,
        'default_limit': DEFAULT_LIMIT,
        'memory_segment': MEMORY_SEGMENT,
        'raw_event_word': False,
        'note': ('Records carry the decoded spill type only. The shared-memory '
                 'layout does not preserve the raw 16-bit event word, so $1D '
                 'cannot be distinguished from $1F, nor $A9 from $AD. See '
                 'server/README-DARPA.md for the change that would add it.'),
    })


def server_host():
    """!@brief Address to bind: TDU_WEB_HOST, else every interface.

    @return host string; DEFAULT_HOST when TDU_WEB_HOST is unset or empty.
    """
    return os.environ.get('TDU_WEB_HOST') or DEFAULT_HOST


def server_port():
    """!@brief Port to bind.

    Checked in order: the first command-line argument, then TDU_WEB_PORT,
    then DEFAULT_PORT.  Both mechanisms exist because they suit different
    callers -- an argument is natural in a shell, while an environment
    variable is what an init script can set when the launch command itself
    is not easily edited.

    @return the first candidate that is an integer in 1..65535, else
            DEFAULT_PORT.  Rejected candidates are reported on stderr and
            skipped, not fatal, so a bad value cannot stop the TDU's web
            service from coming up.
    """
    candidates = []
    if len(sys.argv) > 1:
        candidates.append(('argument', sys.argv[1]))
    if os.environ.get('TDU_WEB_PORT'):
        candidates.append(('TDU_WEB_PORT', os.environ['TDU_WEB_PORT']))

    for origin, value in candidates:
        try:
            port = int(value)
        except ValueError:
            sys.stderr.write('tdu_webserver: ignoring %s %r: not a number\n'
                             % (origin, value))
            continue
        if port < 1 or port > 65535:
            sys.stderr.write('tdu_webserver: ignoring %s %r: out of range\n'
                             % (origin, value))
            continue
        return port
    return DEFAULT_PORT


def serve():
    """!@brief Start the web server on the configured address.

    Blocks for the life of the server.  debug=True is inherited from the
    stock server; it makes bottle return tracebacks in 500 responses.

    @return None (only when bottle's run() returns).
    """
    run(host=server_host(), port=server_port(), debug=True)


# The original tdu_webserver.py called run() at the end of the module with no
# __main__ guard, so merely importing it started the server.  That is preserved
# here, because the TDU's service may launch the file either way -- but it now
# goes through serve(), so the port is honoured in both cases rather than only
# when the file is executed directly.
#
# Set TDU_WEB_NO_SERVE=1 to import the module without starting anything, which
# is what a linter or a test needs.
if __name__ == '__main__':
    serve()
elif os.environ.get('TDU_WEB_NO_SERVE', '').lower() not in ('1', 'true', 'yes'):
    serve()
