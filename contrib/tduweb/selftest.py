#!/usr/bin/env python
"""Self-test for tdu_webserver.py, runnable on the TDU itself.

    python2.5 ./selftest.py

Checks the parts that have no other safety net: the JSON encoder and the
listen-port resolution.  It imports tdu_webserver with TDU_WEB_NO_SERVE set,
so no socket is opened and nothing interferes with a running server.

The encoder is checked against literal expected output rather than by
round-tripping through a JSON parser, because Python 2.5 has no json module
-- which is the very reason the encoder exists.

Written to the same Python 2.5 floor as tdu_webserver.py: no "except X as e",
no str.format, no json, no comprehension syntax beyond list comprehensions.
"""

import os
import sys

os.environ['TDU_WEB_NO_SERVE'] = '1'
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tdu_webserver as tw

FAILURES = []


def check(label, got, expected):
    if got == expected:
        print "  ok    %s" % label
    else:
        print "  FAIL  %s" % label
        print "          got     : %r" % (got,)
        print "          expected: %r" % (expected,)
        FAILURES.append(label)


def check_json(label, value, expected):
    check(label, tw._json_encode(value), expected)


# unichr is Python 2 only; chr covers Python 3. A narrow 2.x build cannot
# hold an astral character in one unichr call, so that case is skipped.
try:
    _ASTRAL = unichr(0x1F600)        # noqa: F821  (Python 2 wide build)
except NameError:                    # pragma: no cover  (Python 3)
    _ASTRAL = chr(0x1F600)
except ValueError:                   # narrow Python 2 build
    _ASTRAL = None

print "JSON encoder"
check_json("null",            None,            'null')
check_json("true",            True,            'true')
check_json("false",           False,           'false')
check_json("zero",            0,               '0')
check_json("negative",        -17,             '-17')
check_json("large integer",   33778458638680249, '33778458638680249')
check_json("empty string",    "",              '""')
check_json("plain string",    "hello",         '"hello"')
check_json("quotes",          'say "hi"',      '"say \\"hi\\""')
check_json("backslash",       "a\\b",          '"a\\\\b"')
check_json("newline",         "a\nb",          '"a\\nb"')
check_json("tab",             "a\tb",          '"a\\tb"')
check_json("carriage return", "a\rb",          '"a\\rb"')
check_json("null byte",       "a\x00b",        '"a\\u0000b"')
check_json("unit separator",  "a\x1fb",        '"a\\u001fb"')
# Built with unichr rather than written literally, so this file stays
# pure ASCII and needs no PEP 263 encoding declaration.
try:
    _E_ACUTE = unichr(0xE9)          # noqa: F821  (Python 2)
except NameError:                    # pragma: no cover  (Python 3)
    _E_ACUTE = chr(0xE9)
check_json("non-ascii",       u"caf" + _E_ACUTE, '"caf\\u00e9"')
if _ASTRAL is not None:
    check_json("astral",      _ASTRAL,         '"\\ud83d\\ude00"')
else:
    print "  skip  astral (narrow Python build)"
check_json("empty list",      [],              '[]')
check_json("list",            [1, 2, 3],       '[1, 2, 3]')
check_json("mixed list",      [None, True, 1], '[null, true, 1]')
check_json("empty dict",      {},              '{}')
check_json("single pair",     {"a": 1},        '{"a": 1}')
check_json("nested",          {"a": [{"b": 2}]}, '{"a": [{"b": 2}]}')
check_json("dollar in text",  {"s": "$1D"},    '{"s": "$1D"}')

# A realistic payload. Dict order is not defined, so check the pieces.
payload = {
    'events': [{'Type': 3, 'Number': 10733, 'Time': 33778458638680249,
                'Delta': 4266769, 'Offset': 14680249}],
    'count': 1,
    'truncated': False,
    'limit': 5000,
}
encoded = tw._json_encode(payload)
print "  ---   sample payload: %s" % encoded
for fragment in ['"count": 1', '"truncated": false', '"limit": 5000',
                 '"Time": 33778458638680249', '"Type": 3']:
    if fragment in encoded:
        print "  ok    payload contains %s" % fragment
    else:
        print "  FAIL  payload missing %s" % fragment
        FAILURES.append("payload fragment %s" % fragment)

if not (encoded.startswith('{') and encoded.endswith('}')):
    print "  FAIL  payload is not a JSON object"
    FAILURES.append("payload braces")

print
print "History CSV parsing"
sample = (
    "Type, No., Time, Time Hex, Delta Prev.\n"
    "===========================================================\n"
    "   3,    10733, 0x0780f1e2c3d4e5f6,    33778458638680249    4266769 \n"
    ",   4,    10734, 0x0780f1e2c3d4e600,    33778458702680249   64000000 \n"
)
events = tw._parse_history(sample)
check("two rows parsed", len(events), 2)
if len(events) == 2:
    check("first row type",   events[0]['Type'],   3)
    check("first row number", events[0]['Number'], 10733)
    check("first row time",   events[0]['Time'],   33778458638680249)
    check("leading comma row", events[1]['Type'],  4)

print
print "Shared-memory failure detection"
try:
    tw._check_output("Failed to attach to existing shared memory segment\n",
                     ['DumpSpillHistory'])
    print "  FAIL  a shared-memory error was not detected"
    FAILURES.append("shm detection")
except RuntimeError:
    print "  ok    shared-memory error detected"
check("normal output passes through",
      tw._check_output("   3, 1, 0x0, 1 2\n", ['x']), "   3, 1, 0x0, 1 2\n")

print
print "Listen port"
check("default", tw.server_port(), tw.DEFAULT_PORT)
check("default host", tw.server_host(), tw.DEFAULT_HOST)

saved_argv = sys.argv
sys.argv = ['tdu_webserver.py', '8081']
check("from argument", tw.server_port(), 8081)
sys.argv = saved_argv

os.environ['TDU_WEB_PORT'] = '8082'
check("from TDU_WEB_PORT", tw.server_port(), 8082)
os.environ['TDU_WEB_PORT'] = 'banana'
check("bad value falls back", tw.server_port(), tw.DEFAULT_PORT)
os.environ['TDU_WEB_PORT'] = '99999'
check("out of range falls back", tw.server_port(), tw.DEFAULT_PORT)
del os.environ['TDU_WEB_PORT']

os.environ['TDU_WEB_HOST'] = '127.0.0.1'
check("host from TDU_WEB_HOST", tw.server_host(), '127.0.0.1')
del os.environ['TDU_WEB_HOST']

print
if FAILURES:
    print "%d FAILURE(S): %s" % (len(FAILURES), ", ".join(FAILURES))
    sys.exit(1)
print "all checks passed"
sys.exit(0)
