"""!
@file contrib/tduweb/run_as_python25.py
@brief Run a script under Python 2.7 with the 2.6+/2.7+ builtins removed.

Run a script under Python 2.7 with the 2.6+/2.7+ builtins removed.

Approximates Python 2.5 closely enough to catch the class of bug a source scan
misses: using a name that simply does not exist there. `bytes` is the case that
motivated it -- 2.7 has it as an alias for str, so code using it runs cleanly
here and dies on the TDU with NameError.

The names are deleted from __builtin__ globally, because per-module builtins
put Python into restricted execution mode, which blocks imports outright. The
standard library does use some of them internally -- sre_compile builds a
bytearray for every character class -- so everything the code under test will
reach is imported and exercised first, while the names are still present.

    python2 as25.py <script> [args...]

(The file is named run_as_python25.py; "as25" in the usage line and in the
stderr banner is its short name.)  The exit status is the script's: it runs
in this process via execfile, so its sys.exit propagates unchanged.
Arguments: argv[1] is the script, the rest become its sys.argv[1:].  Syntax
that 2.7 accepts and 2.5 does not is NOT caught here; use check_python25.py.
"""
import sys, os

## Builtin names that do not exist on Python 2.5 and are removed before the
## script under test runs.
ABSENT_IN_25 = [
    'bytes',        # 2.6, alias for str
    'bytearray',    # 2.6
    'next',         # 2.6
    'bin',          # 2.6
    'format',       # 2.6
    'memoryview',   # 2.7
]

# Present the script with the argv it would see if run directly, and make
# its directory importable (so tdu_webserver is found beside selftest).
script = sys.argv[1]
sys.argv = sys.argv[1:]
directory = os.path.dirname(os.path.abspath(script))
sys.path.insert(0, directory)

# --- warm everything the code under test will reach -----------------------
import re, subprocess, warnings, StringIO, types, collections      # noqa
import pickle, copy_reg, threading, socket, errno, traceback       # noqa
import encodings.utf_8, encodings.latin_1, encodings.ascii         # noqa

# Compile patterns that exercise the character-class path, which is where
# sre_compile reaches for bytearray.
for pattern in (r'^\s*,?\s*(?P<type>\d+)\s*,\s*0x(?P<h>[0-9a-fA-F]+)',
                r'[A-Z][A-Z0-9_]+$', r'[^ -~]', r'\w+\s*[a-z]'):
    re.compile(pattern).match('warm')
subprocess.list2cmdline(['warm'])
u'warm'.encode('utf-8').decode('utf-8', 'replace')

try:
    import bottle                                                   # noqa
except ImportError:
    pass

# --- now hide the newer builtins ------------------------------------------
#
# Warming is not enough on its own: re caches compiled patterns per pattern
# string, so any pattern the code under test compiles for the first time will
# still reach sre_compile's bytearray. So every module already imported at this
# point -- the standard library and bottle, none of which is under test -- is
# given its own copy of the names as a module attribute. Python resolves a
# global from the module namespace before falling back to builtins, so those
# modules keep working while anything imported from here on does not see them.
import __builtin__

already_loaded = [m for m in sys.modules.values() if m is not None]
originals = {}
for name in ABSENT_IN_25:
    if hasattr(__builtin__, name):
        originals[name] = getattr(__builtin__, name)

for module in already_loaded:
    for name, value in originals.items():
        try:
            if not hasattr(module, name):
                setattr(module, name, value)
        except (AttributeError, TypeError):
            pass

hidden = []
for name in originals:
    delattr(__builtin__, name)
    hidden.append(name)
sys.stderr.write("as25: simulating Python 2.5 by hiding: %s\n" % ", ".join(sorted(hidden)))

# A fresh __main__ namespace, so the script's `if __name__ == '__main__'`
# guard fires as it would when run directly.
namespace = {'__name__': '__main__', '__file__': script}
execfile(script, namespace)
