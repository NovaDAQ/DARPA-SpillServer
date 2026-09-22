"""Flag constructs that need Python 2.6 or later. Run under python2."""
import re, sys, tokenize, StringIO

CHECKS = [
    (r'\bexcept\s+[^,:\n]+\s+as\s+\w+\s*:', "'except X as e' needs 2.6+; use 'except X, e:'"),
    (r'\.format\s*\(',                      "str.format() needs 2.6+; use % formatting"),
    (r'^\s*(import|from)\s+json\b',         "the json module needs 2.6+; use simplejson or bottle.json_dumps"),
    (r'^\s*(import|from)\s+argparse\b',     "argparse needs 2.7+"),
    (r'^\s*(import|from)\s+importlib\b',    "importlib needs 2.7+"),
    (r'\bb["\']',                           "bytes literals need 2.6+"),
    (r'\{[^{}\n]*\bfor\b[^{}\n]*\}',        "dict/set comprehensions need 2.7+"),
    (r'\bOrderedDict\b',                    "collections.OrderedDict needs 2.7+"),
    (r'\bCounter\b',                        "collections.Counter needs 2.7+"),
    (r'\bcheck_output\b',                   "subprocess.check_output needs 2.7+"),
    (r'\bnext\s*\(',                        "the next() builtin needs 2.6+"),
    (r'(?<![.\w])bytes(?![\w])',           "the bytes builtin needs 2.6+ (alias for str)"),
    (r'(?<![.\w])bytearray(?![\w])',       "bytearray needs 2.6+"),
    (r'(?<![.\w])memoryview(?![\w])',      "memoryview needs 2.7+"),
    (r'(?<![.\w])bin\s*\(',               "the bin() builtin needs 2.6+"),
    (r'(?<![.\w])format\s*\(',            "the format() builtin needs 2.6+"),
    (r'^\s*with\s+',                        "'with' needs 2.6+ (or a __future__ import on 2.5)"),
    (r'\bprint\s*\(.*,.*\)',                "print(a, b) is not a 2.5 print statement"),
    (r'\bfrom\s+__future__\s+import\s+print_function', "print_function needs 2.6+"),
]

def strings_and_comments(source):
    """Byte ranges that are string literals or comments, to skip."""
    spans = []
    try:
        for tok in tokenize.generate_tokens(StringIO.StringIO(source).readline):
            if tok[0] in (tokenize.STRING, tokenize.COMMENT):
                spans.append((tok[2][0], tok[3][0]))
    except Exception:
        pass
    skip = set()
    for start, end in spans:
        for line in range(start, end + 1):
            skip.add(line)
    return skip

def check(path):
    """Report 2.6+/2.7+ constructs in *path*.

    A line ending in "# py25-ok" is skipped. That is for a newer name used
    deliberately inside a guard that never runs on 2.5 -- for example the
    Python 3 branch of a try/except NameError, which is how this codebase
    names the byte-string type without mentioning `bytes` on 2.5.
    """
    source = open(path).read()
    skip = strings_and_comments(source)
    problems = []
    for number, line in enumerate(source.splitlines(), 1):
        if number in skip:
            continue
        if line.rstrip().endswith('# py25-ok'):
            continue
        for pattern, message in CHECKS:
            if re.search(pattern, line):
                problems.append((number, message, line.strip()[:70]))
    return problems

status = 0
for path in sys.argv[1:]:
    found = check(path)
    print("%s: %s" % (path, "%d problem(s)" % len(found) if found else "clean for Python 2.5"))
    for number, message, text in found:
        print("    line %d: %s" % (number, message))
        print("        %s" % text)
        status = 1
sys.exit(status)
