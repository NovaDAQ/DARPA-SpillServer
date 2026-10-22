"""Reversible timestamp blurring (``darpa-spill-shift``), transform dts1.

Moves every event's timestamp by a small keyed pseudorandom offset, flat or
truncated-Gaussian and never beyond +-500 ms, so that the blur can be undone
exactly by anyone holding the token and by no one else.  The specification,
with proofs, tests and plots, is ``docs/TimestampShift.nb``; its reference
implementation ``docs/shift/dts1.wl`` produced the test vectors this module is
checked against.  In brief:

* A **token** carries the distribution, its parameters in 64 MHz ticks, and a
  256-bit secret: ``dts1:flat:a=A:<hex>`` or
  ``dts1:gauss:sigma=S:cut=C:<hex>``.  It is all inversion needs.
* Event ``n`` of TDU ``s`` (``event_number``, ``source``) gets offset
  ``o(n)`` from ``HMAC-SHA256(secret, P|s|n|j)``.  Those two fields are not
  changed by the transform, so ``o(n)`` can be recomputed from a shifted row.
* ``nova_time += o(n)``; ``pps_offset = (pps_offset + o(n)) mod 1 s``;
  ``delta += o(n) - o(n-1)``.  ``delta`` is the gap to hardware event
  ``n - 1`` and ``pps_offset`` the sub-second phase; left alone either would
  give the true time back.  Every other time column is recomputed from
  ``nova_time``.
* Flat offsets use integer arithmetic only.  Gaussian offsets are AS241 of a
  53-bit uniform, rejection-truncated at the cut and rounded to a tick; the
  float evaluation is replaced by 80-digit :mod:`decimal` whenever it lands
  near a rounding boundary, so an offset is the same on every machine and
  inversion never depends on the C library's ``log``.

File handling preserves the exact layout of ``darpa-spill-dump`` output, so
``invert(apply(f))`` is byte-identical to ``f``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import hmac
import json
import math
import os
import re
import secrets
import stat
import sys
from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from typing import Any, Dict, List, Optional, Sequence, Tuple

from . import __version__
from .novatime import convert

__all__ = [
    "TICKS",
    "MAX_OFFSET",
    "Token",
    "TokenError",
    "ShiftError",
    "OffsetFunction",
    "transform_row",
    "shift_file",
    "parse_duration",
    "main",
]

#: NOvA clock ticks per second.
TICKS = 64_000_000

#: The largest permitted bound, 500 ms.
MAX_OFFSET = TICKS // 2

#: PBKDF2 parameters for seed-derived secrets.
SEED_SALT = "darpa-spill-shift/dts1"
SEED_ITERATIONS = 600_000

#: Draws available to the Gaussian rejection loop: 64 blocks of 4 words.
_MAX_BLOCKS = 64

#: Distance from a rounding or cut boundary, in ticks, inside which the float
#: evaluation is not trusted.  Its error is below 1e-6 ticks for any allowed
#: sigma; the margin is ten times that.
_GUARD = 1e-5

ENV_TOKEN = "DARPA_SPILL_SHIFT_TOKEN"
ENV_TOKEN_FILE = "DARPA_SPILL_SHIFT_TOKEN_FILE"

#: Columns computed from nova_time; recomputed from the shifted value.
DERIVED_COLUMNS = (
    "utc", "utc_string", "unix_sec", "unix_nsec", "gps", "gps_seconds",
    "gps_nsec", "gps_psec", "gps_week", "gps_tow", "gps_tow_exact",
)

#: Columns the transform needs.
REQUIRED_COLUMNS = ("nova_time", "source", "event_number")

MARKER_PREFIX = "darpa-spill-shift:"


class TokenError(ValueError):
    """A token is malformed or its parameters are out of range."""


class ShiftError(RuntimeError):
    """A file cannot be shifted or inverted; the message is for the operator."""


# ------------------------------------------------------------------ tokens


@dataclass(frozen=True)
class Token:
    """A dts1 token: distribution, parameters in ticks, and the secret."""

    dist: str
    secret: bytes
    a: int = 0
    sigma: int = 0
    cut: int = 0

    def __post_init__(self) -> None:
        if len(self.secret) != 32:
            raise TokenError("the secret must be 32 bytes")
        if self.dist == "flat":
            if not 1 <= self.a <= MAX_OFFSET:
                raise TokenError("flat half-width must be 1..{} ticks (500 ms), "
                                 "got {}".format(MAX_OFFSET, self.a))
        elif self.dist == "gauss":
            if not 1 <= self.sigma <= MAX_OFFSET:
                raise TokenError("sigma must be 1..{} ticks (500 ms), got {}"
                                 .format(MAX_OFFSET, self.sigma))
            if not 2 * self.cut >= self.sigma or self.cut > MAX_OFFSET:
                raise TokenError("cut must be between sigma/2 and {} ticks "
                                 "(500 ms), got {}".format(MAX_OFFSET, self.cut))
        else:
            raise TokenError("unknown distribution {!r}".format(self.dist))

    @property
    def params(self) -> str:
        """The public part of the token, mixed into every keyed block."""
        if self.dist == "flat":
            return "dts1:flat:a={}".format(self.a)
        return "dts1:gauss:sigma={}:cut={}".format(self.sigma, self.cut)

    def __str__(self) -> str:
        return "{}:{}".format(self.params, self.secret.hex())

    def __repr__(self) -> str:  # never print the secret by accident
        return "Token({}, fingerprint={})".format(self.params, self.fingerprint)

    @property
    def fingerprint(self) -> str:
        """64 public bits identifying the token; reveal nothing of the secret."""
        return hmac.new(self.secret, ("dts1 fingerprint|" + self.params).encode(),
                        hashlib.sha256).hexdigest()[:16]

    def describe(self) -> str:
        if self.dist == "flat":
            return "flat, +-{}".format(_fmt_ticks(self.a))
        return "Gaussian, sigma {}, cut +-{}".format(
            _fmt_ticks(self.sigma), _fmt_ticks(self.cut))

    @classmethod
    def parse(cls, text: str) -> "Token":
        text = text.strip()
        fields = text.split(":")
        if len(fields) < 3 or fields[0] != "dts1":
            raise TokenError("not a dts1 token (expected dts1:flat:... or "
                             "dts1:gauss:...)")
        dist, secret_hex = fields[1], fields[-1]
        if not re.fullmatch(r"[0-9a-f]{64}", secret_hex):
            raise TokenError("the token's secret must be 64 lower-case hex digits")
        values: Dict[str, int] = {}
        for item in fields[2:-1]:
            key, _, value = item.partition("=")
            if not value.isdigit():
                raise TokenError("bad token parameter {!r}".format(item))
            values[key] = int(value)
        expected = {"flat": ["a"], "gauss": ["sigma", "cut"]}.get(dist)
        if expected is None:
            raise TokenError("unknown distribution {!r}".format(dist))
        if list(values) != expected:
            raise TokenError("a {} token has parameters {}, got {}".format(
                dist, ", ".join(expected), ", ".join(values) or "none"))
        token = cls(dist=dist, secret=bytes.fromhex(secret_hex), **values)
        if str(token) != text:
            raise TokenError("token is not in canonical form")
        return token

    @classmethod
    def generate(cls, dist: str, secret: Optional[bytes] = None,
                 **params: int) -> "Token":
        return cls(dist=dist, secret=secret or secrets.token_bytes(32), **params)


def secret_from_seed(seed: str, iterations: int = SEED_ITERATIONS) -> bytes:
    """Derive a secret from a user seed with PBKDF2-HMAC-SHA256."""
    if not seed:
        raise TokenError("the seed is empty")
    return hashlib.pbkdf2_hmac("sha256", seed.encode("utf-8"),
                               SEED_SALT.encode("utf-8"), iterations, 32)


# ------------------------------------------------------------------- AS241

# Wichura, Applied Statistics 37 (1988), algorithm AS241 PPND16.  The strings
# are the published coefficients; the specification uses their exact values.
_A = ("3.3871328727963666080e0", "1.3314166789178437745e2",
      "1.9715909503065514427e3", "1.3731693765509461125e4",
      "4.5921953931549871457e4", "6.7265770927008700853e4",
      "3.3430575583588128105e4", "2.5090809287301226727e3")
_B = ("1", "4.2313330701600911252e1", "6.8718700749205790830e2",
      "5.3941960214247511077e3", "2.1213794301586595867e4",
      "3.9307895800092710610e4", "2.8729085735721942674e4",
      "5.2264952788528545610e3")
_C = ("1.42343711074968357734e0", "4.63033784615654529590e0",
      "5.76949722146069140550e0", "3.64784832476320460504e0",
      "1.27045825245236838258e0", "2.41780725177450611770e-1",
      "2.27238449892691845833e-2", "7.74545014278341407640e-4")
_D = ("1", "2.05319162663775882187e0", "1.67638483018380384940e0",
      "6.89767334985100004550e-1", "1.48103976427480074590e-1",
      "1.51986665636164571966e-2", "5.47593808499534494600e-4",
      "1.05075007164441684324e-9")
_E = ("6.65790464350110377720e0", "5.46378491116411436990e0",
      "1.78482653991729133580e0", "2.96560571828504891230e-1",
      "2.65321895265761230930e-2", "1.24266094738807843860e-3",
      "2.71155556874348757815e-5", "2.01033439929228813265e-7")
_F = ("1", "5.99832206555887937690e-1", "1.36929880922735805310e-1",
      "1.48753612908506148525e-2", "7.86869131145613259100e-4",
      "1.84631831751005468180e-5", "1.42151175831644588870e-7",
      "2.04426310338993978564e-15")

_FLOAT = tuple(tuple(float(c) for c in cs) for cs in (_A, _B, _C, _D, _E, _F))
_DEC = tuple(tuple(Decimal(c) for c in cs) for cs in (_A, _B, _C, _D, _E, _F))


def _poly(coefficients, r):
    acc = coefficients[-1] * 0
    for c in reversed(coefficients):
        acc = acc * r + c
    return acc


def _as241(p, consts, half, split, r0, five, shift, sqrt, log):
    a, b, c, d, e, f = consts
    q = p - half
    if abs(q) <= split:
        r = r0 - q * q
        return q * _poly(a, r) / _poly(b, r)
    r = sqrt(-log(p if q < 0 else 1 - p))
    if r <= five:
        r = r - shift
        x = _poly(c, r) / _poly(d, r)
    else:
        r = r - five
        x = _poly(e, r) / _poly(f, r)
    return -x if q < 0 else x


def as241_float(p: float) -> float:
    """AS241 in double precision: fast, accurate to about 1e-16."""
    return _as241(p, _FLOAT, 0.5, 0.425, 0.180625, 5.0, 1.6,
                  math.sqrt, math.log)


def as241_decimal(p: Decimal) -> Decimal:
    """AS241 in the current decimal context: slow, platform independent."""
    return _as241(p, _DEC, Decimal("0.5"), Decimal("0.425"),
                  Decimal("0.180625"), Decimal(5), Decimal("1.6"),
                  lambda v: v.sqrt(), lambda v: v.ln())


def _gauss_draw(k: int, sigma: int, cut: int) -> Optional[int]:
    """Offset for the 53-bit draw *k*, or ``None`` if outside the cut.

    ``u = (k + 1/2) / 2**53`` exactly.  The float path is used unless its
    result lies within :data:`_GUARD` ticks of a rounding boundary or of the
    cut, in which case the decision is made in exact decimal arithmetic.
    """
    u = (k + 0.5) / 9007199254740992.0          # exact: k < 2**53
    xs = as241_float(u) * sigma
    frac = abs(xs) % 1.0
    if abs(frac - 0.5) > _GUARD and abs(abs(xs) - cut) > _GUARD:
        return round(xs) if abs(xs) <= cut else None
    with localcontext() as ctx:
        ctx.prec = 80
        ud = (Decimal(k) + Decimal("0.5")) / Decimal(9007199254740992)
        xd = as241_decimal(ud) * sigma
        if abs(xd) > cut:
            return None
        return int(xd.quantize(Decimal(1), rounding=ROUND_HALF_EVEN))


# ----------------------------------------------------------------- offsets


class OffsetFunction:
    """The offset ``o(source, n)`` for one token, with a small cache.

    The cache matters because each row needs ``o(n)`` and ``o(n - 1)``, and
    ``o(n - 1)`` is usually an earlier row's ``o(n)``.
    """

    def __init__(self, token: Token, cache_size: int = 65536):
        self.token = token
        self._key = token.secret
        self._prefix = token.params + "|"
        self._cache: Dict[Tuple[str, int], int] = {}
        self._cache_size = cache_size

    def _block(self, source: str, n: int, j: int) -> bytes:
        message = "{}{}|{}|{}".format(self._prefix, source, n, j).encode("utf-8")
        return hmac.digest(self._key, message, "sha256")

    def __call__(self, source: str, n: int) -> int:
        key = (source, n)
        value = self._cache.get(key)
        if value is None:
            value = self._compute(source, n)
            if len(self._cache) >= self._cache_size:
                self._cache.clear()
            self._cache[key] = value
        return value

    def _compute(self, source: str, n: int) -> int:
        token = self.token
        if token.dist == "flat":
            x = int.from_bytes(self._block(source, n, 0)[:16], "big")
            return x % (2 * token.a + 1) - token.a
        for j in range(_MAX_BLOCKS):
            block = self._block(source, n, j)
            for i in range(4):
                word = int.from_bytes(block[8 * i:8 * i + 8], "big")
                value = _gauss_draw(word >> 11, token.sigma, token.cut)
                if value is not None:
                    return value
        raise ShiftError("no Gaussian draw inside the cut after {} tries; "
                         "this should be impossible".format(4 * _MAX_BLOCKS))


# ------------------------------------------------------------------- rows


def _as_int(value: Any, column: str) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ShiftError("column {} holds {!r}, not an integer".format(
            column, value))


def transform_row(row: Dict[str, Any], offsets: OffsetFunction,
                  sign: int, text: bool) -> None:
    """Shift (``sign=+1``) or restore (``sign=-1``) one row in place.

    *text* is true for CSV rows, whose values are strings, and false for JSON
    rows, which keep their native types.
    """
    source = row.get("source")
    n = _as_int(row.get("event_number"), "event_number")
    t = _as_int(row.get("nova_time"), "nova_time")
    if not source or n is None or t is None:
        raise ShiftError("a row lacks source, event_number or nova_time")

    o = offsets(str(source), n)
    shifted = t + sign * o
    if shifted < 0:
        raise ShiftError("nova_time {} would go negative".format(t))
    out = (lambda v: str(v)) if text else (lambda v: v)
    row["nova_time"] = out(shifted)

    pps = _as_int(row.get("pps_offset"), "pps_offset") if "pps_offset" in row else None
    if pps is not None:
        if not 0 <= pps < TICKS:
            raise ShiftError("pps_offset {} is outside one second".format(pps))
        row["pps_offset"] = out((pps + sign * o) % TICKS)

    delta = _as_int(row.get("delta"), "delta") if "delta" in row else None
    if delta is not None:
        row["delta"] = out(delta + sign * (o - offsets(str(source), n - 1)))

    present = [c for c in DERIVED_COLUMNS if c in row]
    if present:
        times = convert(shifted)
        for column in present:
            row[column] = out(getattr(times, column))


# ------------------------------------------------------------------- files


def _marker_text(token: Token) -> str:
    return "{} dts1 shifted, params={}, fingerprint={}".format(
        MARKER_PREFIX, token.params, token.fingerprint)


def _marker_meta(token: Token) -> Dict[str, str]:
    return {"spec": "dts1", "state": "shifted", "params": token.params,
            "fingerprint": token.fingerprint}


def _parse_marker(text: str) -> Dict[str, str]:
    found = dict(re.findall(r"(params|fingerprint)=([^,\s]+)", text))
    return found


def detect_format(path: str) -> str:
    """``csv`` or ``json``, from the extension or else the first character."""
    lower = path.lower()
    if lower.endswith(".json"):
        return "json"
    if lower.endswith(".csv"):
        return "csv"
    with open(path, encoding="utf-8") as handle:
        first = handle.read(64).lstrip()
    return "json" if first.startswith("{") else "csv"


def _check_state(state: Optional[Dict[str, str]], token: Token, sign: int,
                 force: bool, path: str) -> None:
    """Refuse to shift twice, or to invert unmarked or foreign data."""
    if sign > 0:
        if state is not None and not force:
            raise ShiftError(
                "{} is already shifted (fingerprint {}); invert it first, or "
                "pass --force to shift it again".format(
                    path, state.get("fingerprint", "?")))
        return
    if state is None:
        if not force:
            raise ShiftError(
                "{} carries no darpa-spill-shift marker, so there is no record "
                "that it was shifted; pass --force to invert anyway".format(path))
        return
    if state.get("fingerprint") != token.fingerprint and not force:
        raise ShiftError(
            "{} was shifted with token fingerprint {} ({}), but this token is "
            "{} ({}); inverting with the wrong token gives wrong times "
            "silently. Pass --force to do it anyway".format(
                path, state.get("fingerprint"), state.get("params"),
                token.fingerprint, token.params))


class _AtomicWriter:
    """A text file written as PATH.part and renamed on success."""

    def __init__(self, path: str):
        self.path = path
        if path == "-":
            self.stream = sys.stdout
            self.part = None
        else:
            self.part = path + ".part"
            self.stream = open(self.part, "w", newline="", encoding="utf-8")

    def commit(self) -> None:
        if self.part is None:
            self.stream.flush()
            return
        self.stream.close()
        os.replace(self.part, self.path)

    def abort(self) -> None:
        if self.part is not None:
            self.stream.close()
            try:
                os.unlink(self.part)
            except OSError:
                pass


def _shift_csv(src, out, offsets: OffsetFunction, sign: int, force: bool,
               path: str) -> int:
    comments: List[str] = []
    header_line = ""
    for line in src:
        if line.startswith("#"):
            comments.append(line)
            continue
        header_line = line
        break
    if not header_line:
        raise ShiftError("{} has no CSV header".format(path))
    header = next(csv.reader([header_line]))
    missing = [c for c in REQUIRED_COLUMNS if c not in header]
    if missing:
        raise ShiftError("{} lacks column(s) {}; dump with --columns including "
                         "nova_time, source and event_number".format(
                             path, ", ".join(missing)))

    markers = [i for i, c in enumerate(comments)
               if c[1:].strip().startswith(MARKER_PREFIX)]
    state = _parse_marker(comments[markers[-1]]) if markers else None
    _check_state(state, offsets.token, sign, force, path)
    if sign > 0:
        comments.append("# " + _marker_text(offsets.token) + "\n")
    else:
        for i in reversed(markers):
            del comments[i]

    out.writelines(comments)
    out.write(header_line if header_line.endswith("\n") else header_line + "\n")
    writer = csv.writer(out, lineterminator="\n")
    count = 0
    for values in csv.reader(src):
        if not values:
            continue
        row = dict(zip(header, values))
        transform_row(row, offsets, sign, text=True)
        writer.writerow([row[c] for c in header])
        count += 1
    return count


def _dump_layout(lines: List[str]) -> bool:
    """True for darpa-spill-dump's JSON layout: one event per line."""
    return (len(lines) >= 3 and lines[0] == "{\n"
            and lines[1].startswith('"meta": ')
            and lines[2] in ('"events": [\n', '"events": [],\n'))


def _shift_json(src, out, offsets: OffsetFunction, sign: int, force: bool,
                path: str) -> int:
    head = [src.readline() for _ in range(3)]
    if _dump_layout(head):
        return _shift_json_stream(head, src, out, offsets, sign, force, path)
    # Any other layout (the API's, or pretty-printed): parse it whole.
    doc = json.loads("".join(head) + src.read())
    if not isinstance(doc, dict) or not isinstance(doc.get("events"), list):
        raise ShiftError("{} is not an events JSON document (no 'events' "
                         "list)".format(path))
    meta = doc.setdefault("meta", {})
    _apply_meta(meta, offsets.token, sign, force, path)
    for row in doc["events"]:
        transform_row(row, offsets, sign, text=False)
    out.write(json.dumps(doc))
    out.write("\n")
    return len(doc["events"])


def _apply_meta(meta: Dict[str, Any], token: Token, sign: int, force: bool,
                path: str) -> None:
    state = meta.get("shift")
    _check_state(state, token, sign, force, path)
    if sign > 0:
        meta["shift"] = _marker_meta(token)
    else:
        meta.pop("shift", None)


def _shift_json_stream(head, src, out, offsets, sign, force, path) -> int:
    """darpa-spill-dump's layout: one event per line, streamed."""
    meta_text = head[1][len('"meta": '):].rstrip("\n")
    if not meta_text.endswith(","):
        raise ShiftError("{}: unexpected JSON layout".format(path))
    meta = json.loads(meta_text[:-1])
    _apply_meta(meta, offsets.token, sign, force, path)
    out.write("{\n")
    out.write('"meta": ' + json.dumps(meta, default=str) + ",\n")
    out.write(head[2])
    if head[2] == '"events": [],\n':
        out.writelines(src)
        return 0
    count = 0
    for line in src:
        stripped = line.rstrip("\n")
        if stripped == "":
            continue
        if stripped.startswith("]"):
            out.write(line)
            out.writelines(src)
            break
        comma = stripped.endswith(",")
        row = json.loads(stripped[:-1] if comma else stripped)
        transform_row(row, offsets, sign, text=False)
        out.write(json.dumps(row, default=str) + ("," if comma else "") + "\n")
        count += 1
    return count


def shift_file(in_path: str, out_path: str, token: Token, sign: int,
               force: bool = False) -> int:
    """Shift (``sign=+1``) or invert (``-1``) one file; return the row count."""
    fmt = "csv" if in_path == "-" else detect_format(in_path)
    offsets = OffsetFunction(token)
    src = sys.stdin if in_path == "-" else open(in_path, newline="", encoding="utf-8")
    writer = _AtomicWriter(out_path)
    try:
        if fmt == "csv":
            count = _shift_csv(src, writer.stream, offsets, sign, force, in_path)
        else:
            count = _shift_json(src, writer.stream, offsets, sign, force, in_path)
        writer.commit()
        return count
    except BaseException:
        writer.abort()
        raise
    finally:
        if src is not sys.stdin:
            src.close()


def file_state(path: str) -> Optional[Dict[str, str]]:
    """The shift marker of a data file, or ``None`` if it carries none."""
    if detect_format(path) == "csv":
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                if not line.startswith("#"):
                    return None
                if line[1:].strip().startswith(MARKER_PREFIX):
                    return _parse_marker(line)
        return None
    with open(path, encoding="utf-8") as handle:
        head = [handle.readline() for _ in range(3)]
        if _dump_layout(head):
            meta = json.loads(head[1][len('"meta": '):].rstrip("\n")[:-1])
        else:
            meta = json.loads("".join(head) + handle.read()).get("meta", {})
    return meta.get("shift")


# --------------------------------------------------------------------- CLI

_UNITS = {"s": Decimal(TICKS), "ms": Decimal(TICKS) / 1000,
          "us": Decimal(TICKS) / 1000000, "µs": Decimal(TICKS) / 1000000,
          "ns": Decimal(TICKS) / 1000000000, "tick": Decimal(1),
          "ticks": Decimal(1)}


def parse_duration(text: str) -> int:
    """``50ms``, ``0.02s``, ``500us``, ``1280000ticks`` -> whole ticks."""
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]*)?|\.[0-9]+)\s*([a-zµ]+)\s*",
                         text.lower())
    if not match or match.group(2) not in _UNITS:
        raise TokenError("cannot read {!r} as a duration; give a number with a "
                         "unit: s, ms, us, ns or ticks (e.g. 20ms)".format(text))
    ticks = (Decimal(match.group(1)) * _UNITS[match.group(2)]).quantize(
        Decimal(1), rounding=ROUND_HALF_EVEN)
    if ticks < 1:
        raise TokenError("{!r} is less than one tick (15.625 ns)".format(text))
    return int(ticks)


def _fmt_ticks(ticks: int) -> str:
    return "{:g} ms ({} ticks)".format(ticks / 64000.0, ticks)


def _token_params(args) -> Dict[str, int]:
    if args.dist == "flat":
        if not args.width:
            raise TokenError("--dist flat needs --width, the half-width "
                             "(e.g. --width 50ms for +-50 ms)")
        return {"a": parse_duration(args.width)}
    if not args.sigma:
        raise TokenError("--dist gauss needs --sigma (e.g. --sigma 20ms)")
    sigma = parse_duration(args.sigma)
    cut_text = (args.cut or "3sigma").strip().lower()
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]*)?)\s*sigma", cut_text)
    if match:
        cut = int((Decimal(match.group(1)) * sigma).quantize(
            Decimal(1), rounding=ROUND_HALF_EVEN))
        if args.cut is None:
            cut = min(cut, MAX_OFFSET)
    else:
        cut = parse_duration(cut_text)
    return {"sigma": sigma, "cut": cut}


def _read_secret_text(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.readline().rstrip("\n")


def _load_token(args, environ) -> Token:
    if args.token:
        return Token.parse(args.token)
    path = args.token_file or environ.get(ENV_TOKEN_FILE)
    if args.token_file is None and environ.get(ENV_TOKEN):
        return Token.parse(environ[ENV_TOKEN])
    if path:
        try:
            return Token.parse(_read_secret_text(path))
        except OSError as exc:
            raise TokenError("cannot read token file: {}".format(exc))
    raise TokenError("no token; give --token, --token-file, or set {} or {}"
                     .format(ENV_TOKEN, ENV_TOKEN_FILE))


def _write_token_file(path: str, token: Token, force: bool) -> None:
    refusal = TokenError("{} exists; refusing to overwrite a token file "
                         "(pass --force)".format(path))
    if os.path.exists(path) and not force:
        raise refusal
    # Written whole beside the target, then linked into place: os.link fails
    # if the name exists, so two concurrent runs cannot both "create" the
    # token and one silently replace the other, and no reader ever sees a
    # half-written file.
    tmp = "{}.tmp.{}".format(path, os.getpid())
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                 stat.S_IRUSR | stat.S_IWUSR)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(str(token) + "\n")
        os.chmod(tmp, stat.S_IRUSR | stat.S_IWUSR)
        if force:
            os.replace(tmp, path)
            return
        try:
            os.link(tmp, path)
        except FileExistsError:
            raise refusal from None
        except OSError:
            # No hard links on this file system: O_EXCL is still exclusive.
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                             stat.S_IRUSR | stat.S_IWUSR)
            except FileExistsError:
                raise refusal from None
            with os.fdopen(fd, "w") as handle:
                handle.write(str(token) + "\n")
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def _new_token(args) -> Token:
    params = _token_params(args)
    seed = args.seed
    if args.seed_file:
        seed = _read_secret_text(args.seed_file)
    secret = secret_from_seed(seed) if seed is not None else None
    if seed is not None:
        print("darpa-spill-shift: note: a seed-derived token is only as strong "
              "as the seed", file=sys.stderr)
    return Token.generate(args.dist, secret=secret, **params)


def _default_output(path: str, sign: int) -> str:
    root, ext = os.path.splitext(path)
    if sign > 0:
        return root + ".shifted" + ext
    if root.endswith(".shifted"):
        root = root[:-len(".shifted")]
    return root + ".restored" + ext


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="darpa-spill-shift",
        description="Blur event timestamps reversibly (dts1): shift each by a "
                    "keyed random offset, and undo it exactly with the token.",
        epilog="Specification: docs/TimestampShift.nb. Durations take a unit: "
               "s, ms, us, ns or ticks (64 MHz).",
    )
    parser.add_argument("-V", "--version", action="store_true",
                        help="show version information and exit")
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    def token_options(p):
        g = p.add_argument_group("token")
        g.add_argument("--token", metavar="TOKEN",
                       help="the token itself (visible in ps and shell history; "
                            "prefer --token-file)")
        g.add_argument("--token-file", metavar="FILE",
                       help="read the token from the first line of FILE")

    def new_token_options(p):
        g = p.add_argument_group("new token")
        g.add_argument("--dist", choices=["flat", "gauss"],
                       help="distribution of the offsets")
        g.add_argument("--width", metavar="DURATION",
                       help="flat: half-width; offsets lie in +-DURATION")
        g.add_argument("--sigma", metavar="DURATION",
                       help="gauss: standard deviation before truncation")
        g.add_argument("--cut", metavar="DURATION|Nsigma",
                       help="gauss: truncation, e.g. 60ms or 2.5sigma "
                            "(default 3sigma, at most 500 ms)")
        g.add_argument("--seed", metavar="TEXT",
                       help="derive the secret from TEXT (PBKDF2) instead of "
                            "at random; reproducible, but only as strong as TEXT")
        g.add_argument("--seed-file", metavar="FILE",
                       help="read the seed from the first line of FILE")

    p = sub.add_parser("token", help="create a token")
    new_token_options(p)
    p.add_argument("-o", "--output", metavar="FILE",
                   help="write the token to FILE (mode 0600) instead of stdout")
    p.add_argument("--force", action="store_true",
                   help="overwrite an existing token file")

    for name, verb in (("apply", "shift"), ("invert", "restore")):
        p = sub.add_parser(name, help="{} timestamps in dump files".format(verb))
        p.add_argument("inputs", nargs="+", metavar="FILE",
                       help="darpa-spill-dump CSV or JSON; '-' for stdin (CSV)")
        p.add_argument("-o", "--output", metavar="FILE",
                       help="output file for a single input; '-' for stdout. "
                            "Default: FILE.{}.EXT".format(
                                "shifted" if name == "apply" else "restored"))
        p.add_argument("--force", action="store_true",
                       help="skip the already-shifted / marker / fingerprint "
                            "checks")
        p.add_argument("-q", "--quiet", action="store_true",
                       help="print nothing but errors")
        token_options(p)
        if name == "apply":
            new_token_options(p)
            p.add_argument("--token-out", metavar="FILE",
                           help="with --dist: create a new token, save it here "
                                "(mode 0600), and use it")

    p = sub.add_parser("info", help="describe a token, token file or data file")
    p.add_argument("target", nargs="?", metavar="TOKEN|FILE",
                   help="a token, a token file, or a shifted data file")
    token_options(p)
    return parser


def _err(message: str) -> None:
    print("darpa-spill-shift: error: {}".format(message), file=sys.stderr)


def _cmd_token(args) -> int:
    if not args.dist:
        raise TokenError("give --dist flat --width W, or --dist gauss --sigma S")
    token = _new_token(args)
    if args.output:
        _write_token_file(args.output, token, args.force)
        print("darpa-spill-shift: wrote {} token to {} (fingerprint {})".format(
            token.describe(), args.output, token.fingerprint), file=sys.stderr)
    else:
        print(str(token))
        print("darpa-spill-shift: {}, fingerprint {}. Keep this token: without "
              "it the shift cannot be undone.".format(
                  token.describe(), token.fingerprint), file=sys.stderr)
    return 0


def _cmd_shift(args, sign: int, environ) -> int:
    if args.output and len(args.inputs) > 1:
        raise TokenError("-o names one output; with several inputs, outputs "
                         "are named FILE.{}.EXT".format(
                             "shifted" if sign > 0 else "restored"))
    if sign > 0 and getattr(args, "dist", None):
        if args.token or args.token_file:
            raise TokenError("give either an existing token or --dist for a "
                             "new one, not both")
        if not args.token_out:
            raise TokenError("a new token must be saved before data is shifted "
                             "with it; add --token-out FILE")
        token = _new_token(args)
        _write_token_file(args.token_out, token, args.force)
        if not args.quiet:
            print("darpa-spill-shift: new {} token saved to {} (fingerprint {})"
                  .format(token.describe(), args.token_out, token.fingerprint),
                  file=sys.stderr)
    else:
        token = _load_token(args, environ)

    for path in args.inputs:
        out = args.output or ("-" if path == "-" else _default_output(path, sign))
        if out != "-" and path != "-" and os.path.abspath(out) == os.path.abspath(path):
            raise ShiftError("refusing to overwrite the input {}".format(path))
        count = shift_file(path, out, token, sign, force=args.force)
        if not args.quiet:
            print("darpa-spill-shift: {} {:,} events: {} -> {}".format(
                "shifted" if sign > 0 else "restored", count, path, out),
                file=sys.stderr)
    return 0


def _cmd_info(args, environ) -> int:
    target = args.target
    token = None
    state = None
    if target and target.startswith("dts1:"):
        token = Token.parse(target)
    elif target and os.path.isfile(target):
        with open(target, encoding="utf-8") as handle:
            first = handle.readline().strip()
        if first.startswith("dts1:"):
            token = Token.parse(first)
        else:
            state = file_state(target)
            if state is None:
                print("{}: not shifted (no darpa-spill-shift marker)".format(target))
            else:
                print("{}: shifted, {}, fingerprint {}".format(
                    target, state.get("params"), state.get("fingerprint")))
            try:
                token = _load_token(args, environ)
            except TokenError:
                return 0
    else:
        token = _load_token(args, environ)
    print("token: {}, fingerprint {}".format(token.describe(), token.fingerprint))
    print("params: {}".format(token.params))
    if state is not None:
        match = state.get("fingerprint") == token.fingerprint
        print("this token {} the file".format("matches" if match else "does NOT match"))
        return 0 if match else 1
    return 0


def main(argv: Optional[Sequence[str]] = None,
         environ: Optional[Dict[str, str]] = None) -> int:
    environ = dict(os.environ if environ is None else environ)
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.version:
        print("darpa-spill-shift {}".format(__version__))
        return 0
    if not args.command:
        parser.print_help(sys.stderr)
        return 2
    try:
        if args.command == "token":
            return _cmd_token(args)
        if args.command == "apply":
            return _cmd_shift(args, +1, environ)
        if args.command == "invert":
            return _cmd_shift(args, -1, environ)
        return _cmd_info(args, environ)
    except TokenError as exc:
        _err(str(exc))
        return 2
    except ShiftError as exc:
        _err(str(exc))
        return 1
    except OSError as exc:
        _err(str(exc))
        return 1
    except json.JSONDecodeError as exc:
        _err("not valid JSON: {}".format(exc))
        return 1
    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        return 141
    except KeyboardInterrupt:
        _err("interrupted; partial outputs removed")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
