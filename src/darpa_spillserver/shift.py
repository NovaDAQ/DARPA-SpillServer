"""!
@file src/darpa_spillserver/shift.py
@brief Reversible keyed timestamp blur (transform dts1) and the darpa-spill-shift CLI.

Reversible timestamp blurring (``darpa-spill-shift``), transform dts1.

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

@details
Units.  Every time quantity in this module (``nova_time``, ``pps_offset``,
``delta``, the offsets, ``a``, ``sigma``, ``cut``) is an integer count of
NOvA 64 MHz clock ticks: 1 tick = 15.625 ns, 1 s = 64 000 000 ticks,
1 ms = 64 000 ticks, and the hard bound 500 ms = 32 000 000 ticks.  Because
every offset is a whole number of ticks, shifted times are again integers and
the transform is a bijection on the integer columns, with no rounding loss in
either direction.

Layers, bottom-up:
  - Token / secret_from_seed: the key material and its canonical text form.
  - _as241 / _gauss_draw: the inverse-normal map and the bit-reproducible
    Gaussian draw.
  - OffsetFunction: o(source, n), keyed by HMAC-SHA256, cached.
  - transform_row: the column algebra for one row, sign = +1 (apply) or
    -1 (invert).
  - _shift_csv / _shift_json / shift_file: file-state markers, layout
    preservation and atomic output.
  - build_parser / main: the ``darpa-spill-shift`` command line.
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

## Public API of the module.
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

## NOvA clock ticks per second.
## (64 MHz; one tick is 15.625 ns.  Also the modulus of pps_offset.)
TICKS = 64_000_000

## The largest permitted bound, 500 ms.
## (32 000 000 ticks.  Applies to the flat half-width a, to sigma and to the
## Gaussian cut, so |o(n)| <= 500 ms for every token.  Keeping |o| within half
## a second also means a shifted time never moves by a full PPS period.)
MAX_OFFSET = TICKS // 2

## PBKDF2 parameters for seed-derived secrets.
## Fixed salt: a seed always maps to the same secret, which is the point of a
## seed (reproducible tokens).  The salt only domain-separates this use of the
## seed from any other; strength comes from the seed's entropy, slowed by the
## iteration count.
SEED_SALT = "darpa-spill-shift/dts1"

## PBKDF2-HMAC-SHA256 iteration count for seed-derived secrets (part of the
## spec: changing it changes every seed-derived token).
SEED_ITERATIONS = 600_000

## Draws available to the Gaussian rejection loop: 64 blocks of 4 words.
## (256 draws.  Acceptance per draw is P(|Z| <= cut/sigma) >= P(|Z| <= 1/2)
## ~ 0.383 because Token enforces cut >= sigma/2, so exhausting all 256
## draws has probability below 0.617**256 ~ 1e-54.)
_MAX_BLOCKS = 64

## Distance from a rounding or cut boundary, in ticks, inside which the float
## evaluation is not trusted.  Its error is below 1e-6 ticks for any allowed
## sigma; the margin is ten times that.
## (The bound assumes the float path is handed the exact u; _gauss_draw
## ensures that by evaluating u > 1/2 through the symmetry x(u) = -x(1-u).)
_GUARD = 1e-5

## Environment variable holding a token directly (outranks ENV_TOKEN_FILE;
## outranked by --token and --token-file).
ENV_TOKEN = "DARPA_SPILL_SHIFT_TOKEN"
## Environment variable naming a token file (first line is the token).
ENV_TOKEN_FILE = "DARPA_SPILL_SHIFT_TOKEN_FILE"

## Columns computed from nova_time; recomputed from the shifted value.
## (Leaving any of them at the true value would defeat the blur, and
## recomputing them on invert reproduces the original text exactly.)
DERIVED_COLUMNS = (
    "utc", "utc_string", "unix_sec", "unix_nsec", "gps", "gps_seconds",
    "gps_nsec", "gps_psec", "gps_week", "gps_tow", "gps_tow_exact",
)

## Columns the transform needs.
## (source and event_number key the offset; nova_time is what is moved.)
REQUIRED_COLUMNS = ("nova_time", "source", "event_number")

## Leading text of the CSV comment line that marks a file as shifted.  Its
## presence is the file state "shifted"; its absence is "not shifted".
MARKER_PREFIX = "darpa-spill-shift:"


class TokenError(ValueError):
    """!
    @brief A token is malformed or its parameters are out of range.

    Also used for command-line usage errors (missing --width, unreadable
    durations, conflicting token options); main() maps it to exit status 2.
    """


class ShiftError(RuntimeError):
    """!
    @brief A file cannot be shifted or inverted; the message is for the operator.

    main() maps it to exit status 1.
    """


# ------------------------------------------------------------------ tokens


@dataclass(frozen=True)
class Token:
    """!
    @brief A dts1 token: distribution, parameters in ticks, and the secret.

    @details
    Text form (canonical, see parse() and __str__):

        dts1:flat:a=<A>:<64 hex>
        dts1:gauss:sigma=<S>:cut=<C>:<64 hex>

    A, S and C are decimal integers in 64 MHz ticks; the 64 lower-case hex
    digits are the 256-bit HMAC key.  Everything before the last ':' is the
    public parameter string (params), which is mixed into every keyed block,
    so two tokens sharing a secret but differing in parameters still give
    unrelated offsets.  The dataclass is frozen so a token cannot change
    under an OffsetFunction that has cached values for it.
    """

    ## "flat" or "gauss".
    dist: str
    ## 32-byte HMAC-SHA256 key.
    secret: bytes
    ## Flat half-width in ticks; offsets lie in [-a, +a].  1..MAX_OFFSET.
    a: int = 0
    ## Gaussian standard deviation before truncation, in ticks.  1..MAX_OFFSET.
    sigma: int = 0
    ## Gaussian truncation |o| <= cut, in ticks.  sigma/2..MAX_OFFSET.
    cut: int = 0

    def __post_init__(self) -> None:
        """!
        @brief Validate the secret length and the distribution's parameters.
        @throws TokenError if the secret is not 32 bytes, the distribution is
                unknown, or a parameter is outside its allowed range.
        """
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
            # cut >= sigma/2 bounds the rejection rate (see _MAX_BLOCKS);
            # cut <= 500 ms is the hard bound on any offset.
            if not 2 * self.cut >= self.sigma or self.cut > MAX_OFFSET:
                raise TokenError("cut must be between sigma/2 and {} ticks "
                                 "(500 ms), got {}".format(MAX_OFFSET, self.cut))
        else:
            raise TokenError("unknown distribution {!r}".format(self.dist))

    @property
    def params(self) -> str:
        """!
        @brief The public part of the token, mixed into every keyed block.
        @return ``dts1:flat:a=A`` or ``dts1:gauss:sigma=S:cut=C`` (the string
                P in the HMAC message P|s|n|j).
        """
        if self.dist == "flat":
            return "dts1:flat:a={}".format(self.a)
        return "dts1:gauss:sigma={}:cut={}".format(self.sigma, self.cut)

    def __str__(self) -> str:
        """!
        @brief The full canonical token text, secret included.
        @return ``<params>:<64 lower-case hex digits>``.
        """
        return "{}:{}".format(self.params, self.secret.hex())

    def __repr__(self) -> str:  # never print the secret by accident
        """!
        @brief Debug representation that shows params and fingerprint only.
        @return ``Token(<params>, fingerprint=<16 hex>)``; never the secret.
        """
        return "Token({}, fingerprint={})".format(self.params, self.fingerprint)

    @property
    def fingerprint(self) -> str:
        """!
        @brief 64 public bits identifying the token; reveal nothing of the secret.
        @return First 16 hex digits of HMAC-SHA256(secret, "dts1 fingerprint|"
                + params).  Written into shifted files so that inversion with
                the wrong token is refused rather than silently wrong.
        """
        # The fixed "dts1 fingerprint|" prefix cannot collide with an offset
        # block message (which starts with "dts1:"), so the fingerprint leaks
        # nothing about any o(n).
        return hmac.new(self.secret, ("dts1 fingerprint|" + self.params).encode(),
                        hashlib.sha256).hexdigest()[:16]

    def describe(self) -> str:
        """!
        @brief Human-readable summary of the distribution and its parameters.
        @return e.g. ``flat, +-50 ms (3200000 ticks)`` or
                ``Gaussian, sigma 20 ms (1280000 ticks), cut +-60 ms (...)``.
        """
        if self.dist == "flat":
            return "flat, +-{}".format(_fmt_ticks(self.a))
        return "Gaussian, sigma {}, cut +-{}".format(
            _fmt_ticks(self.sigma), _fmt_ticks(self.cut))

    @classmethod
    def parse(cls, text: str) -> "Token":
        """!
        @brief Parse a token from its text form, accepting canonical form only.
        @param text Token text; surrounding whitespace is ignored.
        @return The validated Token.
        @throws TokenError if the text is not a dts1 token, the secret is not
                64 lower-case hex digits, the parameter names or order are
                wrong for the distribution, a value is out of range, or the
                text is not canonical (e.g. leading zeros, upper-case hex).
        """
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
        # Names and order must match exactly: params is part of every HMAC
        # message, so only one spelling of a token may exist.
        if list(values) != expected:
            raise TokenError("a {} token has parameters {}, got {}".format(
                dist, ", ".join(expected), ", ".join(values) or "none"))
        token = cls(dist=dist, secret=bytes.fromhex(secret_hex), **values)
        # Round-trip check: rejects "a=050" and similar, which would otherwise
        # parse to the same Token but differ as text (and hence as P).
        if str(token) != text:
            raise TokenError("token is not in canonical form")
        return token

    @classmethod
    def generate(cls, dist: str, secret: Optional[bytes] = None,
                 **params: int) -> "Token":
        """!
        @brief Create a token, with a fresh random secret unless one is given.
        @param dist "flat" or "gauss".
        @param secret 32-byte key (e.g. from secret_from_seed); None (or empty)
               draws 256 bits from the OS CSPRNG via secrets.token_bytes.
        @param params a=... for flat; sigma=..., cut=... for gauss (ticks).
        @return The new Token.
        @throws TokenError if the parameters are invalid (see __post_init__).
        """
        return cls(dist=dist, secret=secret or secrets.token_bytes(32), **params)


def secret_from_seed(seed: str, iterations: int = SEED_ITERATIONS) -> bytes:
    """!
    @brief Derive a secret from a user seed with PBKDF2-HMAC-SHA256.

    @details
    secret = PBKDF2-HMAC-SHA256(password = UTF-8(seed),
    salt = UTF-8(SEED_SALT), iterations, dkLen = 32): a single 32-byte PBKDF2
    block (RFC 8018).  Deterministic, so the same seed and parameters always
    reproduce the same token; the iteration count only slows brute force of a
    low-entropy seed, it does not add entropy.
    @param seed User-supplied seed text; must be non-empty.
    @param iterations PBKDF2 iteration count (the spec value is SEED_ITERATIONS;
           tests pass a smaller one).
    @return The 32-byte secret.
    @throws TokenError if the seed is empty.
    """
    if not seed:
        raise TokenError("the seed is empty")
    return hashlib.pbkdf2_hmac("sha256", seed.encode("utf-8"),
                               SEED_SALT.encode("utf-8"), iterations, 32)


# ------------------------------------------------------------------- AS241

# Wichura, Applied Statistics 37 (1988), algorithm AS241 PPND16.  The strings
# are the published coefficients; the specification uses their exact values.
# Each tuple is in ascending powers of r (c0 + c1 r + ... + c7 r^7).
#   _A/_B: central region |p - 1/2| <= 0.425, numerator/denominator in
#          r = 0.180625 - q^2 (0.180625 = 0.425^2).
#   _C/_D: tail, r = sqrt(-ln min(p, 1-p)) <= 5, polynomial in r - 1.6.
#   _E/_F: far tail, r > 5, polynomial in r - 5.
## AS241 central-region numerator coefficients.
_A = ("3.3871328727963666080e0", "1.3314166789178437745e2",
      "1.9715909503065514427e3", "1.3731693765509461125e4",
      "4.5921953931549871457e4", "6.7265770927008700853e4",
      "3.3430575583588128105e4", "2.5090809287301226727e3")
## AS241 central-region denominator coefficients.
_B = ("1", "4.2313330701600911252e1", "6.8718700749205790830e2",
      "5.3941960214247511077e3", "2.1213794301586595867e4",
      "3.9307895800092710610e4", "2.8729085735721942674e4",
      "5.2264952788528545610e3")
## AS241 intermediate-tail numerator coefficients (r <= 5).
_C = ("1.42343711074968357734e0", "4.63033784615654529590e0",
      "5.76949722146069140550e0", "3.64784832476320460504e0",
      "1.27045825245236838258e0", "2.41780725177450611770e-1",
      "2.27238449892691845833e-2", "7.74545014278341407640e-4")
## AS241 intermediate-tail denominator coefficients (r <= 5).
_D = ("1", "2.05319162663775882187e0", "1.67638483018380384940e0",
      "6.89767334985100004550e-1", "1.48103976427480074590e-1",
      "1.51986665636164571966e-2", "5.47593808499534494600e-4",
      "1.05075007164441684324e-9")
## AS241 far-tail numerator coefficients (r > 5).
_E = ("6.65790464350110377720e0", "5.46378491116411436990e0",
      "1.78482653991729133580e0", "2.96560571828504891230e-1",
      "2.65321895265761230930e-2", "1.24266094738807843860e-3",
      "2.71155556874348757815e-5", "2.01033439929228813265e-7")
## AS241 far-tail denominator coefficients (r > 5).
_F = ("1", "5.99832206555887937690e-1", "1.36929880922735805310e-1",
      "1.48753612908506148525e-2", "7.86869131145613259100e-4",
      "1.84631831751005468180e-5", "1.42151175831644588870e-7",
      "2.04426310338993978564e-15")

## The six coefficient sets as IEEE doubles (fast path).
_FLOAT = tuple(tuple(float(c) for c in cs) for cs in (_A, _B, _C, _D, _E, _F))
## The six coefficient sets as exact Decimals (Decimal(str) is exact; the
## context precision applies only to arithmetic on them).
_DEC = tuple(tuple(Decimal(c) for c in cs) for cs in (_A, _B, _C, _D, _E, _F))


def _poly(coefficients, r):
    """!
    @brief Evaluate a polynomial by Horner's rule, in the type of its inputs.
    @param coefficients Coefficients in ascending powers (c0, c1, ..., cN).
    @param r The argument (float or Decimal).
    @return sum_i c_i r^i, as float or Decimal according to the inputs.
    """
    # coefficients[-1] * 0 is a zero of the coefficient type (0.0 or
    # Decimal 0), so the same code serves both precisions.
    acc = coefficients[-1] * 0
    for c in reversed(coefficients):
        acc = acc * r + c
    return acc


def _as241(p, consts, half, split, r0, five, shift, sqrt, log):
    """!
    @brief Wichura's AS241 PPND16 inverse standard-normal CDF, type-generic.

    @details
    Returns x with Phi(x) = p.  Every constant and the sqrt/log functions are
    supplied by the caller, so the same algorithm runs in IEEE double
    (as241_float) and in decimal at the current context precision
    (as241_decimal) with no change of branch structure.
    @param p Probability in (0, 1), exclusive (p = 0 or 1 makes log fail).
    @param consts The six coefficient tuples (A, B, C, D, E, F).
    @param half 1/2.
    @param split 0.425, the central-region half-width in q = p - 1/2.
    @param r0 0.180625 = split^2.
    @param five 5, the boundary between the two tail approximations.
    @param shift 1.6, the centre of the intermediate-tail expansion.
    @param sqrt Square-root function for the chosen type.
    @param log Natural-log function for the chosen type.
    @return The normal quantile x, in the type of p.
    """
    a, b, c, d, e, f = consts
    q = p - half
    if abs(q) <= split:
        # Central region: x = q * A(r)/B(r), r = 0.425^2 - q^2.
        r = r0 - q * q
        return q * _poly(a, r) / _poly(b, r)
    # Tails: work with the smaller tail probability, so r > 0, and restore
    # the sign at the end (Phi^-1 is odd about p = 1/2).
    r = sqrt(-log(p if q < 0 else 1 - p))
    if r <= five:
        r = r - shift
        x = _poly(c, r) / _poly(d, r)
    else:
        r = r - five
        x = _poly(e, r) / _poly(f, r)
    return -x if q < 0 else x


def as241_float(p: float) -> float:
    """!
    @brief AS241 in double precision: fast, accurate to about 1e-16.
    @param p Probability in (0, 1).
    @return Normal quantile x = Phi^-1(p) as a float.
    """
    return _as241(p, _FLOAT, 0.5, 0.425, 0.180625, 5.0, 1.6,
                  math.sqrt, math.log)


def as241_decimal(p: Decimal) -> Decimal:
    """!
    @brief AS241 in the current decimal context: slow, platform independent.

    @details
    Decimal.sqrt and Decimal.ln are correctly rounded in the current context,
    so the result depends only on the context precision, never on the host's
    C library.
    @param p Probability in (0, 1) as a Decimal.
    @return Normal quantile x = Phi^-1(p) as a Decimal.
    """
    return _as241(p, _DEC, Decimal("0.5"), Decimal("0.425"),
                  Decimal("0.180625"), Decimal(5), Decimal("1.6"),
                  lambda v: v.sqrt(), lambda v: v.ln())


def _gauss_draw(k: int, sigma: int, cut: int) -> Optional[int]:
    """!
    @brief Offset for the 53-bit draw *k*, or ``None`` if outside the cut.

    ``u = (k + 1/2) / 2**53`` exactly.  The float path is used unless its
    result lies within :data:`_GUARD` ticks of a rounding boundary or of the
    cut, in which case the decision is made in exact decimal arithmetic.

    @details
    Uniform: u is the midpoint of the k-th of 2^53 equal cells of (0, 1), so
    it is never 0 or 1 (the log in AS241 stays finite), and the set of u is
    symmetric, u(k) = 1 - u(2^53 - 1 - k), giving an exactly symmetric
    offset distribution.

    Gaussian: x = AS241(u) is a standard-normal deviate; the candidate offset
    is x*sigma ticks.  It is rejected when |x*sigma| > cut (truncation by
    rejection, so no erf/renormalisation is needed and |o| <= cut is a hard
    guarantee); otherwise it is rounded half-to-even to a whole tick, which
    is the rounding of Mathematica's Round in the reference dts1.wl.

    Reproducibility: only two decisions depend on x*sigma, "which integer is
    nearest" and "is |x*sigma| <= cut".  Each changes only at a boundary
    (half-integers, and +-cut).  Away from those boundaries, any evaluation
    error smaller than _GUARD cannot change the result, so the float path
    (whose last-ulp behaviour depends on the platform's libm log/sqrt) gives
    the same integer everywhere.  Within _GUARD of a boundary the result is
    recomputed with 80 significant decimal digits, which is far beyond the
    accuracy needed to settle the boundary and is the same on every machine;
    the reference evaluates the same exact rationals at 60 digits.  Offsets
    are therefore bit-identical across platforms and Python builds, which
    inversion requires: a one-tick disagreement would restore a wrong time.

    Exact float input: u is an odd multiple of 2^-54, representable as a
    double only for u < 1/2.  AS241 is exactly odd about 1/2 (the central
    rational is odd in q = u - 1/2 and the tails use min(u, 1-u) with the
    sign of q), so for k >= 2^52 the float path evaluates -x(1 - u), where
    1 - u = (2^53 - 1 - k + 1/2) / 2^53 is exact.  Versions up to 1.5.2
    formed u directly in float, which gave u = 1.0 for k = 2^53 - 1
    (log(0)) and errors of thousands of ticks in the far tail for wide cuts.
    @param k Top 53 bits of a 64-bit HMAC word, 0 <= k < 2**53.
    @param sigma Gaussian standard deviation in ticks.
    @param cut Truncation bound in ticks.
    @return The offset in ticks, or None if the draw is rejected by the cut.
    """
    # u is an odd multiple of 2**-54, which a double holds exactly only for
    # u < 1/2.  AS241 is odd about 1/2, so the upper half is evaluated as
    # -x(1 - u) with 1 - u = (2**53 - 1 - k + 1/2) / 2**53, again exact.
    if k < 4503599627370496:                    # 2**52
        xs = as241_float((k + 0.5) / 9007199254740992.0) * sigma
    else:
        xs = -as241_float((9007199254740991 - k + 0.5)
                          / 9007199254740992.0) * sigma
    # Distance of |xs| from the nearest half-integer rounding boundary.
    frac = abs(xs) % 1.0
    if abs(frac - 0.5) > _GUARD and abs(abs(xs) - cut) > _GUARD:
        # Clear of both boundaries: the float decision is the exact decision.
        # Python's round() on a float is half-to-even.
        return round(xs) if abs(xs) <= cut else None
    # Near a boundary: redo the draw in decimal.  u is formed exactly from
    # the integer k, so this path is the specification's value.
    with localcontext() as ctx:
        ctx.prec = 80
        ud = (Decimal(k) + Decimal("0.5")) / Decimal(9007199254740992)
        xd = as241_decimal(ud) * sigma
        if abs(xd) > cut:
            return None
        return int(xd.quantize(Decimal(1), rounding=ROUND_HALF_EVEN))


# ----------------------------------------------------------------- offsets


class OffsetFunction:
    """!
    @brief The offset ``o(source, n)`` for one token, with a small cache.

    The cache matters because each row needs ``o(n)`` and ``o(n - 1)``, and
    ``o(n - 1)`` is usually an earlier row's ``o(n)``.

    @details
    Keyed block.  Block j for event n of source s is

        B_j = HMAC-SHA256(secret, UTF-8("P|s|n|j"))   (32 bytes)

    where P is Token.params and n, j are signed decimal integers (n may be
    -1 when o(n-1) is wanted for n = 0).  P binds the distribution and its
    parameters into every block; s and n identify the hardware event; j
    extends the stream when the Gaussian rejection loop needs more than four
    draws.  Without the secret, the HMAC is a PRF: an observer who knows any
    number of other offsets cannot predict this one.

    Words.  B_j is read as four 64-bit big-endian words
    w_i = B_j[8i : 8i+8], i = 0..3.

    Cache.  A plain dict keyed (source, n), cleared wholesale when it reaches
    cache_size entries.  Not an LRU: rows arrive roughly in event_number
    order per source, so the only reuse that matters is o(n-1) right after
    o(n-1) was computed as the previous row's o(n), and a full clear costs at
    most one recomputation per source.  The bound keeps memory flat on
    day-long files (~1e6 rows).
    """

    def __init__(self, token: Token, cache_size: int = 65536):
        """!
        @brief Bind the offset function to a token.
        @param token The dts1 token supplying distribution, parameters and key.
        @param cache_size Maximum number of cached (source, n) entries.
        """
        self.token = token
        self._key = token.secret
        # "P|" is the constant head of every HMAC message for this token.
        self._prefix = token.params + "|"
        self._cache: Dict[Tuple[str, int], int] = {}
        self._cache_size = cache_size

    def _block(self, source: str, n: int, j: int) -> bytes:
        """!
        @brief The j-th 32-byte keyed block for event n of source.
        @param source TDU source name (the ``source`` column).
        @param n Hardware event number (may be negative, e.g. -1).
        @param j Block index, 0.._MAX_BLOCKS-1.
        @return HMAC-SHA256(secret, "P|source|n|j") as 32 bytes.
        """
        # str.format of an int gives the signed decimal ("-1"), matching
        # intStr in dts1.wl.
        message = "{}{}|{}|{}".format(self._prefix, source, n, j).encode("utf-8")
        return hmac.digest(self._key, message, "sha256")

    def __call__(self, source: str, n: int) -> int:
        """!
        @brief o(source, n), from the cache when possible.
        @param source TDU source name.
        @param n Hardware event number.
        @return The offset in ticks: |o| <= a (flat) or |o| <= cut (gauss).
        @throws ShiftError if the Gaussian rejection loop is exhausted.
        """
        key = (source, n)
        value = self._cache.get(key)
        if value is None:
            value = self._compute(source, n)
            if len(self._cache) >= self._cache_size:
                self._cache.clear()
            self._cache[key] = value
        return value

    def _compute(self, source: str, n: int) -> int:
        """!
        @brief Compute o(source, n) from the keyed blocks (no cache).

        @details
        Flat, half-width a: X = first 16 bytes of B_0 as a 128-bit big-endian
        integer; o = (X mod (2a+1)) - a, uniform on {-a, ..., +a}.  Pure
        integer arithmetic, so it is exact on every platform.  No rejection
        step is used: the modulo bias is at most (2a+1)/2^128 < 2^-102 per
        residue (2a+1 <= 64e6+1 < 2^26), far below anything measurable.

        Gaussian: draws are taken in the fixed order (j = 0, i = 0..3),
        (j = 1, i = 0..3), ...; each 64-bit word w gives k = w >> 11 (its top
        53 bits) and _gauss_draw(k) either accepts and returns the offset or
        rejects it, in which case the next word is tried.  The order is part
        of the specification.
        @param source TDU source name.
        @param n Hardware event number.
        @return The offset in ticks.
        @throws ShiftError if all 4 * _MAX_BLOCKS Gaussian draws are rejected
                (probability < 1e-54 for any valid token).
        """
        token = self.token
        if token.dist == "flat":
            x = int.from_bytes(self._block(source, n, 0)[:16], "big")
            return x % (2 * token.a + 1) - token.a
        for j in range(_MAX_BLOCKS):
            block = self._block(source, n, j)
            for i in range(4):
                word = int.from_bytes(block[8 * i:8 * i + 8], "big")
                # Top 53 bits: the full mantissa width of a double.
                value = _gauss_draw(word >> 11, token.sigma, token.cut)
                if value is not None:
                    return value
        raise ShiftError("no Gaussian draw inside the cut after {} tries; "
                         "this should be impossible".format(4 * _MAX_BLOCKS))


# ------------------------------------------------------------------- rows


def _as_int(value: Any, column: str) -> Optional[int]:
    """!
    @brief Read an integer column value from a CSV string or JSON value.
    @param value The cell (str for CSV, native type for JSON); None or "" is
           treated as absent.
    @param column Column name, for the error message.
    @return The integer, or None if the cell is empty.
    @throws ShiftError if the value is present but not an integer.
    """
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ShiftError("column {} holds {!r}, not an integer".format(
            column, value))


def transform_row(row: Dict[str, Any], offsets: OffsetFunction,
                  sign: int, text: bool) -> None:
    """!
    @brief Shift (``sign=+1``) or restore (``sign=-1``) one row in place.

    *text* is true for CSV rows, whose values are strings, and false for JSON
    rows, which keep their native types.

    @details
    With o = o(source, n) and o' = o(source, n-1), all in 64 MHz ticks:

    | column       | apply (sign = +1)    | invert (sign = -1)   |
    |--------------|----------------------|----------------------|
    | nova_time    | t + o                | t' - o               |
    | pps_offset   | (p + o) mod 1 s      | (p' - o) mod 1 s     |
    | delta        | D + o - o'           | D' - o + o'          |
    | derived      | from new nova_time   | from new nova_time   |

    Invertibility: source and event_number are never modified, so o and o'
    computed from a shifted row are the same values used to shift it; the
    inverse is the same formula with the sign flipped.  Each row inverts on
    its own, without neighbours, ordering, or other files.

    Why pps_offset is transformed: pps_offset == nova_time mod 1 s on every
    row.  Left unchanged, the true time would be the unique value congruent
    to pps_offset (mod 1 s) within 0.5 s of the shifted nova_time, and
    |o| <= 500 ms makes that value exactly t.  Shifting it by the same o keeps
    the identity p' == t' mod 1 s.

    Why delta is transformed: delta is the exact gap t(n) - t(n-1) to
    hardware event n-1 of the same source.  Left unchanged it fixes all
    times in a run up to one common constant, which the zero-mean offsets
    then pin to ~sigma/sqrt(N).  The algebraic form D + o(n) - o(n-1) equals
    t'(n) - t'(n-1) whenever event n-1 is in the data, and still inverts
    exactly when it is not (about 19% of ppc-01 predecessors were never
    archived), because o(n-1) is computable from (source, n-1) alone.

    Empty pps_offset / delta cells (None or "") are left as they are.
    @param row One event as a column-name -> value dict; modified in place.
    @param offsets The offset function of the token in use.
    @param sign +1 to apply the blur, -1 to invert it.
    @param text True for CSV (values are str and written back as str); False
           for JSON (values keep native types).
    @throws ShiftError if a required column is missing or not an integer,
            the result would make nova_time negative, or pps_offset is not
            in [0, 1 s).
    """
    source = row.get("source")
    n = _as_int(row.get("event_number"), "event_number")
    t = _as_int(row.get("nova_time"), "nova_time")
    if not source or n is None or t is None:
        raise ShiftError("a row lacks source, event_number or nova_time")

    # str(source): JSON could carry a non-string source; the HMAC message
    # uses its text form either way.
    o = offsets(str(source), n)
    shifted = t + sign * o
    if shifted < 0:
        raise ShiftError("nova_time {} would go negative".format(t))
    # CSV cells are written back as text; JSON values keep their type.
    out = (lambda v: str(v)) if text else (lambda v: v)
    row["nova_time"] = out(shifted)
    if "nova_time_exact" in row:
        row["nova_time_exact"] = str(shifted)

    pps = _as_int(row.get("pps_offset"), "pps_offset") if "pps_offset" in row else None
    if pps is not None:
        if not 0 <= pps < TICKS:
            raise ShiftError("pps_offset {} is outside one second".format(pps))
        # Python's % with a positive modulus is non-negative, so this also
        # wraps correctly for sign = -1 and negative offsets.
        row["pps_offset"] = out((pps + sign * o) % TICKS)

    delta = _as_int(row.get("delta"), "delta") if "delta" in row else None
    if delta is not None:
        row["delta"] = out(delta + sign * (o - offsets(str(source), n - 1)))

    # Recompute every derived time column from the new nova_time.  On invert,
    # convert() of the restored integer reproduces the original values, which
    # is what makes the restored file byte-identical.
    present = [c for c in DERIVED_COLUMNS if c in row]
    if present:
        times = convert(shifted)
        for column in present:
            row[column] = out(getattr(times, column))


# ------------------------------------------------------------------- files
#
# File state.  A shifted file carries a marker naming the token's params and
# fingerprint: in CSV a "# darpa-spill-shift: ..." comment line appended
# after any existing comments, in JSON a meta["shift"] object.  apply refuses
# a file that has one (no double shift), invert refuses a file without one or
# with a different fingerprint, and invert removes it, so the restored file is
# the original byte for byte.  --force overrides each check.


def _marker_text(token: Token) -> str:
    """!
    @brief The CSV marker comment body for a file shifted with token.
    @param token The token used.
    @return ``darpa-spill-shift: dts1 shifted, params=<P>, fingerprint=<F>``
            (without the leading "# ").  Never contains the secret.
    """
    return "{} dts1 shifted, params={}, fingerprint={}".format(
        MARKER_PREFIX, token.params, token.fingerprint)


def _marker_meta(token: Token) -> Dict[str, str]:
    """!
    @brief The JSON meta["shift"] object for a file shifted with token.
    @param token The token used.
    @return {"spec", "state", "params", "fingerprint"}; never the secret.
    """
    return {"spec": "dts1", "state": "shifted", "params": token.params,
            "fingerprint": token.fingerprint}


def _parse_marker(text: str) -> Dict[str, str]:
    """!
    @brief Extract params and fingerprint from a CSV marker line.
    @param text The marker comment line.
    @return Dict with whichever of "params" and "fingerprint" were found.
    """
    found = dict(re.findall(r"(params|fingerprint)=([^,\s]+)", text))
    return found


def detect_format(path: str) -> str:
    """!
    @brief ``csv`` or ``json``, from the extension or else the first character.
    @param path File path; read only if the extension is neither .csv nor .json.
    @return "csv" or "json".
    @throws OSError if the file must be read and cannot be.
    """
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
    """!
    @brief Refuse to shift twice, or to invert unmarked or foreign data.
    @param state The file's marker (params/fingerprint dict), or None if the
           file is unmarked.
    @param token The token about to be used.
    @param sign +1 for apply, -1 for invert.
    @param force Skip all three checks.
    @param path File name, for the message.
    @throws ShiftError apply on a marked file; invert on an unmarked file;
            invert with a token whose fingerprint differs from the marker's.
    """
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
    # A wrong token inverts to plausible but wrong times with no other
    # symptom, so a fingerprint mismatch is a hard error.
    if state.get("fingerprint") != token.fingerprint and not force:
        raise ShiftError(
            "{} was shifted with token fingerprint {} ({}), but this token is "
            "{} ({}); inverting with the wrong token gives wrong times "
            "silently. Pass --force to do it anyway".format(
                path, state.get("fingerprint"), state.get("params"),
                token.fingerprint, token.params))


class _AtomicWriter:
    """!
    @brief A text file written as PATH.part and renamed on success.

    @details
    commit() renames PATH.part over PATH with os.replace, which is atomic on
    POSIX and on Windows within one volume, so PATH is either absent, the old
    file, or the complete new file; abort() deletes PATH.part.  A refusal or
    error mid-file (including Ctrl-C) therefore leaves no partial output.
    The file is opened with newline="" so "\\n" is written as-is on every
    platform (no CRLF translation on Windows), which byte identity requires.
    Path "-" writes to stdout without atomicity.
    """

    def __init__(self, path: str):
        """!
        @brief Open PATH.part for writing (or bind stdout for "-").
        @param path Final output path, or "-" for stdout.
        @throws OSError if PATH.part cannot be created.
        """
        self.path = path
        if path == "-":
            self.stream = sys.stdout
            self.part = None
        else:
            self.part = path + ".part"
            self.stream = open(self.part, "w", newline="", encoding="utf-8")

    def commit(self) -> None:
        """!
        @brief Close and move PATH.part into place (flush only, for stdout).
        @throws OSError if the close or rename fails.
        """
        if self.part is None:
            self.stream.flush()
            return
        self.stream.close()
        os.replace(self.part, self.path)

    def abort(self) -> None:
        """!
        @brief Close and delete PATH.part; PATH is left untouched.
        """
        if self.part is not None:
            self.stream.close()
            try:
                os.unlink(self.part)
            except OSError:
                pass


def _shift_csv(src, out, offsets: OffsetFunction, sign: int, force: bool,
               path: str) -> int:
    """!
    @brief Shift or invert a darpa-spill-dump CSV stream.

    @details
    Layout preservation: leading "#" comment lines and the header line are
    copied verbatim (the header is parsed only to learn the column names);
    each data row is parsed with csv.reader and re-emitted with csv.writer
    using "\\n" line endings, the same dialect darpa-spill-dump writes, so
    unchanged cells come out as they went in.  On apply one marker comment is
    appended after the existing comments; on invert every marker comment is
    removed, so invert(apply(f)) == f byte for byte.
    @param src Input text stream (opened with newline="").
    @param out Output text stream.
    @param offsets The offset function of the token in use.
    @param sign +1 apply, -1 invert.
    @param force Skip the file-state checks.
    @param path Input name, for messages.
    @return Number of data rows transformed.
    @throws ShiftError no header, missing required columns, a refused file
            state, or a bad row (see transform_row).
    """
    comments: List[str] = []
    header_line = ""
    # The header is the first non-comment line; everything before it is kept.
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
    # The last marker is authoritative (there is normally only one; several
    # appear only after a forced double shift).
    state = _parse_marker(comments[markers[-1]]) if markers else None
    _check_state(state, offsets.token, sign, force, path)
    if sign > 0:
        comments.append("# " + _marker_text(offsets.token) + "\n")
    else:
        # Delete from the end so earlier indices stay valid.
        for i in reversed(markers):
            del comments[i]

    out.writelines(comments)
    out.write(header_line if header_line.endswith("\n") else header_line + "\n")
    writer = csv.writer(out, lineterminator="\n")
    count = 0
    # csv.reader continues the same iterator, just after the header line.
    for values in csv.reader(src):
        if not values:
            continue
        row = dict(zip(header, values))
        transform_row(row, offsets, sign, text=True)
        writer.writerow([row[c] for c in header])
        count += 1
    return count


def _dump_layout(lines: List[str]) -> bool:
    """!
    @brief True for darpa-spill-dump's JSON layout: one event per line.

    @details
    That layout is: line 1 "{", line 2 '"meta": {...},', line 3
    '"events": [' (or '"events": [],' when empty), then one event object
    per line, then the closing lines.
    @param lines The first three lines of the file.
    @return True if they match that layout exactly.
    """
    return (len(lines) >= 3 and lines[0] == "{\n"
            and lines[1].startswith('"meta": ')
            and lines[2] in ('"events": [\n', '"events": [],\n'))


def _shift_json(src, out, offsets: OffsetFunction, sign: int, force: bool,
                path: str) -> int:
    """!
    @brief Shift or invert a JSON events document.

    @details
    darpa-spill-dump's line-per-event layout is streamed and preserved byte
    for byte (_shift_json_stream).  Any other layout is parsed whole and
    re-serialised compactly: the data round-trips exactly, the formatting
    does not.
    @param src Input text stream.
    @param out Output text stream.
    @param offsets The offset function of the token in use.
    @param sign +1 apply, -1 invert.
    @param force Skip the file-state checks.
    @param path Input name, for messages.
    @return Number of events transformed.
    @throws ShiftError not an events document, a refused file state, or a
            bad row.
    @throws json.JSONDecodeError if the input is not valid JSON.
    """
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
    """!
    @brief Check and update the JSON meta["shift"] marker in place.
    @param meta The document's meta object; gains "shift" on apply, loses it
           on invert.
    @param token The token in use.
    @param sign +1 apply, -1 invert.
    @param force Skip the file-state checks.
    @param path Input name, for messages.
    @throws ShiftError if the file state is refused (see _check_state).
    """
    state = meta.get("shift")
    _check_state(state, token, sign, force, path)
    if sign > 0:
        meta["shift"] = _marker_meta(token)
    else:
        # Popping the last-inserted key restores the original key order, so
        # the re-serialised meta line matches the original.
        meta.pop("shift", None)


def _shift_json_stream(head, src, out, offsets, sign, force, path) -> int:
    """!
    @brief darpa-spill-dump's layout: one event per line, streamed.

    @details
    Memory is O(1) in the number of events.  The meta line is re-serialised
    with json.dumps(default=str), as darpa-spill-dump writes it; each event
    line is parsed, transformed and re-serialised the same way, keeping its
    trailing comma.  The closing "]" is copied verbatim; the trailer after
    it (at most 1 MiB) is validated as JSON, with an optional summary whose
    ``returned`` count must match, before it is written.  A truncated or
    malformed document raises, so shift_file never commits it.
    @param head The first three lines, already read and matched by _dump_layout.
    @param src Input text stream, positioned after those lines.
    @param out Output text stream.
    @param offsets The offset function of the token in use.
    @param sign +1 apply, -1 invert.
    @param force Skip the file-state checks.
    @param path Input name, for messages.
    @return Number of events transformed.
    @throws ShiftError unexpected layout, a refused file state, or a bad row.
    """
    # Line 2 is '"meta": {...},\n'; drop the key, newline and trailing comma.
    meta_text = head[1][len('"meta": '):].rstrip("\n")
    if not meta_text.endswith(","):
        raise ShiftError("{}: unexpected JSON layout".format(path))
    meta = json.loads(meta_text[:-1])
    _apply_meta(meta, offsets.token, sign, force, path)
    out.write("{\n")
    out.write('"meta": ' + json.dumps(meta, default=str) + ",\n")
    out.write(head[2])
    def finish(trailer, count):
        # Only the small document trailer is buffered; events remain streamed.
        rest = src.read(1024 * 1024 + 1)
        if len(rest) > 1024 * 1024:
            raise ShiftError("{}: JSON trailer is too large".format(path))
        trailer += rest
        document = json.loads('{"events": []' + trailer)
        if document.get("events") != []:
            raise ShiftError("{}: duplicate events in JSON trailer".format(path))
        summary = document.get("summary")
        if summary is not None and (not isinstance(summary, dict)
                or summary.get("returned", count) != count):
            raise ShiftError("{}: summary does not match event count".format(path))
        out.write(trailer)
        return count

    if head[2] == '"events": [],\n':
        # No events: the comma has already been written as part of head[2],
        # and the remainder must be the validated close of the document.
        rest = src.read(1024 * 1024 + 1)
        if len(rest) > 1024 * 1024:
            raise ShiftError("{}: JSON trailer is too large".format(path))
        document = json.loads('{"events": [],' + rest)
        if document.get("events") != []:
            raise ShiftError("{}: duplicate events in JSON trailer".format(path))
        summary = document.get("summary")
        if summary is not None and (not isinstance(summary, dict)
                or summary.get("returned", 0) != 0):
            raise ShiftError("{}: summary does not match event count".format(path))
        out.write(rest)
        return 0
    count = 0
    previous_comma = False
    for line in src:
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("]"):
            if previous_comma:
                raise ShiftError("{}: trailing comma in events".format(path))
            # Preserve the exact closing-array layout, then validate the
            # remainder as a small JSON object with an empty event list.
            closing = line.index("]")
            out.write(line[:closing + 1])
            return finish(line[closing + 1:], count)
        # Every event line but the last ends with ","; keep it as found.
        if count and not previous_comma:
            raise ShiftError("{}: missing comma between events".format(path))
        comma = stripped.endswith(",")
        row = json.loads(stripped[:-1] if comma else stripped)
        if not isinstance(row, dict):
            raise ShiftError("{}: event must be an object".format(path))
        transform_row(row, offsets, sign, text=False)
        out.write(json.dumps(row, default=str) + ("," if comma else "") + "\n")
        previous_comma = comma
        count += 1
    raise ShiftError("{}: unexpected EOF in events array".format(path))


def shift_file(in_path: str, out_path: str, token: Token, sign: int,
               force: bool = False) -> int:
    """!
    @brief Shift (``sign=+1``) or invert (``-1``) one file; return the row count.

    @details
    Output goes through _AtomicWriter: on any exception (including
    KeyboardInterrupt, hence BaseException) the partial output is removed and
    the exception re-raised.  Input "-" is stdin and is always read as CSV.
    @param in_path Input file, or "-" for stdin.
    @param out_path Output file, or "-" for stdout.
    @param token The token to apply or invert with.
    @param sign +1 apply, -1 invert.
    @param force Skip the file-state checks.
    @return Number of rows/events transformed.
    @throws ShiftError see _shift_csv / _shift_json.
    @throws OSError on I/O failure.
    @throws json.JSONDecodeError on malformed JSON input.
    """
    fmt = "csv" if in_path == "-" else detect_format(in_path)
    # One OffsetFunction per file: its cache serves o(n-1) lookups within it.
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
    """!
    @brief The shift marker of a data file, or ``None`` if it carries none.
    @param path A CSV or JSON data file.
    @return For CSV, the params/fingerprint parsed from the marker comment;
            for JSON, the meta["shift"] object; None if unmarked.
    @throws OSError if the file cannot be read.
    @throws json.JSONDecodeError if a JSON file is malformed.
    """
    if detect_format(path) == "csv":
        with open(path, encoding="utf-8") as handle:
            # Markers live only in the leading comment block.
            for line in handle:
                if not line.startswith("#"):
                    return None
                if line[1:].strip().startswith(MARKER_PREFIX):
                    return _parse_marker(line)
        return None
    with open(path, encoding="utf-8") as handle:
        head = [handle.readline() for _ in range(3)]
        if _dump_layout(head):
            # Read only the meta line, not the whole (possibly large) file.
            meta = json.loads(head[1][len('"meta": '):].rstrip("\n")[:-1])
        else:
            meta = json.loads("".join(head) + handle.read()).get("meta", {})
    return meta.get("shift")


# --------------------------------------------------------------------- CLI

## Ticks per unit for parse_duration (exact Decimals: 1 ns = 0.064 ticks).
_UNITS = {"s": Decimal(TICKS), "ms": Decimal(TICKS) / 1000,
          "us": Decimal(TICKS) / 1000000, "µs": Decimal(TICKS) / 1000000,
          "ns": Decimal(TICKS) / 1000000000, "tick": Decimal(1),
          "ticks": Decimal(1)}


def parse_duration(text: str) -> int:
    """!
    @brief ``50ms``, ``0.02s``, ``500us``, ``1280000ticks`` -> whole ticks.

    @details
    The number is read as an exact Decimal, multiplied by the unit's tick
    count and rounded half-to-even to an integer number of 64 MHz ticks.
    A unit is mandatory, so a bare number is never misread as ms or ticks.
    @param text Duration text: non-negative decimal number and a unit
           (s, ms, us, µs, ns, tick, ticks), case-insensitive.
    @return The duration in ticks, >= 1.
    @throws TokenError if the text cannot be parsed or rounds below one tick.
    """
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
    """!
    @brief Format a tick count as milliseconds with the raw ticks.
    @param ticks Duration in 64 MHz ticks.
    @return e.g. ``50 ms (3200000 ticks)``.
    """
    # 64 000 ticks per millisecond.
    return "{:g} ms ({} ticks)".format(ticks / 64000.0, ticks)


def _token_params(args) -> Dict[str, int]:
    """!
    @brief Token parameters, in ticks, from --dist/--width/--sigma/--cut.

    @details
    --cut accepts a duration or "N sigma".  The default is 3 sigma, clamped
    to 500 ms; an explicit cut is never clamped, so an out-of-range one is
    reported by Token validation instead of being silently changed.
    @param args Parsed argparse namespace.
    @return {"a": A} for flat, {"sigma": S, "cut": C} for gauss.
    @throws TokenError if a required option is missing or unparsable.
    """
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
    """!
    @brief First line of a token or seed file, without its newline.
    @param path File to read.
    @return The first line.
    @throws OSError if the file cannot be read.
    """
    with open(path, encoding="utf-8") as handle:
        return handle.readline().rstrip("\n")


def _load_token(args, environ) -> Token:
    """!
    @brief Find the token: command line, then environment.

    @details
    Precedence: --token > --token-file > $DARPA_SPILL_SHIFT_TOKEN >
    $DARPA_SPILL_SHIFT_TOKEN_FILE.  Deliberately never read from the YAML
    configuration: the token is a secret.
    @param args Parsed argparse namespace (token and token_file attributes).
    @param environ Environment mapping.
    @return The parsed Token.
    @throws TokenError if no token is given, the file is unreadable, or the
            token is invalid.
    """
    if args.token:
        return Token.parse(args.token)
    path = args.token_file or environ.get(ENV_TOKEN_FILE)
    # An explicit --token-file outranks the environment token.
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
    """!
    @brief Write a token to a mode-0600 file, never replacing one without force.
    @param path Destination file.
    @param token Token to write (its full text, secret included).
    @param force Replace an existing file.
    @throws TokenError if the file exists (or appears concurrently) and force
            is not set.
    @throws OSError on other I/O failures.
    """
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
        # After os.link both names share the inode; removing tmp leaves path.
        if os.path.exists(tmp):
            os.unlink(tmp)


def _new_token(args) -> Token:
    """!
    @brief Build a new token from the "new token" options.
    @param args Parsed namespace (dist, width/sigma/cut, seed, seed_file).
    @return A token with a random secret, or a PBKDF2 secret if a seed is given
            (--seed-file outranks --seed).
    @throws TokenError on missing or invalid parameters or an empty seed.
    @throws OSError if the seed file cannot be read.
    """
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
    """!
    @brief Default output name: d.csv -> d.shifted.csv -> d.restored.csv.
    @param path Input file name.
    @param sign +1 apply, -1 invert.
    @return ``<root>.shifted<ext>`` on apply; on invert ``<root>.restored<ext>``
            with one trailing ".shifted" removed from root.
    """
    root, ext = os.path.splitext(path)
    if sign > 0:
        return root + ".shifted" + ext
    if root.endswith(".shifted"):
        root = root[:-len(".shifted")]
    return root + ".restored" + ext


def build_parser() -> argparse.ArgumentParser:
    """!
    @brief The darpa-spill-shift argument parser (token, apply, invert, info).
    @return The configured ArgumentParser.
    """
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
        """!
        @brief Add --token / --token-file to a subcommand.
        @param p The subparser.
        """
        g = p.add_argument_group("token")
        g.add_argument("--token", metavar="TOKEN",
                       help="the token itself (visible in ps and shell history; "
                            "prefer --token-file)")
        g.add_argument("--token-file", metavar="FILE",
                       help="read the token from the first line of FILE")

    def new_token_options(p):
        """!
        @brief Add the options that define a new token to a subcommand.
        @param p The subparser.
        """
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
    """!
    @brief Print an error message to stderr with the program prefix.
    @param message The message text.
    """
    print("darpa-spill-shift: error: {}".format(message), file=sys.stderr)


def _cmd_token(args) -> int:
    """!
    @brief The ``token`` subcommand: create a token, print or save it.
    @param args Parsed namespace.
    @return Exit status 0.
    @throws TokenError if --dist is missing or the parameters are invalid, or
            the output file exists without --force.
    """
    if not args.dist:
        raise TokenError("give --dist flat --width W, or --dist gauss --sigma S")
    token = _new_token(args)
    if args.output:
        _write_token_file(args.output, token, args.force)
        print("darpa-spill-shift: wrote {} token to {} (fingerprint {})".format(
            token.describe(), args.output, token.fingerprint), file=sys.stderr)
    else:
        # Token on stdout (for redirection), commentary on stderr.
        print(str(token))
        print("darpa-spill-shift: {}, fingerprint {}. Keep this token: without "
              "it the shift cannot be undone.".format(
                  token.describe(), token.fingerprint), file=sys.stderr)
    return 0


def _cmd_shift(args, sign: int, environ) -> int:
    """!
    @brief The ``apply`` (sign=+1) and ``invert`` (sign=-1) subcommands.

    @details
    With apply --dist, a new token is created and must be saved with
    --token-out before any data is shifted, so shifted data can never exist
    without the token that inverts it.
    @param args Parsed namespace.
    @param sign +1 apply, -1 invert.
    @param environ Environment mapping, for the token variables.
    @return Exit status 0.
    @throws TokenError on conflicting or missing token options.
    @throws ShiftError if an output would overwrite its input, or a file is
            refused (see shift_file).
    """
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
    """!
    @brief The ``info`` subcommand: describe a token, token file or data file.

    @details
    A target starting "dts1:" is a token; a file whose first line starts
    "dts1:" is a token file; any other file is a data file, whose marker is
    reported and, if a token is also available, compared with it.
    @param args Parsed namespace.
    @param environ Environment mapping, for the token variables.
    @return 0, or 1 if a data file's fingerprint does not match the token.
    @throws TokenError if a token is needed and none is found or it is invalid.
    """
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
            # The token is optional when describing a data file.
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
    """!
    @brief Entry point of ``darpa-spill-shift``.

    @details
    Exit status: 0 success; 1 file/shift/I-O/JSON error, or info mismatch;
    2 usage or token error (also argparse errors); 130 interrupted; 141
    (128 + SIGPIPE) when the reader of stdout closes the pipe early.
    @param argv Arguments (default sys.argv[1:]).
    @param environ Environment (default os.environ); copied, never modified.
    @return The exit status.
    """
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
    # BrokenPipeError is a subclass of OSError, so it must be caught first.
    except BrokenPipeError:
        # Point stdout at /dev/null so the interpreter's final flush cannot
        # raise again; 141 = 128 + SIGPIPE, the shell convention.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        return 141
    except OSError as exc:
        _err(str(exc))
        return 1
    except json.JSONDecodeError as exc:
        _err("not valid JSON: {}".format(exc))
        return 1
    except KeyboardInterrupt:
        # shift_file has already removed any .part output.
        _err("interrupted; partial outputs removed")
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
