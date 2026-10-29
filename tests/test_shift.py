"""Tests for darpa-spill-shift, the dts1 reversible timestamp blur.

The reference values come from docs/shift/dts1_test_vectors.json, written by
the Mathematica reference implementation when docs/TimestampShift.nb is
built.  Agreement with it is the definition of a correct implementation.
"""

import csv
import json
import os
import stat
from pathlib import Path

import pytest

from darpa_spillserver import dump, shift
from darpa_spillserver.novatime import convert
from darpa_spillserver.shift import (
    MAX_OFFSET,
    TICKS,
    OffsetFunction,
    ShiftError,
    Token,
    TokenError,
    _gauss_draw,
    as241_float,
    main,
    parse_duration,
    secret_from_seed,
    shift_file,
    transform_row,
)

from conftest import BASE, SECOND

DOCS = Path(__file__).resolve().parent.parent / "docs" / "shift"
VECTORS = json.loads((DOCS / "dts1_test_vectors.json").read_text())
SAMPLE = DOCS / VECTORS["sample_file"]
TOKENS = {name: Token.parse(text) for name, text in VECTORS["tokens"].items()}


def tagged(path, tag):
    """d.csv -> d.<tag>.csv, as darpa-spill-shift names its outputs."""
    root, ext = os.path.splitext(str(path))
    return root + "." + tag + ext


def sample_rows():
    with open(SAMPLE) as handle:
        return list(csv.DictReader(l for l in handle if not l.startswith("#")))


# ----------------------------------------------------- reference vectors


def test_hmac_matches_rfc4231():
    import hashlib
    import hmac
    digest = hmac.new(b"Jefe", b"what do ya want for nothing?",
                      hashlib.sha256).hexdigest()
    assert digest == VECTORS["hmac_rfc4231_case2"]


def test_seed_derivation_matches_reference():
    p = VECTORS["pbkdf2"]
    assert p["salt"] == shift.SEED_SALT
    assert secret_from_seed(p["seed"], p["iterations"]).hex() == p["secret_hex"]


def test_fingerprints_match_reference():
    for name, token in TOKENS.items():
        assert token.fingerprint == VECTORS["fingerprints"][name]


@pytest.mark.parametrize("vector", VECTORS["offsets"],
                         ids=lambda v: "{token}-{source}-{n}".format(**v))
def test_offsets_match_reference(vector):
    offsets = OffsetFunction(TOKENS[vector["token"]])
    assert offsets(vector["source"], vector["n"]) == vector["offset"]


@pytest.mark.parametrize("tie", VECTORS["near_ties"], ids=lambda t: str(t["k"]))
def test_near_ties_round_exactly(tie):
    # No cut: these test the rounding alone.
    assert _gauss_draw(tie["k"], tie["sigma"], 10**12) == tie["offset"]


def test_float_alone_would_get_a_tie_wrong():
    """The reason for the decimal fallback: one reference case lands on
    exactly .5 in double precision and rounds to the wrong tick."""
    wrong = 0
    for tie in VECTORS["near_ties"]:
        xs = as241_float((tie["k"] + 0.5) / 2**53) * tie["sigma"]
        wrong += round(xs) != tie["offset"]
    assert wrong >= 1


@pytest.mark.parametrize("name", ["flat", "gauss"])
def test_shifted_sample_matches_reference(name):
    offsets = OffsetFunction(TOKENS[name])
    expected = VECTORS["shifted_sample"][name]
    for row, want in zip(sample_rows(), expected):
        transform_row(row, offsets, +1, text=True)
        assert [int(row["nova_time"]), int(row["delta"]),
                int(row["pps_offset"])] == want


# ---------------------------------------------------------- properties


@pytest.mark.parametrize("name", ["flat", "gauss"])
def test_rows_invert_exactly_and_stay_consistent(name):
    offsets = OffsetFunction(TOKENS[name])
    original = sample_rows()
    shifted = [dict(r) for r in original]
    for row in shifted:
        transform_row(row, offsets, +1, text=True)
    times = {(r["source"], int(r["event_number"])): int(r["nova_time"])
             for r in shifted}
    for row in shifted:
        t = int(row["nova_time"])
        assert int(row["pps_offset"]) == t % TICKS
        prev = times.get((row["source"], int(row["event_number"]) - 1))
        if prev is not None:
            assert int(row["delta"]) == t - prev
    restored = [dict(r) for r in shifted]
    for row in restored:
        transform_row(row, OffsetFunction(TOKENS[name]), -1, text=True)
    assert restored == original


def test_offsets_stay_inside_their_bounds():
    flat, gauss = TOKENS["flat"], TOKENS["gauss"]
    f, g = OffsetFunction(flat), OffsetFunction(gauss)
    fo = [f("s", n) for n in range(3000)]
    go = [g("s", n) for n in range(3000)]
    assert max(map(abs, fo)) <= flat.a
    assert max(map(abs, go)) <= gauss.cut
    assert abs(sum(fo) / len(fo)) < 0.1 * flat.a
    sd = (sum(x * x for x in go) / len(go)) ** 0.5
    assert 0.9 * gauss.sigma < sd < 1.05 * gauss.sigma


def test_parameters_and_secret_both_matter():
    secret = TOKENS["flat"].secret
    a = OffsetFunction(Token.generate("flat", secret=secret, a=3200000))
    b = OffsetFunction(Token.generate("flat", secret=secret, a=3200001))
    c = OffsetFunction(Token.generate("flat", secret=bytes(32), a=3200000))
    same_ab = sum(a("s", n) == b("s", n) for n in range(200))
    same_ac = sum(a("s", n) == c("s", n) for n in range(200))
    assert same_ab < 5 and same_ac < 5


def test_derived_columns_follow_nova_time():
    row = {"nova_time": BASE, "source": "s", "event_number": 7, "delta": None,
           "pps_offset": BASE % TICKS}
    times = convert(BASE)
    for column in shift.DERIVED_COLUMNS:
        row[column] = getattr(times, column)
    transform_row(row, OffsetFunction(TOKENS["gauss"]), +1, text=False)
    moved = convert(row["nova_time"])
    for column in shift.DERIVED_COLUMNS:
        assert row[column] == getattr(moved, column)
    assert row["delta"] is None


# --------------------------------------------------------------- tokens


def test_token_round_trip_and_canonical_form():
    token = Token.generate("gauss", sigma=1280000, cut=3840000)
    assert Token.parse(str(token)) == token
    assert token.secret.hex() not in repr(token)
    with pytest.raises(TokenError):
        Token.parse(str(token).upper())
    with pytest.raises(TokenError):
        Token.parse(str(token).replace("sigma=", "sigma=0"))


@pytest.mark.parametrize("bad", [
    "dts2:flat:a=1:" + "0" * 64,
    "dts1:flat:a=1:" + "0" * 63,
    "dts1:flat:a=0:" + "0" * 64,
    "dts1:flat:a={}:".format(MAX_OFFSET + 1) + "0" * 64,
    "dts1:gauss:sigma=100:" + "0" * 64,
    "dts1:gauss:sigma=100:cut=49:" + "0" * 64,
    "dts1:gauss:sigma=100:cut={}:".format(MAX_OFFSET + 1) + "0" * 64,
    "dts1:poisson:a=1:" + "0" * 64,
])
def test_bad_tokens_are_rejected(bad):
    with pytest.raises(TokenError):
        Token.parse(bad)


def test_parse_duration():
    assert parse_duration("20ms") == 1280000
    assert parse_duration("0.5s") == MAX_OFFSET
    assert parse_duration("500us") == 32000
    assert parse_duration("15.625ns") == 1
    assert parse_duration("1280000ticks") == 1280000
    for bad in ("20", "ms", "5ns", "-1ms", "1 fortnight"):
        with pytest.raises(TokenError):
            parse_duration(bad)


# ----------------------------------------------------------------- files


@pytest.fixture
def dumps(populated, tmp_path):
    """A CSV and a JSON dump of the populated archive, by darpa-spill-dump."""
    out_csv, out_json = tmp_path / "d.csv", tmp_path / "d.json"
    code = dump.main(["-d", populated.path, "-q", "--start", "nova:{}".format(BASE),
                      "--end", "nova:{}".format(BASE + 61 * SECOND),
                      "--csv", str(out_csv), "--json", str(out_json)], environ={})
    assert code == 0
    return out_csv, out_json


@pytest.fixture
def token_file(tmp_path):
    path = tmp_path / "tok"
    path.write_text(VECTORS["tokens"]["gauss"] + "\n")
    return str(path)


def test_dump_files_round_trip_byte_identical(dumps, token_file, tmp_path):
    for original in dumps:
        assert main(["apply", "-q", "--token-file", token_file, str(original)],
                    environ={}) == 0
        shifted = Path(tagged(original, "shifted"))
        assert shifted.read_bytes() != original.read_bytes()
        assert main(["invert", "-q", "--token-file", token_file, str(shifted)],
                    environ={}) == 0
        restored = Path(tagged(original, "restored"))
        assert restored.read_bytes() == original.read_bytes()


def test_shifted_files_carry_a_marker_but_no_secret(dumps, token_file):
    csv_path, json_path = dumps
    main(["apply", "-q", "--token-file", token_file, str(csv_path), str(json_path)],
         environ={})
    secret = TOKENS["gauss"].secret.hex()
    shifted_csv = Path(tagged(csv_path, "shifted"))
    shifted_json = Path(tagged(json_path, "shifted"))
    text = shifted_csv.read_text()
    assert "fingerprint=" + TOKENS["gauss"].fingerprint in text
    assert secret not in text and secret not in shifted_json.read_text()
    meta = json.loads(shifted_json.read_text())["meta"]["shift"]
    assert meta == {"spec": "dts1", "state": "shifted",
                    "params": TOKENS["gauss"].params,
                    "fingerprint": TOKENS["gauss"].fingerprint}
    assert shift.file_state(str(shifted_csv))["fingerprint"] == \
        TOKENS["gauss"].fingerprint


def test_shifted_json_times_are_consistent(dumps, token_file):
    _, json_path = dumps
    main(["apply", "-q", "--token-file", token_file, str(json_path)], environ={})
    doc = json.loads(Path(tagged(json_path, "shifted")).read_text())
    original = json.loads(json_path.read_text())["events"]
    moved = 0
    for before, row in zip(original, doc["events"]):
        assert row["utc"] == convert(row["nova_time"]).utc
        assert row["gps"] == convert(row["nova_time"]).gps
        assert row["event_number"] == before["event_number"]
        moved += row["nova_time"] != before["nova_time"]
    assert moved > 0.9 * len(original)


def test_api_style_json_is_handled(tmp_path, token_file):
    doc = {"meta": {"total": 1}, "events": [
        {"nova_time": BASE, "source": "s", "event_number": 3,
         "delta": 1000, "pps_offset": BASE % TICKS}]}
    src = tmp_path / "api.json"
    src.write_text(json.dumps(doc))
    assert main(["apply", "-q", "--token-file", token_file, str(src)], environ={}) == 0
    shifted = tmp_path / "api.shifted.json"
    assert main(["invert", "-q", "--token-file", token_file, str(shifted)],
                environ={}) == 0
    assert json.loads((tmp_path / "api.restored.json").read_text()) == doc


def test_refuses_double_shift_and_foreign_inversion(dumps, token_file, tmp_path):
    csv_path, _ = dumps
    main(["apply", "-q", "--token-file", token_file, str(csv_path)], environ={})
    shifted = tagged(csv_path, "shifted")
    # Shifting again is refused.
    assert main(["apply", "-q", "--token-file", token_file, shifted,
                 "-o", str(tmp_path / "x.csv")], environ={}) == 1
    # Inverting with another token is refused, and leaves nothing behind.
    other = tmp_path / "other"
    other.write_text(VECTORS["tokens"]["flat"] + "\n")
    out = tmp_path / "wrong.csv"
    assert main(["invert", "-q", "--token-file", str(other), shifted,
                 "-o", str(out)], environ={}) == 1
    assert not out.exists() and not Path(str(out) + ".part").exists()
    # Inverting an unshifted file is refused.
    assert main(["invert", "-q", "--token-file", token_file, str(csv_path),
                 "-o", str(out)], environ={}) == 1
    # --force overrides.
    assert main(["invert", "-q", "--force", "--token-file", str(other), shifted,
                 "-o", str(out)], environ={}) == 0


def test_refuses_to_overwrite_input(dumps, token_file):
    csv_path, _ = dumps
    assert main(["apply", "-q", "--token-file", token_file, str(csv_path),
                 "-o", str(csv_path)], environ={}) == 1


def test_missing_columns_are_explained(tmp_path, token_file, capsys):
    src = tmp_path / "c.csv"
    src.write_text("nova_time,utc\n{},x\n".format(BASE))
    assert main(["apply", "--token-file", token_file, str(src)], environ={}) == 1
    assert "source, event_number" in capsys.readouterr().err


def test_split_directory_in_one_call(populated, tmp_path, token_file):
    out = tmp_path / "split"
    dump.main(["-d", populated.path, "-q", "--timezone", "UTC",
               "--start", "nova:{}".format(BASE), "--end", "nova:{}".format(BASE + 61 * SECOND),
               "--split", "hour", "--csv", str(out / "h.csv")], environ={})
    pieces = sorted(str(p) for p in out.glob("h_*.csv"))
    assert pieces
    assert main(["apply", "-q", "--token-file", token_file, *pieces], environ={}) == 0
    assert len(list(out.glob("*.shifted.csv"))) == len(pieces)


# ----------------------------------------------------------------- CLI


def test_token_command_writes_private_file(tmp_path, capsys):
    path = tmp_path / "t"
    assert main(["token", "--dist", "flat", "--width", "50ms", "-o", str(path)],
                environ={}) == 0
    assert stat.S_IMODE(os.stat(path).st_mode) == 0o600
    token = Token.parse(path.read_text())
    assert token.a == 3200000
    # Not overwritten without --force.
    assert main(["token", "--dist", "flat", "--width", "50ms", "-o", str(path)],
                environ={}) == 2
    assert Token.parse(path.read_text()) == token


def test_token_created_concurrently_is_never_replaced(tmp_path, monkeypatch):
    """Two runs that both found the file missing: the second must refuse,
    not truncate the token the first may already have shifted data with."""
    from darpa_spillserver import shift as shift_module
    path = tmp_path / "t"
    real_exists = os.path.exists
    # Pretend the check ran before the other run created the file.
    monkeypatch.setattr(shift_module.os.path, "exists",
                        lambda p: False if p == str(path) else real_exists(p))
    path.write_text("first\n")
    assert main(["token", "--dist", "flat", "--width", "50ms", "-o", str(path)],
                environ={}) == 2
    assert path.read_text() == "first\n"
    monkeypatch.undo()
    assert [p.name for p in tmp_path.iterdir()] == ["t"]   # no tmp left


def test_token_command_defaults_and_seed(capsys):
    assert main(["token", "--dist", "gauss", "--sigma", "20ms", "--seed", "x"],
                environ={}) == 0
    out = capsys.readouterr()
    token = Token.parse(out.out)
    assert (token.sigma, token.cut) == (1280000, 3840000)
    assert "only as strong as the seed" in out.err
    # The same seed and parameters give the same token.
    main(["token", "--dist", "gauss", "--sigma", "20ms", "--seed", "x"], environ={})
    assert Token.parse(capsys.readouterr().out) == token
    # 3 sigma of 200 ms is capped at 500 ms by default...
    main(["token", "--dist", "gauss", "--sigma", "200ms"], environ={})
    assert Token.parse(capsys.readouterr().out).cut == MAX_OFFSET
    # ...but an explicit cut beyond it is an error.
    assert main(["token", "--dist", "gauss", "--sigma", "200ms", "--cut", "3sigma"],
                environ={}) == 2


def test_apply_with_new_token_requires_saving_it(dumps, tmp_path):
    csv_path, _ = dumps
    assert main(["apply", "-q", "--dist", "flat", "--width", "10ms", str(csv_path)],
                environ={}) == 2
    tok = tmp_path / "new.tok"
    assert main(["apply", "-q", "--dist", "flat", "--width", "10ms",
                 "--token-out", str(tok), str(csv_path)], environ={}) == 0
    assert Token.parse(tok.read_text()).a == 640000


def test_token_from_environment(dumps, token_file):
    csv_path, _ = dumps
    env = {shift.ENV_TOKEN: VECTORS["tokens"]["gauss"]}
    assert main(["apply", "-q", str(csv_path)], environ=env) == 0
    env = {shift.ENV_TOKEN_FILE: token_file}
    shifted = tagged(csv_path, "shifted")
    assert main(["invert", "-q", shifted], environ=env) == 0
    assert main(["apply", "-q", str(csv_path), "-o", "/dev/null"], environ={}) == 2


def test_info(dumps, token_file, capsys):
    csv_path, _ = dumps
    main(["apply", "-q", "--token-file", token_file, str(csv_path)], environ={})
    shifted = tagged(csv_path, "shifted")
    capsys.readouterr()
    assert main(["info", shifted, "--token-file", token_file], environ={}) == 0
    assert "matches" in capsys.readouterr().out
    assert main(["info", VECTORS["tokens"]["flat"]], environ={}) == 0
    assert "flat, +-50 ms" in capsys.readouterr().out
    other = Path(token_file).parent / "o"
    other.write_text(VECTORS["tokens"]["flat"])
    assert main(["info", shifted, "--token-file", str(other)], environ={}) == 1


@pytest.mark.parametrize("sigma,cut", [(1280000, 3840000), (1, 32000000)])
def test_maximum_gaussian_draw_uses_exact_open_interval(sigma, cut):
    from decimal import Decimal, localcontext, ROUND_HALF_EVEN
    from darpa_spillserver.shift import as241_decimal

    k = 2**53 - 1
    with localcontext() as context:
        context.prec = 80
        uniform = (Decimal(k) + Decimal('0.5')) / Decimal(2**53)
        exact = as241_decimal(uniform) * sigma
        expected = (None if abs(exact) > cut else
                    int(exact.quantize(Decimal(1), rounding=ROUND_HALF_EVEN)))
    assert _gauss_draw(k, sigma, cut) == expected
