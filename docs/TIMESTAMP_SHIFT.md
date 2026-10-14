# Reversible timestamp blurring (dts1)

`darpa-spill-shift` moves every event's timestamp in a `darpa-spill-dump` file
by a small keyed random offset, and undoes the move exactly given the token
that made it. The full specification, with proofs, statistical tests and
plots on real data, is the Mathematica notebook
[`TimestampShift.nb`](TimestampShift.nb). This page summarises the design,
the decisions behind it, how to use the tool, and how it was verified.

## Requirements

1. Start from `darpa-spill-dump` output (CSV or JSON).
2. Shift each timestamp by a random amount, positive or negative.
3. Offsets follow a chosen variate: flat or Gaussian.
4. Offsets derive from a token or seed, generated or supplied by the user.
5. Invertible knowing only the token and the shifted data point.
6. The blur is user-set and small: never beyond ±500 ms.

## The transform

Each hardware event, identified by `(source, event_number)`, gets an offset
`o(n)` from HMAC-SHA256 keyed with the token's secret. The transform never
changes those two columns, so a shifted row still says which offset was
applied to it.

| Column | Shifted | Restored |
|---|---|---|
| `nova_time` | `t + o(n)` | `t' − o(n)` |
| `pps_offset` | `(p + o(n)) mod 1 s` | `(p' − o(n)) mod 1 s` |
| `delta` | `Δ + o(n) − o(n−1)` | `Δ' − o(n) + o(n−1)` |
| `utc`, `gps`, `unix_*`, ... | recomputed from `t'` | recomputed from `t` |
| everything else | unchanged | unchanged |

**Variates.**
- **Flat** with half-width A: the first 128 bits of the HMAC, taken
  `mod (2A+1) − A`. This uses integer arithmetic only.
- **Gaussian** with σ and cut C: a 53-bit uniform `u`, then `x` = AS241(u),
  rejected while `|x|σ > C`, and the offset is `round(xσ)` in ticks.

All parameters are whole 64 MHz ticks, so shifted times stay integers.

**Token.** `dts1:flat:a=A:<64 hex>` or `dts1:gauss:sigma=S:cut=C:<64 hex>`.
The token is self-contained: it is the only thing inversion needs.

## Decisions and why

| Decision | Reason |
|---|---|
| Key the offset on `(source, event_number)` | It's unchanged by the transform, so a single shifted row plus the token is enough to invert. Ordering, neighbours and other files aren't needed. Over 1,052,663 real rows the pair never repeats. |
| Transform `pps_offset` and `delta` instead of keeping them | `pps_offset` equals `nova_time mod 1 s` on every row, and `delta` is the exact gap to hardware event `n−1`. Keeping either would let anyone recover the true times. |
| Transform `delta` algebraically, using `o(n−1)` even if event `n−1` isn't in the file | 19% of `ppc-01` predecessors were never archived, so `delta` can't be recomputed from neighbouring rows. The algebraic form inverts exactly regardless, and equals the gap between shifted times whenever the predecessor is present. |
| Put the parameters in the token | One object to keep; nothing in the data file needs to be trusted for inversion. |
| Truncate the Gaussian by rejection, at a cut (default 3σ, ≤ 500 ms) | Every offset is guaranteed within the bound, and no `erf` is needed. |
| HMAC-SHA256 as the pseudorandom function | Standard and in the Python stdlib. Without the secret, offsets can't be predicted even given many others. |
| Exact decimal fallback near rounding ties | A float evaluation can round to the wrong tick when it lands on .5. One reference vector does exactly that. The fallback makes every offset identical on every machine. |
| Public fingerprint in shifted files | Inverting with the wrong token gives wrong times silently; the fingerprint turns that into an error. |
| Shifted files marked; restored files unmarked | `invert(apply(f))` is byte-identical to `f`, and shifting twice is refused. |
| Token not read from YAML config | It is a secret. Command line, token file (mode 0600) or environment only. |
| Python only | Matches the rest of the package. No new dependencies. |

## What the blur does and does not hide

- **Hidden:** each event's time, to within the chosen distribution; its
  sub-second phase; the exact spacing between events.
- **Not hidden:** event order (`event_number` must remain), counts, types and
  sources.
- **Predictable signals:** events whose timing follows a known pattern can be
  snapped back to their true times. The 1 Hz pulser fires 22 µs after each
  second, so rounding to the second recovers it.
- **Seeds:** a seed-derived token is only as strong as the seed.

Successive events start to come out of time order once σ approaches their
spacing. In the sample: 0.06% of successive events swap at σ = 10 ms, 0.6% at
20 ms, and 9% at 50 ms. Sort a shifted file by `event_number`, not by time.

## Usage

```console
# A Gaussian token, sigma 20 ms, cut at 3 sigma = 60 ms, saved privately
$ darpa-spill-shift token --dist gauss --sigma 20ms -o ~/nd-blur.tok

# Shift (writes day.shifted.csv, day.shifted.json)
$ darpa-spill-shift apply --token-file ~/nd-blur.tok day.csv day.json

# Restore (writes day.restored.csv, byte-identical to day.csv)
$ darpa-spill-shift invert --token-file ~/nd-blur.tok day.shifted.csv

# New flat ±50 ms token and a week of split files, in one step
$ darpa-spill-shift apply --dist flat --width 50ms --token-out week.tok week/nd_*.csv

# Which token does this file need?
$ darpa-spill-shift info day.shifted.csv --token-file ~/nd-blur.tok
```

See `man -l man/darpa-spill-shift.1` for every option, and
[COMMAND_LINE.md](COMMAND_LINE.md#11-blurring-timestamps-reversibly) for the
tool in context.

## Verification

| Check | Where | Result |
|---|---|---|
| HMAC-SHA256 against RFC 4231 test case 2 | notebook §5.2, `test_shift.py` | identical |
| PBKDF2 against Python `hashlib` | notebook §12, `test_shift.py` | identical |
| AS241 against the exact inverse normal CDF | notebook §7.1 | max relative error 7.4 × 10⁻¹⁷ |
| Flat uniformity, 20,000 offsets | notebook §6 | Pearson χ² p = 0.52 |
| Gaussian vs truncated normal, 20,000 offsets | notebook §7.2 | KS D = 0.0062, p = 0.43; σ 1.2610 × 10⁶ vs 1.2628 × 10⁶ ticks expected |
| Autocorrelation, 40 lags; one-bit key change | notebook §10 | within ±2/√N; correlation 0.012 (N = 3000) |
| Round trip, 4,701 real rows × 2 tokens, all columns | notebook §9 | exact |
| `delta'`, `pps_offset'` consistent with shifted times | notebook §9, `test_shift.py` | 4,089/4,089 and 4,701/4,701 |
| Python vs Mathematica: 40 edge-case offsets, 6 near ties, 500 rows × 2 tokens | `test_shift.py` | all identical |
| Near tie where float rounding alone is wrong | `test_shift.py` | caught by the decimal fallback |
| `darpa-spill-dump` CSV and JSON round trip | `test_shift.py`, 56,343 gateway rows | byte-identical |
| Refusals: double shift, wrong token, unmarked file, overwrite of the input, missing columns | `test_shift.py` | all refused, no partial output |

Throughput is about 22 µs per row, roughly 25 s per format for a day of data.

## Rebuilding the notebook

The notebook is generated, not hand-edited:

```console
$ wolframscript -file docs/shift/build_notebook.wl
```

This evaluates `docs/shift/dts1.wl` against
`docs/shift/sample_2026-10-12T0000_10min.csv`, writes
`docs/TimestampShift.nb`, and regenerates
`docs/shift/dts1_test_vectors.json`. It takes about 2.5 minutes with
Mathematica 12.0. It prints `__DONE__` when finished; `wolframscript` may not
exit on its own afterwards.

The secret in the test tokens is the SHA-256 of a public string. Never use
those tokens for real data.
