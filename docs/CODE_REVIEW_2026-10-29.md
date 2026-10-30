# DARPA repository review — 2026-10-29

## Scope

GitHub inventory and local remotes identified one matching repository: `NovaDAQ/DARPA-SpillServer`. The local `DARPA-SpillServer-conformance`, `DARPA-SpillServer-doxygen`, and `DARPA-SpillServer-gaussfix` directories are existing worktrees of that repository, not additional repositories. No additional organization or repository was supplied during the review.

Review baseline: `origin/main`, commit `7cc263f`. The existing conformance PR and other feature branches were not included in the default-branch review. OIDC implementation, configuration, and integration are deferred at the owner's request.

Four fresh worktrees were created on `review/core-20261029`, `review/web-20261029`, `review/deploy-20261029`, and `review/integration-20261029`. Agent spawning was attempted twice but returned `unsupported call`; the review proceeded with one agent and independent test commands in parallel. Existing worktrees and untracked data were not used for edits.

## Coverage

| Area | Review coverage |
| --- | --- |
| Ingestion and storage | Poll lifecycle, error recovery, retention, watermarks, source overrides, SQLite migrations, queries, deduplication, TDU response decoding and capability probing |
| API, CLI and UI | Event/latest/export routes, selection, pagination, CSV/JSON serialization, time parsing, runtime admin-token controls, app lifecycle, query and configuration templates |
| Data tooling | Local/remote dump readers, adaptive windows, atomic sinks, split intervals and DST labels; backfill probes; shift tokens, draws, row transformations and file handling |
| Publisher | Configuration, state bookkeeping, ingest gate, dump/shift/upload orchestration, checksum verification, locking, retries, status reporting and deployment integration; OIDC deferred |
| Deployment and recovery | Bootstrap/start/stop, gateway install/cron/watchdog/logrotate, log recovery and withdrawal tools, TDU mirror and companion patch inspection |
| Supporting material | Package metadata, existing tests, configuration examples and relevant API/configuration/deployment/time-shift specifications |

The TDU mirror identifies `NovaDAQ/TDUWeb` on `Darpa-Modifications` as authoritative. No hardware, remote deployment, live FDP upload/removal, credential refresh, or email alert was exercised. Python 2.5 hardware execution and Mathematica notebook regeneration were not available in this validation; these are not claimed as tested.

## Findings and fixes

17 actionable issues were filed, including the pre-existing test failure. All 17 are implemented in the draft PR. “Implemented” below means code and regression coverage are in this PR, not deployed or merged.

| Issue | Finding | Fix status |
| --- | --- | --- |
| [#2](https://github.com/NovaDAQ/DARPA-SpillServer/issues/2) | [P2] Latest-event queries discard exact signal and multi-selection filters | Implemented |
| [#3](https://github.com/NovaDAQ/DARPA-SpillServer/issues/3) | [P2] Local query and dump omit explicitly requested types when combined with a signal | Implemented |
| [#4](https://github.com/NovaDAQ/DARPA-SpillServer/issues/4) | [P2] CSV alias loses repeated filters and corrupts encoded query values | Implemented |
| [#5](https://github.com/NovaDAQ/DARPA-SpillServer/issues/5) | [P2] Export ceiling silently produces a successful incomplete dataset | Implemented |
| [#6](https://github.com/NovaDAQ/DARPA-SpillServer/issues/6) | [P2] Parsing exact GPS strings loses NOvA ticks through float conversion | Implemented |
| [#7](https://github.com/NovaDAQ/DARPA-SpillServer/issues/7) | [P2] Maximum Gaussian draw rounds the uniform to one and crashes shifting | Fixed on main by 98246cf (exact upper half); this PR keeps that implementation and adds a regression |
| [#8](https://github.com/NovaDAQ/DARPA-SpillServer/issues/8) | [P2] Automatic backfill treats a recent or gapped ring as empty | Implemented |
| [#9](https://github.com/NovaDAQ/DARPA-SpillServer/issues/9) | [P1] A transient retention failure kills the poller while health still says running | Implemented |
| [#10](https://github.com/NovaDAQ/DARPA-SpillServer/issues/10) | [P2] Split dumps omit completed quiet hours after the last stored event | Implemented |
| [#11](https://github.com/NovaDAQ/DARPA-SpillServer/issues/11) | [P1] Gateway installer skips installing cron jobs when the existing crontab is empty | Implemented |
| [#12](https://github.com/NovaDAQ/DARPA-SpillServer/issues/12) | [P2] Browser query table rounds 64-bit NOvA timestamps | Implemented |
| [#13](https://github.com/NovaDAQ/DARPA-SpillServer/issues/13) | [P2] Backfill accepts zero or negative windows and cannot advance | Implemented |
| [#14](https://github.com/NovaDAQ/DARPA-SpillServer/issues/14) | [P2] Streaming shifter commits truncated JSON as a successful output | Implemented |
| [#15](https://github.com/NovaDAQ/DARPA-SpillServer/issues/15) | [P2] Log backfill computes delta from timestamp order instead of hardware event order | Implemented |
| [#16](https://github.com/NovaDAQ/DARPA-SpillServer/issues/16) | [P1] Withdrawal script does not hold publisher locks while deleting files and state | Implemented |
| [#17](https://github.com/NovaDAQ/DARPA-SpillServer/issues/17) | [P2] Default retention settings leave ingest_log growing without bound | Implemented |
| [#18](https://github.com/NovaDAQ/DARPA-SpillServer/issues/18) | [P2] Remote/local dump comparison reads unflushed temporary files | Implemented |

## Validation

- Isolated Python 3.12.1 virtual environment, with the sibling `nova-time-decoder` 1.2.0 installed as documented. Runtime dependencies were resolved from the package metadata.
- Baseline: **537 passed, 1 failed**. The failure reads unflushed `.part` files; issue #18 now compares committed outputs.
- Core focused suite: **121 passed**. API focused suite: **57 passed**. Cron merge: **2 passed**. Withdrawal locking: **3 passed**.
- Initial integrated suite: **557 passed**, 2 dependency warnings (81.43 seconds).
- Final integrated suite after all remaining fixes: **605 passed**, 2 dependency warnings (87.38 seconds), including 48 new remaining-finding regressions. `git diff --check` passes. Publisher subprocess tests share the simulated publisher clock for deterministic future/DST cases.
- Cron tests execute the actual merge program against temporary crontabs; withdrawal tests use separate local processes to verify lock exclusion and release. No real cron jobs or FDP data were changed.
- Remaining warnings originate in dependency deprecations (Starlette HTTPX, FastAPI root_path, and pytest-asyncio fixture-loop defaults).

## Remaining-finding implementation

- #2/#3: shared selection resolution unions explicit types with exact signals and legacy unknown-code rows. Latest selection honors all CLI filters and ranges; remote `--last` retains its JSON envelope and supports CSV. The latest API avoids a whole-archive count.
- #5: exports above the configured ceiling fail before streaming. Concurrent growth beyond the ceiling fails the stream rather than completing a partial export.
- #6/#12: decimal GPS parsing rounds directly to ticks; API JSON includes `nova_time_exact` for browser rendering without integer precision loss. CSV columns are unchanged; shifting updates the optional exact field.
- #8: bounded, contiguous five-minute discovery windows preserve recent and gapped ring history. The default 30-day lookback can cost 8,640 requests; explicit `--start` bypasses discovery and `--lookback` controls its extent. Network failures abort discovery.
- #10: historical quiet split intervals produce empty files; future intervals are omitted. Empty files do not establish ingest coverage; the publisher retains its separate coverage gate.
- #14: streamed JSON must close its events array and document with valid separators and a consistent optional summary before the output is committed. Invalid input leaves existing output untouched.
- #15: log-derived deltas use the unique hardware predecessor, including rows outside the selected type/range. Missing or ambiguous numbering leaves delta unknown.

OIDC remains deferred. GitHub issues remain open until the draft PR is merged; implementation here is not a deployment claim.

## Merge with main (2026-10-30)

`main` advanced 18 commits after the review baseline (Doxygen pass, 98246cf exact upper-half Gaussian draw, 41e7c3d broken-pipe exit). Merging it into this branch conflicted in 13 files (21 regions). Resolution policy:

- Code: this branch's fixes are kept; `main`'s Doxygen blocks are carried over and reworded where the fix changed the documented behaviour (`_probe_earliest`, `_gps_string_to_nova`, `_maybe_prune`, `_query_server`, `_shift_json_stream`, `api._resolve_selection`, `signals.resolve_selection`).
- `_gauss_draw` (#7): `main`'s implementation is kept. It evaluates the upper half as `-x(1-u)` with exact `1-u`, which removes both the `log(0)` endpoint and the far-tail precision loss for k >= 2^52; this branch's endpoint-only fallback is dropped. The branch's k = 2^53 - 1 regression test is retained and passes against `main`'s code.
- Tests: `main`'s docstrings plus this branch's assertions and renamed tests.

Merged suite: **615 passed**, 2 dependency deprecation warnings (90.26 s), Python 3.12.1.
