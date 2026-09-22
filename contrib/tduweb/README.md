# Upstream changes needed on the TDU

> **The deployable copy lives elsewhere.** `tdu_webserver.py`, `selftest.py`
> and the two compatibility checkers in this directory are mirrors of
> `server/` on the [`Darpa-Modifications`](https://github.com/NovaDAQ/TDUWeb/tree/Darpa-Modifications)
> branch of TDUWeb, which is authoritative and is what gets installed. They are
> duplicated here so this repository explains, in one place, what the TDU needs
> in order to serve a real event history. Change them there, then re-sync.

This directory holds the changes to **other** NOvA DAQ packages that the DARPA
Spill Information Server depends on. Nothing here is installed by
`bootstrap.sh`; each change is deployed deliberately, by hand, after review.

They are kept in this repository rather than committed straight to `TDUWeb`,
`SHM_Utilities` and `NovaSpillServer` because they exist to serve this project
and should be reviewed alongside it. Once accepted, they belong upstream.

---

## Why any of this is necessary

The server needs a history of accelerator events. The TDU cannot currently
supply one.

### The existing routes return a single event

`TDUWeb/server/tdu_webserver.py` exposes two routes that report event data,
`/tcr_status` and `/onehz_status`. Both run `DumpSpillHistory --last`, which
returns exactly one record.

They also return the **same** record. In `SHM_Utilities/cxx/src/DumpSpillHistory.cc`
the `--last` branch indexes `data_entries - 1` directly and never consults the
`--tcr` / `--onehertz` / `--numi` flags:

```c
if(last_flag){
    AccelData = (AccelEventData*)(shmptr_start)+2 + (*DataMap).data_entries - 1 ;
    ...
    print_last_event(AccelData, AccelData_previous, json_flag, start_flag);
    return 1;
};
```

Verify on a live TDU — the two routes answer identically:

```console
$ curl -s http://tdu-near-master-ppc-01:8080/tcr_status   | grep Number
	"Number": 10795,
$ curl -s http://tdu-near-master-ppc-01:8080/onehz_status | grep Number
	"Number": 10795,
```

### Events arrive far faster than polling can follow

Sampling `/tcr_status` once a second and recording the `Number` field shows how
much is lost. Measured on `tdu-near-master-ppc-01`, 2026-09-22:

```
captured event numbers: 20230, 20235, 20240, 20245, 20250, 20255, 20260, ...
span: 105 hardware events -> 21 captured (20%)
```

Four events in five are never seen, and no polite polling rate fixes that: the
events are produced at roughly 6–15 Hz while each poll can retrieve one. This
is a protocol limitation, not a tuning problem.

### The raw event word is not preserved

`TCRMonitor` decodes the 16-bit event word into a `SpillType` and stores only
that. The mapping is many-to-one, so `$1D` cannot be told from `$1F`, nor `$A9`
from `$AD`, once a record is in shared memory. The server therefore leaves the
`signal` column empty for such records rather than inventing one of the two.

### Only the TCR reference signal is recorded at all

In `NovaSpillServer/cxx/src/TCRMonitor.cc` the `memcpy` into shared memory sits
inside `if (evt == 0x041B)`. Every other signal — including NuMI `$74` and the
1 Hz `$8F` that Design.md names explicitly — is decoded and then discarded.

---

## The changes, in order of risk

### 1. `tdu_webserver.py` — add `/spill_history` (low risk, recommended)

Drop-in replacement for `TDUWeb/server/tdu_webserver.py`. Every existing route
keeps its behaviour; one route is added.

* **Risk:** low. Additive; no shared-memory change; no DAQ process touched.
* **Fixes:** the one-event-per-poll limit. The archive becomes complete for
  whatever signals shared memory currently holds.
* **Does not fix:** the missing signals (change 3) or the missing raw event
  word (change 4).

It works against an **unpatched** `DumpSpillHistory` by parsing that tool's CSV
output, avoiding the malformed JSON described below.

It targets the TDU's stock **Python 2.5**, which rules out more than it first
appears — `except X as e`, `str.format`, the `json` module and the `bytes`
builtin are all 2.6 or later. It therefore carries its own small JSON encoder
rather than using `bottle.json_dumps`, which is a stub that raises
`ImportError` when neither `simplejson` nor `json` is installed, as on the TDU.

Two checkers guard that floor, neither needing a 2.5 interpreter:

```console
$ python2 check_python25.py tdu_webserver.py selftest.py
$ TDU_WEB_NO_SERVE=1 python2 run_as_python25.py selftest.py
```

The first scans source for 2.6+ syntax and names. The second runs the code with
the 2.6+ builtins hidden, which is what catches a name like `bytes` that exists
on a development machine's 2.7 and not on the TDU. Running `selftest.py` needs
`bottle.py` on the path, so from this directory pass
`PYTHONPATH=/path/to/TDUWeb/server`.

```console
# On the TDU, as root:
cd /path/to/TDUWeb/server
cp tdu_webserver.py tdu_webserver.py.orig-$(date +%Y%m%d)
cp /path/from/here/tdu_webserver.py tdu_webserver.py
# restart however the service is managed, then:
curl -s 'http://localhost:8080/spill_history?limit=5' | head
curl -s 'http://localhost:8080/tcr_status'            # must be unchanged
```

To roll back, restore the `.orig-` copy and restart.

The server detects the new route automatically — no configuration change. Watch
for the warning to stop appearing:

```
TDU at ... has no /spill_history route; falling back to /tcr_status
```

### 2. `0001-DumpSpillHistory-json-and-time-window.patch` (low risk, optional)

Fixes `DumpSpillHistory`'s bulk JSON, which is not valid JSON today:

```c
printf("{\nEvents: [\n");   /* `Events` is unquoted */
```

and adds `--since` / `--until` so the utility filters by NOvA time itself
instead of dumping the whole ring for the web layer to filter.

* **Risk:** low. Output-only; no shared-memory change. `--json` output changes
  shape, so check for other consumers of it first.
* **Benefit:** `/spill_history` gets faster on a full ring. Not required —
  the server and the new route both cope without it.

### 3. Record every signal, not just `$041B` (medium risk — needs review)

One-line change in `TCRMonitor.cc`: move the shared-memory write out of the
`if (evt == 0x041B)` block so every decoded event is stored.

* **Risk:** medium. `TCRMonitor` is a running DAQ process. Storing every signal
  rather than one raises the write rate into the ring by roughly the ratio of
  all events to TCR references, so **the ring buffer will wrap much sooner**
  unless its size is raised to match. Size it for the retention the ingest
  poller needs, with margin for the poller being briefly down.
* **Benefit:** `$74` and `$8F` become available at all. Without this, the
  archive can never answer the query Design.md gives as its main example.

This one is left as a described change rather than a patch file, because the
right ring size is a deployment decision and the change should be made and
staged by someone who can watch the DAQ while it runs.

### 4. Preserve the raw event word (higher risk — design decision needed)

To distinguish `$1D` from `$1F`, the 16-bit word must reach shared memory. The
obvious approach breaks compatibility:

```c
struct AccelEventData {
  uint32_t evt_type;
  uint32_t evt_no;
  uint64_t time;
};
```

Adding a field changes `sizeof`, so every reader and writer of the segment must
be rebuilt and restarted together. Packing the word into the spare upper bits
of `evt_type` avoids the size change but breaks old readers differently: their
`switch ((*AccelData).evt_type)` would stop matching and silently skip events.

A third option looks cleaner. `DumpSpillHistory` and `TCRMonitor` both locate
the event array at `(AccelEventData*)(DataMap + 2)` — two whole
`SHMSpillDataMap` structures past the start, about 64 bytes on x86-64, of which
only ~28 are used. That leaves room for a layout version and a flags word in
the header, letting a new writer advertise an extended record while old readers
continue to read the old one.

**This needs a decision from whoever owns the shared-memory contract**, and is
not attempted here. Until it is made, records carry the decoded type only, and
the server reports that honestly: the `signal` column is empty, and any query
whose answer is affected carries a warning naming the signals that cannot be
told apart.

---

## What each change unlocks

| Capability | ships today | +1 `/spill_history` | +3 all signals | +4 raw word |
|---|:--:|:--:|:--:|:--:|
| Query TCR references by time range | partial | yes | yes | yes |
| Complete history, no dropped events | no | yes | yes | yes |
| Query `$74` NuMI spills | no | no | yes | yes |
| Query `$8F` 1 Hz events | no | no | yes | yes |
| Tell `$1D` from `$1F` | no | no | no | yes |

Changes 1 and 2 are safe to deploy on their own and are worth doing first.
