# Getting started

This walks from a bare checkout to a running server answering queries.

## 1. Get a Python 3.9+ interpreter

The server needs Python 3.9 or newer. The NOvA gateway nodes ship **Python
3.6**, which is too old, and installing a system package needs root:

```console
$ python3 -V
Python 3.6.8
```

`bootstrap.sh` looks for a newer interpreter automatically, including under
`~/.local/opt`. Pick whichever of these suits your access.

### Option A — a private interpreter, no root (recommended)

A relocatable CPython build unpacks into your home directory and needs nothing
from the system:

```console
$ mkdir -p ~/.local/opt && cd /tmp
$ curl -sLO https://github.com/astral-sh/python-build-standalone/releases/download/20260901/cpython-3.12.14%2B20260901-x86_64-unknown-linux-gnu-install_only.tar.gz
$ tar xzf cpython-3.12.14+20260901-x86_64-unknown-linux-gnu-install_only.tar.gz -C ~/.local/opt
$ mv ~/.local/opt/python ~/.local/opt/python3.12
$ ~/.local/opt/python3.12/bin/python3 -V
Python 3.12.14
```

`bootstrap.sh` finds this path without being told.

### Option B — the system package, with root

```console
$ sudo dnf install python3.12
$ PYTHON=python3.12 ./bootstrap.sh
```

### Option C — an interpreter you already have

```console
$ PYTHON=/path/to/python3.11 ./bootstrap.sh
```

## 2. Bootstrap

```console
$ cd DARPA-SpillServer
$ ./bootstrap.sh
>> Using interpreter: Python 3.12.14 (/home/novadaq/.local/opt/python3.12/bin/python3)
>> Creating virtual environment in .../venv
>> Installing nova-time-decoder from sibling checkout: .../nova-time-decoder
>> Installing darpa-spillserver (editable, extras: test)
>> Done.
$ source venv/bin/activate
```

`nova-time-decoder` is a sibling NOvA DAQ package, not a PyPI release. If a
checkout sits next to this one, `bootstrap.sh` installs from it in editable
mode. Otherwise clone it first:

```console
$ git clone https://github.com/normanajn/nova-time-decoder.git ../nova-time-decoder
```

## 3. Check the pieces

```console
$ darpa-spill-server --version
darpa-spill-server 1.0.0
Python 3.12.14
nova-time-decoder 1.2.0
fastapi 0.141.1

$ python -m pytest -q
237 passed
```

## 4. Confirm the TDU is reachable

The TDU is on the NOvA DAQ network; the gateway node is on it, your laptop is
not.

```console
$ curl -s http://tdu-near-master-ppc-01:8080/tcr_status
{
	"Type": 3,
	"Number": 10733,
	"Time": 33778458638680249,
	...
}
```

Nothing back means either you are off the DAQ network or `TCRMonitor` is not
running on the TDU — check `http://tdu-near-master-ppc-01:8080/tcr_running`.

To record from more than one TDU, list each under `tdu.sources` (see
[CONFIGURATION](CONFIGURATION.md#tdu)); `config/spillserver-near.yaml` lists
all three Near Detector master TDUs. Check each one the same way.

## 5. Run it

```console
$ darpa-spill-server -c config/spillserver.yaml
2026-09-22 11:25:46 INFO  darpa_spillserver.cli: serving on http://0.0.0.0:8080
2026-09-22 11:25:46 WARNING darpa_spillserver.cli: authentication is disabled; ...
2026-09-22 11:25:46 INFO  darpa_spillserver.poller: ingest started: polling ... every 1s
```

Then open <http://localhost:8080/> for the query page, or
<http://localhost:8080/docs> for the generated API reference.

### Expect a degraded-mode warning

Against a TDU that has not had the `/spill_history` route installed you will
see:

```
WARNING darpa_spillserver.tdu_client: TDU at http://tdu-near-master-ppc-01:8080/
has no /spill_history route; falling back to /tcr_status, which returns only the
newest event and will miss most of them.
```

This is expected and important. In that mode the TDU can only report its single
newest event per poll, while events arrive at 6–15 Hz — measured on a live TDU,
about **20% of events are captured**. Every affected response carries the same
warning in its metadata. See [`contrib/tduweb/README.md`](../contrib/tduweb/README.md)
for the fix.

## 6. Ask it something

Design.md's own examples, from the command line:

```console
# All $74 events between 1 July 2026 and today
$ curl -s 'http://localhost:8080/api/events?signal=$74&start=Jul+1,+2026&end=today'

# All $8F events between 09:15 and 11:34 today, as CSV
$ curl -s 'http://localhost:8080/api/events?signal=$8f&start=09:15&end=11:34&format=csv'
```

Or without a browser or a running server, straight from the archive:

```console
$ darpa-spill-query --signal '$74' --start 2026-07-01 --end today
$ darpa-spill-query --signal '$8f' --start 09:15 --end 11:34 --format csv -o spills.csv
$ darpa-spill-query --last
```

Quote `$74` in a shell — unquoted it expands to nothing. `0x74` and `74` also
work.

## 7. What to read next

* [CONFIGURATION.md](CONFIGURATION.md) — every setting and the four layers.
* [API.md](API.md) — endpoints, parameters, output columns.
* [DEPLOYMENT.md](DEPLOYMENT.md) — running it as a service, and enabling SSO.
* [`contrib/tduweb/README.md`](../contrib/tduweb/README.md) — the TDU-side
  changes and what each one unlocks.
