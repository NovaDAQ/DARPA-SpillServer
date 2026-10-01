# Client libraries and the `darpa-spill-client` command line

The server's HTTP API (`docs/API.md`) is reachable from three client surfaces,
all of which share the configuration keys, the command set and the output
rules defined here:

| Surface | Language | Location | Installed as |
|---|---|---|---|
| `darpa_spillserver.client` | Python 3.9+ | `src/darpa_spillserver/client.py` | part of the `darpa-spillserver` package |
| `darpa-spill-client` | Python CLI | `src/darpa_spillserver/client_cli.py` | pip console script |
| `libdarpa_spill_client` | C++17 with a C ABI | `src/cpp/`, `src/include/darpa_spill/` | CMake target `DarpaSpill::client` |
| `darpa-spill-client-cpp` | C++ CLI | `src/cpp/cli/` | CMake install to `bin/` |

The two command-line programs are interchangeable: the same invocation gives
byte-identical standard output from either one, and the test suite checks
that (`tests/test_client_equivalence.py`). They have different names only
because both can be installed on the same `PATH`.

**Why two implementations rather than Python bindings over the C++ library.**
The Python client uses only the standard library (plus PyYAML for the
configuration file), so it runs on a gateway node's Python 3.9 with no
compiler and no Boost. The C++ library gives C and C++ programs, such as DAQ
components, the same access without embedding Python. Keeping both small and
driven by one written contract (this file), with a test that compares their
output, costs less than maintaining a binding layer.

## Configuration

Settings are resolved in this order, highest first:

1. command-line options,
2. environment variables,
3. a `.env` file,
4. the YAML configuration file,
5. built-in defaults.

### YAML file

The file holds one `client:` section. Unknown keys are an error.

```yaml
client:
  url: http://localhost:8080      # server base URL, http:// or https://
  timeout: 30.0                   # seconds, per request
  admin_token: ""                 # prefer admin_token_file
  admin_token_file: ""            # file whose first line is the admin token
  ca_file: ""                     # PEM bundle for verifying an https server
  verify_tls: true                # false accepts any certificate
  format: table                   # table | json | csv
  timezone: ""                    # sent as tz= on queries when not empty
```

It is found at `--config FILE`, or `$DARPA_SPILL_CLIENT_CONFIG`, or otherwise
the first of these that exists:

1. `./config/spillclient.yaml`
2. `~/.config/darpa-spillserver/spillclient.yaml`
3. `/etc/darpa-spillserver/spillclient.yaml` (on Windows,
   `%PROGRAMDATA%\darpa-spillserver\spillclient.yaml`)

### Environment and `.env`

Each key has a variable named `DARPA_SPILL_CLIENT_<KEY>`, for example
`DARPA_SPILL_CLIENT_URL` or `DARPA_SPILL_CLIENT_VERIFY_TLS`. Booleans accept
`1/0`, `true/false`, `yes/no` and `on/off`, in any case.

The `.env` file is `--env-file FILE`, or `$DARPA_SPILL_ENV_FILE`, or `./.env`
if it exists. It holds `KEY=VALUE` lines. Blank lines and lines starting with
`#` are skipped, a leading `export ` is ignored, and a value wrapped in single
or double quotes has them removed. Only `DARPA_SPILL_` variables are used from
it. A variable that is also set in the real environment keeps the
environment's value.

## Command line

```
darpa-spill-client [GLOBAL OPTIONS] COMMAND [COMMAND OPTIONS]
```

### Global options

| Option | Key | Meaning |
|---|---|---|
| `-c, --config FILE` | | YAML configuration file |
| `--env-file FILE` | | `.env` file to read |
| `-u, --url URL` | `url` | server base URL |
| `-t, --timeout SECONDS` | `timeout` | per-request timeout |
| `--admin-token TOKEN` | `admin_token` | admin token for source changes |
| `--admin-token-file FILE` | `admin_token_file` | file holding the admin token |
| `--ca-file FILE` | `ca_file` | CA bundle for https |
| `-k, --insecure` | `verify_tls: false` | skip certificate verification |
| `-f, --format FMT` | `format` | `table`, `json` or `csv` |
| `--tz ZONE` | `timezone` | zone for time inputs that carry none |
| `--print-config` | | print the merged configuration as YAML and exit |
| `-V, --version` | | print the version and exit |
| `-h, --help` | | print usage and exit |

Global options come before the command.

### Commands

| Command | Request | Notes |
|---|---|---|
| `health` | `GET /api/health` | |
| `status` | `GET /api/status` | |
| `sources` | `GET /api/sources` | |
| `signals` | `GET /api/signals` | |
| `types` | `GET /api/types` | |
| `convert TIME` | `GET /api/time/convert?t=TIME` | adds `tz=` from `--tz` |
| `time-help` | `GET /api/time/help` | |
| `latest [--signal SIG] [--source NAME]...` | `GET /api/latest` | |
| `events [SELECTION] [--limit N] [--offset N] [--desc] [--columns LIST]` | `GET /api/events` | |
| `export [SELECTION] [--columns LIST] [-o FILE]` | `GET /api/export` | streamed |
| `admin-check` | `GET /api/admin/check` | needs the admin token |
| `source enable NAME` | `PATCH /api/sources/NAME` `{"enabled": true}` | needs the admin token |
| `source disable NAME` | `PATCH /api/sources/NAME` `{"enabled": false}` | needs the admin token |
| `source set-url NAME URL` | `PATCH /api/sources/NAME` `{"base_url": URL}` | needs the admin token |
| `source reset NAME` | `POST /api/sources/NAME/reset` | needs the admin token |

`SELECTION` is any of `--start TIME`, `--end TIME`, `--signal SIG`
(repeatable), `--type TYPE` (repeatable) and `--source NAME` (repeatable).
Each maps to the query parameter of the same name, and repeated options send
the parameter repeatedly. `--desc` sends `order=desc`. `--tz` adds `tz=` to
`convert`, `events` and `export`. The admin token goes in the `X-Admin-Token`
header, and only when one is configured.

Query parameters are sent in this order, and only when given: `start`, `end`,
`signal`…, `type`…, `source`…, `format`, `limit`, `offset`, `order`, `tz`,
`columns`. Values are percent-encoded per RFC 3986: only `A-Z a-z 0-9 - . _ ~`
are left as they are, so `$74` goes on the wire as `%2474`.

### Output

`--format json` writes the response body exactly as the server sent it,
followed by a newline if the body does not already end in one.

`--format csv`:

* `events` and `export` ask the server for `format=csv` and write its body
  verbatim, including the `#` comment header.
* `sources`, `signals` and `types` are written as client-rendered CSV using
  the table columns below: comma-separated, a field quoted with `"` only if it
  contains a comma, a quote or a newline (quotes doubled), and `\n` line
  endings.
* For every other command, csv is the same as table.

`--format table` (the default):

* **Tables.** Columns are separated by two spaces and left-aligned to the
  widest cell, including the header. Trailing spaces are removed from each
  line.
  * `sources`: `name`, `base_url`, `enabled`, `overridden`, `events`
  * `signals`: `hex`, `name`, `spill_type_name`, `description`
  * `types`: `value`, `name`, `ambiguous`, `signals`
  * `events`: the client asks for `format=csv`, drops the `#` comment lines
    and lays the remaining CSV rows out as a table. If `--columns` is not
    given, it sends `columns=utc_string,gps_week,gps_tow_exact,signal,spill_type_name,event_number,source`.
    Each `# WARNING: ...` comment line is written to standard error as
    `warning: ...`.
* **Key/value.** Every other command flattens the JSON object into one
  `key: value` line per leaf, in the server's key order. Nested objects join
  their keys with `.` and list elements use their index (`sources.0.name`).
  An empty object is written `{}` and an empty list `[]`. `latest` flattens
  the `event` object, and the `source` commands flatten the `source` object.

The same cell rules apply to tables and to key/value output:

| JSON value | Written as |
|---|---|
| string | as is |
| integer | decimal |
| non-integer number | fixed-point with 6 decimals, trailing zeros then a trailing `.` removed (`printf("%.6f")`) |
| `true` / `false` | `yes` / `no` |
| `null` | `-` |
| list of scalars (in a table cell) | the cells joined with `,` |

`export -o FILE` writes the body to `FILE` instead of standard output. The
body is streamed in both implementations, so memory use does not grow with
the size of the export.

### Errors and exit status

| Status | Meaning |
|---|---|
| 0 | success |
| 1 | the server answered with an HTTP error |
| 2 | usage or configuration error |
| 3 | the server could not be reached (connection, TLS or timeout) |

On an HTTP error, standard error gets `error: HTTP <status>: <message>`,
followed by `hint: <hint>` when the server supplied one. The message and hint
are read from the server's `{"detail": {"error": ..., "hint": ...}}` body,
also accepting `{"error": ...}` and a string `detail`.

## Library interfaces

### Python

```python
from darpa_spillserver.client import SpillClient, Selection

client = SpillClient("http://novadaq-near-gateway-01.fnal.gov:8080", timeout=30.0)
client.health()                                    # -> dict
page = client.events(Selection(start="today", signals=["$74"]), limit=100)
print(page["meta"]["total"], len(page["events"]))
with open("numi.csv", "wb") as out:
    client.export(Selection(start="-1d", signals=["$74"]), out, fmt="csv")
```

Errors raise `SpillHTTPError` (with `.status`, `.message` and `.hint`) or
`SpillConnectionError`, both of which are `SpillClientError`s.

### C++

```cpp
#include <darpa_spill/client.hpp>

darpa::spill::ClientOptions options;
options.url = "http://novadaq-near-gateway-01.fnal.gov:8080";
darpa::spill::Client client(options);

darpa::spill::Selection sel;
sel.start = "today";
sel.signals = {"$74"};
boost::json::value page = client.events(sel, darpa::spill::Paging{100});
client.export_to(sel, std::cout, darpa::spill::Format::csv);
```

Errors throw `darpa::spill::HttpError` (with `status()`, `message()` and
`hint()`) or `darpa::spill::ConnectionError`, both derived from
`darpa::spill::Error`, which is derived from `std::runtime_error`.

### C

```c
#include <darpa_spill/client.h>

dsc_client *c = dsc_client_new("http://localhost:8080", 30.0);
char *body = NULL;
long status = 0;
if (dsc_get(c, "/api/latest?signal=%2474", &body, &status) == DSC_OK) {
    puts(body);
    dsc_string_free(body);
} else {
    fprintf(stderr, "%s\n", dsc_last_error(c));
}
dsc_client_free(c);
```

The C++ and C interfaces are documented in `darpa_spill_client(3)`, and the
Python module in `darpa_spillserver(3)`.
