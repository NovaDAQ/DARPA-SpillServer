# C/C++ client library

`libdarpa_spill_client` is a C++17 client for the server's HTTP API, with a C
ABI for programs that are not C++. `darpa-spill-client-cpp` is its command
line, a drop-in twin of the Python `darpa-spill-client`. Both follow
[`docs/CLIENT.md`](CLIENT.md), which fixes the commands, configuration and
output byte for byte.

| Piece | Source | Installed as |
|---|---|---|
| C++ API | `src/include/darpa_spill/client.hpp`, `config.hpp`, `format.hpp` | `include/darpa_spill/` |
| C API | `src/include/darpa_spill/client.h` | `include/darpa_spill/` |
| Library | `src/cpp/*.cpp` | `lib/libdarpa_spill_client.{so,dylib,dll}` |
| Command line | `src/cpp/cli/main.cpp` | `bin/darpa-spill-client-cpp` |
| C example | `examples/c/latest.c` | built only (`example_c_latest`) |
| Unit tests | `tests/cpp/` (CppUnit) | not installed |
| Integration tests | `tests/test_cpp_client.py` (pytest, live server) | not installed |
| Man pages | `man/darpa-spill-client-cpp.1`, `man/darpa_spill_client.3` | `share/man/man{1,3}` |
| CMake package | `cmake/DarpaSpillClientConfig.cmake.in` | `lib/cmake/DarpaSpillClient/` |
| pkg-config | `cmake/darpa_spill_client.pc.in` | `lib/pkgconfig/darpa_spill_client.pc` |

## Design

- **Transport.** The client uses Boost.Beast over Boost.Asio. Each call makes
  one connection and sends one request, with `Connection: close`. That keeps
  the client stateless, and so a `const Client` is safe to share between
  threads. Operations run asynchronously on a private `io_context` that is
  driven to completion. The reason is timeouts: `beast::tcp_stream` applies
  its expiry only to asynchronous operations, so each resolve, connect, TLS
  handshake, write and read gets its own `timeout` without a helper thread.
- **Streaming.** The server sends query results with chunked transfer
  encoding. `http::response_parser<buffer_body>` decodes the chunks into a
  fixed 16 KiB buffer, so `export_to` copies an export of any size to its
  `std::ostream` in constant memory. The parser's body limit is raised from
  Beast's default of 8 MB to unlimited.
- **TLS.** TLS support is optional (`DARPA_SPILL_WITH_TLS`, which is on when
  OpenSSL is found). It sets SNI and verifies the peer against the host name.
  With no `ca_file`, the default store is OpenSSL's
  (`set_default_verify_paths`). On Windows that is not the Windows
  certificate store, so pass `--ca-file`.
- **Dependencies.** Boost.JSON appears in the public headers, because parsed
  responses are `boost::json::value`, so consumers link it too. yaml-cpp,
  Boost.Program_options (used only by the CLI) and OpenSSL are private.
  Boost.URL is not used, so the minimum is **Boost 1.75**, which EL9 ships.
- **Symbols.** The library is built with hidden visibility, and
  `GenerateExportHeader` provides `DARPA_SPILL_EXPORT`, so the same headers
  work for a Linux `.so`, a macOS `.dylib` and a Windows `.dll`. A static
  build (`-DBUILD_SHARED_LIBS=OFF`) defines `DARPA_SPILL_STATIC_DEFINE` for
  its consumers.

## Dependencies

| | Minimum | Linux (dnf / apt) | macOS (Homebrew) | Windows 11 (vcpkg) |
|---|---|---|---|---|
| CMake | 3.16 (presets need 3.21) | `cmake` | `cmake` | Visual Studio 2022 or `cmake` |
| C++17 compiler | GCC 8, Clang 7, MSVC 19.20 | `gcc-c++` / `g++` | Xcode command line tools | Visual Studio 2022 |
| Boost (json, program_options, beast, asio) | 1.75 | `boost-devel` / `libboost-all-dev` | `boost` | `boost-beast boost-json boost-program-options` |
| yaml-cpp | 0.6 | `yaml-cpp-devel` / `libyaml-cpp-dev` | `yaml-cpp` | `yaml-cpp` |
| OpenSSL (optional) | 1.1.1 | `openssl-devel` / `libssl-dev` | `openssl@3` | `openssl` |
| CppUnit (tests only) | 1.13 | `cppunit-devel` / `libcppunit-dev` | `cppunit` | `cppunit` |
| pkg-config (tests only) | | `pkgconf` / `pkg-config` | `pkg-config` | not needed |

`vcpkg.json` at the top of the tree lists the Windows packages. On EL9,
`yaml-cpp-devel` and `cppunit-devel` come from EPEL. On Ubuntu, 24.04 has
Boost 1.83; 22.04's Boost 1.74 predates Boost.JSON and is too old.

## Building

### Linux

```sh
# Alma/Rocky/RHEL 9
sudo dnf install -y epel-release
sudo dnf install -y cmake gcc-c++ boost-devel yaml-cpp-devel openssl-devel cppunit-devel pkgconf

# Ubuntu 24.04 / Debian 12
sudo apt-get install -y cmake g++ libboost-all-dev libyaml-cpp-dev libssl-dev libcppunit-dev pkg-config

cmake --preset linux
cmake --build build -j
ctest --test-dir build --output-on-failure
sudo cmake --install build --prefix /usr/local
```

### macOS

```sh
brew install cmake boost yaml-cpp openssl@3 cppunit pkg-config

cmake --preset macos          # finds Homebrew's openssl@3 without OPENSSL_ROOT_DIR
cmake --build build -j
ctest --test-dir build --output-on-failure
cmake --install build --prefix "$HOME/.local"
```

### Windows 11

From a *Developer PowerShell for VS 2022*:

```powershell
git clone https://github.com/microsoft/vcpkg $env:USERPROFILE\vcpkg
& $env:USERPROFILE\vcpkg\bootstrap-vcpkg.bat
$env:VCPKG_ROOT = "$env:USERPROFILE\vcpkg"

cmake --preset windows-msvc   # vcpkg installs vcpkg.json's packages on first run
cmake --build --preset windows-msvc
ctest --preset windows-msvc
cmake --install build --config Release --prefix "$env:LOCALAPPDATA\darpa-spillserver"
```

The Visual Studio generator puts binaries in `build\Release\`. The pytest
integration tests search `build\*\` as well, so they find them there.

### Options

| Option | Default | Effect |
|---|---|---|
| `BUILD_SHARED_LIBS` | `ON` | shared or static library |
| `DARPA_SPILL_WITH_TLS` | `ON` | https support; turned off with a warning if OpenSSL is missing |
| `DARPA_SPILL_BUILD_TESTS` | `ON` | CppUnit tests; skipped with a message if CppUnit is missing |
| `DARPA_SPILL_BUILD_EXAMPLES` | `ON` | `example_c_latest` |

The presets are `default` (RelWithDebInfo into `build/`), `release`
(`build/release/`), `linux`, `macos` and `windows-msvc`. Without presets:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF
```

## Testing

```sh
ctest --test-dir build --output-on-failure        # CppUnit suites + CLI --version check
./build/test_darpa_spill_client                   # the CppUnit runner on its own

# The CLI against a real server (needs the Python environment from bootstrap.sh):
venv/bin/python -m pytest tests/test_cpp_client.py -q
venv/bin/python -m pytest tests/test_client_equivalence.py -q   # C++ vs Python, byte for byte
```

The pytest files find the binary at `$DARPA_SPILL_CPP_CLIENT`, else
`build/darpa-spill-client-cpp`, else `build/*/darpa-spill-client-cpp`, and
skip themselves when it is not built.

## Using the library

### CMake

```cmake
find_package(DarpaSpillClient 1.3 REQUIRED)
target_link_libraries(myprog PRIVATE DarpaSpill::client)
```

If it was installed outside the default search path, configure with
`-DCMAKE_PREFIX_PATH=/path/to/prefix`. The package finds Boost.JSON for you,
and for a static build yaml-cpp and OpenSSL too.

### pkg-config

```sh
c++ -std=c++17 prog.cpp $(pkg-config --cflags --libs darpa_spill_client) -o prog
cc latest.c $(pkg-config --cflags --libs darpa_spill_client) -o latest
```

The `.pc` file finds its prefix from its own location, so an install tree
can be moved. Add `-L` for Boost if it is not in a default library directory
(for example, `-L/opt/homebrew/lib` on macOS).

### C++

```cpp
#include <darpa_spill/client.hpp>
#include <iostream>

int main() {
    darpa::spill::ClientOptions options;
    options.url = "http://novadaq-near-gateway-01.fnal.gov:8080";
    darpa::spill::Client client(options);

    darpa::spill::Selection numi;
    numi.start = "today";
    numi.signals = {"$74"};
    try {
        client.export_to(numi, std::cout);              // CSV, streamed
    } catch (const darpa::spill::HttpError& e) {         // status >= 400
        std::cerr << e.what() << "\nhint: " << e.hint() << "\n";
        return 1;
    } catch (const darpa::spill::ConnectionError& e) {   // DNS, refused, TLS, timeout
        std::cerr << e.what() << "\n";
        return 3;
    }
}
```

To read the same configuration files and environment as the command line,
use `darpa_spill/config.hpp`:

```cpp
#include <darpa_spill/config.hpp>

darpa::spill::LoadRequest request;                  // defaults: env, ./.env, standard search paths
auto config = darpa::spill::load_config(request);   // throws ConfigError
darpa::spill::Client client(config.to_options());
```

### C

```c
#include <darpa_spill/client.h>
#include <stdio.h>

int main(void) {
    dsc_client *c = dsc_client_new("http://localhost:8080", 30.0);
    char *signal = dsc_percent_encode("$74");
    char path[256];
    char *body = NULL;
    long status = 0;
    int rc;

    snprintf(path, sizeof path, "/api/latest?signal=%s", signal);
    dsc_string_free(signal);
    rc = dsc_get(c, path, &body, &status);
    if (rc == DSC_OK)
        puts(body);
    else
        fprintf(stderr, "error: %s\n", dsc_last_error(c));
    dsc_string_free(body);
    dsc_client_free(c);
    return rc;   /* 0 ok, 1 HTTP error, 2 usage, 3 unreachable */
}
```

`examples/c/latest.c` is a complete version, and the tests build it and run
it against a live server.

## Platform notes

- **Windows output.** The CLI puts standard output into binary mode
  (`_setmode(_O_BINARY)`) so it writes `\n`, not `\r\n`, and stays
  byte-identical to the Python client.
- **Windows config path.** The system-wide configuration file is
  `%PROGRAMDATA%\darpa-spillserver\spillclient.yaml`, which the Python client
  also reads. `~` in paths expands to `%USERPROFILE%` when `HOME` is not set.
- **Relocatable installs.** The installed `darpa-spill-client-cpp` finds the
  library through a relative rpath (`$ORIGIN/../lib` on Linux,
  `@loader_path/../lib` on macOS). On Windows, the DLL is installed next to
  the executable in `bin\`.
