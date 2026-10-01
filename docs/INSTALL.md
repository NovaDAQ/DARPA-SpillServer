# Install, bootstrap, build and run

The project has two parts with separate prerequisites:

* **The server and Python client**: pure Python 3.9+, set up by the
  bootstrap script into `./venv`. This is all a deployment needs.
* **The C/C++ client library and `darpa-spill-client-cpp`**: a CMake build
  into `./build`. It is optional. The bootstrap scripts build it when CMake is
  on `PATH` and skip it otherwise, unless told `BUILD_CPP=yes`.

| Step | Linux | macOS | Windows 11 |
|---|---|---|---|
| Prerequisites | see [Linux](#linux) | see [macOS](#macos) | see [Windows 11](#windows-11) |
| Bootstrap | `./bootstrap.sh` | `./bootstrap.sh` | `.\bootstrap.ps1` |
| Run in the foreground | `venv/bin/darpa-spill-server -c config/spillserver.yaml` | same | `venv\Scripts\darpa-spill-server -c config\spillserver.yaml` |
| Start in the background | `./start-darpa-spillserver.sh` | same | `.\start-darpa-spillserver.ps1` |
| Stop | `./stop-darpa-spillserver.sh` | same | `.\stop-darpa-spillserver.ps1` |
| Python tests | `venv/bin/python -m pytest` | same | `venv\Scripts\python -m pytest` |
| C/C++ build only | `cmake --preset default && cmake --build build` | same | `cmake --preset windows-msvc && cmake --build build --config Release` |
| C/C++ tests | `ctest --test-dir build` | same | `ctest --test-dir build -C Release` |
| Install the C/C++ library | `cmake --install build --prefix /usr/local` | same | `cmake --install build --config Release --prefix C:\opt\darpa-spill` |
| Service | systemd, see [DEPLOYMENT.md](DEPLOYMENT.md) | launchd (not provided) | Task Scheduler (not provided) |

The bootstrap scripts are safe to re-run: they update the venv and the build
tree in place. Every option they take is listed in
`darpa-spillserver-bootstrap(1)`.

## Linux

Python 3.9 or newer is required. The NOvA gateway nodes ship Python 3.6, so
see [GETTING_STARTED.md](GETTING_STARTED.md#1-get-a-python-39-interpreter)
for three ways to get a newer one, one of which needs no root.

```console
$ ./bootstrap.sh                                   # finds a 3.9+ interpreter itself
$ PYTHON=~/.local/opt/python3.12/bin/python3 ./bootstrap.sh
$ NOVA_TIME_DECODER_DIR=~/src/nova-time-decoder ./bootstrap.sh
$ BUILD_CPP=no ./bootstrap.sh                      # Python only
```

For the C/C++ library, on Debian or Ubuntu 24.04:

```console
$ sudo apt install cmake g++ libboost-all-dev libyaml-cpp-dev libssl-dev libcppunit-dev pkg-config
```

On AlmaLinux or RHEL 9:

```console
$ sudo dnf install cmake gcc-c++ boost-devel yaml-cpp-devel openssl-devel cppunit-devel pkgconf
```

RHEL 9 ships Boost 1.75, which is the minimum the
library supports: it uses Beast, Asio, JSON and Program_options, but not
Boost.URL, which would need 1.81. A newer Boost can come from vcpkg
(`vcpkg install` in the checkout reads `vcpkg.json`) or be passed with
`-DBoost_ROOT=/opt/boost-1.87`. [CPP_LIBRARY.md](CPP_LIBRARY.md) has the
details.

## macOS

```console
$ brew install python@3.12 cmake boost yaml-cpp openssl@3 cppunit pkg-config
$ ./bootstrap.sh
```

The start and stop scripts do not depend on anything specific to Linux: they
identify the server by `ps -o command=` rather than `/proc`, and detach it
with `os.setsid()` because macOS has no `setsid(1)`.

## Windows 11

1. Python 3.9+: `winget install Python.Python.3.12`, or the installer from
   python.org. `bootstrap.ps1` uses the `py` launcher if it is present.
2. For the C/C++ library: Visual Studio 2022 (or its Build Tools) with the
   *Desktop development with C++* workload, which includes CMake and Ninja,
   and [vcpkg](https://learn.microsoft.com/vcpkg/get_started/get-started):

   ```powershell
   git clone https://github.com/microsoft/vcpkg C:\vcpkg
   C:\vcpkg\bootstrap-vcpkg.bat
   setx VCPKG_ROOT C:\vcpkg
   ```

   The `windows-msvc` preset uses vcpkg's toolchain file, and vcpkg installs
   Boost, OpenSSL, yaml-cpp and CppUnit from `vcpkg.json` on the first
   configure.
3. Bootstrap from a *Developer PowerShell for VS 2022*, so that the compiler
   is on `PATH`:

   ```powershell
   powershell -ExecutionPolicy Bypass -File .\bootstrap.ps1
   .\bootstrap.ps1 -BuildCpp no                      # Python only
   .\bootstrap.ps1 -Python C:\Python312\python.exe -DecoderDir ..\nova-time-decoder
   ```

`stop-darpa-spillserver.ps1` ends the server with a hard terminate, because
Windows has no equivalent of SIGTERM for a process with no window. The
archive runs in SQLite WAL mode, so this loses at most the last poll, the
same as SIGKILL on Linux.

## Configuration

Every program resolves its settings in the same order, highest first:
command line > environment > `.env` > YAML file > defaults. See
[CONFIGURATION.md](CONFIGURATION.md) for the server and
[CLIENT.md](CLIENT.md) for the clients. Check the result without starting
anything:

```console
$ darpa-spill-server -c config/spillserver.yaml --print-config
$ darpa-spill-client --print-config
```

## Running

Open <http://localhost:8080/> for the query page. The server also documents
itself at `/api` (routes), `/about` (version and dependencies) and `/sitemap`.
From a shell, on any host that can reach the server:

```console
$ darpa-spill-client -u http://localhost:8080 status
$ darpa-spill-client events --start -1h --signal '$8f'
$ build/darpa-spill-client-cpp events --start -1h --signal '$8f'   # same output
```
