#!/usr/bin/env bash
#
# bootstrap.sh -- set up the DARPA-SpillServer Python environment.
#
# Creates a virtual environment in ./venv, upgrades pip, and installs the
# package (with test extras) in editable mode. Safe to re-run: it updates an
# existing venv in place.
#
# The server needs Python 3.9 or newer. The NOvA gateway nodes ship only
# Python 3.6, so this script looks for a newer interpreter in the usual
# places, including a private one under ~/.local/opt. See
# docs/GETTING_STARTED.md for how to install one without root.
#
# nova-time-decoder is a sibling NOvA DAQ package rather than a PyPI release.
# If a checkout is found next to this one it is installed from there in
# editable mode, so a fix in the decoder is picked up without reinstalling.
#
# Usage:
#   ./bootstrap.sh                      # create/update venv and install
#   PYTHON=python3.12 ./bootstrap.sh    # pick a specific interpreter
#   EXTRAS=dev ./bootstrap.sh           # install extras other than "test"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

EXTRAS="${EXTRAS:-test}"
MIN_MAJOR=3
MIN_MINOR=9

# --------------------------------------------------------------------------
# Pick an interpreter: $PYTHON wins, otherwise search newest-first.
# --------------------------------------------------------------------------
version_ok() {
    "$1" -c "import sys; sys.exit(0 if sys.version_info[:2] >= ($MIN_MAJOR, $MIN_MINOR) else 1)" \
        >/dev/null 2>&1
}

PYTHON="${PYTHON:-}"
if [ -n "$PYTHON" ]; then
    if ! command -v "$PYTHON" >/dev/null 2>&1; then
        echo "error: PYTHON=$PYTHON is not on PATH" >&2
        exit 1
    fi
    if ! version_ok "$PYTHON"; then
        echo "error: $PYTHON is $("$PYTHON" -V 2>&1), but Python ${MIN_MAJOR}.${MIN_MINOR}+ is required" >&2
        exit 1
    fi
else
    for candidate in \
        "$HOME/.local/opt/python3.13/bin/python3" \
        "$HOME/.local/opt/python3.12/bin/python3" \
        "$HOME/.local/opt/python3.11/bin/python3" \
        python3.13 python3.12 python3.11 python3.10 python3.9 python3 python
    do
        if command -v "$candidate" >/dev/null 2>&1 && version_ok "$candidate"; then
            PYTHON="$candidate"
            break
        fi
    done
fi

if [ -z "$PYTHON" ]; then
    cat >&2 <<'MSG'
error: no Python 3.9+ interpreter found.

The stock interpreter on the NOvA gateway nodes is Python 3.6, which is too
old. Install a newer one without root, for example:

  mkdir -p ~/.local/opt && cd /tmp
  curl -sLO https://github.com/astral-sh/python-build-standalone/releases/download/20260901/cpython-3.12.14%2B20260901-x86_64-unknown-linux-gnu-install_only.tar.gz
  tar xzf cpython-3.12.14+20260901-x86_64-unknown-linux-gnu-install_only.tar.gz -C ~/.local/opt
  mv ~/.local/opt/python ~/.local/opt/python3.12

then re-run ./bootstrap.sh. See docs/GETTING_STARTED.md for alternatives.
MSG
    exit 1
fi

echo ">> Using interpreter: $("$PYTHON" --version 2>&1) ($PYTHON)"

VENV_DIR="$SCRIPT_DIR/venv"
if [ ! -d "$VENV_DIR" ]; then
    echo ">> Creating virtual environment in $VENV_DIR"
    "$PYTHON" -m venv "$VENV_DIR"
else
    echo ">> Reusing existing virtual environment in $VENV_DIR"
fi

if [ -x "$VENV_DIR/bin/python" ]; then
    VPY="$VENV_DIR/bin/python"
else
    VPY="$VENV_DIR/Scripts/python.exe"
fi

echo ">> Upgrading pip"
"$VPY" -m pip install --upgrade pip --quiet

# --------------------------------------------------------------------------
# nova-time-decoder: prefer a sibling checkout over the index.
# --------------------------------------------------------------------------
DECODER_DIR=""
for candidate in \
    "$SCRIPT_DIR/../nova-time-decoder" \
    "$SCRIPT_DIR/../../nova-time-decoder"
do
    if [ -f "$candidate/pyproject.toml" ]; then
        DECODER_DIR="$(cd "$candidate" && pwd)"
        break
    fi
done

if [ -n "$DECODER_DIR" ]; then
    echo ">> Installing nova-time-decoder from sibling checkout: $DECODER_DIR"
    "$VPY" -m pip install -e "$DECODER_DIR" --quiet
else
    echo ">> No sibling nova-time-decoder checkout found; relying on the package index"
fi

echo ">> Installing darpa-spillserver (editable, extras: $EXTRAS)"
"$VPY" -m pip install -e ".[$EXTRAS]" --quiet

echo ""
echo ">> Done. Activate the environment with:"
echo "     source venv/bin/activate"
echo ">> Then run:"
echo "     darpa-spill-server --print-config      # check the merged configuration"
echo "     darpa-spill-server -c config/spillserver.yaml"
echo ">> Run tests:  python -m pytest"
