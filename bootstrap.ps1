<#
.SYNOPSIS
    Set up DARPA-SpillServer on Windows 11.

.DESCRIPTION
    Creates a virtual environment in .\venv, upgrades pip, installs
    requirements.txt and then the package (with test extras) in editable
    mode. If CMake is available it also configures, builds and tests the
    C/C++ client library in .\build, using the windows-msvc preset (vcpkg
    supplies Boost, OpenSSL, yaml-cpp and CppUnit; set VCPKG_ROOT). Safe to
    re-run. The Linux and macOS equivalent is bootstrap.sh.

.PARAMETER Python
    Interpreter to use. Default: the newest 3.9+ found by the py launcher,
    then python on PATH.

.PARAMETER Extras
    pip extras to install with the package. Default: test.

.PARAMETER BuildCpp
    auto (default), yes or no: whether to build the C/C++ library.

.PARAMETER DecoderDir
    A nova-time-decoder checkout to install in editable mode. Default: a
    sibling ..\nova-time-decoder, if present.

.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\bootstrap.ps1

.EXAMPLE
    .\bootstrap.ps1 -Python C:\Python312\python.exe -BuildCpp no
#>
[CmdletBinding()]
param(
    [string]$Python = $env:PYTHON,
    [string]$Extras = $(if ($env:EXTRAS) { $env:EXTRAS } else { "test" }),
    [ValidateSet("auto", "yes", "no")]
    [string]$BuildCpp = $(if ($env:BUILD_CPP) { $env:BUILD_CPP } else { "auto" }),
    [string]$DecoderDir = $env:NOVA_TIME_DECODER_DIR
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

function Test-Version([string]$Exe, [string[]]$Prefix = @()) {
    try {
        & $Exe @Prefix -c "import sys; sys.exit(0 if sys.version_info[:2] >= (3, 9) else 1)" 2>$null
        return $LASTEXITCODE -eq 0
    } catch { return $false }
}

# -- interpreter -------------------------------------------------------------
$PyPrefix = @()
if ($Python) {
    if (-not (Test-Version $Python)) { throw "$Python is not Python 3.9 or newer" }
} elseif ((Get-Command py -ErrorAction SilentlyContinue) -and (Test-Version "py" @("-3"))) {
    $Python = "py"; $PyPrefix = @("-3")
} elseif ((Get-Command python -ErrorAction SilentlyContinue) -and (Test-Version "python")) {
    $Python = "python"
} else {
    throw "no Python 3.9+ found; install one from https://www.python.org/downloads/windows/ or 'winget install Python.Python.3.12'"
}
Write-Host ">> Using interpreter: $(& $Python @PyPrefix --version 2>&1)"

# -- virtual environment -----------------------------------------------------
$VenvDir = Join-Path $PSScriptRoot "venv"
if (-not (Test-Path $VenvDir)) {
    Write-Host ">> Creating virtual environment in $VenvDir"
    & $Python @PyPrefix -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { throw "venv creation failed" }
} else {
    Write-Host ">> Reusing existing virtual environment in $VenvDir"
}
$Vpy = Join-Path $VenvDir "Scripts\python.exe"

function Invoke-Pip {
    & $Vpy -m pip @args --quiet
    if ($LASTEXITCODE -ne 0) { throw "pip $args failed" }
}

Write-Host ">> Upgrading pip"
Invoke-Pip install --upgrade pip

# -- nova-time-decoder -------------------------------------------------------
if (-not $DecoderDir) {
    foreach ($candidate in @("..\nova-time-decoder", "..\..\nova-time-decoder")) {
        if (Test-Path (Join-Path $candidate "pyproject.toml")) { $DecoderDir = $candidate; break }
    }
}
$Requirements = "requirements.txt"
if ($DecoderDir) {
    $DecoderDir = (Resolve-Path $DecoderDir).Path
    Write-Host ">> Installing nova-time-decoder from checkout: $DecoderDir"
    Invoke-Pip install -e $DecoderDir
    $Requirements = Join-Path $VenvDir "requirements.bootstrap.txt"
    Get-Content requirements.txt | Where-Object { $_ -notmatch '^nova-time-decoder' } |
        Set-Content -Encoding ascii $Requirements
} else {
    Write-Host ">> No nova-time-decoder checkout found; relying on the package index"
}

Write-Host ">> Installing requirements.txt"
Invoke-Pip install --upgrade -r $Requirements
Write-Host ">> Installing darpa-spillserver (editable, extras: $Extras)"
Invoke-Pip install -e ".[$Extras]"

# -- C/C++ client library ----------------------------------------------------
$CppStatus = "skipped (-BuildCpp no)"
if ($BuildCpp -ne "no") {
    if (Get-Command cmake -ErrorAction SilentlyContinue) {
        if (-not $env:VCPKG_ROOT) {
            Write-Warning "VCPKG_ROOT is not set; the windows-msvc preset needs vcpkg (see docs\CPP_LIBRARY.md)"
        }
        Write-Host ">> Building the C/C++ client library in build\"
        cmake --preset windows-msvc
        if ($LASTEXITCODE -eq 0) { cmake --build build --config Release --parallel }
        if ($LASTEXITCODE -eq 0) { ctest --test-dir build -C Release --output-on-failure }
        if ($LASTEXITCODE -eq 0) {
            $CppStatus = "built: build\Release\darpa-spill-client-cpp.exe"
        } elseif ($BuildCpp -eq "yes") {
            throw "the C/C++ build failed; see docs\CPP_LIBRARY.md"
        } else {
            $CppStatus = "FAILED (see above; docs\CPP_LIBRARY.md lists the dependencies)"
        }
    } elseif ($BuildCpp -eq "yes") {
        throw "-BuildCpp yes but cmake is not on PATH"
    } else {
        $CppStatus = "skipped (cmake not found)"
    }
}

Write-Host ""
Write-Host ">> Done. Activate the environment with:"
Write-Host "     .\venv\Scripts\Activate.ps1"
Write-Host ">> Then run:"
Write-Host "     darpa-spill-server --print-config"
Write-Host "     .\start-darpa-spillserver.ps1"
Write-Host ">> Run tests:  python -m pytest"
Write-Host ">> C/C++ library: $CppStatus"
