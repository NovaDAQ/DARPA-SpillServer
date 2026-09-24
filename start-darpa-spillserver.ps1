<#
.SYNOPSIS
    Start the DARPA Spill Information Server in the background on Windows.

.DESCRIPTION
    Runs bootstrap.ps1 first if .\venv does not exist, then starts
    darpa-spill-server as a hidden process. Its PID goes to
    run\spillserver.pid and its output to run\spillserver.log and
    run\spillserver.err.log. Waits for /api/health to answer (checked with
    darpa-spill-client) before reporting success, so a bad configuration or
    a port in use is reported here. Stop it with stop-darpa-spillserver.ps1.
    The Linux and macOS equivalent is start-darpa-spillserver.sh.

.PARAMETER Config
    Configuration file. Default: $env:SPILL_CONFIG, else .\spillserver.yaml
    if present, else config\spillserver.yaml.

.PARAMETER Wait
    Seconds to wait for the health check. Default 30.

.PARAMETER ServerArgs
    Further options passed to darpa-spill-server, e.g. --port 8081.

.EXAMPLE
    .\start-darpa-spillserver.ps1 -Config config\spillserver-near.yaml -- --port 8081
#>
[CmdletBinding()]
param(
    [string]$Config = $env:SPILL_CONFIG,
    [int]$Wait = $(if ($env:SPILL_WAIT) { [int]$env:SPILL_WAIT } else { 30 }),
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ServerArgs = @()
)

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot

$Scripts = Join-Path $PSScriptRoot "venv\Scripts"
$Server = Join-Path $Scripts "darpa-spill-server.exe"
$Client = Join-Path $Scripts "darpa-spill-client.exe"
$RunDir = Join-Path $PSScriptRoot "run"
$PidFile = if ($env:SPILL_PIDFILE) { $env:SPILL_PIDFILE } else { Join-Path $RunDir "spillserver.pid" }
$LogFile = if ($env:SPILL_LOGFILE) { $env:SPILL_LOGFILE } else { Join-Path $RunDir "spillserver.log" }
$ErrFile = [System.IO.Path]::ChangeExtension($LogFile, ".err.log")

if (-not (Test-Path $Server)) {
    Write-Host ">> $Server not found; running bootstrap.ps1"
    & (Join-Path $PSScriptRoot "bootstrap.ps1") -BuildCpp no
}
if (-not $Config) {
    $Config = if (Test-Path "spillserver.yaml") { "spillserver.yaml" } else { "config\spillserver.yaml" }
}
if (-not (Test-Path $Config)) { throw "config file $Config not found" }

# True if PID is a live darpa-spill-server. The command line is checked, not
# just that the PID exists, so a stale PID file never matches an unrelated
# process that has since been given the same PID.
function Test-SpillServer([string]$ProcessId) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $([int]$ProcessId)" -ErrorAction SilentlyContinue
    return [bool]($proc -and $proc.CommandLine -match 'darpa-spill-server')
}

if (Test-Path $PidFile) {
    $old = (Get-Content $PidFile -Raw).Trim()
    if ($old -and (Test-SpillServer $old)) {
        Write-Host "darpa-spill-server is already running (PID $old)"
        exit 0
    }
    Write-Host ">> Removing stale PID file $PidFile"
    Remove-Item $PidFile -Force
}

# Ask the server where it will listen; this contacts nothing and fails fast
# on a configuration error.
$merged = & $Server -c $Config @ServerArgs --print-config
if ($LASTEXITCODE -ne 0) { Write-Error "configuration rejected; nothing started"; exit 2 }
$settings = ($merged | Out-String | ConvertFrom-Json).server
$bindHost = if (@("0.0.0.0", "", "::") -contains $settings.host) { "127.0.0.1" } else { $settings.host }
$scheme = if ($settings.ssl_certfile) { "https" } else { "http" }
$Url = "${scheme}://${bindHost}:$($settings.port)"

New-Item -ItemType Directory -Force -Path (Split-Path $PidFile), (Split-Path $LogFile) | Out-Null
Write-Host ">> Starting darpa-spill-server with $Config"
$process = Start-Process -FilePath $Server -ArgumentList (@("-c", $Config) + $ServerArgs) `
    -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $LogFile -RedirectStandardError $ErrFile
Set-Content -Path $PidFile -Value $process.Id -Encoding ascii

for ($i = 0; $i -lt $Wait; $i++) {
    if ($process.HasExited) {
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        Write-Error "darpa-spill-server exited during startup; see $ErrFile"
        Get-Content $ErrFile -Tail 20 | Write-Host
        exit 1
    }
    & $Client -u $Url -k -t 2 health *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host ">> darpa-spill-server is up (PID $($process.Id)), health check at $Url/api/health"
        Write-Host ">> Log: $LogFile, $ErrFile"
        exit 0
    }
    Start-Sleep -Seconds 1
}
Write-Warning "darpa-spill-server (PID $($process.Id)) is running but $Url/api/health did not answer within ${Wait}s; check $ErrFile"
exit 1
