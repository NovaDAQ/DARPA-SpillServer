<#
.SYNOPSIS
    Stop a DARPA Spill Information Server started by start-darpa-spillserver.ps1.

.DESCRIPTION
    Reads run\spillserver.pid, checks that the PID is still a
    darpa-spill-server (so a stale file never kills an unrelated process),
    and stops it. Windows has no SIGTERM for a windowless console process, so
    the stop is a hard terminate; the archive runs in SQLite WAL mode, so at
    most the last poll is lost, as with SIGKILL on Linux. Removes the PID
    file afterwards. The Linux and macOS equivalent is stop-darpa-spillserver.sh.

.EXAMPLE
    .\stop-darpa-spillserver.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$PidFile = if ($env:SPILL_PIDFILE) { $env:SPILL_PIDFILE } else { Join-Path $PSScriptRoot "run\spillserver.pid" }
# True if PID is a live darpa-spill-server. The command line is checked, not
# just that the PID exists, so a stale PID file never matches an unrelated
# process that has since been given the same PID.
function Test-SpillServer([string]$ProcessId) {
    $proc = Get-CimInstance Win32_Process -Filter "ProcessId = $([int]$ProcessId)" -ErrorAction SilentlyContinue
    return [bool]($proc -and $proc.CommandLine -match 'darpa-spill-server')
}

if (-not (Test-Path $PidFile)) {
    Write-Host "darpa-spill-server is not running (no PID file at $PidFile)"
    exit 0
}
$id = (Get-Content $PidFile -Raw).Trim()
if (-not $id -or -not (Test-SpillServer $id)) {
    Write-Host "darpa-spill-server is not running; removing stale PID file $PidFile"
    Remove-Item $PidFile -Force
    exit 0
}

Write-Host ">> Stopping darpa-spill-server (PID $id)"
# /T also ends the Python child that the console-script launcher starts.
taskkill /PID $id /T /F | Out-Null
for ($i = 0; $i -lt 15; $i++) {
    if (-not (Test-SpillServer $id)) {
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
        Write-Host ">> Stopped"
        exit 0
    }
    Start-Sleep -Seconds 1
}
Write-Error "PID $id is still running"
exit 1
