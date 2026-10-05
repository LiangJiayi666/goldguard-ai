[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$StateFile,
    [Parameter(Mandatory = $true)]
    [string]$ServiceName
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $StateFile -PathType Leaf)) {
    Write-Host "$ServiceName service is already stopped (no state file)."
    exit 0
}

try {
    $state = Get-Content -LiteralPath $StateFile -Raw | ConvertFrom-Json
    $servicePid = [int]$state.pid
}
catch {
    Write-Error "$ServiceName state file is invalid: $StateFile ($($_.Exception.Message))"
    exit 1
}

$process = Get-Process -Id $servicePid -ErrorAction SilentlyContinue
if ($null -eq $process) {
    Remove-Item -LiteralPath $StateFile -Force
    Write-Host "$ServiceName service is already stopped; removed stale state file."
    exit 0
}

# A stale PID file must never terminate an unrelated process after Windows has
# recycled the PID. Check both the executable and the process start timestamp.
try {
    $expectedPython = [System.IO.Path]::GetFullPath([string]$state.python)
    $actualPython = [System.IO.Path]::GetFullPath([string]$process.Path)
}
catch {
    Write-Error "$ServiceName process identity could not be verified; refusing to stop PID $servicePid."
    exit 1
}

if (-not [string]::Equals($expectedPython, $actualPython, [System.StringComparison]::OrdinalIgnoreCase)) {
    Remove-Item -LiteralPath $StateFile -Force
    Write-Warning "$ServiceName PID $servicePid now belongs to another executable; removed stale state without stopping it."
    exit 0
}

$recordedStartText = if ($state.process_started_at) { $state.process_started_at } else { $state.started_at }
try {
    $recordedStart = [DateTimeOffset]::Parse([string]$recordedStartText)
    $actualStart = [DateTimeOffset]$process.StartTime
    $startDeltaSeconds = [Math]::Abs(($actualStart - $recordedStart).TotalSeconds)
}
catch {
    Write-Error "$ServiceName start time could not be verified; refusing to stop PID $servicePid."
    exit 1
}

$startToleranceSeconds = if ($state.process_started_at) { 5 } else { 600 }
if ($startDeltaSeconds -gt $startToleranceSeconds) {
    Remove-Item -LiteralPath $StateFile -Force
    Write-Warning "$ServiceName PID $servicePid has a different start time; removed stale state without stopping it."
    exit 0
}

Write-Host "Stopping $ServiceName service (PID $servicePid) ..."
$taskkill = Join-Path $env:SystemRoot "System32\taskkill.exe"
& $taskkill /PID $servicePid /T /F | Out-Host
if ($LASTEXITCODE -ne 0) {
    Write-Error "$ServiceName service could not be stopped (taskkill exit $LASTEXITCODE)."
    exit 1
}

$deadline = (Get-Date).AddSeconds(10)
do {
    if ($null -eq (Get-Process -Id $servicePid -ErrorAction SilentlyContinue)) {
        Remove-Item -LiteralPath $StateFile -Force
        Write-Host "$ServiceName service stopped."
        exit 0
    }
    Start-Sleep -Milliseconds 200
} while ((Get-Date) -lt $deadline)

Write-Error "$ServiceName service did not exit within 10 seconds."
exit 1
