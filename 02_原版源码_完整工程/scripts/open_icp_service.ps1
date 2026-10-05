[CmdletBinding()]
param(
    [string]$ServiceRoot = "",
    [string]$PythonExecutable = "",
    [int]$Port = 16181,
    [int]$StartupTimeoutSeconds = 60,
    [string]$LogRoot = "",
    [switch]$ValidateOnly
)

$ErrorActionPreference = "Stop"

if (-not $ServiceRoot) {
    $ServiceRoot = Join-Path $PSScriptRoot "..\ICP_Query"
}

if (-not $LogRoot) {
    $LogRoot = Join-Path $PSScriptRoot "..\icp_audit\service_logs"
}

function Test-TcpPort {
    param(
        [string]$HostName = "127.0.0.1",
        [int]$TargetPort,
        [int]$TimeoutMilliseconds = 800
    )

    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $task = $client.ConnectAsync($HostName, $TargetPort)
        if (-not $task.Wait($TimeoutMilliseconds)) {
            return $false
        }
        return $client.Connected
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

function Repair-DuplicatePathEnvironment {
    # Some agent/IDE hosts inject both Path and PATH. Windows PowerShell's
    # Start-Process builds a case-insensitive dictionary and otherwise throws
    # "Item has already been added" before the child is created.
    $variables = [Environment]::GetEnvironmentVariables([EnvironmentVariableTarget]::Process)
    $pathKeys = @($variables.Keys | Where-Object { [string]$_ -ieq "Path" })
    if ($pathKeys.Count -le 1) {
        return
    }
    $pathValue = [string]$variables[$pathKeys[0]]
    foreach ($pathKey in $pathKeys) {
        [Environment]::SetEnvironmentVariable(
            [string]$pathKey,
            $null,
            [EnvironmentVariableTarget]::Process
        )
    }
    [Environment]::SetEnvironmentVariable(
        "Path",
        $pathValue,
        [EnvironmentVariableTarget]::Process
    )
}

function Test-ServicePython {
    param(
        [string]$Candidate,
        [string]$ResolvedServiceRoot
    )

    $previousErrorAction = $ErrorActionPreference
    try {
        $ErrorActionPreference = "SilentlyContinue"
        Push-Location -LiteralPath $ResolvedServiceRoot
        $entryScript = Join-Path $ResolvedServiceRoot "src\python\icpApi.py"
        & $Candidate $entryScript --help *> $null
        return $LASTEXITCODE -eq 0
    }
    finally {
        Pop-Location
        $ErrorActionPreference = $previousErrorAction
    }
}

function Resolve-Python {
    param(
        [string]$Requested,
        [string]$ResolvedServiceRoot
    )

    if ($Requested) {
        if ([System.IO.Path]::IsPathRooted($Requested)) {
            if (-not (Test-Path -LiteralPath $Requested -PathType Leaf)) {
                throw "Python executable not found: $Requested"
            }
            $resolved = (Resolve-Path -LiteralPath $Requested).Path
        }
        else {
            $command = Get-Command $Requested -ErrorAction SilentlyContinue
            if ($null -eq $command) {
                throw "Python command not found in PATH: $Requested"
            }
            $resolved = $command.Source
        }

        if (-not (
            Test-ServicePython `
                -Candidate $resolved `
                -ResolvedServiceRoot $ResolvedServiceRoot
        )) {
            throw "Python is missing ICP service dependencies: $resolved"
        }
        return $resolved
    }

    $candidates = [System.Collections.Generic.List[string]]::new()
    $candidates.Add((Join-Path $PSScriptRoot "..\.venv\Scripts\python.exe"))
    $candidates.Add((Join-Path $ResolvedServiceRoot ".venv\Scripts\python.exe"))
    $candidates.Add((Join-Path $ResolvedServiceRoot "venv\Scripts\python.exe"))
    foreach ($directory in ([Environment]::GetEnvironmentVariable("PATH") -split ";")) {
        if ($directory) {
            $candidates.Add((Join-Path $directory "python.exe"))
        }
    }

    foreach ($candidate in ($candidates | Select-Object -Unique)) {
        if (
            (Test-Path -LiteralPath $candidate -PathType Leaf) -and
            (Test-ServicePython `
                -Candidate $candidate `
                -ResolvedServiceRoot $ResolvedServiceRoot)
        ) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    throw (
        "No Python with ICP service dependencies was found. " +
        "Install src\python\requirements.txt or pass -PythonExecutable."
    )
}

if ($Port -lt 1 -or $Port -gt 65535) {
    throw "Port must be between 1 and 65535."
}
if ($StartupTimeoutSeconds -lt 1) {
    throw "StartupTimeoutSeconds must be greater than zero."
}

if (Test-TcpPort -TargetPort $Port) {
    Write-Host "ICP service is already running: http://127.0.0.1:$Port" -ForegroundColor Green
    exit 0
}

if (-not (Test-Path -LiteralPath $ServiceRoot -PathType Container)) {
    throw "ICP service root does not exist: $ServiceRoot"
}

$serviceRootPath = (Resolve-Path -LiteralPath $ServiceRoot).Path
$serviceScript = Join-Path $serviceRootPath "src\python\icpApi.py"
if (-not (Test-Path -LiteralPath $serviceScript -PathType Leaf)) {
    throw "ICP service entry script does not exist: $serviceScript"
}

$pythonPath = Resolve-Python `
    -Requested $PythonExecutable `
    -ResolvedServiceRoot $serviceRootPath
$logRootPath = [System.IO.Path]::GetFullPath($LogRoot)
New-Item -ItemType Directory -Force -Path $logRootPath | Out-Null

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$stdoutLog = Join-Path $logRootPath "icp_service_${stamp}_stdout.log"
$stderrLog = Join-Path $logRootPath "icp_service_${stamp}_stderr.log"
$stateFile = Join-Path $logRootPath "icp_service_state.json"

Write-Host "Starting ICP service..."
Write-Host "Service root: $serviceRootPath"
Write-Host "Python: $pythonPath"

if ($ValidateOnly) {
    Write-Host "Validation succeeded; service was not started." -ForegroundColor Green
    exit 0
}

Repair-DuplicatePathEnvironment
$process = Start-Process `
    -FilePath $pythonPath `
    -ArgumentList @("`"$serviceScript`"") `
    -WorkingDirectory $serviceRootPath `
    -WindowStyle Hidden `
    -RedirectStandardOutput $stdoutLog `
    -RedirectStandardError $stderrLog `
    -PassThru

$state = [ordered]@{
    pid = $process.Id
    port = $Port
    url = "http://127.0.0.1:$Port"
    status = "starting"
    started_at = (Get-Date).ToString("o")
    process_started_at = $process.StartTime.ToString("o")
    service_root = $serviceRootPath
    python = $pythonPath
    stdout_log = $stdoutLog
    stderr_log = $stderrLog
}
$temporaryState = "$stateFile.tmp"
$state | ConvertTo-Json | Set-Content -LiteralPath $temporaryState -Encoding UTF8
Move-Item -LiteralPath $temporaryState -Destination $stateFile -Force

$deadline = (Get-Date).AddSeconds($StartupTimeoutSeconds)
$ready = $false
while ((Get-Date) -lt $deadline) {
    if ($process.HasExited) {
        break
    }
    if (Test-TcpPort -TargetPort $Port) {
        $ready = $true
        break
    }
    Start-Sleep -Milliseconds 500
}

if (-not $ready) {
    if (-not $process.HasExited) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        $process.WaitForExit(5000)
    }
    Remove-Item -LiteralPath $stateFile -Force -ErrorAction SilentlyContinue

    Write-Error "ICP service did not become ready in ${StartupTimeoutSeconds} seconds."
    if (Test-Path -LiteralPath $stderrLog) {
        Write-Host "Last 30 stderr lines:" -ForegroundColor Yellow
        Get-Content -LiteralPath $stderrLog -Tail 30
    }
    Write-Host "Full stdout: $stdoutLog"
    Write-Host "Full stderr: $stderrLog"
    exit 1
}

$state.status = "ready"
$state["ready_at"] = (Get-Date).ToString("o")
$state | ConvertTo-Json | Set-Content -LiteralPath $temporaryState -Encoding UTF8
Move-Item -LiteralPath $temporaryState -Destination $stateFile -Force

Write-Host "ICP service started: http://127.0.0.1:$Port" -ForegroundColor Green
Write-Host "PID: $($process.Id)"
Write-Host "State file: $stateFile"
Write-Host "stdout: $stdoutLog"
Write-Host "stderr: $stderrLog"
