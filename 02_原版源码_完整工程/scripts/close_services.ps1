[CmdletBinding()]
param()

$ErrorActionPreference = "Continue"
$failures = 0
$services = @(
    @{ Name = "OCR"; State = (Join-Path $PSScriptRoot "..\ocr_audit\service_logs\ocr_service_state.json") },
    @{ Name = "ICP"; State = (Join-Path $PSScriptRoot "..\icp_audit\service_logs\icp_service_state.json") }
)

foreach ($service in $services) {
    & (Join-Path $PSHOME "powershell.exe") `
        -NoProfile `
        -ExecutionPolicy Bypass `
        -File (Join-Path $PSScriptRoot "close_managed_service.ps1") `
        -StateFile $service.State `
        -ServiceName $service.Name
    if ($LASTEXITCODE -ne 0) {
        $failures++
    }
}

if ($failures -gt 0) {
    Write-Error "$failures managed service(s) could not be stopped."
    exit 1
}
exit 0
