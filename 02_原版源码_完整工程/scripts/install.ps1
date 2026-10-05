# 安装脚本：在任意 Windows 10/11 机器上部署本项目运行环境。
# 用法：双击根目录 install.bat，或手动执行：
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\install.ps1 [-Mirror <pip镜像url>] [-SkipPlaywright]
[CmdletBinding()]
param(
    [string]$Mirror = "",
    [switch]$SkipPlaywright
)

$ErrorActionPreference = "Stop"
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))

function Write-Step {
    param([string]$Message)
    Write-Host ""
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Stop-WithError {
    param([string]$Message)
    Write-Host ""
    Write-Host "[错误] $Message" -ForegroundColor Red
    exit 1
}

# ---------- 1. 检查系统环境 ----------
Write-Step "检查系统环境"
$osVersion = [Environment]::OSVersion.Version
if ($osVersion.Major -lt 10) {
    Stop-WithError "本项目仅支持 Windows 10 / 11（64 位），当前系统版本：$osVersion"
}
if ($PSVersionTable.PSVersion.Major -lt 5) {
    Stop-WithError "PowerShell 版本过低（需要 5.1 及以上），当前：$($PSVersionTable.PSVersion)"
}
Write-Host "Windows 版本: $osVersion，PowerShell 版本: $($PSVersionTable.PSVersion) —— 通过"

# ---------- 2. 查找 Python 3.10+ ----------
Write-Step "查找 Python 3.10 及以上版本"
# Python 3.11.9 静默安装（用户级，无需管理员权限，不改系统 PATH，不影响机器上已有的 Python）。
function Install-Python {
    $installDir = Join-Path $env:LOCALAPPDATA "Programs\Python\Python311"
    $pythonExePath = Join-Path $installDir "python.exe"
    if (Test-Path -LiteralPath $pythonExePath -PathType Leaf) {
        return $pythonExePath
    }

    $installer = Join-Path $env:TEMP "python-3.11.9-amd64.exe"
    $downloaded = $false
    foreach ($url in @(
        "https://mirrors.huaweicloud.com/python/3.11.9/python-3.11.9-amd64.exe",
        "https://www.python.org/ftp/python/3.11.9/python-3.11.9-amd64.exe"
    )) {
        Write-Host "下载 Python 3.11.9 安装器: $url"
        try {
            # TLS 1.2+（部分旧环境默认未启用会导致下载失败）
            [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
            Invoke-WebRequest -Uri $url -OutFile $installer -UseBasicParsing
            $downloaded = $true
            break
        }
        catch {
            Write-Host "下载失败（$($_.Exception.Message)），尝试下一个源" -ForegroundColor Yellow
        }
    }
    if (-not $downloaded) {
        Stop-WithError "Python 安装器下载失败。请手动安装 Python 3.11（https://www.python.org/downloads/ ，勾选 Add to PATH）后重跑本脚本。"
    }

    Write-Host "静默安装 Python 3.11.9 到 $installDir（不需要管理员权限，约 1-2 分钟）"
    # InstallAllUsers=0   用户级安装，免 UAC
    # PrependPath=0       不改系统 PATH，不影响机器上已有的 python 命令（如 3.9）
    # Include_launcher=1  安装 py 启动器（InstallLauncherAllUsers=0 装到当前用户）
    # Include_test=0      不装测试套件，省空间
    $arguments = "/quiet InstallAllUsers=0 TargetDir=`"$installDir`" PrependPath=0 Include_launcher=1 InstallLauncherAllUsers=0 Include_test=0"
    $process = Start-Process -FilePath $installer -ArgumentList $arguments -Wait -PassThru
    $exitCode = $process.ExitCode
    Remove-Item -LiteralPath $installer -Force -ErrorAction SilentlyContinue
    if ($exitCode -ne 0) {
        Stop-WithError "Python 静默安装失败（退出码 $exitCode）。请手动安装 Python 3.11 后重跑本脚本。"
    }
    if (-not (Test-Path -LiteralPath $pythonExePath -PathType Leaf)) {
        Stop-WithError "安装流程结束但未找到 $pythonExePath。请手动安装 Python 3.11 后重跑本脚本。"
    }
    Write-Host "Python 3.11.9 安装完成: $pythonExePath" -ForegroundColor Green
    return $pythonExePath
}

function Test-PythonCandidate {
    param([string]$ExePath)
    if (-not $ExePath -or -not (Test-Path -LiteralPath $ExePath -PathType Leaf)) { return $null }
    # PS 5.1 里 py/python 的 stderr/非零退出码在 ErrorActionPreference=Stop 下会被包装成 NativeCommandError。
    # 临时切到 Continue，让调用失败只返回空，不会 abort 整个脚本。
    $oldEAP = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $versionText = $null
    try { $versionText = & $ExePath -c "import sys; print('%d.%d' % sys.version_info[:2])" 2>$null } catch { }
    $ErrorActionPreference = $oldEAP
    if ($LASTEXITCODE -ne 0 -or -not $versionText) { return $null }
    $versionText = $versionText.Trim()
    if ($versionText -notmatch '^3\.(\d+)$') { return $null }
    if ([int]$Matches[1] -ge 10) { return $versionText }
    return $null
}

$pythonExe = $null
foreach ($candidate in @("py -3.11", "py -3", "python")) {
    $commandParts = $candidate.Split(" ")
    $exe = $commandParts[0]
    $prefix = @()
    if ($commandParts.Count -gt 1) { $prefix = @($commandParts[1..($commandParts.Count - 1)]) }
    $command = Get-Command $exe -ErrorAction SilentlyContinue
    if (-not $command) { continue }
    # py 启动器是转发器：解析出它实际指向的 python.exe 绝对路径，统一按绝对路径处理
    $probe = if ($prefix.Count) {
        # py 找不到目标版本时会往 stderr 打印可用版本列表，继续探测即可，不能 abort
        $oldEAP = $ErrorActionPreference
        $ErrorActionPreference = 'Continue'
        $resolved = $null
        try { $resolved = & $exe @prefix -c "import sys; print(sys.executable)" 2>$null } catch { }
        $ErrorActionPreference = $oldEAP
        if ($LASTEXITCODE -eq 0 -and $resolved) { $resolved.Trim() } else { $null }
    }
    else { $command.Source }
    if (-not $probe) { continue }
    $found = Test-PythonCandidate $probe
    if ($found) {
        $pythonExe = $probe
        Write-Host "找到 Python ${found}: $probe"
        break
    }
}
if (-not $pythonExe) {
    # per-user 固定路径兜底（静默安装/官方安装器默认位置不在 PATH 时 py 可能也找不到）
    foreach ($fixed in @(
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python311\python.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python310\python.exe")
    )) {
        $found = Test-PythonCandidate $fixed
        if ($found) {
            $pythonExe = $fixed
            Write-Host "找到 Python ${found}: $fixed"
            break
        }
    }
}
if (-not $pythonExe) {
    Write-Host "未找到 Python 3.10+，自动安装 Python 3.11.9（用户级静默安装，不影响机器上已有的 Python）" -ForegroundColor Yellow
    $pythonExe = Install-Python
}

# ---------- 3. 创建虚拟环境 ----------
Write-Step "创建虚拟环境 .venv"
$venvDir = Join-Path $ProjectRoot ".venv"
$venvPython = Join-Path $venvDir "Scripts\python.exe"
if (Test-Path -LiteralPath $venvPython -PathType Leaf) {
    Write-Host ".venv 已存在，跳过创建（如需重装请先删除 .venv 目录）"
}
else {
    & $pythonExe -m venv $venvDir
    if ($LASTEXITCODE -ne 0) { Stop-WithError "创建虚拟环境失败，请检查 Python 安装是否完整。" }
    Write-Host "虚拟环境已创建: $venvDir"
}

# ---------- 4. 安装 Python 依赖 ----------
# 中文 Windows 默认代码页 936，编译 stringzilla 等 C 扩展时会报 C4819；强制 UTF-8 可避免
$env:PYTHONUTF8 = "1"
[Environment]::SetEnvironmentVariable("CL", "/utf-8", "Process")
[Environment]::SetEnvironmentVariable("CXX", "/utf-8", "Process")

$vendorWhlDir = Join-Path $ProjectRoot "vendor\whl"
$offlineMode = Test-Path -LiteralPath $vendorWhlDir -PathType Container

Write-Step "升级 pip"
if ($offlineMode) {
    & $venvPython -m pip install --upgrade pip --no-index --find-links $vendorWhlDir
}
else {
    & $venvPython -m pip install --upgrade pip
}
if ($LASTEXITCODE -ne 0) { Stop-WithError "pip 升级失败。" }

if ($offlineMode) {
    Write-Step "离线安装主依赖 requirements.txt（来自 vendor\whl）"
    # -v 让 pip 在 Installing collected packages 阶段逐包打印，离线装 paddle 体积大，
    # 没有逐包输出会误以为卡死；加 -v 后每个包都能看到 Installing ... 进度。
    & $venvPython -m pip install -r (Join-Path $ProjectRoot "requirements.txt") --no-index --find-links $vendorWhlDir -v
    if ($LASTEXITCODE -ne 0) {
        Stop-WithError "主依赖离线安装失败。可能是 vendor\whl 中的 wheel 与当前 Python 版本/架构不匹配。"
    }
}
else {
    $mirrorArgs = @()
    if ($Mirror) {
        $mirrorArgs = @("-i", $Mirror)
        Write-Host "使用镜像源: $Mirror"
    }
    Write-Step "安装主依赖 requirements.txt（paddleocr 体积较大，可能需要 10-30 分钟，请耐心等待）"
    & $venvPython -m pip install -r (Join-Path $ProjectRoot "requirements.txt") @mirrorArgs --only-binary=:all:
    if ($LASTEXITCODE -ne 0) {
        Stop-WithError "主依赖安装失败。网络较慢时可换国内镜像重试：powershell -NoProfile -ExecutionPolicy Bypass -File scripts\install.ps1 -Mirror https://pypi.tuna.tsinghua.edu.cn/simple"
    }
}

$icpRequirements = Join-Path $ProjectRoot "ICP_Query\src\python\requirements.txt"
if (Test-Path -LiteralPath $icpRequirements -PathType Leaf) {
    Write-Step "安装 ICP 服务依赖 ICP_Query\src\python\requirements.txt"
    if ($offlineMode) {
        & $venvPython -m pip install -r $icpRequirements --no-index --find-links $vendorWhlDir
    }
    else {
        & $venvPython -m pip install -r $icpRequirements @mirrorArgs
    }
    if ($LASTEXITCODE -ne 0) {
        Stop-WithError "ICP 服务依赖安装失败。"
    }
}
else {
    Write-Host "未找到 $icpRequirements，跳过（根 requirements.txt 已包含 ICP 服务依赖）"
}

# ---------- 5. 安装 Playwright 浏览器 ----------
if ($SkipPlaywright) {
    Write-Step "跳过 Playwright 浏览器安装（-SkipPlaywright）"
}
else {
    Write-Step "安装 Playwright Chromium 浏览器"
    & $venvPython -m playwright install chromium
    if ($LASTEXITCODE -ne 0) {
        Stop-WithError "Playwright 浏览器下载失败。多为网络问题，请重试本脚本；也可稍后手动执行：.venv\Scripts\python.exe -m playwright install chromium"
    }
}

# ---------- 6. 准备配置文件 ----------
Write-Step "检查配置文件"
$configDir = Join-Path $ProjectRoot "config"
$configPairs = @(
    @{ Example = "llm_config.example.txt"; Target = "llm_config.txt"; Hint = "【必须编辑】填入你的 LLM API key 后才能运行流水线" },
    @{ Example = "companies.example.txt"; Target = "companies.txt"; Hint = "【必须编辑】填入待排查的企业名单（每行一个企业）" }
)
$needEditLlm = $false
foreach ($pair in $configPairs) {
    $targetPath = Join-Path $configDir $pair.Target
    if (Test-Path -LiteralPath $targetPath -PathType Leaf) {
        Write-Host "已存在，跳过: config\$($pair.Target)"
        continue
    }
    $examplePath = Join-Path $configDir $pair.Example
    if (-not (Test-Path -LiteralPath $examplePath -PathType Leaf)) {
        Stop-WithError "配置模板缺失: config\$($pair.Example)，请确认 zip 解压完整。"
    }
    Copy-Item -LiteralPath $examplePath -Destination $targetPath
    Write-Host "已从模板生成: config\$($pair.Target) —— $($pair.Hint)" -ForegroundColor Yellow
    if ($pair.Target -eq "llm_config.txt") { $needEditLlm = $true }
}

# ---------- 7. 自检 ----------
Write-Step "自检：查询工具"
Push-Location -LiteralPath $ProjectRoot
try {
    & $venvPython "scripts\report\extract_runs.py" --format md
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[警告] extract_runs.py 执行返回非零。若 pipeline_output 中还没有运行数据属正常现象，可先忽略。" -ForegroundColor Yellow
    }
    else {
        Write-Host "查询工具自检通过"
    }
}
finally {
    Pop-Location
}

Write-Step "自检：ICP 服务（端口 16181）"
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $ProjectRoot "scripts\open_icp_service.ps1") -ValidateOnly
if ($LASTEXITCODE -ne 0) { Stop-WithError "ICP 服务自检失败，请查看上方输出排查（常见问题：依赖未装全、端口被占用）。" }

Write-Step "自检：OCR 服务（端口 16191，首次加载 paddleocr 较慢）"
& powershell.exe -NoProfile -ExecutionPolicy Bypass -File (Join-Path $ProjectRoot "scripts\open_ocr_service.ps1") -ValidateOnly
if ($LASTEXITCODE -ne 0) { Stop-WithError "OCR 服务自检失败，请查看上方输出排查（常见问题：依赖未装全、端口被占用）。" }

# ---------- 完成 ----------
Write-Host ""
Write-Host "==========================================" -ForegroundColor Green
Write-Host " 安装完成，全部自检通过！" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host ""
Write-Host "下一步："
if ($needEditLlm) {
    Write-Host "  1. 编辑 config\llm_config.txt，填入你的 LLM API key（必须）" -ForegroundColor Yellow
}
else {
    Write-Host "  1. 确认 config\llm_config.txt 中的 LLM API key 有效"
}
Write-Host "  2. 编辑 config\companies.txt，填入待排查企业名单（每行一个）"
Write-Host "  3. 运行主流程：双击 run.bat"
Write-Host "  4. 复跑企业：双击 replay.bat"
Write-Host ""
Write-Host "详细说明见项目根目录的《安装指导.md》"
exit 0
