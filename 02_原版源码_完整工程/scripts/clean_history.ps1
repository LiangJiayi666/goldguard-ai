# 交付前清理历史运行数据。两种模式：
#   默认（无 -Before）：只删「每家企业最新批次之前」的旧 run，保留每家企业最新批次。
#       判定口径与 export_history_latest_excel.py 一致：按企业取历史最新 run，合并成保留集。
#   -Before <YYYYMMDD> 或 <YYYYMMDD_HHMMSS>：删除指定时间之前启动的所有 run（不管是否每企业最新）。
#       时间用 run_id 前缀判断（run_id 形如 20260820_143549_xxxx，自带启动时间）。
# 两种模式都删 DB 行 + runs/<run_id> 磁盘目录。不碰 config/、社媒 cookie、源码；
# enterprise_aliases 按企业累积、跨 run，保留。
# 依赖 sqlite3 CLI（Windows 下随系统或 Git 分发，已随环境自带）。
# 用法（在项目根目录）：
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\clean_history.ps1 -DryRun   # 预览将删的 run，不实际删
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\clean_history.ps1 -Before 20260801   # 删 8/1 之前所有 run
#   powershell -NoProfile -ExecutionPolicy Bypass -File scripts\clean_history.ps1 -Force   # 默认建议方式，跳过确认直接删
# 注意：PS 5.1 的 [CmdletBinding()]+switch 参数在 -File 调用下偶发 AmbiguousParameterSet，这里手动解析 $args。
$DryRun = $false
$Force = $false
$Before = ""
foreach ($arg in $args) {
    if ($arg -eq "-DryRun" -or $arg -eq "-dryrun") { $DryRun = $true }
    if ($arg -eq "-Force" -or $arg -eq "-force") { $Force = $true }
    if ($arg -eq "-Before" -or $arg -eq "-before") { $BeforeMode = $true; continue }
    if ($BeforeMode) { $Before = $arg; $BeforeMode = $false }
}
# 归一 -Before 到可比较前缀：20260801 或 20260801_000000 都行，补全到 15 位便于字典序比较
if ($Before) {
    $Before = ($Before -replace "[^0-9_]", "")
    if ($Before -notmatch "^\d{8}(_\d{6})?$") {
        Write-Host "[错误] -Before 需为 YYYYMMDD 或 YYYYMMDD_HHMMSS，收到：$Before" -ForegroundColor Red
        exit 1
    }
    if ($Before.Length -eq 8) { $Before += "_000000" }
}
$ErrorActionPreference = "Stop"

$Root = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$OutputDir = Join-Path $Root "pipeline_output"
$DB = Join-Path (Join-Path $OutputDir "data") "main_agent.db"
$runsDir = Join-Path $OutputDir "runs"

# 路径护栏：只允许清理项目根下的 pipeline_output，防脚本被挪走时误删别的目录。
# 叶子目录用 .NET GetFileName 取（Split-Path 的 -LiteralPath 与 -Leaf 属不同参数集，混用报 AmbiguousParameterSet）。
if ([System.IO.Path]::GetFileName($OutputDir) -ne "pipeline_output") {
    Write-Host "[错误] 输出目录不是 pipeline_output：$OutputDir" -ForegroundColor Red
    exit 1
}
if (-not (Test-Path -LiteralPath $DB -PathType Leaf)) {
    Write-Host "未找到台账 $DB，无需清理。"
    exit 0
}
if (-not (Get-Command sqlite3 -ErrorAction SilentlyContinue)) {
    Write-Host "[错误] 找不到 sqlite3 命令，无法清理。请先安装 sqlite3 或改用整摊清理脚本。" -ForegroundColor Red
    exit 1
}

# 单实例锁：流水线还在跑时禁止清理（锁里的 PID 存活即中止）
$lockFile = Join-Path $OutputDir "main_agent.lock"
if (Test-Path -LiteralPath $lockFile -PathType Leaf) {
    try {
        $lockPid = [int](Get-Content -LiteralPath $lockFile -Raw).Trim()
        if (Get-Process -Id $lockPid -ErrorAction SilentlyContinue) {
            Write-Host "[错误] main_agent 正在运行（PID $lockPid）。请先关闭流水线再清理。" -ForegroundColor Red
            exit 1
        }
    }
    catch { }
}

# 待删 run 集合，按模式分叉：
if ($Before) {
    # -Before 模式：删 run_id 前缀 < Before 的所有 run（不管是否每企业最新）
    $allRuns = @(& sqlite3 "$DB" "SELECT run_id FROM runs;" 2>&1 | Where-Object { $_ -and $_.Trim() })
    $deleteRuns = @($allRuns | Where-Object {
        $prefix = ($_ -split "_")[0..1] -join "_"   # 取 YYYYMMDD_HHMMSS
        $prefix -lt $Before
    })
    $keepRuns = @($allRuns | Where-Object { $_ -notin $deleteRuns })
    Write-Host "时间阈值：删除 $Before 之前启动的批次"
    Write-Host "将删除 $($deleteRuns.Count) 个 run，保留 $($keepRuns.Count) 个 run"
}
else {
    # 默认建议方式：保留集 = 每企业最新批次（与 export_history_latest_excel.py 同口径），删其余
    $keepRuns = @()
    if (Test-Path -LiteralPath $runsDir -PathType Container) {
        $runDirs = @(Get-ChildItem -LiteralPath $runsDir -Directory | Sort-Object Name -Descending)
        $seenCompany = @{}
        foreach ($runDir in $runDirs) {
            $rid = $runDir.Name
            $exportDir = Join-Path $runDir.FullName "exports"
            if (-not (Test-Path -LiteralPath $exportDir -PathType Container)) { continue }
            foreach ($entry in Get-ChildItem -LiteralPath $exportDir -File -Filter "*.json") {
                $data = $null
                try { $data = Get-Content -LiteralPath $entry.FullName -Raw -Encoding UTF8 | ConvertFrom-Json } catch { }
                if (-not $data) { continue }
                $completed = @($data.completed)
                if ($data.status -eq "INCOMPLETE" -and $completed.Count -eq 0) { continue }
                $name = [string]$data.company
                if (-not $name) { $name = [System.IO.Path]::GetFileNameWithoutExtension($entry.Name) }
                $key = ($name -replace "[\s()（）_\-]", "").ToLowerInvariant()
                if (-not $seenCompany.ContainsKey($key)) {
                    $seenCompany[$key] = $rid
                    $keepRuns += $rid
                }
            }
        }
    }
    $keepRuns = @($keepRuns | Select-Object -Unique)
    if ($keepRuns.Count -eq 0) {
        Write-Host "未在 runs/ 下找到可判定保留的导出，终止（避免误删全部）。" -ForegroundColor Yellow
        exit 1
    }
    $allRuns = @(& sqlite3 "$DB" "SELECT run_id FROM runs;" 2>&1 | Where-Object { $_ -and $_.Trim() })
    $deleteRuns = @($allRuns | Where-Object { $_ -notin $keepRuns })
    Write-Host "保留（每企业最新批次）：$($keepRuns.Count) 个 run"
    Write-Host "将删除旧批次：$($deleteRuns.Count) 个 run"
}
$deleteRuns | ForEach-Object { Write-Host "  - $_" }

if ($DryRun) {
    Write-Host "[DryRun] 预览结束，未执行删除。"
    exit 0
}
if (-not $Force) {
    $answer = Read-Host "确认删除以上 $($deleteRuns.Count) 个旧 run？输入 y 继续"
    if ($answer -ine "y") { Write-Host "已取消。"; exit 0 }
}

# DB 删除：拼 DELETE SQL，按依赖顺序删（child → parent）。enterprise_aliases 跨 run，保留。
$placeholders = ($deleteRuns | ForEach-Object { "'$_'" }) -join ","
if ($placeholders) {
    $sql = @"
PRAGMA foreign_keys=OFF;
DELETE FROM attempt_output_items WHERE attempt_id IN (SELECT attempt_id FROM task_attempts WHERE task_id IN (SELECT task_id FROM tasks WHERE run_id IN ($placeholders)));
DELETE FROM resource_leases WHERE task_id IN (SELECT task_id FROM tasks WHERE run_id IN ($placeholders));
DELETE FROM resource_waiters WHERE task_id IN (SELECT task_id FROM tasks WHERE run_id IN ($placeholders));
DELETE FROM events WHERE (scope='run' AND target_id IN ($placeholders)) OR (scope='task' AND target_id IN (SELECT task_id FROM tasks WHERE run_id IN ($placeholders)));
DELETE FROM task_attempts WHERE task_id IN (SELECT task_id FROM tasks WHERE run_id IN ($placeholders));
DELETE FROM tasks WHERE run_id IN ($placeholders);
DELETE FROM companies WHERE run_id IN ($placeholders);
DELETE FROM control_commands WHERE run_id IN ($placeholders);
DELETE FROM runs WHERE run_id IN ($placeholders);
"@
    # sqlite3 需要换行符分隔语句；写临时 SQL 文件，用 stdin 管道喂入，避免 .read 的路径转义坑
    # （PS 里 .read "C:\Users\..." 的反斜杠会被吃掉，sqlite3 打不开文件）。
    $sqlFile = Join-Path $env:TEMP ("clean_history_" + [guid]::NewGuid().ToString("N") + ".sql")
    [System.IO.File]::WriteAllText($sqlFile, $sql, [System.Text.Encoding]::UTF8)
    try {
        Get-Content -LiteralPath $sqlFile -Raw | & sqlite3 "$DB"
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[错误] 删除台账失败（exit $LASTEXITCODE）。DB 未删干净。" -ForegroundColor Red
            exit 1
        }
    }
    finally {
        Remove-Item -LiteralPath $sqlFile -Force -ErrorAction SilentlyContinue
    }
}

# DB 删除成功后删磁盘产物（先 DB 后磁盘：DB 删失败不会留"磁盘已清、台账有孤儿"的不一致）
foreach ($runId in $deleteRuns) {
    $dir = Join-Path $runsDir $runId
    if (Test-Path -LiteralPath $dir -PathType Container) {
        Remove-Item -LiteralPath $dir -Recurse -Force
    }
}

if ($Before) {
    Write-Host "已删除 $($deleteRuns.Count) 个 $Before 之前的旧批次，保留 $($keepRuns.Count) 个批次。" -ForegroundColor Green
}
else {
    Write-Host "已删除 $($deleteRuns.Count) 个旧批次，保留 $($keepRuns.Count) 个最新批次。历史数据保留每企业最新，导出脚本可正常读取。" -ForegroundColor Green
}
exit 0
