---
name: pipeline-query
description: 查询 icbc_gold_final 黄金企业网络载体风险排查流水线的运行状态与结果。当用户询问批次/运行进度、企业排查完成情况与当前阶段、ICP 备案载体、官网抓取、社媒账号发现复核与活跃度、作品抓取、风险判定结论（风险词/黄金回购/S1-S5）、任务失败重试原因、审计事件、证据截图位置，或要求汇总最新批次结果、写企业排查报告、自动生成/导出所有企业「历史最新批次」的 Excel 总表时使用。
agent_created: true
---

# 流水线状态与结果查询（WorkBuddy 版）

> 本 skill 由 `.codex/skills/pipeline-query` 移植。WorkBuddy 适配：
> - 脚本调用统一用 bash 形式：`.venv/Scripts/python.exe scripts/report/xxx.py`（项目自带 `.venv`，已含 openpyxl / python-docx）。
> - `agents/openai.yaml` 的 `display_name / short_description / default_prompt` 已并入本文件：显示名「流水线状态查询」，开场提示「查询最新批次的企业排查状态与结果」。
> - 按用户要求，已移除"历史重跑/重建"与"内置口径与约定（数据位置索引、关键约定）"两块内容。

项目：`icbc_gold_final`（黄金/珠宝企业网络公开信息风险排查流水线：ICP → 官网抓取 → OCR → 关键词 → 社媒账号发现/复核/定级 → 作品抓取 → 风险判定 → 证据截图 → 导出）。

## 数据在哪（速查）

| 信息 | 位置 |
| --- | --- |
| 批次/企业/任务/事件台账 | `pipeline_output/data/main_agent.db`（SQLite） |
| 单企业最终结论（首选） | `pipeline_output/runs/<run_id>/exports/<企业名>.json` |
| 任务级原始产物 | `pipeline_output/runs/<run_id>/companies/<企业名>/<PHASE>/task_*/attempt_*/final.json`（+ result.json / payload.json） |
| 批次摘要 | `pipeline_output/runs/<run_id>/manifest.json` |
| 详细数据地图 | 各 `scripts/report/*.py` 的 `--help` 与源码 docstring（本项目未单独维护数据字典文档） |

## 提取脚本（scripts/report/；只读）

统一约定：`--run <run_id|latest>`（默认 latest = 最新批次）；`--company <名称>` 支持子串匹配；输出 JSON（部分支持 `--format md|csv`）。从项目根目录运行。

```bash
.venv/Scripts/python.exe scripts/report/extract_runs.py --format md
.venv/Scripts/python.exe scripts/report/extract_run_status.py --run latest
.venv/Scripts/python.exe scripts/report/extract_batch_table.py --run latest --format md
.venv/Scripts/python.exe scripts/report/extract_company.py --run latest --company 金雅福 --section risk
.venv/Scripts/python.exe scripts/report/extract_phase.py --run latest --company 金雅福 --phase WEB_ICP
.venv/Scripts/python.exe scripts/report/extract_task_io.py --run latest --company 金雅福 --phase RISK_ANALYSIS
.venv/Scripts/python.exe scripts/report/extract_audit.py --run latest --failed
.venv/Scripts/python.exe scripts/report/extract_events.py --run latest --tail 20
```

Excel 总表导出（口径不同于上述 JSON 脚本：按企业取历史最新批次，不接 `--run/--company`）：

```bash
.venv/Scripts/python.exe scripts/report/export_history_latest_excel.py [--out <输出路径.xlsx>]
```

**导出路径约定（Excel 与 Word 统一）**：所有导出默认放到 `outputs/<时间戳>/` 目录下（时间戳格式 `YYYYmmdd_HHMMSS`，如 `outputs/20260821_064916/`），由脚本在运行时刻自动生成，不覆盖旧文件。只有用户显式要求指定路径时，才用 `--out / --out-dir` 覆盖。

Excel 总表默认输出 `outputs/<时间戳>/所有企业历史最新批次结果_带来源.xlsx`：官网/app/小程序/各平台账号/业务/黄金业务/风险词/风险现象 9 块，每块带「数据来源批次」「数据来源运行时间」两列——HISTORY_REUSE 任务标其 source_run_id，新跑任务标本批次，多来源按时间旧→新以 `;` 分隔。

截图证据 Word 批量导出（每企业一个 docx）：

```bash
.venv/Scripts/python.exe scripts/report/batch_evidence_docx.py [--companies 金雅福,简戒] [--companies-file 名单.txt] [--sections website,social,risk] [--run <run_id>] [--out-dir <目录>]
```

**使用前必问用户**（不要默认全量/全选）：① 哪些企业——全量，还是指定名单（给企业名或文件）；② 要哪些分区——`website`（官网截图 + 官网图片 OCR 文字）、`social`（社媒搜索页 + 账号主页截图，附复核理由/主页 URL）、`risk`（风险证据页截图 + 命中关键词/页面原文/判定），三选一/二/三。用户未说全时逐项确认，缺省才回落到全量+全选。

默认输出到 `outputs/<时间戳>/evidence_docx/`，企业名支持子串匹配。

口径：截图/OCR 按「每个阶段取历史最新一批真实产物」跨所有批次扫文件系统（官网=WEB_CRAWL、搜索页=ACCOUNT_DISCOVERY、主页=ACCOUNT_REVIEW、风险证据=RISK_EVIDENCE、OCR=OCR），规避空壳 exports 漏数据；LLM 结论文字（摘要/回购/关键词/账号复核/风险判定）取自该企业历史最新 exports JSON。`--run` 可锁定企业清单到单批次，但截图仍跨批次取最新。

## 提问 → 用什么（映射表）

| 用户问什么 | 用什么 |
| --- | --- |
| 有哪些批次/哪个最新 | `extract_runs.py` |
| 批次进度、企业完成数、任务统计 | `extract_run_status.py` |
| **最新/进行中批次已经找到了什么（网络载体、账号候选等中间结果）** | 先 `extract_run_status.py` 看哪些阶段已 TERMINAL，再逐阶段 `extract_phase.py --phase WEB_ICP / WEB_SEARCH / WEB_CRAWL / ACCOUNT_DISCOVERY / ACCOUNT_RANK ... --kind summary\|full`。批次未跑完 ≠ 没数据：任一阶段任务 TERMINAL 即可查（详见下方「批次未完成时怎么查」） |
| 汇总所有企业结果（表） | `extract_batch_table.py --format md`（列含 ICP 载体数、候选官网数、互联网载体候选数、活跃域名、社媒账号数、有风险、回购线索、风险词） |
| 导出所有企业「历史最新批次」结果 + 每块数据来源批次标注（Excel） | `export_history_latest_excel.py [--out 路径]`（默认输出到 `outputs/<时间戳>/`；按企业取最新批次，官网/app/小程序/账号/业务/黄金/风险 9 块各带来源批次与运行时间，复用任务标 source_run_id） |
| 批量导出企业截图证据 Word（每企业一个 docx，含官网/社媒/风险截图 + OCR 文字 + 证据解析 + URL） | `batch_evidence_docx.py [--companies 名单] [--sections website,social,risk] [--out-dir 目录]`（默认输出到 `outputs/<时间戳>/evidence_docx/`） |
| 某企业完整结论 | `extract_company.py --company <名>`；分节用 `--section icp|website|social|activity|risk|evidence|defects|completed|raw` |
| 某企业某阶段原始数据 | `extract_phase.py --company <名> --phase <PHASE> [--kind summary|posts|corpus|full]` |
| 某任务输入输出（payload/result） | `extract_task_io.py --company <名> --phase <PHASE> [--round <n|last>]` |
| 失败/重试/错误分布 | `extract_audit.py --failed | --retried [--detail] [--phase X]` |
| 审计事件/过程还原 | `extract_events.py [--event attempt_finished] [--company 名] [--tail N]` |

## 业务术语 → 阶段名映射（社媒账号）

用户口语里的"初筛/精筛/终筛"对应流水线阶段（显示名见 `src/control/main_agent.py` 的 PHASE 显示名表）：

| 用户说法 | 阶段（--phase） | 选中账号在哪 |
| --- | --- | --- |
| 初筛（账号候选初筛） | `ACCOUNT_RANK` | `extract_phase.py --phase ACCOUNT_RANK --kind summary` → `summary.results.<平台>.top_accounts`，其中 `llm_selected=true` 的为初筛选中（`final_shortlist_count` 为各平台选中数） |
| 精筛/终筛（账号复核） | `ACCOUNT_REVIEW` | 每个候选账号一个任务，`summary.diagnosis` 是逐账号终端判定；批次级结论汇总在 exports 的 `social_account_verifications`（`extract_company.py --section social` 的 `decision`/`official_status` 字段） |
| 账号确认 | `ACCOUNT_VERIFY` | 复核后的确认环节，任务量很小 |

注意：
- 问"最新批次的初筛/终筛名单"或要**终稿结论**（风险判定、账号复核 decision、exports 字段）时，先 `extract_runs.py` 确认批次状态——`latest` 可能指向仍在 RUNNING、账号阶段大部分 PENDING 的批次；此类问题通常应选最近的 COMPLETED/INCOMPLETE 但账号阶段已跑完的批次。
- **批次未完成时怎么查（重要，勿因批次 INCOMPLETE 就退回旧批次）**：企业未 CLOSED / 无 exports JSON 时，`extract_batch_table.py` 与 `extract_company.py` 返回空表——空表 ≠ "没查到任何东西"，只是"尚未导出终稿"。此时改用 `extract_phase.py` / `extract_task_io.py` 直接读 latest 批次里已 TERMINAL 阶段的中间产物：WEB_ICP（备案载体，看 `records`）、WEB_SEARCH（候选官网，看 `candidates`/`carrier_candidates` 的 `domain`）、WEB_CRAWL（官网抓取文本 + `domain_activity` 活跃度）、ACCOUNT_DISCOVERY（各平台账号候选 `accounts`，看昵称/认证/sec_uid）、ACCOUNT_RANK（初筛）等**全部支持中途查看，与账号阶段无关**。查"找到了什么网络载体/官网/账号"类问题，即使批次 RUNNING 也照此办理；只有用户明确要"最近有结果的批次"或需要终稿结论时，才回退选最近的 COMPLETED 批次。

## ID 口径与对账（出账号名单前必读）

各阶段产物里一个账号有多套 ID，口径不同，混用会导致"同一个账号两表对不上"：

| 字段 | 含义 | 可信度 |
| --- | --- | --- |
| `account_id` | 平台公开 ID（抖音号/小红书号 red_id/快手号） | 抖音侧**不可信**：已知被搜索关键词、粉丝数文本污染（如 `jianjie608` 撞 3 个主页、`353204687242.6万`）；小红书/快手侧基本可信 |
| `user_id` | 平台内部 ID（抖音 sec_uid、小红书 24 位 hex、快手 kwai 号） | 可信，是跨阶段定位同一账号的锚点 |
| `red_id` / `douyin_id` 等 | 平台公开 ID 的原始字段 | 同 `account_id` |

规则：
- 抖音账号定位一律用 sec_uid（`user_id`），不要信 `account_id`。
- 出名单前先与别名台账对账：`enterprise_aliases` 表中 `alias_type='platform_account_id' AND is_confirmed=1` 的记录是业务方确认过的公开 ID。候选账号的公开 ID 与 confirmed ID 不一致时，必须标注"与台账确认 ID 不符，待人工核实"，不能静默采用抓取值。
- 对不上时的排查路径：找到该企业按该 confirmed ID 搜索的 `ACCOUNT_DISCOVERY` 任务（`payload_json` 里 `keyword=<confirmed_id>`），看其 `accounts` 是否为空、以及 `screenshots/search.jpg`——平台按 ID 搜用户可能零召回（小红书已实测：搜小红书号显示"没找到相关用户"），此时正确账号根本不在候选池，属召回问题而非提取错误。
- 初筛名单的完整口径 = `top_accounts` 全体（`llm_selected=true` 的 LLM 选中 + `selection_source=deterministic_protection` 的规则保护入围），`llm_selected=false` 不代表落选。

## 回答格式

- 状态类问题：先给批次/企业状态一句话结论，再给关键数字（表），最后注明异常（失败任务、LLM fallback）。
- 结果类问题：直接引用 `exports` JSON 字段（ICP 载体、账号复核、风险结论），数据取自哪个 run、生成时间一并给出。
- 引用证据/截图时，用 `exports` 里 `artifacts`/`screenshots` 的相对路径拼出绝对路径（以对应 run 目录为基准）。
- 需要原始输入输出佐证时用 `extract_task_io.py`，说明输入（payload）是什么、输出（result）关键字段是什么。

## 示例

问："最新批次跑完了吗？" → `extract_run_status.py`，答：批次 ID、CLOSED/OPEN 企业数、各阶段任务分布、失败任务数。

问："金雅福查得怎么样，有没有风险？" → `extract_company.py --company 金雅福 --section risk`，答：overall_risk_found、executive_summary、回购线索、S1-S5 结论；如 fallback 注明"LLM 未完成语义判定"。

问："有哪些任务失败了？" → `extract_audit.py --failed --detail`，答：按错误码归因（如 `deterministic_dns_error` 域名失效、`target_identity_mismatch` 账号身份不符）。

问："最新批次找到了什么网络载体？"（批次可能未跑完）→ `extract_run_status.py --run latest` 看哪些阶段已 TERMINAL → 对已 TERMINAL 的阶段逐个 `extract_phase.py --run latest --company <名> --phase WEB_ICP / WEB_SEARCH / WEB_CRAWL / ACCOUNT_DISCOVERY --kind summary`，答：ICP 备案记录数（`records`）、候选官网（`candidates` 的 `domain`）、抓取结果与 `domain_activity`、各平台账号候选（`accounts` 的昵称/认证信息/sec_uid）；务必注明"批次未完成，以上为中间结果，未终稿"。
