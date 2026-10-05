# AGENTS.md — 维护者须知

给接手这个项目的 agent / 维护者的契约说明。先读这一份，再动代码。

## 项目是什么

黄金/珠宝企业网络公开信息风险排查流水线。`run.bat` 双击启动 `src/control/main_agent.py`，一家公司一条完整链路：ICP 备案 → 官网搜索/抓图 → OCR → LLM 关键词 → 社媒账号发现/初筛/复核/定级 → 作品抓取 → 风险判定 → 证据截图 → 导出。调度、重试、登录刷新、资源节流全部在 `main_agent.py` 里。

技术栈：Python 3.11（自带 `.venv`），SQLite（`pipeline_output/data/main_agent.db`），PaddleOCR，Playwright。社媒依赖登录态（`src/social/*_cookies.json`，`*_login_refresh.py` 扫码生成）。

## 分目录

| 目录 | 作用 | 约定 |
| --- | --- | --- |
| `src/control/main_agent.py` | 大脑，公司级状态机 + 调度 | 改了要和 `pipeline_store.py` / 各原子脚本对上 |
| `src/control/pipeline_control.py` | 运行时 skip 指令 CLI | 见下方「pipeline_control 的坑」 |
| `src/pipeline_store.py` | 控制平面 SQLite 层 | 全项目对 DB 的唯一写入口，改 schema 前看「红线」 |
| `src/control/` 目录（`pipeline_runtime.py` 等） | 诊断/指纹/输出工具 | 通用、可被多个模块引用，改动影响面大 |
| `src/crawl/` `src/social/` `src/ocr/` `src/icp/` `src/analysis/` | 各原子脚本 | 见「原子脚本」 |
| `scripts/` | 服务启停（`open_*_service.ps1` / `close_*`）与报告导出 | 服务脚本小心别 kill 错进程 |
| `scripts/report/` | 只读查询/报告脚本 | 只读 DB 与产物，别改 schema |
| `tests/` | pytest | 改动后跑 `pytest tests/ -x` |
| `.workbuddy/skills/` | WorkBuddy 查询技能 | 供代理查询，别当成运行入口改坏 |

## 红线（改了会崩，必须全局同步）

这些是跨模块的隐式契约，workbuddy 接手后最容易踩的坑。改任何一处，先确认它被谁引用。

1. **stderr 分类契约**：`main_agent.py` 的 `STDERR_CLASSIFIERS`（约 317 行）按文本匹配原子脚本 stderr 判定任务状态。原子脚本改报错措辞前，必须同步这张表——改错一个词，重试/登录刷新/终态判定全错。
2. **request_fingerprint**：`pipeline_runtime.py` 的 `request_fingerprint` 对请求 payload 做哈希，replay（`replay.bat`）靠它复用历史结果。改 payload 字段名、删除/重命名 key，会让所有旧批次指纹失配，replay 全部失效。**禁止改 payload 结构。**
3. **DB schema**：`pipeline_store.py` 建表 + `scripts/report/` 直接写 SQL 读。改表名/列名/表结构，会崩掉所有查询和导出脚本。新增列走 `_ensure_column` 迁移。
4. **阶段状态机**：`main_agent.py` 的 `PHASES` 顺序、`PLATFORM_SPECS`（三平台差异）、`ICP_CARRIERS`，是全部 fan-out 的派生来源。改阶段名/平台字段，必须三平台一致、四类载体一致。
5. **result.json 的 status 取值**：`canonical_attempt_diagnosis`（`pipeline_runtime.py`）只认 `ok/empty/empty_success/login_required/cookie_invalid/rate_limited/captcha_required/timeout/error/failed/final_error/missing_output`。原子脚本写 `status` 必须用这些词，乱写会被误判成成功或失败。
6. **argparse 契约**：`_command_for`（`main_agent.py`）给每个原子脚本造命令，参数必须和脚本自己的 argparse 一一对上。对不上 = `invalid_input` 缺陷，写进 `defects.md` 工单，整条任务 4 次重试白费。

## 原子脚本约定

每个原子脚本是独立子进程，由 `main_agent.py` 用固定 argv 启动，用固定 JSON 键写 `result.json`。

- 必须支持 `--output-file`（输出 result.json）、`--output-dir`。
- `result.json` 用 `pipeline_runtime.atomic_result()` 包一层 `diagnosis` 字段（status/error_code/reason）。
- 不要自己 `sys.exit` 特殊码；失败写 `status: error` + stderr 说明，主控自己判重试。
- 改任何脚本前，先 grep 它的调用方（`_command_for`、`PLATFORM_SPECS`、`_SCRIPT_SUBDIRS`）确认参数。

## pipeline_control 的坑

`pipeline_control.py` 是运行时 skip 指令（`--db` 默认指向项目根 `pipeline_output/data/main_agent.db`，不要用 `__file__.with_name` 算路径——那会指向 `src/control/pipeline_output` 建出空库）。它只写 `control_commands` 表，主流程轮询后执行。

## 历史数据与产物

- `pipeline_output/data/main_agent.db` 是权威台账（tasks / task_attempts / attempt_output_items / events 等）。主流程结束时 `tasks.result_json` 与 `task_attempts.summary_json` 留空，权威数据在 `attempt_output_items.data_json`，读取走 `_resolve_*` 回退。**别改成三份字节级副本**——会撑爆 DB。
- 产物路径约定：`pipeline_output/runs/<run_id>/companies/<企业名>/<PHASE>/task_*/attempt_*/`。`final.json` 是调试产物，成功判据以 DB 为准，别 glob final.json。
- 删除 `pipeline_output/` 不影响重新运行，只是失去历史复用和查询。

## 风格

精简高效，无冗余。改代码前先看周围代码的风格，保持一致。中文注释，解释「为什么」而不是「做了什么」。加功能前先问：能不能用已有机制（去重 seen、lane 熔断、history 复用）实现？
