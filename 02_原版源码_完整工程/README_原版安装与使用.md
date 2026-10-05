# 原版完整工程 · 安装与使用说明（脱敏）

本目录是本次笔试产品背后的**真实工程源码**：一套已经在实习场景跑通的
**黄金/珠宝企业网络公开信息风险排查流水线**。它不是演示壳子——`01_可运行Web产品_离线演示/`
的看板界面、阶段状态机、证据字段与数据结构，都来自这里。

完整安装与使用步骤以本目录的《**安装使用指南.pdf**》为准；本文件补充「本提交包做了什么脱敏/裁剪」与「如何补回被裁掉的大文件」。

---

## 一、它是什么

入口 `run.bat`（或看板 `dashboard.bat`）→ `src/control/main_agent.py`，对每家企业执行一条完整链路：

```
ICP 备案查询 → 官网搜索/抓图 → OCR → LLM 关键词筛查
→ 社媒账号发现/初筛/人工复核/身份核验 → 作品内容抓取
→ 风险判定 → 证据截图 → 报告/Excel/Word 导出
```

- 技术栈：Python 3.11（自带 `.venv`）、SQLite 控制平面、PaddleOCR、Playwright；
- 调度、重试、登录刷新、资源节流都在 `src/control/main_agent.py`；
- 产物：`pipeline_output/runs/<run_id>/companies/<企业>/<PHASE>/task_*/attempt_*/`；
- 关键契约见 `AGENTS.md`。

## 二、安装（Windows，详见 PDF 指南）

1. 把本目录解压到**没有中文和空格**的路径，例如 `D:\icbc_gold`；
2. 双击 `install.bat`：检测/静默安装 Python 3.11.9 → 创建 `.venv` → 装依赖 → 装 Playwright Chromium
   → 从模板生成 `config/llm_config.txt` 与 `config/companies.txt` → 自检 ICP(16181)/OCR(16191) 服务；
   看到「安装完成，全部自检通过！」即成功；
3. 配置（两个文件，`config/` 下）：
   - `companies.txt`：每行一个企业全称（UTF-8）；
   - `llm_config.txt`：三行 `base_url=` / `api_key=` / `model=`（LLM 是关键词生成与风险判定的必需项）；
4. 运行：双击 `run.bat`，或双击 `dashboard.bat` 在 `http://127.0.0.1:18080` 看板上点「开始排查」；
5. 抓社媒前扫码：`refresh_dy_login.bat` / `refresh_ks_login.bat` / `refresh_xhs_login.bat`；
6. 查结果/导出：用 WorkBuddy 打开本目录，直接中文提问（技能在 `.workbuddy/skills/pipeline-query`）。

环境要求：Windows 10/11 64 位，预留约 10GB 磁盘。

## 三、本提交包做的脱敏与裁剪

为满足「不得提交 API Key、用户隐私、受限数据」，并受单压缩包 ≤30MB 限制，本副本相对原始工程做了如下处理：

**已删除（隐私/密钥）**
1. 社媒登录态：`src/social/{douyin,kuaishou,xhs}_cookies.json` 已全部移除；
2. 真实企业名单：`config/companies*.txt`、`replay_companies.txt` 中的真实企业名已替换为「示例企业A/B…」；
3. 运行产物：`pipeline_output/`、`outputs/`、日志、`*.db`（含 `ICP_Query/icp_history.db`）未包含。

**已剔除（超体积，可用脚本/上游恢复）**

| 被剔除项 | 体积 | 如何恢复 |
|---|---|---|
| `vendor/whl/`（离线依赖轮子） | ~365MB | 联网 `install.bat` 直接 pip 安装；或从原始完整包复制；或换国内镜像重跑 `scripts/install.ps1 -Mirror https://pypi.tuna.tsinghua.edu.cn/simple` |
| `ICP_Query/model_data/*.onnx`（验证码识别模型） | ~94MB | 从上游 `HG-ha/ICP_Query` Releases 下载，放回 `ICP_Query/model_data/` |
| `ICP_Query/.git` | ~90MB | 无需，源码已收录 |
| `.venv/` | ~1GB | `install.bat` 自动创建 |

**保留**
- `src/`、`scripts/`、`dashboard/`、`ICP_Query/src|static|templates`、`.workbuddy/skills/`、`*.bat`、`*.ps1`、`requirements.txt`、`AGENTS.md`、`config/*.example.txt`。

## 四、安全提示

- `config/llm_config.txt` 含 API Key，**外发前务必删除**（删掉后重跑 `install.bat` 会从模板重建）；
- `ICP_Query/config.yml` 中的 `admin/admin123`、`secret: change-me` 是**上游第三方默认值**，启用其 WebUI 前必须修改；
- 本包中的 `config/` 只放模板；请勿把填好 Key 的文件提交到任何仓库。

## 五、目录

```
02_原版源码_完整工程/
├─ 安装使用指南.pdf
├─ README_原版安装与使用.md（本文件）
├─ AGENTS.md                # 原始工程契约（跨模块红线）
├─ requirements.txt         # 真实依赖（离线演示产品不需要）
├─ install.bat / run.bat / dashboard.bat / replay.bat / clean_history.bat
├─ refresh_dy_login.bat / refresh_ks_login.bat / refresh_xhs_login.bat
├─ src/                     # control 状态机、crawl、ocr、icp、social、analysis、llm_client、pipeline_store
├─ scripts/                 # 服务启停 + report/ 只读查询与导出
├─ dashboard/               # 原始看板 server.py + index.html + static/app.js
├─ config/                  # 仅 example 模板
├─ ICP_Query/               # ICP 备案查询子工程（第三方开源，代码已收录）
└─ .workbuddy/skills/       # WorkBuddy 查询技能
```

## 六、与演示产品的关系

| 原版引擎 | 演示产品中的体现 |
|---|---|
| 12 阶段状态机（`PHASES`） | 驾驶舱「开始排查」按同序推进 |
| 企业风险字段（`enterprise_assessment`） | 企业详情「风险」「AI 证据链」标签 |
| ICP/官网/社媒多源证据 | 证据卡的来源、时点、状态 |
| 失败重试与终态判定 | 缺失=`unavailable`、模型不可用=降级标注 |
| 报告/证据导出 | 证据可回溯 + 审计事件 |
