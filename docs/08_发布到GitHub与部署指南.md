# 08 · 发布到 GitHub 与部署公网 URL（逐步操作）

> 题面要求「源代码仓库与 README」以及「可访问的 Web 产品 URL」。
> 本文手把手教你把本包发布到 GitHub，并（可选）部署成公网 URL。
> 前置：本机已安装 Git（你已装 2.51），GitHub 账号（没有就现注册）。

---

## 第 0 步：上传前自查（很重要）

在提交文件夹里执行下面命令，**确认没有把 Key / cookie / 名单提交上去**：

```bash
cd "D:/Workspace/shelter/GoldGuard_AI_Native_笔试提交"
git status --short                 # 看看将要提交的文件
grep -rIn -E "sk-[A-Za-z0-9]{12,}|api_key=[^<[:space:]]" . --exclude-dir=.git || echo "无明文密钥"
```

`.gitignore` 已忽略 `**/llm_config.txt`、`**/*_cookies.json`、`**/*.db`、`**/.venv/` 等。
本包已删除真实企业名与社媒登录态，可直接公开。

---

## 第 1 步：在 GitHub 网页新建空仓库

1. 打开 https://github.com/new （先登录）；
2. **Repository name**：填 `goldguard-ai`（或 `GoldGuard-AI-Native`，随意）；
3. **Description**：填 `面向黄金产业链的 AI 原生风险雷达与合规证据引擎（笔试提交）`；
4. **Public / Private**：
   - 想省事、让阅卷人直接点开 → 选 **Public**（本包已脱敏，推荐）；
   - 想保密 → 选 **Private**，但必须把阅卷人加为 Collaborator，或提交时临时改公开；
5. **不要**勾选 “Add a README / .gitignore / license”（我们本地已有文件，勾了会冲突）；
6. 点 **Create repository**。

创建后会看到一个空仓库页面，里面有 `https://github.com/<你的用户名>/goldguard-ai.git` 这个地址，记下来。

---

## 第 2 步：本地初始化并推送

打开 **Git Bash** 或 PowerShell，逐行执行：

```bash
cd "D:/Workspace/shelter/GoldGuard_AI_Native_笔试提交"

git init
git add -A
git commit -m "GoldGuard AI：AI Native 投资产品创新笔试提交"

git branch -M main
git remote add origin https://github.com/<你的用户名>/goldguard-ai.git
git push -u origin main
```

第一次 `push` 时，Git Credential Manager 会弹出一个浏览器窗口让你登录 GitHub 授权（或要求输入用户名 + Personal Access Token）。
授权成功后代码就上去了，刷新 GitHub 页面即可看到。

> 如果你在 `shelter` 这个更大的仓库里执行 `git status`，会看到本文件夹是一个嵌套仓库，属正常现象；**只在本文件夹内做 git 操作即可**，不要在 `shelter` 里 `git add` 它。

---

## 第 3 步（可选）：给仓库加一个好封面

1. 进入仓库 → 右上 **Add file → Create new file**，文件名填 `README.md` 已存在，无需新建（我们的 README 会直接显示）；
2. 在仓库 **About** 右侧齿轮里填 **Description** 和 **Website**（填第 4 步拿到的公网 URL）；
3. **Topics** 建议：`ai-native`、`fintech`、`risk-control`、`gold`、`llm-agent`。

---

## 第 3.5 步（最省事）：用 GitHub Pages 发布静态版，零额外账号

仓库里已经带了纯静态版（`site/`）和一个自动部署工作流（`.github/workflows/pages.yml`）。你只需开启一次：

1. 打开仓库 **Settings → Pages**；
2. **Build and deployment → Source** 选 **GitHub Actions**（不要选 Deploy from a branch）；
3. 回到 **Actions** 标签页，找到 “Deploy GoldGuard static demo to GitHub Pages”，点 **Run workflow**（或随便 push 一次触发）；
4. 等 1 分钟，访问：<https://liangjiayi666.github.io/goldguard-ai/> 。

这就是可提交的公网 Web 产品 URL。静态版用 `ui/mock-api.js` 在浏览器内模拟 `/api/*`，
“开始排查 / 人工复核 / AI 证据链”均可操作。

> 重新生成静态版：`cd 01_可运行Web产品_离线演示 && python tools/build_static_site.py && node tests/test_mock_api.cjs`

---

## 第 4 步（可选）：用 Render 部署带后端的版本

因为产品零依赖、只监听端口，用 **Render 免费档**最省事：

1. 打开 https://render.com ，用 GitHub 账号登录（Sign in with GitHub）；
2. **New → Web Service**；
3. 选择刚推上去的 `goldguard-ai` 仓库 → **Connect**；
4. 配置：
   - **Language / Environment**：`Docker`（仓库根目录已有 `Dockerfile`，会自动识别）；
   - 其他保持默认；
5. 点 **Create Web Service**，等 1–2 分钟构建完成；
6. 顶部会给出形如 `https://goldguard-ai-xxxx.onrender.com` 的公网 URL —— **这就是可提交给阅卷人的 Web 产品 URL**。

把该 URL 填回：
- GitHub 仓库 About 的 Website；
- `README.md` 第一行表格里的「GitHub 仓库」旁，可再加一行「在线体验：<URL>」。

> 免费档一段时间无人访问会休眠，首次打开需等待十几秒唤醒，属正常。
> 也可用 Railway / Fly.io / 任意云主机，方式同理：暴露容器端口即可。

---

## 第 5 步：更新提交说明里的链接

把以下两处占位替换成真实地址：

- `README.md`：`https://github.com/____/____` → 你的仓库地址；如已部署，再加一行「在线体验：<公网 URL>」；
- `00_提交说明.md`：第 1、2 项后补上仓库 URL 与在线 URL。

改完再提交一次：

```bash
git add -A
git commit -m "补充仓库与在线体验地址"
git push
```

---

## 常见问题

**Q：push 报 `remote origin already exists`**
```bash
git remote set-url origin https://github.com/<你的用户名>/goldguard-ai.git
```

**Q：push 报鉴权失败**
GitHub 已不支持密码。用 Git Credential Manager 弹窗登录，或到
Settings → Developer settings → Personal access tokens 生成 token，用户名填 GitHub 用户名、密码填 token。

**Q：要不要把 `02_原版源码_完整工程/` 一起公开？**
本包已脱敏（删除登录态、真实企业名、历史库）。若仍不希望公开内部工程，可把该目录移到另一个 **Private** 仓库，
只把 `01_可运行Web产品_离线演示/` + `docs/` + `README.md` 放公开仓库；两者在提交说明里都给出地址即可。

**Q：想让阅卷人直接体验但不想公网部署？**
本地 `python run.py` 也完全可以；README 里已写清。公网 URL 只是加分项。
