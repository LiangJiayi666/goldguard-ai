# 06 · 部署与公网 URL 说明

> 题面要求「可访问、可实际操作的 Web 产品 URL」。本产品为**零依赖**（仅 Python 标准库），
> 因此既可以本地跑，也可以极低成本部署成公网 URL。

## 一、本地运行（最简，评委开箱即用）

```bash
cd 01_可运行Web产品_离线演示
python run.py
# 打开 http://127.0.0.1:8765
```

Windows 可直接双击顶层 `启动演示.bat`。

> 说明：`http://127.0.0.1:8765` 只在**运行者本机**可访问，严格意义上不是“公网可访问 URL”。
> 若阅卷需要公网地址，请用下面任一方式部署。

## 二、Docker（一条命令）

仓库根目录已提供 `Dockerfile`：

```bash
docker build -t goldguard-demo .
docker run -p 8000:8000 goldguard-demo
# 打开 http://localhost:8000
```

## 三、GitHub Pages 静态版（已上线）

公网 URL：<https://liangjiayi666.github.io/goldguard-ai/>

- Pages 构建源已设为 **gh-pages 分支**（Settings → Pages → Deploy from a branch → `gh-pages` / `(root)`）；
- `.github/workflows/pages.yml` 会把 `site/` 自动发布到 `gh-pages`；
- 静态版由 `01_可运行Web产品_离线演示/tools/build_static_site.py` 生成，`ui/mock-api.js` 在浏览器内
  把 `/api/*` 路由到预生成数据，行为与本地服务版一致（“开始排查/复核/证据链”可操作）。

---

## 四、Render 容器部署（可选，带真实后端）

因应用无外部依赖、只监听端口，适合任何容器平台：

**Render（免费档）**
1. 把本提交包推到 GitHub 仓库；
2. Render → New → Web Service → 选择仓库；
3. Environment 选 Docker（读根目录 `Dockerfile`），或用 Native：
   - Build Command：`echo no build needed`
   - Start Command：`python 01_可运行Web产品_离线演示/run.py --host 0.0.0.0 --port $PORT`
4. 部署完成后即得到形如 `https://goldguard-demo.onrender.com` 的公网 URL。

**Railway / Fly.io / 任意云主机**：同理，暴露容器端口即可。

**纯静态替代方案**：若只需“看界面”，可把 `01_可运行Web产品_离线演示/ui/` 与 `data/fixtures.json`
打包为静态站点（前端读取 JSON）后托管到 GitHub Pages / Netlify。但**推荐用服务端方案**，因为它保留了
“开始排查/人工复核”等可操作接口。

## 四、部署检查清单

- [ ] 服务监听 `0.0.0.0` 与平台注入的 `$PORT`；
- [ ] 页面可打开，且「开始排查」「人工复核」可点击生效；
- [ ] 无任何密钥写入镜像/仓库（本产品本就不需要密钥）；
- [ ] `Dockerfile` 只 COPY `01_可运行Web产品_离线演示/`，镜像体积小；
- [ ] 记录 URL 与访问有效期，写入提交说明。

## 五、原版完整工程的部署（可选）

原版是 Windows 桌面流水线，非 Web 服务，不建议公网部署。其安装运行见 `02_原版源码_完整工程/安装使用指南.pdf`。
其内置看板默认地址为 `http://127.0.0.1:18080`。
