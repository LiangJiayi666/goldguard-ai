# -*- coding: utf-8 -*-
"""重新生成静态站点并发布到 GitHub Pages（gh-pages 分支）。

GitHub Pages 的构建源已设为 gh-pages 分支，本站点由 GitHub 自动构建发布。
本脚本负责：构建 site/ → 提交到 gh-pages 分支并推送。

用法（在本文件所在目录或任意位置）：
    python tools/publish_pages.py
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
DEMO = HERE.parent
REPO = DEMO.parent
SITE = REPO / "site"


def run(cmd: list[str], cwd: Path) -> None:
    print("$", " ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd), check=True)


def main() -> int:
    # 1) 重新生成静态站点
    run([sys.executable, str(HERE / "build_static_site.py")], DEMO)

    # 2) 取远程地址
    remote = subprocess.run(["git", "remote", "get-url", "origin"], cwd=str(REPO),
                            capture_output=True, text=True, check=True).stdout.strip()
    if not remote:
        print("未找到 git 远程 origin，请先在仓库根目录配置。")
        return 1

    # 3) 在临时目录以 gh-pages 分支提交并强推
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        shutil.copytree(SITE, tmp_path, dirs_exist_ok=True)
        (tmp_path / ".nojekyll").write_text("", encoding="utf-8")
        run(["git", "init", "-q", "-b", "gh-pages"], tmp_path)
        run(["git", "add", "-A"], tmp_path)
        run(["git", "-c", "core.quotepath=false", "commit", "-q", "-m", "deploy: GoldGuard AI 静态演示站"], tmp_path)
        run(["git", "remote", "add", "origin", remote], tmp_path)
        run(["git", "push", "-f", "origin", "gh-pages"], tmp_path)

    print("\n已发布。稍等 1 分钟，访问：https://liangjiayi666.github.io/goldguard-ai/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
