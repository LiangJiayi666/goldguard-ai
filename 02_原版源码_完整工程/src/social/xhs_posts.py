from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)
"""小红书 第2步：抓指定账号主页全部帖子，单函数增量抓取，输出帖子列表 JSON。

从首页开始持续 scroll：任一帖子命中历史已爬集合即停（增量锚点），否则滚到底
（帖子数停滞）停。替代旧的 home + scroll 分页（scroll 每次重新 goto、翻页失灵）。

用法:
  .venv/Scripts/python.exe xhs_posts.py --user-id <id> --xsec-token <token> \
      [--existing-ids id1 id2 ...] [--headless] [--output-dir output] [--output-file x.json]

输出: JSON {status, items:[{note_id,title,type,likes,xsec_token,detail_href}], has_more, stopped_early, count}
"""

import argparse
import json
import os
import random
from pathlib import Path
from typing import Any

from playwright.sync_api import sync_playwright
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError

from pipeline_runtime import resolve_output, safe_name, write_json
from xhs_search import (
    check_login_required, check_rate_limited, human_pause, launch_browser, new_context, save_cookies,
)
from xhs_login_state import has_login_state_lost_text

ROOT = Path(__file__).parent


def resolve_output_path(args: argparse.Namespace, user_id: str) -> Path:
    return resolve_output(args.output_dir, args.output_file, ROOT / "output", f"{safe_name(user_id)}_xhs_notes.json")


# 从 __INITIAL_STATE__ 抓当前已加载帖子（SSR 初始数据 + 懒加载回写）。
PROFILE_NOTES_JS = """(() => {
    const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
    const u = (window.__INITIAL_STATE__ || {}).user || {};
    const notes = uw(u.notes) || [];
    const flat = Array.isArray(notes[0]) ? notes.flat() : notes;
    return flat.map(raw => {
        const item = uw(raw) || {};
        const n = uw(item.noteCard || item.note_card || item) || {};
        return {
            note_id: n.noteId || n.note_id,
            title: n.displayTitle || n.display_title || n.title,
            type: n.type,
            likes: (uw(n.interactInfo || n.interact_info) || {}).likedCount
                || (uw(n.interactInfo || n.interact_info) || {}).liked_count,
            xsec_token: n.xsecToken || n.xsec_token || item.xsecToken || item.xsec_token,
        };
    }).filter(n => n.note_id);
})()"""

# 从 DOM 补每条帖子的详情 href（detail 子任务需要）。
PROFILE_HREFS_JS = """(() => {
    const out = {};
    document.querySelectorAll('a[href]').forEach(a => {
        const href = a.getAttribute('href') || '';
        const m = href.match(/\\/(?:user\\/profile\\/[\\w]+|explore|search_result)\\/([0-9a-f]{24})/);
        if (m && !out[m[1]]) out[m[1]] = href;
    });
    return out;
})()"""


class LoginRequiredError(Exception):
    pass


class RateLimitedError(Exception):
    pass


def _atomic_write_ids(path: Path, ids) -> None:
    """原子写 checkpoint：先写 .tmp 再 os.replace，避免中断留下半截文件。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(sorted(ids)) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _load_checkpoint(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def wait_for_notes_load(page, timeout: int = 30000) -> bool:
    try:
        page.wait_for_function(
            """() => {
                const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
                const u = window.__INITIAL_STATE__ && window.__INITIAL_STATE__.user;
                if (!u) return false;
                const notes = uw(u.notes) || [];
                const flat = Array.isArray(notes[0]) ? notes.flat() : notes;
                return flat.length > 0;
            }""",
            timeout=timeout,
        )
        return True
    except Exception:
        return False


def fetch_one_account(
    page, user_id: str, xsec_token: str, existing_ids: set[str], max_items: int = 30,
    skip_ids: set[str] | None = None, resume_file: Path | None = None,
    checkpoint_every: int = 10,
) -> tuple[list[dict], bool, int]:
    """抓取一个小红书账号主页帖子。

    返回 (帖子列表, 是否因命中历史锚点提前停止, 跳过的已见帖子数)。
    ``skip_ids`` 与 ``existing_ids`` 语义不同：命中 ``existing_ids`` 是增量锚点，立即停；
    命中 ``skip_ids``（如 review 样本 / checkpoint 恢复）只跳过不收集、不触发锚点停，继续往下滚。
    ``resume_file``：每 ``checkpoint_every`` 轮原子写已收集 note_id，供超时 kill 后恢复。
    """
    skip_ids = skip_ids or set()
    profile_url = (
        f"https://www.xiaohongshu.com/user/profile/{user_id}"
        f"?xsec_token={xsec_token}&xsec_source=pc_search"
    )
    page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
    human_pause()
    if check_login_required(page):
        raise LoginRequiredError("小红书帖子页需要登录")
    if check_rate_limited(page):
        raise RateLimitedError("小红书帖子页操作过于频繁")
    if not wait_for_notes_load(page, timeout=30000):
        return [], False, 0
    if has_login_state_lost_text(page):
        raise LoginRequiredError("小红书帖子页登录态已失效")

    notes: dict[str, dict] = {}
    stagnant = 0
    last_count = 0
    stopped_early = False
    skipped = 0
    round_no = 0

    # 无轮数上限：滚动终止只依赖 停滞检测（连续 3 轮无新增=到底）与增量锚点。
    while True:
        round_no += 1
        if has_login_state_lost_text(page):
            raise LoginRequiredError("小红书帖子页登录态已失效")
        for n in page.evaluate(PROFILE_NOTES_JS):
            nid = n.get("note_id")
            if nid in existing_ids:
                stopped_early = True
                break
            if nid and nid in skip_ids:
                skipped += 1
                continue
            if nid:
                notes[nid] = n
            if max_items and len(notes) >= max_items:
                break
        if stopped_early or (max_items and len(notes) >= max_items):
            break
        if len(notes) == last_count:
            stagnant += 1
            if stagnant >= 3:
                break
        else:
            stagnant = 0
        last_count = len(notes)
        if resume_file and round_no % checkpoint_every == 0:
            _atomic_write_ids(resume_file, notes.keys())
        page.mouse.wheel(0, random.randint(1200, 2400))
        human_pause()
        # 有界增量等待：等 __INITIAL_STATE__ 的 note 总数超过滚动前，3s 超时即认为本轮无新增。
        # 取代 networkidle(8s 吞超时)——快网络提前返回、慢网络多等几轮，降低停滞假到底。
        try:
            page.wait_for_function(
                """(prev) => {
                    const uw = v => (v && typeof v === 'object' && '_value' in v) ? v._value : v;
                    const u = window.__INITIAL_STATE__ && window.__INITIAL_STATE__.user;
                    if (!u) return false;
                    const notes = uw(u.notes) || [];
                    const flat = Array.isArray(notes[0]) ? notes.flat() : notes;
                    return flat.length > prev;
                }""",
                arg=len(notes),
                timeout=3000,
            )
        except PlaywrightTimeoutError:
            pass
        human_pause()

    if has_login_state_lost_text(page):
        raise LoginRequiredError("小红书帖子页登录态已失效")
    hrefs = page.evaluate(PROFILE_HREFS_JS)
    for note in notes.values():
        href = hrefs.get(note.get("note_id"))
        if href:
            note["detail_href"] = href if href.startswith("http") else "https://www.xiaohongshu.com" + href
    if max_items:
        return list(notes.values())[:max_items], stopped_early, skipped
    return list(notes.values()), stopped_early, skipped


def build_summary(items: list[dict], stopped_early: bool, status: str = "ok", skipped: int = 0) -> dict[str, Any]:
    return {
        "status": status,
        "items": items,
        "has_more": False,
        "stopped_early": stopped_early,
        "skipped": skipped,
        "count": len(items),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="XHS account notes: incremental single-pass crawl.")
    parser.add_argument("--user-id", help="目标账号 user_id（单条模式）")
    parser.add_argument("--xsec-token", default="")
    parser.add_argument("--existing-ids", nargs="*", default=[], help="历史已爬 note_id 集合，命中任一即停")
    parser.add_argument("--skip-ids", nargs="*", default=[], help="已见 note_id（如 review 样本），跳过不收集、不触发锚点停")
    parser.add_argument("--max-items", type=int, default=30, help="单账号最多抓取的作品卡片数；0=无上限（滚到底）")
    parser.add_argument("--resume-file", type=Path, default=None, help="中途 checkpoint 路径：启动读入已收 note_id 作跳过集，每 10 轮原子写入，正常完成删除")
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "output")
    parser.add_argument("--output-file")
    args = parser.parse_args()

    if args.max_items < 0:
        parser.error("--max-items 必须大于等于 0（0 表示无上限）")

    if not args.user_id:
        parser.error("单条模式需要 --user-id 参数")

    output_path = resolve_output_path(args, args.user_id)
    skip_ids = set(args.skip_ids or [])
    if args.resume_file and args.resume_file.is_file():
        skip_ids |= _load_checkpoint(args.resume_file)
    with sync_playwright() as p:
        browser = launch_browser(p, args.headless)
        try:
            context = new_context(browser)
            page = context.new_page()
            try:
                items, stopped_early, skipped = fetch_one_account(
                    page, args.user_id, args.xsec_token, set(args.existing_ids or []), args.max_items,
                    skip_ids, args.resume_file,
                )
                try:
                    save_cookies(context)
                except Exception:
                    pass
                if args.resume_file:
                    args.resume_file.unlink(missing_ok=True)
                write_json(output_path, build_summary(items, stopped_early, skipped=skipped))
            except LoginRequiredError:
                write_json(output_path, build_summary([], False, status="login_required"))
                return 100
            except RateLimitedError:
                write_json(output_path, build_summary([], False, status="rate_limited"))
                return 100
            finally:
                try:
                    context.close()
                except Exception:
                    pass
        finally:
            browser.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
