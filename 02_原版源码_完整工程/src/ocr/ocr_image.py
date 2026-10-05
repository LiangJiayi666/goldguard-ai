from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

"""图片转文字（OCR）—— PaddleOCR 常驻服务的薄客户端。

引擎由 ocr_service.py 常驻加载（用 open_ocr_service.ps1 手动启动），
本脚本把图片路径 POST 给服务：识别文本写 --text-file，统一结果（含诊断）写 --output-file。
main_agent 调用：python ocr_image.py <image> --output-file result.json --text-file result.txt。

用法:
  .venv/Scripts/python.exe ocr_image.py <image> [--output-file result.json]
      [--text-file result.txt] [--threshold 0.75] [--max-side 1280]
      [--timeout 120] [--service-url http://127.0.0.1:16191/ocr]

退出码 0 成功 / 1 错误 / 2 超时。stderr 文案保留中文「超时」与 connection 等 token，
供 main_agent 的 stderr 分类器识别，勿随意改成英文。
"""
import argparse
import json
import socket
import sys
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

from pipeline_runtime import atomic_result, resolve_output, write_json


TASK_LABEL = "图片转文字"
ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "output"
DEFAULT_SERVICE_URL = "http://127.0.0.1:16191/ocr"

# 绕开 HTTP_PROXY/HTTPS_PROXY 对 localhost 的劫持（ICP 客户端没做、是隐患）。
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class _Timeout(Exception):
    pass


class _ConnError(Exception):
    pass


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def resolve_input_path(raw: str) -> Path:
    path = Path(raw.strip())
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    return path


def write_text_result(txt_path: Path, text: str) -> None:
    txt_path.parent.mkdir(parents=True, exist_ok=True)
    txt_path.write_text(text.rstrip() + "\n", encoding="utf-8")


def call_ocr(service_url: str, image_path: Path, threshold: float, max_side: int, timeout: int) -> dict:
    body = json.dumps(
        {"image_path": str(image_path), "threshold": threshold, "max_side": max_side},
        ensure_ascii=False,
    ).encode("utf-8")
    req = urllib.request.Request(
        service_url, data=body,
        headers={"Content-Type": "application/json; charset=utf-8", "User-Agent": "OcrClient/1.0"},
        method="POST",
    )
    try:
        with _OPENER.open(req, timeout=timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            return json.loads(resp.read().decode(charset, errors="replace"))
    except urllib.error.HTTPError as exc:
        # 服务端 4xx/5xx 也带 JSON 错误体；尽量解析。
        try:
            return json.loads(exc.read().decode("utf-8", errors="replace"))
        except Exception:
            return {"code": exc.code, "success": False, "msg": f"http {exc.code}"}
    except urllib.error.URLError as exc:
        reason = getattr(exc, "reason", exc)
        if isinstance(reason, (socket.timeout, TimeoutError)):
            raise _Timeout()
        raise _ConnError(str(reason))
    except (socket.timeout, TimeoutError):
        raise _Timeout()


def main() -> int:
    parser = argparse.ArgumentParser(description="OCR one image via the long-lived PaddleOCR service.")
    parser.add_argument("image", nargs="?", help="image path, absolute or relative")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="输出目录")
    parser.add_argument("--output-file", help="结果 JSON 路径（含诊断）；默认 <image_stem>.json")
    parser.add_argument("--text-file", help="识别文本 txt 路径；默认 <image_stem>.txt")
    parser.add_argument("--timeout", type=int, default=120, help="单次请求超时秒数")
    parser.add_argument("--threshold", type=float, default=0.75)
    parser.add_argument("--max-side", type=int, default=1280)
    parser.add_argument("--service-url", default=DEFAULT_SERVICE_URL)
    args = parser.parse_args()

    if not args.image:
        parser.error("需要 image 参数")

    image_path = resolve_input_path(args.image)
    result_path = resolve_output(args.output_dir, args.output_file, DEFAULT_OUTPUT_DIR, f"{image_path.stem}.json")
    txt_path = resolve_output(args.output_dir, args.text_file, DEFAULT_OUTPUT_DIR, f"{image_path.stem}.txt")

    def write_result(payload: dict) -> None:
        write_json(result_path, atomic_result(payload, source="ocr"))

    if not image_path.is_file():
        write_result({"status": "error", "image": str(image_path), "error": "input file does not exist"})
        print(f"{now_stamp()} {TASK_LABEL} {image_path} 失败 文件不存在", file=sys.stderr, flush=True)
        return 1

    try:
        payload = call_ocr(args.service_url, image_path, args.threshold, args.max_side, args.timeout)
    except _Timeout:
        write_result({"status": "timeout", "image": str(image_path), "reason": "OCR request timeout"})
        print(f"{now_stamp()} {TASK_LABEL} {image_path} 超时", file=sys.stderr, flush=True)
        return 2
    except _ConnError as exc:
        write_result({"status": "error", "image": str(image_path), "error": f"connection refused: {exc}"})
        print(f"{now_stamp()} {TASK_LABEL} {image_path} 失败 connection refused: {exc}", file=sys.stderr, flush=True)
        return 1

    if not payload.get("success"):
        msg = payload.get("msg") or "service error"
        write_result({"status": "error", "image": str(image_path), "error": f"service error: {msg}"})
        print(f"{now_stamp()} {TASK_LABEL} {image_path} 失败 service error: {msg}", file=sys.stderr, flush=True)
        return 1

    data = payload.get("data") or {}
    if not data.get("ok"):
        err = data.get("error") or "no_result"
        write_result({"status": "error", "image": str(image_path), "error": err})
        print(f"{now_stamp()} {TASK_LABEL} {image_path} 失败 {err}", file=sys.stderr, flush=True)
        return 1

    text = str(data.get("text") or "")
    write_text_result(txt_path, text)
    write_result({"status": "ok", "image": str(image_path), "text_file": str(txt_path), "text_length": len(text)})
    print(f"{now_stamp()} {TASK_LABEL} {image_path} 成功 {txt_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
