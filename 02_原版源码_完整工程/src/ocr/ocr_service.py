from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)
"""PaddleOCR 常驻 HTTP 服务。

启动时加载一次 PaddleOCR 引擎，之后每个 POST /ocr 请求复用同一引擎，
省掉每张图重新加载模型的代价。用 open_ocr_service.ps1 在跑 main_agent 前手动启动。
绑定 127.0.0.1 仅本地；端口默认 16191（避让 ICP 的 16181）。

端点：
  POST /ocr   body {image_path, threshold?, max_side?} -> {code, success, data}
  GET  /health -> {code, success, data:{status}}
"""

import argparse
import io
import json
import logging
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 16191
DEFAULT_MODEL_TIER = "tiny"
DEFAULT_THRESHOLD = 0.75
DEFAULT_MAX_SIDE = 1280


def _rewrap_utf8() -> None:
    # Windows 默认 GBK/cp936，paddle 的中文日志会乱码；统一刷成 UTF-8。
    if sys.stdout.encoding != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
    if sys.stderr.encoding != "utf-8":
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)


def extract_text_scores(payload) -> list[tuple[str, float]]:
    if payload is None:
        return []
    if isinstance(payload, dict):
        if "res" in payload:
            return extract_text_scores(payload["res"])
        texts = payload.get("rec_texts")
        scores = payload.get("rec_scores")
        if isinstance(texts, list) and isinstance(scores, list):
            return [(str(text), float(score)) for text, score in zip(texts, scores)]
        if "text" in payload and "score" in payload:
            return [(str(payload["text"]), float(payload["score"]))]
        for key in ("data", "result", "items", "predictions"):
            if key in payload:
                collected = extract_text_scores(payload[key])
                if collected:
                    return collected
        return []
    if isinstance(payload, (list, tuple)):
        if len(payload) == 2 and not isinstance(payload[1], (list, tuple, dict)):
            return [(str(payload[0]), float(payload[1]))]
        if len(payload) == 2 and isinstance(payload[1], (list, tuple)) and len(payload[1]) >= 2:
            leaf = payload[1]
            if not isinstance(leaf[0], (list, tuple, dict)):
                return [(str(leaf[0]), float(leaf[1]))]
        collected: list[tuple[str, float]] = []
        for item in payload:
            collected.extend(extract_text_scores(item))
        return collected
    return []


def load_ocr_engine(model_tier: str):
    """导入 paddleocr 并构造引擎；重，每进程只调一次。"""
    from paddleocr import PaddleOCR

    model_map = {
        "tiny": ("PP-OCRv6_tiny_det", "PP-OCRv6_tiny_rec"),
        "small": ("PP-OCRv6_small_det", "PP-OCRv6_small_rec"),
        "medium": ("PP-OCRv6_medium_det", "PP-OCRv6_medium_rec"),
    }
    detector, recognizer = model_map[model_tier]
    os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
    return PaddleOCR(
        text_detection_model_name=detector,
        text_recognition_model_name=recognizer,
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
    )


def load_image_array(image_path: str, max_side: int):
    import numpy as np
    from PIL import Image, ImageOps

    with Image.open(image_path) as source:
        image = ImageOps.exif_transpose(source)
        if image.mode in {"RGBA", "LA"} or (image.mode == "P" and "transparency" in image.info):
            rgba = image.convert("RGBA")
            background = Image.new("RGBA", rgba.size, "white")
            image = Image.alpha_composite(background, rgba).convert("RGB")
        else:
            image = image.convert("RGB")
        scale = min(1.0, max_side / max(image.size))
        if scale < 1.0:
            resized = (
                max(1, round(image.width * scale)),
                max(1, round(image.height * scale)),
            )
            image = image.resize(resized, Image.Resampling.LANCZOS)
        return np.asarray(image)[:, :, ::-1].copy()


def recognize(ocr, image_path: str, threshold: float, max_side: int) -> dict:
    """用已加载引擎 OCR 一张图；永不抛异常。"""
    try:
        array = load_image_array(image_path, max_side)
    except Exception as exc:
        return {"ok": False, "error": f"image_load_failed: {exc}"}
    try:
        ocr_fn = getattr(ocr, "ocr", None)
        raw_result = ocr_fn(array, cls=False) if callable(ocr_fn) else ocr(array, cls=False)
        predictions = extract_text_scores(raw_result)
        if not predictions:
            return {"ok": True, "text": "", "detected_count": 0, "kept_count": 0}
        kept: list[str] = []
        for text, score in predictions:
            normalized = " ".join(str(text).split())
            if normalized and float(score) >= threshold:
                kept.append(normalized)
        return {
            "ok": True,
            "text": "\n".join(kept),
            "detected_count": len(predictions),
            "kept_count": len(kept),
        }
    except Exception as exc:
        return {"ok": False, "error": f"ocr_failed: {exc}"}


class _State:
    ocr = None
    lock = threading.Lock()  # PaddleOCR 非线程安全；串行推理。
    threshold = DEFAULT_THRESHOLD
    max_side = DEFAULT_MAX_SIDE


def _json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict) -> None:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class OcrHandler(BaseHTTPRequestHandler):
    server_version = "OcrService/1.0"

    def log_message(self, fmt: str, *args) -> None:
        sys.stderr.write(f"{self.address_string()} - {fmt % args}\n")

    def do_GET(self) -> None:
        if self.path.split("?", 1)[0] == "/health":
            _json_response(self, 200, {"code": 200, "success": True, "data": {"status": "ok"}})
        else:
            _json_response(self, 404, {"code": 404, "success": False, "msg": "not found"})

    def do_POST(self) -> None:
        if self.path.split("?", 1)[0] != "/ocr":
            _json_response(self, 404, {"code": 404, "success": False, "msg": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length).decode("utf-8")) if length else {}
        except Exception as exc:
            _json_response(self, 400, {"code": 400, "success": False, "msg": f"bad_request: {exc}"})
            return
        image_path = str(body.get("image_path") or "").strip()
        if not image_path:
            _json_response(self, 400, {"code": 400, "success": False, "msg": "missing image_path"})
            return
        if not Path(image_path).is_file():
            _json_response(self, 200, {"code": 200, "success": True,
                                       "data": {"ok": False, "error": "image_load_failed: file not found"}})
            return
        threshold = body.get("threshold")
        max_side = body.get("max_side")
        threshold = float(threshold) if threshold is not None else _State.threshold
        max_side = int(max_side) if max_side is not None else _State.max_side
        try:
            with _State.lock:
                result = recognize(_State.ocr, image_path, threshold, max_side)
        except Exception as exc:
            _json_response(self, 500, {"code": 500, "success": False, "msg": f"server_error: {exc}"})
            return
        _json_response(self, 200, {"code": 200, "success": True, "data": result})


def main() -> int:
    parser = argparse.ArgumentParser(description="PaddleOCR 常驻 HTTP 服务")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--model-tier", choices=("tiny", "small", "medium"), default=DEFAULT_MODEL_TIER)
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--max-side", type=int, default=DEFAULT_MAX_SIDE)
    args = parser.parse_args()

    _rewrap_utf8()
    for name in ("ppocr", "paddle", "paddleocr"):
        logging.getLogger(name).setLevel(logging.WARNING)

    print(f"[ocr_service] loading PaddleOCR tier={args.model_tier} ...", flush=True)
    try:
        _State.ocr = load_ocr_engine(args.model_tier)
    except ImportError as exc:
        print(f"[ocr_service] missing_dependency: {exc}", file=sys.stderr, flush=True)
        return 1
    except Exception as exc:
        print(f"[ocr_service] model_load_failed: {exc}", file=sys.stderr, flush=True)
        return 1
    _State.threshold = args.threshold
    _State.max_side = args.max_side

    server = ThreadingHTTPServer((args.host, args.port), OcrHandler)
    print(f"[ocr_service] serving on http://{args.host}:{args.port}/ocr", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
