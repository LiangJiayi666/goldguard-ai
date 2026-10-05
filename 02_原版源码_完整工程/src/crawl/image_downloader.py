from __future__ import annotations
import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import os
import re
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from pipeline_runtime import atomic_result, resolve_output, write_json
from site_fetch_common import (
    HTTP_REDIRECT_STATUSES,
    MAX_REDIRECTS,
    PublicUrlTarget,
    UnsafeUrlError,
    curl_resolve_args,
    redirected_url,
    resolve_public_url,
    ssrf_block_payload,
)


TASK_LABEL = "图片下载"
ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = ROOT / "output"


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def infer_filename(url: str) -> str:
    path = urlsplit(url).path
    name = Path(path).name.strip()
    if not name:
        return "image.bin"
    if "." not in name:
        return name + ".bin"
    return name


# Known image magic bytes for validation before OCR consumes the file.
IMAGE_MAGIC_BYTES: dict[bytes, str] = {
    b'\xff\xd8\xff': "jpeg",
    b'\x89PNG\r\n\x1a\n': "png",
    b'GIF87a': "gif",
    b'GIF89a': "gif",
    b'RIFF': "webp",    # RIFF....WEBP
    b'\x00\x00\x01\x00': "ico",
    b'BM': "bmp",
}

MIN_IMAGE_BYTES = 64  # anything smaller is definitely not a real image


def validate_image_file(path: Path) -> str | None:
    """Return None if the file looks like a valid image, or an error message string."""
    if not path.is_file():
        return "file not found after download"
    size = path.stat().st_size
    if size < MIN_IMAGE_BYTES:
        return f"file too small ({size} bytes), likely not an image"
    # Check magic bytes
    try:
        head = path.read_bytes()[:12]
    except OSError as exc:
        return f"cannot read file: {exc}"
    for magic, label in IMAGE_MAGIC_BYTES.items():
        if head.startswith(magic):
            # Additional check for webp: RIFF....WEBP at offset 8
            if label == "webp" and head[8:12] != b'WEBP':
                continue
            return None  # valid
    # Try to detect HTML/JSON error pages masquerading as images
    text_head = head.lstrip()
    if text_head.startswith(b'<') or text_head.startswith(b'{'):
        snippet = head[:80].decode("ascii", errors="replace")
        return f"downloaded content is not an image (starts with: {snippet})"
    return f"unrecognised file format, first bytes: {head[:8].hex()}"


def _curl_download_once(
    source: PublicUrlTarget,
    body_path: Path,
    header_path: Path,
    timeout: int,
) -> tuple[int, str | None]:
    cmd = [
        "curl",
        "--globoff",
        "--fail",
        "--silent",
        "--show-error",
        "--compressed",
        "--proto",
        "=http,https",
        "--max-time",
        str(timeout),
        "--connect-timeout",
        str(min(timeout, 15)),
        "-A",
        "Mozilla/5.0 ImageDownloader/1.0",
        "--dump-header",
        str(header_path),
        "-o",
        str(body_path),
        "--write-out",
        "%{http_code}",
    ]
    cmd.extend(curl_resolve_args(source))
    cmd.append(source.url)
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=timeout + 5)
    except subprocess.TimeoutExpired as exc:
        raise TimeoutError(f"curl timed out after {timeout + 5} seconds") from exc
    if proc.returncode:
        raise RuntimeError(proc.stderr.decode("utf-8", "replace").strip())
    try:
        status = int(proc.stdout.decode("ascii", "strict").strip())
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeError(f"curl returned no valid HTTP status for {source.url!r}") from exc
    headers = header_path.read_bytes()
    location_matches = re.findall(br"(?im)^location:\s*([^\r\n]+)", headers)
    location = location_matches[-1].decode("latin1").strip() if location_matches else None
    return status, location


def detect_image_extension(path: Path) -> str:
    """Return the extension for the detected magic bytes, or '' when unknown."""
    try:
        head = path.read_bytes()[:12]
    except OSError:
        return ""
    for magic, label in IMAGE_MAGIC_BYTES.items():
        if head.startswith(magic):
            if label == "webp" and head[8:12] != b"WEBP":
                continue
            return label
    return ""


def rename_by_content(path: Path) -> Path:
    """Give an extensionless/`.bin` fallback file a real image extension.

    ``infer_filename`` falls back to ``.bin`` when the URL exposes no extension;
    the downloaded content is always validated first, so the magic bytes are the
    authoritative type.  Consumers filter by suffix, so a real extension keeps
    such files in scope.
    """
    if path.suffix.lower() != ".bin":
        return path
    ext = detect_image_extension(path)
    if not ext:
        return path
    renamed = path.with_suffix(f".{ext}")
    if renamed.exists():
        return path
    os.replace(path, renamed)
    return renamed


def _check_content_type(header_path: Path) -> str | None:
    """Return None if Content-Type is image/*, or an error message string."""
    try:
        headers = header_path.read_bytes()
    except OSError:
        return None  # can't check, proceed
    ct_match = re.search(br"(?im)^content-type:\s*([^\r\n]+)", headers)
    if not ct_match:
        return None  # no Content-Type header, proceed to magic byte check
    content_type = ct_match.group(1).decode("latin1").strip().lower().split(";")[0].strip()
    if content_type.startswith("image/"):
        return None  # valid image content type
    return f"non-image Content-Type: {content_type}"


def download_to(url: str, target: Path, timeout: int) -> Path:
    """Download one image with public-address validation on every redirect hop."""
    target.parent.mkdir(parents=True, exist_ok=True)
    body_fd, body_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".part", dir=target.parent)
    header_fd, header_name = tempfile.mkstemp(prefix=".image-headers.", suffix=".tmp", dir=target.parent)
    os.close(body_fd)
    os.close(header_fd)
    body_path = Path(body_name)
    header_path = Path(header_name)
    current = url
    visited: set[str] = set()
    try:
        for redirect_count in range(MAX_REDIRECTS + 1):
            source = resolve_public_url(current)
            if source.url in visited:
                raise RuntimeError(f"redirect loop detected at {source.url!r}")
            visited.add(source.url)
            status, location = _curl_download_once(source, body_path, header_path, timeout)
            if status in HTTP_REDIRECT_STATUSES:
                if not location:
                    raise RuntimeError(f"redirect response from {source.url!r} has no Location header")
                if redirect_count >= MAX_REDIRECTS:
                    raise RuntimeError(f"too many redirects (>{MAX_REDIRECTS}) from {url!r}")
                current = redirected_url(source.url, location)
                continue
            if not 200 <= status < 300:
                raise RuntimeError(f"image request returned HTTP {status} for {source.url!r}")

            # Content-Type check (before moving to final path)
            ct_error = _check_content_type(header_path)
            if ct_error:
                body_path.unlink(missing_ok=True)
                raise RuntimeError(ct_error)

            # Pre-validate the downloaded content is actually an image
            img_error = validate_image_file(body_path)
            if img_error:
                body_path.unlink(missing_ok=True)
                raise RuntimeError(f"downloaded file is not a valid image: {img_error}")

            os.replace(body_path, target)
            return target
        raise RuntimeError(f"too many redirects (>{MAX_REDIRECTS}) from {url!r}")
    finally:
        body_path.unlink(missing_ok=True)
        header_path.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download one image URL and print success/failure.")
    parser.add_argument("url", help="image URL, e.g. https://yoostone.com/_nuxt/logo.D6T3g7JU.png")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="输出目录")
    parser.add_argument("--output-file", help="输出图片路径；覆盖默认目录与文件名")
    parser.add_argument("--result-file", help="统一诊断 JSON 输出路径")
    parser.add_argument("--timeout", type=int, default=20)
    args = parser.parse_args()

    url = args.url.strip()
    target = resolve_output(args.output_dir, args.output_file, DEFAULT_OUTPUT_DIR, infer_filename(url))

    def write_result(payload: dict) -> None:
        if args.result_file:
            write_json(Path(args.result_file), atomic_result(payload, source="image_downloader"))

    try:
        target = rename_by_content(download_to(url, target, args.timeout))
        write_result({"status": "ok", "url": url, "output_file": str(target), "bytes": target.stat().st_size})
        print(f"{now_stamp()} {TASK_LABEL} {url} 成功 {target}", flush=True)
        return 0
    except UnsafeUrlError as exc:
        write_result({**ssrf_block_payload(exc), "url": url, "output_file": str(target)})
        print(f"{now_stamp()} {TASK_LABEL} {url} 跳过 {exc}", flush=True)
        return 0
    except TimeoutError:
        write_result({"status": "timeout", "url": url, "output_file": str(target), "reason": "download timeout"})
        print(f"{now_stamp()} {TASK_LABEL} {url} 超时", flush=True)
        return 2
    except Exception as exc:
        write_result({"status": "error", "url": url, "output_file": str(target), "error": str(exc)[:500]})
        print(f"{now_stamp()} {TASK_LABEL} {url} 失败 {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

