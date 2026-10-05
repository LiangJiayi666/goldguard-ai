from __future__ import annotations

import sys as _sys
from pathlib import Path as _Path
_SRC_ROOT = str(_Path(__file__).resolve().parents[1])
if _SRC_ROOT not in _sys.path:
    _sys.path.insert(0, _SRC_ROOT)

import argparse
import json
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

from pipeline_runtime import resolve_output, safe_name, write_json


BASE_URL = "http://127.0.0.1:16181"
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "output"

CARRIER_TASK_LABELS = {
    "web": "query website",
    "app": "query app",
    "mapp": "query miniapp",
    "kapp": "query kuaishou app",
    "bweb": "query illegal website",
    "bapp": "query illegal app",
    "bmapp": "query illegal miniapp",
    "bkapp": "query illegal kuaishou app",
}


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_company(raw: str) -> str:
    value = raw.strip().lstrip("\ufeff")
    if not value:
        raise ValueError("empty company name")
    return value


def load_carrier(raw: str) -> str:
    carrier = raw.strip().lower()
    if carrier not in CARRIER_TASK_LABELS:
        raise ValueError("invalid carrier")
    return carrier


def load_page(raw: str) -> int:
    page = int(raw)
    if page <= 0:
        raise ValueError("page must be positive")
    return page


def default_output_file(company: str, carrier: str, page: int) -> str:
    return f"{safe_name(company)}_{carrier}_p{page}.json"


def fetch_json(url: str, timeout: int) -> dict:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 ICPQueryOne/1.0"}, method="GET")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        text = response.read().decode(charset, errors="replace")
        return json.loads(text)


def recent_service_block(log_path: Path | None, within_seconds: float) -> str | None:
    if log_path is None or within_seconds <= 0 or not log_path.is_file():
        return None
    try:
        tail = log_path.read_text(encoding="utf-8", errors="replace")[-65536:]
    except OSError:
        return None
    now = datetime.now()
    latest: str | None = None
    for line in tail.splitlines():
        if "blocked" not in line.lower():
            continue
        try:
            stamp = datetime.strptime(line[:23], "%Y-%m-%d %H:%M:%S,%f")
        except ValueError:
            continue
        if 0 <= (now - stamp).total_seconds() <= within_seconds:
            latest = line.strip()
    return latest


def normalize_record(company: str, carrier: str, item: dict) -> dict:
    return {"company": company, "carrier_type": carrier, **item}


def fetch_carrier(
    company: str,
    carrier: str,
    page_num: int,
    base_url: str,
    timeout: int,
    page_size: int,
    service_log: Path | None = None,
    block_window: float = 120.0,
) -> dict:
    url = f"{base_url}/query/{carrier}?" + urllib.parse.urlencode(
        {"search": company, "pageNum": str(page_num), "pageSize": str(page_size)}
    )

    def fail(error: str) -> dict:
        return {
            "status": "failed",
            "pages": [{"page_num": page_num, "status": "error", "error": error}],
            "records": [],
            "last_error": error,
        }

    try:
        payload = fetch_json(url, timeout)
        if payload.get("code") != 200 or payload.get("success") is False:
            return fail(f"service error: code={payload.get('code')} msg={payload.get('msg')}")
        params = payload.get("params")
        if not isinstance(params, dict):
            return fail("response missing params")

        rows = params.get("list") or []
        records = [normalize_record(company, carrier, item) for item in rows if isinstance(item, dict)]
        page_entry = {"page_num": page_num, "status": "ok", "rows": len(rows)}

        if not rows:
            block = recent_service_block(service_log, block_window)
            if block:
                return fail(f"upstream block detected: {block[:100]}")
            return {"status": "ok", "pages": [page_entry], "records": records, "last_error": None}

        return {
            "status": "ok",
            "pages": [page_entry],
            "records": records,
            "last_error": None,
            "has_next_page": bool(params.get("hasNextPage")),
        }
    except Exception as exc:
        return fail(str(exc))


def main() -> int:
    parser = argparse.ArgumentParser(description="Query one ICP page for one company and one carrier.")
    parser.add_argument("company", help="enterprise name")
    parser.add_argument("--carrier", required=True, help="one carrier only")
    parser.add_argument("--page", type=int, required=True, help="specific page number to query")
    parser.add_argument("--timeout", type=int, default=20, help="seconds per page request")
    parser.add_argument("--page-size", type=int, default=10)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="输出目录")
    parser.add_argument("--output-file", help="输出 JSON 文件路径；覆盖默认目录与文件名")
    parser.add_argument("--service-url", default=BASE_URL, help="local ICP service base URL")
    parser.add_argument("--service-log", help="service stderr log path")
    parser.add_argument("--block-window-seconds", type=float, default=120, help="lookback window for block detection")
    args = parser.parse_args()

    company = load_company(args.company)
    carrier = load_carrier(args.carrier)
    page_num = load_page(args.page)
    task_label = CARRIER_TASK_LABELS[carrier]
    service_url = args.service_url.rstrip("/")

    output_file = resolve_output(args.output_dir, args.output_file, DEFAULT_OUTPUT_DIR, default_output_file(company, carrier, page_num))

    print(f"{now_stamp()} {task_label} {company} page {page_num} start", flush=True)
    service_log = Path(args.service_log) if args.service_log else None
    result = fetch_carrier(
        company,
        carrier,
        page_num,
        service_url,
        args.timeout,
        args.page_size,
        service_log,
        args.block_window_seconds,
    )
    payload = {
        "schema_version": 1,
        "record_type": "final",
        "company": company,
        "carrier": carrier,
        "page": page_num,
        "status": result["status"],
        "pages": result["pages"],
        "records": result["records"],
        "last_error": result.get("last_error"),
        "has_next_page": result.get("has_next_page"),
        "query_time": now_stamp(),
    }
    write_json(output_file, payload)
    print(f"{now_stamp()} {task_label} {company} page {page_num} end {output_file}", flush=True)
    return 0 if result["status"] != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
