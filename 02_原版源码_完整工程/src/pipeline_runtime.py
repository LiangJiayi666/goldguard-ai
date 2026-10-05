from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


ATTEMPT_STATUSES = {"SUCCEEDED", "RETRY_WAIT", "EMPTY_SUCCESS", "FAILED_FINAL", "SKIPPED"}

# 这些字段保存的是 attempt 的输入文件。路径本身会随 run/task_id 变化，
# 因此请求指纹使用文件内容，而不是绝对路径。
REQUEST_FILE_KEYS = {"image", "corpus_file", "evidence_file"}
REQUEST_TRANSIENT_KEYS = {"browser_probe_error"}


@dataclasses.dataclass(slots=True)
class AttemptDiagnosis:
    """统一的原子结果/attempt 诊断协议。

    原子脚本先填写语义字段；MainAgent 在进程结束后补齐 returncode、attempt_no
    和最终的重试/终态策略字段。
    """

    status: str
    error_code: str | None
    reason: str
    source: str
    returncode: int | None = None
    attempt_no: int | None = None
    summary_status: str | None = None
    retryable: bool = False
    terminal: bool = False
    refresh_login: bool = False
    evidence: dict[str, Any] = dataclasses.field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def canonical_attempt_diagnosis(
    raw_status: str | None,
    *,
    reason: str = "",
    source: str = "atomic",
    error_code: str | None = None,
    evidence: dict[str, Any] | None = None,
) -> AttemptDiagnosis:
    """把原子脚本自己的 status 映射成统一四态诊断。"""
    value = str(raw_status or "ok").strip().lower()
    if value in {"login_required", "cookie_invalid", "login_expired"}:
        # 登录失效也先进入可重试态；主控会优先切换到登录刷新，而不是
        # 把首次发现登录失效直接写成最终失败。
        status, code = "RETRY_WAIT", error_code or "login_required"
    elif value in {"rate_limited", "captcha_required", "timeout", "timed_out"}:
        status, code = "RETRY_WAIT", error_code or value
    elif value in {"empty", "empty_success"}:
        status, code = "EMPTY_SUCCESS", None
    elif value in {"error", "failed", "final_error", "missing_output"}:
        # 崩溃类（脚本 except 兜底 / 缺产物）：以瞬时为主，先判可重试，3 次仍败由 _diagnose_attempt 升格 FAILED_FINAL。
        status, code = "RETRY_WAIT", error_code or ("missing_output" if value == "missing_output" else "process_error")
    else:
        status, code = "SUCCEEDED", None
    return AttemptDiagnosis(
        status=status,
        error_code=code,
        reason=reason or value,
        source=source,
        summary_status=value if value else None,
        retryable=status == "RETRY_WAIT",
        terminal=status in {"SUCCEEDED", "EMPTY_SUCCESS", "FAILED_FINAL"},
        refresh_login=code == "login_required",
        evidence=dict(evidence or {}),
    )


def atomic_result(
    data: Any,
    *,
    status: str | None = None,
    reason: str = "",
    source: str = "atomic",
    error_code: str | None = None,
    evidence: dict[str, Any] | None = None,
) -> Any:
    """给所有原子脚本加上 diagnosis；保留原有业务字段以兼容历史消费者。"""
    raw_status = status
    if raw_status is None and isinstance(data, dict):
        raw_status = str(data.get("status") or "ok")
    diagnosis = canonical_attempt_diagnosis(
        raw_status,
        reason=reason,
        source=source,
        error_code=error_code,
        evidence=evidence,
    ).to_dict()
    if isinstance(data, dict):
        result = dict(data)
        result["diagnosis"] = diagnosis
        return result
    return {"diagnosis": diagnosis, "data": data}


def extract_atomic_diagnosis(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    diagnosis = payload.get("diagnosis")
    return dict(diagnosis) if isinstance(diagnosis, dict) else None


def unwrap_atomic_data(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    if "data" in payload and isinstance(payload.get("diagnosis"), dict):
        return payload.get("data")
    result = dict(payload)
    result.pop("diagnosis", None)
    return result


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime | None = None) -> str:
    value = dt or utc_now()
    return value.astimezone(timezone.utc).isoformat(timespec="seconds")


def stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def _fingerprint_payload(value: Any, *, key: str = "", hash_files: bool = True) -> Any:
    """Return a stable, run-independent representation of request input.

    ``hash_files=False`` is only used for legacy rows created before request
    fingerprints were persisted. It deliberately gives a weaker match and is
    reported as such by the replay provenance.
    """
    if isinstance(value, dict):
        return {
            str(item_key): _fingerprint_payload(item_value, key=str(item_key), hash_files=hash_files)
            for item_key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
            if str(item_key) not in REQUEST_TRANSIENT_KEYS and str(item_key) != "company"
        }
    if isinstance(value, (list, tuple)):
        return [_fingerprint_payload(item, key=key, hash_files=hash_files) for item in value]
    if isinstance(value, Path):
        value = str(value)
    if isinstance(value, str) and (key in REQUEST_FILE_KEYS or key.endswith("_file")):
        path = Path(value)
        if hash_files:
            try:
                if path.is_file():
                    return {
                        "input_file_sha256": sha256_file(path),
                        "size": path.stat().st_size,
                    }
            except OSError:
                pass
        # Old task rows did not freeze input hashes. Keep only the semantic
        # input slot so run ids and random task ids cannot prevent a match.
        return {"legacy_input_slot": key, "suffix": path.suffix.lower()}
    return value


def request_fingerprint(
    company_id: str,
    phase: str,
    platform: str | None,
    payload: dict[str, Any],
    *,
    hash_files: bool = True,
) -> str:
    """Hash the business identity of one attempt, independent of execution mode."""
    identity = {
        "version": 1,
        "company_id": company_id,
        "phase": phase,
        "platform": platform,
        "payload": _fingerprint_payload(payload, hash_files=hash_files),
    }
    return hashlib.sha256(stable_json(identity).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_within(root: Path, candidate: Path) -> Path:
    root = root.resolve()
    candidate = candidate.resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"path escapes root: {candidate}") from exc
    return candidate


def safe_name(value: str, limit: int = 80) -> str:
    cleaned = []
    for ch in value.strip():
        if ch.isalnum() or ch in {"_", "-", "."}:
            cleaned.append(ch)
        else:
            cleaned.append("_")
    name = "".join(cleaned).strip("_.")
    return (name[:limit] or "item")


@dataclasses.dataclass(slots=True)
class OutputManifestEntry:
    path: str
    kind: str
    size: int
    sha256: str


def build_manifest(root: Path, paths: Iterable[Path]) -> list[OutputManifestEntry]:
    items: list[OutputManifestEntry] = []
    root = root.resolve()
    for path in paths:
        resolved = ensure_within(root, path)
        items.append(
            OutputManifestEntry(
                path=str(resolved.relative_to(root)).replace("\\", "/"),
                kind=resolved.suffix.lstrip(".") or "file",
                size=resolved.stat().st_size,
                sha256=sha256_file(resolved),
            )
        )
    return items


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, dict) and "diagnosis" not in payload:
        payload = atomic_result(payload, source="atomic.write_json")
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(stable_json(payload))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def resolve_output(output_dir_arg: Any, output_file_arg: Any, default_dir: Path, default_name: str) -> Path:
    """统一原子脚本输出路径：--output-file 给定则直接用（绝对路径或取 name 落到目录），否则用 目录/默认名。只建最终父目录。"""
    directory = Path(output_dir_arg) if output_dir_arg else default_dir
    if output_file_arg:
        path = Path(output_file_arg)
        output_file = path if path.is_absolute() else directory / path.name
    else:
        output_file = directory / default_name
    output_file.parent.mkdir(parents=True, exist_ok=True)
    return output_file


def append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(stable_json(payload))
        handle.write("\n")


@dataclasses.dataclass(slots=True)
class StreamItem:
    schema_version: int
    record_type: str
    operation: str
    job_id: str
    seq: int
    item_key: str
    item_kind: str
    data: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(slots=True)
class StreamFinal:
    schema_version: int
    record_type: str
    operation: str
    job_id: str
    status: str
    error_code: str | None
    message: str | None
    item_count: int
    output_manifest: list[dict[str, Any]]
    started_at: str
    finished_at: str
    elapsed_ms: int

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def normalize_result_status(status: str) -> str:
    mapping = {
        "ok": "success",
        "success": "success",
        "empty": "empty_success",
        "empty_success": "empty_success",
        "retryable": "retryable_error",
        "retryable_error": "retryable_error",
        "failed": "final_error",
        "final_error": "final_error",
        "skip": "skipped",
        "skipped": "skipped",
    }
    return mapping.get(status, status)
