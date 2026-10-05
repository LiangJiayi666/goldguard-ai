from __future__ import annotations
"""LLM 调用公共管道：配置加载 + 双格式调用（Anthropic Messages / OpenAI Chat） + JSON 解析。
social_account_rank 与 social_keyword_extract 共用这套管道，差异仅在 system_message
与是否剥引号，通过参数注入；各自的异常策略留在调用方。关键词生成采用 AI-only，
调用失败必须显式失败并交给主控重试，不再走本地关键词回退。

v2: 双格式 fallback（Anthropic → OpenAI），完整的 provider error 检测与日志。
"""


import json
import os
import re
import urllib.request
import urllib.error as _urllib_error
from pathlib import Path
from typing import Any


ROOT = Path(__file__).parent
# Runtime modules live under ``src/``, while user-editable settings live under
# ``config/`` at the project root.  Keeping the former path here after the
# repository reorganisation made every LLM caller behave as if no key had
# been configured.
LLM_CONFIG_FILE = ROOT.parent / "config" / "llm_config.txt"
_LEGACY_LLM_CONFIG_FILE = ROOT / "llm_config.txt"


def _resolve_llm_config_file() -> Path | None:
    """Prefer the documented config path, retaining the pre-reorganisation path as a fallback."""
    if LLM_CONFIG_FILE.is_file():
        return LLM_CONFIG_FILE
    if _LEGACY_LLM_CONFIG_FILE.is_file():
        return _LEGACY_LLM_CONFIG_FILE
    return None
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")


def load_llm_config() -> dict[str, str]:
    config: dict[str, str] = {"base_url": "https://opencode.ai/zen/go", "api_key": "", "model": "deepseek-v4-flash"}
    config_file = _resolve_llm_config_file()
    if config_file:
        for line in config_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lower()
            if key in config:
                config[key] = value.strip()
    config["base_url"] = os.environ.get("LLM_BASE_URL", config["base_url"]).rstrip("/")
    config["api_key"] = os.environ.get("LLM_API_KEY", config["api_key"])
    config["model"] = os.environ.get("LLM_MODEL", config["model"])
    return config


def _extract_content(body, strip_quotes):
    if isinstance(body.get("content"), list):
        # Prefer text blocks; fall back to thinking blocks (DeepSeek R1-style reasoning).
        text_blocks = [
            block.get("text", "") for block in body["content"]
            if isinstance(block, dict) and block.get("type") == "text"
        ]
        thinking_blocks = [
            block.get("thinking", "") for block in body["content"]
            if isinstance(block, dict) and block.get("type") == "thinking"
        ]
        content = "".join(text_blocks or thinking_blocks).strip()
        if content:
            return content.strip('"').strip("'") if strip_quotes else content
    choices = body.get("choices")
    if isinstance(choices, list) and choices:
        msg = choices[0].get("message") if isinstance(choices[0], dict) else None
        if isinstance(msg, dict):
            content = str(msg.get("content") or "").strip()
            if content:
                return content.strip('"').strip("'") if strip_quotes else content
    error = body.get("error")
    if isinstance(error, dict):
        raise RuntimeError(f"LLM provider error: {error.get('message', error)}")
    raise RuntimeError(
        f"LLM unrecognised response: {json.dumps(body, ensure_ascii=False)[:500]}"
    )


def call_llm(prompt, base_url, model, api_key, timeout,
             system_message=None, max_tokens=4096, strip_quotes=False):
    api_key = (api_key or "").strip()
    if not api_key:
        raise RuntimeError("missing LLM API key: 请在 config/llm_config.txt 中设置 api_key")
    # 占位符/非 ASCII key 会在 HTTP header 编码阶段触发 latin-1 错误，提前给出清晰提示。
    if api_key.startswith("<") or api_key.endswith(">") or "在此填入" in api_key:
        raise RuntimeError(
            "LLM API key 仍是占位符（<在此填入你的API密钥>），请在 config/llm_config.txt 中替换为真实 key"
        )
    if not api_key.isascii():
        raise RuntimeError(
            "LLM API key 包含非 ASCII 字符，请检查 config/llm_config.txt 中的 api_key"
        )

    errors = []

    payload_anthropic = {
        "model": model, "max_tokens": max_tokens, "temperature": 0.1,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system_message:
        payload_anthropic["system"] = system_message
    data = json.dumps(payload_anthropic, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        f"{base_url}/v1/messages",
        data=data,
        headers={
            "x-api-key": api_key, "anthropic-version": "2023-06-01",
            "Content-Type": "application/json", "User-Agent": BROWSER_UA,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            raw = resp.read().decode(charset, errors="replace")
    except _urllib_error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        errors.append(f"anthropic HTTP {exc.code}: {raw[:300]}")
    else:
        if not raw.strip():
            errors.append("anthropic: empty body")
        else:
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                errors.append(f"anthropic: non-JSON ({len(raw)}B): {raw[:200]}")
            else:
                err = body.get("error")
                if isinstance(err, dict):
                    errors.append(f"anthropic provider: {err.get('message', err)}")
                else:
                    return _extract_content(body, strip_quotes)

    payload_openai = {
        "model": model, "max_tokens": max_tokens, "temperature": 0.1,
        "messages": [{"role": "user", "content": prompt}],
    }
    if system_message:
        payload_openai["messages"].insert(0, {"role": "system", "content": system_message})
    data2 = json.dumps(payload_openai, ensure_ascii=False).encode("utf-8")
    req2 = urllib.request.Request(
        f"{base_url}/v1/chat/completions",
        data=data2,
        headers={
            # OpenAI-compatible gateways conventionally authenticate with
            # Bearer.  Keep x-api-key too for gateways that accept either.
            "Authorization": f"Bearer {api_key}", "x-api-key": api_key,
            "Content-Type": "application/json",
            "User-Agent": BROWSER_UA,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req2, timeout=timeout) as resp:
            charset = resp.headers.get_content_charset() or "utf-8"
            raw2 = resp.read().decode(charset, errors="replace")
    except _urllib_error.HTTPError as exc:
        raw2 = exc.read().decode("utf-8", errors="replace")
        errors.append(f"openai HTTP {exc.code}: {raw2[:300]}")
        raise RuntimeError("all LLM formats failed: " + "; ".join(errors))

    if not raw2.strip():
        errors.append("openai: empty body")
        raise RuntimeError("all LLM formats failed: " + "; ".join(errors))
    try:
        body2 = json.loads(raw2)
    except json.JSONDecodeError:
        errors.append(f"openai: non-JSON ({len(raw2)}B): {raw2[:200]}")
        raise RuntimeError("all LLM formats failed: " + "; ".join(errors))
    err2 = body2.get("error")
    if isinstance(err2, dict):
        errors.append(f"openai provider: {err2.get('message', err2)}")
        raise RuntimeError("all LLM formats failed: " + "; ".join(errors))
    return _extract_content(body2, strip_quotes)


def _balanced_json_fragments(text: str):
    """Yield balanced JSON candidates without being confused by quoted braces."""
    for start, opening in enumerate(text):
        if opening not in "[{":
            continue
        stack = [opening]
        in_string = False
        escaped = False
        for index in range(start + 1, len(text)):
            char = text[index]
            if in_string:
                if escaped:
                    escaped = False
                elif char == "\\\\":
                    escaped = True
                elif char == '"':
                    in_string = False
                continue
            if char == '"':
                in_string = True
            elif char in "[{":
                stack.append(char)
            elif char in "]}":
                expected_opening = "[" if char == "]" else "{"
                if not stack or stack[-1] != expected_opening:
                    break
                stack.pop()
                if not stack:
                    yield text[start:index + 1]
                    break


def parse_llm_json(text):
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()
    original_text = text
    if not text.startswith(("{", "[")):
        # 模型不稳定：偶尔把思考过程/解释输出在 JSON 前后（"我需要根据证据……{...}"）。
        # 提取第一个 { 到最后一个 } 的片段再解析；解析失败抛原始错误便于诊断。
        start = text.find("{")
        if start < 0:
            start = text.find("[")
        if start >= 0:
            end = text.rfind("}") if text[start] == "{" else text.rfind("]")
            if end > start:
                text = text[start:end + 1].strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as direct_error:
        for fragment in _balanced_json_fragments(original_text):
            if fragment == original_text:
                continue
            try:
                return json.loads(fragment)
            except json.JSONDecodeError:
                continue
        raise direct_error
