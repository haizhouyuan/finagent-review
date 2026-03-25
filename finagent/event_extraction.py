from __future__ import annotations

import hashlib
import ipaddress
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

from .sentinel import build_spec_prompt_context


class ExtractionError(RuntimeError):
    pass


def provider_attempt_label(kind: str, preset: str) -> str:
    return f"{str(kind).strip()}::{str(preset).strip()}"


def _is_loopback_url(url: str) -> bool:
    try:
        parsed = urllib.parse.urlparse(str(url or "").strip())
    except Exception:
        return False
    if parsed.scheme not in {"http", "https"}:
        return False
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return False
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def extract_answer_kind_for_ask_kind(kind: str) -> str | None:
    normalized_kind = str(kind or "").strip()
    if normalized_kind == "chatgpt_web.ask":
        return "chatgpt_web.extract_answer"
    if normalized_kind == "gemini_web.ask":
        return "gemini_web.extract_answer"
    return None


def _urlopen(req: urllib.request.Request, *, timeout_sec: float) -> Any:
    if not _is_loopback_url(req.full_url):
        return urllib.request.urlopen(req, timeout=float(timeout_sec))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return opener.open(req, timeout=float(timeout_sec))


def _http_json(
    *,
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    body: dict[str, Any] | None = None,
    timeout_sec: float = 30.0,
    retries: int = 0,
    retry_sleep_sec: float = 2.0,
) -> dict[str, Any]:
    attempts = max(1, int(retries) + 1)
    last_error: Exception | None = None
    for attempt in range(attempts):
        hdrs = dict(headers or {})
        data: bytes | None = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            hdrs.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(url, data=data, headers=hdrs, method=str(method).upper())
        try:
            with _urlopen(req, timeout_sec=timeout_sec) as resp:
                text = resp.read().decode("utf-8", errors="replace")
                return json.loads(text) if text.strip() else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace") if hasattr(exc, "read") else ""
            if exc.code == 401:
                raise ExtractionError(
                    "HTTP 401 Unauthorized: set CHATGPTREST_API_TOKEN or point --api-base to a token-free local endpoint"
                ) from exc
            raise ExtractionError(f"HTTP {exc.code} {exc.reason}: {raw}") from exc
        except urllib.error.URLError as exc:
            last_error = exc
            reason = str(getattr(exc, "reason", None) or exc)
            if attempt >= attempts - 1 or "111" not in reason and "Connection refused" not in reason:
                break
            time.sleep(max(0.5, float(retry_sleep_sec)))
    raise ExtractionError(f"URLError: {getattr(last_error, 'reason', None) or last_error}") from last_error


def completion_quality(job: dict[str, Any] | None) -> str:
    obj = job or {}
    return str(obj.get("completion_quality") or obj.get("reason_type") or "").strip().lower()


def is_usable_completion(job: dict[str, Any] | None) -> bool:
    if str((job or {}).get("status") or "").strip().lower() != "completed":
        return False
    return completion_quality(job) not in {"completed_under_min_chars", "suspect_short_answer"}


def _strip_code_fence(text: str) -> str:
    stripped = text.strip()
    if not stripped.startswith("```"):
        return stripped
    lines = stripped.splitlines()
    if lines and lines[0].startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def parse_event_batch_text(answer_text: str) -> list[dict[str, Any]]:
    candidate = _strip_code_fence(answer_text)
    decoder = json.JSONDecoder()
    for start in range(len(candidate)):
        if candidate[start] != "[":
            continue
        try:
            parsed, _end = decoder.raw_decode(candidate[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, list) and all(isinstance(item, dict) for item in parsed):
            return parsed
    raise ExtractionError("could not parse JSON event batch from answer text")


def classify_contract_miss(answer_text: str) -> str:
    raw = str(answer_text or "")
    lowered = raw.lower()
    if not raw.strip():
        return "empty_answer"
    if all(token in lowered for token in ("qlib", "finrl", "openbb")):
        return "semantic_drift_benchmark_answer"
    if "[" not in raw and "```json" not in lowered:
        return "missing_json_array"
    return "unparseable_json_array"


def default_provider_fallbacks(kind: str, preset: str) -> list[tuple[str, str]]:
    normalized_kind = str(kind or "").strip()
    normalized_preset = str(preset or "").strip()
    if normalized_kind == "gemini_web.ask":
        return [("chatgpt_web.ask", "auto")]
    if normalized_kind == "chatgpt_web.ask" and normalized_preset != "auto":
        return [("chatgpt_web.ask", "auto")]
    return []


def provider_attempt_plan(
    primary_kind: str,
    primary_preset: str,
    *,
    fallbacks: list[tuple[str, str]] | None = None,
) -> list[tuple[str, str]]:
    plan = [(str(primary_kind).strip(), str(primary_preset).strip())]
    plan.extend(default_provider_fallbacks(primary_kind, primary_preset))
    if fallbacks:
        plan.extend((str(kind).strip(), str(preset).strip()) for kind, preset in fallbacks)
    deduped: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for item in plan:
        if not item[0] or not item[1] or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def sentinel_context_from_spec(spec: dict[str, Any]) -> str:
    return build_spec_prompt_context(spec)


def make_idempotency_key(*parts: str) -> str:
    payload = "::".join(str(part) for part in parts)
    return "finagent-event-extract-" + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def _load_env_file_values(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'\"")
    return values


def _candidate_env_files() -> list[Path]:
    explicit = str(os.environ.get("CHATGPTREST_CREDENTIALS_ENV") or "").strip()
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    candidates.extend(
        [
            Path("/home/yuanhaizhou/.config/chatgptrest/chatgptrest.env"),
            Path("/vol1/maint/MAIN/secrets/credentials.env"),
        ]
    )
    deduped: list[Path] = []
    seen: set[str] = set()
    for path in candidates:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(path)
    return deduped


def load_chatgptrest_env_fallback() -> dict[str, str]:
    merged: dict[str, str] = {}
    for path in _candidate_env_files():
        values = _load_env_file_values(path)
        if not values:
            continue
        for key in ("CHATGPTREST_BASE_URL", "CHATGPTREST_URL", "CHATGPTREST_API_TOKEN"):
            if key in values and key not in merged:
                merged[key] = values[key]
    return merged


@dataclass(frozen=True)
class ChatgptRestClient:
    base_url: str
    api_token: str | None = None
    client_name: str = "finagent-event-extractor"
    client_instance: str = "local"
    http_retries: int = 3
    retry_sleep_sec: float = 2.0

    @classmethod
    def from_env(
        cls,
        *,
        client_name: str = "finagent-event-extractor",
        client_instance: str = "local",
    ) -> "ChatgptRestClient":
        fallback = load_chatgptrest_env_fallback()
        base_url = (
            (os.environ.get("CHATGPTREST_BASE_URL") or "").strip()
            or (os.environ.get("CHATGPTREST_URL") or "").strip()
            or str(fallback.get("CHATGPTREST_BASE_URL") or "").strip()
            or str(fallback.get("CHATGPTREST_URL") or "").strip()
            or "http://127.0.0.1:18711"
        )
        token = (
            (os.environ.get("CHATGPTREST_API_TOKEN") or "").strip()
            or str(fallback.get("CHATGPTREST_API_TOKEN") or "").strip()
            or None
        )
        return cls(
            base_url=base_url.rstrip("/"),
            api_token=token,
            client_name=client_name,
            client_instance=client_instance,
            http_retries=3,
            retry_sleep_sec=2.0,
        )

    def _headers(self, *, request_id: str | None = None) -> dict[str, str]:
        headers: dict[str, str] = {
            "X-Client-Name": self.client_name,
            "X-Client-Instance": self.client_instance,
        }
        if request_id:
            headers["X-Request-ID"] = request_id
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def create_job(
        self,
        *,
        idempotency_key: str,
        kind: str,
        input_payload: dict[str, Any],
        params: dict[str, Any],
        request_id: str,
        timeout_sec: float = 30.0,
    ) -> dict[str, Any]:
        headers = self._headers(request_id=request_id)
        headers["Idempotency-Key"] = idempotency_key
        return _http_json(
            method="POST",
            url=f"{self.base_url}/v1/jobs",
            headers=headers,
            body={"kind": kind, "input": input_payload, "params": params, "client": {"name": self.client_name}},
            timeout_sec=timeout_sec,
            retries=self.http_retries,
            retry_sleep_sec=self.retry_sleep_sec,
        )

    def wait_job(
        self,
        *,
        job_id: str,
        timeout_seconds: int,
        poll_seconds: float = 2.0,
        timeout_sec: float | None = None,
    ) -> dict[str, Any]:
        query = urllib.parse.urlencode({"timeout_seconds": int(timeout_seconds), "poll_seconds": float(poll_seconds)})
        http_timeout = float(timeout_sec) if timeout_sec is not None else float(max(15.0, int(timeout_seconds) + 30))
        return _http_json(
            method="GET",
            url=f"{self.base_url}/v1/jobs/{job_id}/wait?{query}",
            headers=self._headers(),
            timeout_sec=http_timeout,
            retries=self.http_retries,
            retry_sleep_sec=self.retry_sleep_sec,
        )

    def get_job(self, *, job_id: str, timeout_sec: float = 30.0) -> dict[str, Any]:
        return _http_json(
            method="GET",
            url=f"{self.base_url}/v1/jobs/{job_id}",
            headers=self._headers(),
            timeout_sec=timeout_sec,
            retries=self.http_retries,
            retry_sleep_sec=self.retry_sleep_sec,
        )

    def poll_job(
        self,
        *,
        job_id: str,
        timeout_seconds: int,
        poll_seconds: float = 5.0,
    ) -> dict[str, Any]:
        deadline = time.time() + float(timeout_seconds)
        last_seen: dict[str, Any] = {}
        while time.time() < deadline:
            last_seen = self.get_job(job_id=job_id)
            status = str(last_seen.get("status") or "").strip().lower()
            if status in {"completed", "error", "canceled"}:
                return last_seen
            time.sleep(max(0.5, float(poll_seconds)))
        return last_seen

    def read_full_answer(self, *, job_id: str, max_total_bytes: int = 5_000_000) -> str:
        offset = 0
        chunks: list[str] = []
        total_bytes = 0
        while True:
            query = urllib.parse.urlencode({"offset": offset, "max_chars": 20000})
            res = _http_json(
                method="GET",
                url=f"{self.base_url}/v1/jobs/{job_id}/answer?{query}",
                headers=self._headers(),
                timeout_sec=30.0,
                retries=self.http_retries,
                retry_sleep_sec=self.retry_sleep_sec,
            )
            chunk = str(res.get("chunk") or "")
            chunks.append(chunk)
            total_bytes += len(chunk.encode("utf-8", errors="replace"))
            if total_bytes > max_total_bytes:
                raise ExtractionError("answer exceeded max_total_bytes while reading ChatgptREST answer")
            if bool(res.get("done")):
                break
            next_offset = res.get("next_offset")
            if next_offset is None:
                break
            offset = int(next_offset)
        return "".join(chunks)


def evaluate_extracted_batch(batch: list[dict[str, Any]], spec: dict[str, Any] | None = None) -> dict[str, Any]:
    spec_entries = list((spec or {}).get("sentinel") or [])
    tracked_pairs = {
        (str(entry.get("entity") or ""), str(entry.get("product") or ""))
        for entry in spec_entries
        if entry.get("entity")
    }
    grammar_hits: dict[str, int] = {}
    entity_hits: dict[str, int] = {}
    source_tier_breakdown: dict[str, int] = {}
    event_type_breakdown: dict[str, int] = {}
    tracked_hits = 0
    candidate_hits = 0
    stage_transitions = 0
    for item in batch:
        entity = str(item.get("entity") or "")
        product = str(item.get("product") or "")
        entity_hits[entity] = entity_hits.get(entity, 0) + 1
        if (entity, product) in tracked_pairs:
            tracked_hits += 1
        if item.get("candidate_thesis"):
            candidate_hits += 1
        if item.get("stage_from") or item.get("stage_to"):
            stage_transitions += 1
        event_type = str(item.get("event_type") or "unknown")
        event_type_breakdown[event_type] = event_type_breakdown.get(event_type, 0) + 1
        source_tier = str(item.get("source_tier") or "unknown")
        source_tier_breakdown[source_tier] = source_tier_breakdown.get(source_tier, 0) + 1
    for entry in spec_entries:
        grammar_key = str(entry.get("grammar_key") or "")
        if grammar_key:
            grammar_hits[grammar_key] = 0
    for item in batch:
        for entry in spec_entries:
            if str(item.get("entity") or "") != str(entry.get("entity") or ""):
                continue
            grammar_key = str(entry.get("grammar_key") or "")
            if grammar_key:
                grammar_hits[grammar_key] = grammar_hits.get(grammar_key, 0) + 1
    return {
        "batch_size": len(batch),
        "tracked_hits": tracked_hits,
        "candidate_hits": candidate_hits,
        "stage_transition_count": stage_transitions,
        "entity_hits": entity_hits,
        "source_tier_breakdown": source_tier_breakdown,
        "event_type_breakdown": event_type_breakdown,
        "grammar_hits": grammar_hits,
    }
