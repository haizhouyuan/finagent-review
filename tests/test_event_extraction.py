from __future__ import annotations

from pathlib import Path
from urllib.error import URLError

from finagent.event_extraction import (
    ChatgptRestClient,
    _http_json,
    _is_loopback_url,
    classify_contract_miss,
    default_provider_fallbacks,
    evaluate_extracted_batch,
    extract_answer_kind_for_ask_kind,
    load_chatgptrest_env_fallback,
    make_idempotency_key,
    parse_event_batch_text,
    provider_attempt_plan,
    sentinel_context_from_spec,
)
from finagent.sentinel import build_extraction_prompt


def test_parse_event_batch_text_accepts_fenced_json_array() -> None:
    raw = """```json
[
  {
    "entity": "Jinpan",
    "product": "SST",
    "event_type": "product_milestone"
  }
]
```"""
    batch = parse_event_batch_text(raw)
    assert len(batch) == 1
    assert batch[0]["entity"] == "Jinpan"


def test_parse_event_batch_text_finds_embedded_array() -> None:
    raw = """
模型说明：

[
  {
    "entity": "GE Vernova",
    "product": "Onsite power",
    "event_type": "financial"
  }
]

以上是抽取结果。
"""
    batch = parse_event_batch_text(raw)
    assert len(batch) == 1
    assert batch[0]["product"] == "Onsite power"


def test_sentinel_context_from_spec_formats_entries() -> None:
    spec = {
        "sentinel": [
            {
                "entity": "Rocket Lab",
                "product": "Neutron",
                "bucket_role": "option",
                "entity_role": "tracked",
                "grammar_key": "commercial_space_launch_progress",
                "current_stage": "prototype",
                "expected_next_stage": "qualification",
                "trigger_code": "B1",
                "evidence_text": "样机推进中",
            }
        ]
    }
    context = sentinel_context_from_spec(spec)
    assert "Source Policy Snapshot" in context
    assert "Sector Grammar Hints" in context
    assert "Rocket Lab" in context
    assert "bucket=option" in context
    assert "grammar=commercial_space_launch_progress" in context
    assert "trigger=B1" in context


def test_make_idempotency_key_is_stable() -> None:
    left = make_idempotency_key("gemini_web.ask", "pro", "/tmp/raw.md", "/tmp/out.json", "prompt-body")
    right = make_idempotency_key("gemini_web.ask", "pro", "/tmp/raw.md", "/tmp/out.json", "prompt-body")
    assert left == right


def test_build_extraction_prompt_includes_kol_digest_source_role() -> None:
    prompt = build_extraction_prompt("raw text", sentinel_context=None)
    assert "kol_digest" in prompt
    assert "tertiary source" in prompt


def test_chatgptrest_client_poll_job_stops_on_completed(monkeypatch) -> None:
    client = ChatgptRestClient(base_url="http://127.0.0.1:18711")
    seen = iter(
        [
            {"status": "queued"},
            {"status": "in_progress"},
            {"status": "completed", "completion_quality": "ok"},
        ]
    )

    monkeypatch.setattr(
        ChatgptRestClient,
        "get_job",
        lambda self, *, job_id, timeout_sec=30.0: next(seen),
    )
    monkeypatch.setattr("finagent.event_extraction.time.sleep", lambda *_args, **_kwargs: None)

    result = client.poll_job(job_id="job-1", timeout_seconds=10, poll_seconds=0.01)
    assert result["status"] == "completed"


def test_load_chatgptrest_env_fallback_reads_explicit_file(monkeypatch, tmp_path: Path) -> None:
    env_path = tmp_path / "chatgptrest.env"
    env_path.write_text(
        "CHATGPTREST_API_TOKEN=test-token\nCHATGPTREST_BASE_URL=http://127.0.0.1:19999\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("CHATGPTREST_CREDENTIALS_ENV", str(env_path))
    values = load_chatgptrest_env_fallback()
    assert values["CHATGPTREST_API_TOKEN"] == "test-token"
    assert values["CHATGPTREST_BASE_URL"] == "http://127.0.0.1:19999"


def test_evaluate_extracted_batch_counts_tracked_and_candidate_hits() -> None:
    batch = [
        {
            "entity": "Rocket Lab",
            "product": "Neutron",
            "event_type": "product_milestone",
            "source_tier": "primary",
            "stage_from": "prototype",
            "stage_to": "sample",
            "candidate_thesis": None,
        },
        {
            "entity": "Terran Orbital Supplier",
            "product": "Space components",
            "event_type": "candidate",
            "source_tier": "tertiary",
            "stage_from": None,
            "stage_to": None,
            "candidate_thesis": "space_supply_chain",
        },
    ]
    spec = {
        "sentinel": [
            {
                "entity": "Rocket Lab",
                "product": "Neutron",
                "grammar_key": "commercial_space_launch_progress",
            }
        ]
    }
    payload = evaluate_extracted_batch(batch, spec)
    assert payload["batch_size"] == 2
    assert payload["tracked_hits"] == 1
    assert payload["candidate_hits"] == 1
    assert payload["grammar_hits"]["commercial_space_launch_progress"] == 1


def test_http_json_retries_connection_refused_then_succeeds(monkeypatch) -> None:
    attempts = {"count": 0}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"ok": true}'

    def _fake_urlopen(_req, *, timeout_sec):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise URLError("[Errno 111] Connection refused")
        return _Resp()

    monkeypatch.setattr("finagent.event_extraction._urlopen", _fake_urlopen)
    monkeypatch.setattr("finagent.event_extraction.time.sleep", lambda *_args, **_kwargs: None)
    payload = _http_json(method="GET", url="http://127.0.0.1:18711/v1/jobs/demo", retries=1)
    assert payload["ok"] is True
    assert attempts["count"] == 2


def test_default_provider_fallbacks_promote_chatgpt_auto_for_gemini() -> None:
    assert default_provider_fallbacks("gemini_web.ask", "pro") == [("chatgpt_web.ask", "auto")]
    assert default_provider_fallbacks("chatgpt_web.ask", "pro_extended") == [("chatgpt_web.ask", "auto")]
    assert default_provider_fallbacks("chatgpt_web.ask", "auto") == []


def test_extract_answer_kind_for_ask_kind_is_provider_specific() -> None:
    assert extract_answer_kind_for_ask_kind("chatgpt_web.ask") == "chatgpt_web.extract_answer"
    assert extract_answer_kind_for_ask_kind("gemini_web.ask") == "gemini_web.extract_answer"
    assert extract_answer_kind_for_ask_kind("qwen_web.ask") is None


def test_provider_attempt_plan_dedupes_primary_and_fallbacks() -> None:
    plan = provider_attempt_plan(
        "gemini_web.ask",
        "pro",
        fallbacks=[("chatgpt_web.ask", "auto"), ("gemini_web.ask", "pro")],
    )
    assert plan == [("gemini_web.ask", "pro"), ("chatgpt_web.ask", "auto")]


def test_classify_contract_miss_flags_benchmark_style_semantic_drift() -> None:
    text = "对基于公共信息的事件驱动研究引擎进行基准测试，并将其与 Qlib、FinRL、OpenBB 对比。"
    assert classify_contract_miss(text) == "semantic_drift_benchmark_answer"


def test_is_loopback_url_accepts_ipv4_and_ipv6_loopback_only_for_http() -> None:
    assert _is_loopback_url("http://127.0.0.1:18711/v1/jobs") is True
    assert _is_loopback_url("https://127.0.0.42:18711/v1/jobs") is True
    assert _is_loopback_url("http://[::1]:18711/v1/jobs") is True
    assert _is_loopback_url("ftp://127.0.0.1:21") is False
    assert _is_loopback_url("http://10.0.0.8:18711/v1/jobs") is False


def test_classify_contract_miss_flags_missing_json() -> None:
    assert classify_contract_miss("只是解释，没有任何数组。") == "missing_json_array"
