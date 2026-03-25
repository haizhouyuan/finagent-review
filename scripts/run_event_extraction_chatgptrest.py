from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from finagent.db import connect, init_db
from finagent.event_runs import record_event_mining_run
from finagent.event_extraction import (
    ChatgptRestClient,
    classify_contract_miss,
    default_provider_fallbacks,
    extract_answer_kind_for_ask_kind,
    ExtractionError,
    evaluate_extracted_batch,
    is_usable_completion,
    make_idempotency_key,
    parse_event_batch_text,
    provider_attempt_label,
    provider_attempt_plan,
    sentinel_context_from_spec,
)
from finagent.paths import ensure_runtime_dirs, resolve_paths
from finagent.sentinel import (
    build_extraction_prompt,
    import_events,
    load_sentinel_spec,
    normalize_event,
    validate_event,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract event batches from raw text via ChatgptREST.")
    parser.add_argument("--root", required=True, help="finagent root")
    parser.add_argument("--path", required=True, help="raw text / markdown file")
    parser.add_argument("--output", required=True, help="where to write extracted event batch JSON")
    parser.add_argument("--spec", help="sentinel spec path; defaults to root/specs/sentinel_v1.yaml when present")
    parser.add_argument("--kind", default="gemini_web.ask")
    parser.add_argument("--preset", default="pro")
    parser.add_argument("--purpose", default="prod")
    parser.add_argument("--max-wait-seconds", type=int, default=900)
    parser.add_argument("--min-chars", type=int, default=1200)
    parser.add_argument(
        "--fallback-provider",
        action="append",
        default=[],
        help="optional fallback provider in kind:preset form; may be passed multiple times",
    )
    parser.add_argument("--client-name", default="chatgptrestctl")
    parser.add_argument("--client-instance", default="finagent-event-extractor")
    parser.add_argument("--api-base", default=None)
    parser.add_argument("--credentials-env", default="", help="optional env file containing CHATGPTREST_* fallback values")
    parser.add_argument("--write-answer", help="optional path to persist raw model answer markdown")
    parser.add_argument("--import-events", action="store_true", help="validate + import batch into finagent db")
    return parser.parse_args()


def _resolve(root: Path, raw: str) -> Path:
    path = Path(raw)
    return path if path.is_absolute() else (root / path)


def _parse_fallback_provider(raw: str) -> tuple[str, str]:
    text = str(raw or "").strip()
    if ":" not in text:
        raise ExtractionError(f"fallback provider must be kind:preset, got: {raw!r}")
    kind, preset = text.split(":", 1)
    kind = kind.strip()
    preset = preset.strip()
    if not kind or not preset:
        raise ExtractionError(f"fallback provider must be kind:preset, got: {raw!r}")
    return kind, preset


def _attempt_answer_path(base_path: Path | None, *, kind: str, preset: str, index: int) -> Path | None:
    if base_path is None:
        return None
    suffix = base_path.suffix or ".md"
    stem = base_path.stem
    safe_kind = kind.replace(".", "_")
    safe_preset = preset.replace(".", "_")
    return base_path.with_name(f"{stem}__attempt{index}_{safe_kind}_{safe_preset}{suffix}")


def main() -> None:
    args = parse_args()
    if args.credentials_env:
        import os

        os.environ["CHATGPTREST_CREDENTIALS_ENV"] = str(_resolve(Path.cwd(), args.credentials_env))
    root = Path(args.root).resolve()
    raw_path = _resolve(root, args.path)
    output_path = _resolve(root, args.output)
    answer_path = _resolve(root, args.write_answer) if args.write_answer else None
    spec_path = _resolve(root, args.spec) if args.spec else (root / "specs" / "sentinel_v1.yaml")

    raw_text = raw_path.read_text(encoding="utf-8")
    sentinel_context = None
    if spec_path.exists():
        spec = load_sentinel_spec(spec_path)
        sentinel_context = sentinel_context_from_spec(spec)
    prompt = build_extraction_prompt(raw_text, sentinel_context=sentinel_context)

    client = ChatgptRestClient.from_env(
        client_name=args.client_name,
        client_instance=args.client_instance,
    )
    if args.api_base:
        client = ChatgptRestClient(
            base_url=str(args.api_base).rstrip("/"),
            api_token=client.api_token,
            client_name=args.client_name,
            client_instance=args.client_instance,
        )

    fallback_plan = [_parse_fallback_provider(raw) for raw in args.fallback_provider]
    attempt_plan = provider_attempt_plan(args.kind, args.preset, fallbacks=fallback_plan)
    attempt_failures: list[dict[str, object]] = []
    attempt_records: list[dict[str, object]] = []
    batch: list[dict[str, object]] | None = None
    answer_text = ""
    selected_kind = ""
    selected_preset = ""
    selected_job_id = ""
    selected_source_job_id = ""
    selected_conversation_url = ""
    used_extract_fallback = False

    for attempt_index, (attempt_kind, attempt_preset) in enumerate(attempt_plan, start=1):
        idempotency_key = make_idempotency_key(
            attempt_kind,
            attempt_preset,
            str(raw_path),
            str(output_path),
            prompt,
        )
        request_id = f"{args.client_name}-{uuid4().hex[:12]}"
        created = client.create_job(
            idempotency_key=idempotency_key,
            kind=attempt_kind,
            input_payload={"question": prompt},
            params={
                "preset": attempt_preset,
                "purpose": args.purpose,
                "answer_format": "markdown",
                "max_wait_seconds": args.max_wait_seconds,
                "min_chars": args.min_chars,
            },
            request_id=request_id,
        )
        job_id = str(created.get("job_id") or "")
        if not job_id:
            raise ExtractionError(f"ChatgptREST create_job did not return job_id: {created}")

        deadline = time.time() + float(args.max_wait_seconds)
        primary_job = client.poll_job(job_id=job_id, timeout_seconds=args.max_wait_seconds, poll_seconds=5.0)
        final_job = primary_job
        final_job_id = job_id
        fallback_used = False
        if not is_usable_completion(primary_job):
            status = str(primary_job.get("status") or "").strip().lower()
            if status in {"error", "canceled"}:
                attempt_failures.append(
                    {
                        "provider": provider_attempt_label(attempt_kind, attempt_preset),
                        "kind": attempt_kind,
                        "preset": attempt_preset,
                        "job_id": job_id,
                        "failure": "provider_terminal_error",
                        "status": status,
                        "reason_type": primary_job.get("reason_type"),
                    }
                )
                continue
            conversation_url = str(primary_job.get("conversation_url") or "").strip()
            if not conversation_url:
                attempt_failures.append(
                    {
                        "provider": provider_attempt_label(attempt_kind, attempt_preset),
                        "kind": attempt_kind,
                        "preset": attempt_preset,
                        "job_id": job_id,
                        "failure": "missing_conversation_url",
                        "status": status,
                    }
                )
                continue
            extract_kind = extract_answer_kind_for_ask_kind(attempt_kind)
            if not extract_kind:
                attempt_failures.append(
                    {
                        "provider": provider_attempt_label(attempt_kind, attempt_preset),
                        "kind": attempt_kind,
                        "preset": attempt_preset,
                        "job_id": job_id,
                        "failure": "unsupported_extract_fallback_kind",
                        "status": status,
                    }
                )
                continue
            extract_timeout = max(60, int(deadline - time.time()))
            extract_job = client.create_job(
                idempotency_key=make_idempotency_key(extract_kind, conversation_url, str(raw_path)),
                kind=extract_kind,
                input_payload={"conversation_url": conversation_url},
                params={"timeout_seconds": extract_timeout},
                request_id=f"{args.client_name}-extract-{uuid4().hex[:12]}",
            )
            extract_job_id = str(extract_job.get("job_id") or "")
            if not extract_job_id:
                raise ExtractionError(f"extract_answer job did not return job_id: {extract_job}")
            extract_job_status = client.poll_job(
                job_id=extract_job_id,
                timeout_seconds=extract_timeout,
                poll_seconds=5.0,
            )
            if not is_usable_completion(extract_job_status):
                attempt_failures.append(
                    {
                        "provider": provider_attempt_label(attempt_kind, attempt_preset),
                        "kind": attempt_kind,
                        "preset": attempt_preset,
                        "job_id": job_id,
                        "extract_job_id": extract_job_id,
                        "failure": "extract_fallback_unusable",
                        "status": str(extract_job_status.get("status") or "").strip().lower(),
                    }
                )
                continue
            final_job = extract_job_status
            final_job_id = extract_job_id
            fallback_used = True

        attempt_answer_text = client.read_full_answer(job_id=final_job_id)
        attempt_answer_path = _attempt_answer_path(answer_path, kind=attempt_kind, preset=attempt_preset, index=attempt_index)
        if attempt_answer_path is not None:
            attempt_answer_path.parent.mkdir(parents=True, exist_ok=True)
            attempt_answer_path.write_text(attempt_answer_text, encoding="utf-8")
        try:
            parsed_batch = parse_event_batch_text(attempt_answer_text)
        except ExtractionError:
            failure = classify_contract_miss(attempt_answer_text)
            attempt_failures.append(
                {
                    "provider": provider_attempt_label(attempt_kind, attempt_preset),
                    "kind": attempt_kind,
                    "preset": attempt_preset,
                    "job_id": job_id,
                    "final_job_id": final_job_id,
                    "failure": failure,
                    "used_extract_fallback": fallback_used,
                }
            )
            attempt_records.append(
                {
                    "provider": provider_attempt_label(attempt_kind, attempt_preset),
                    "job_id": job_id,
                    "final_job_id": final_job_id,
                    "status": "contract_miss",
                    "failure": failure,
                    "answer_path": str(attempt_answer_path) if attempt_answer_path else None,
                }
            )
            continue

        batch = parsed_batch
        answer_text = attempt_answer_text
        selected_kind = attempt_kind
        selected_preset = attempt_preset
        selected_job_id = final_job_id
        selected_source_job_id = job_id
        selected_conversation_url = str(primary_job.get("conversation_url") or "")
        used_extract_fallback = fallback_used
        attempt_records.append(
            {
                "provider": provider_attempt_label(attempt_kind, attempt_preset),
                "job_id": job_id,
                "final_job_id": final_job_id,
                "status": "completed",
                "used_extract_fallback": fallback_used,
                "answer_path": str(attempt_answer_path) if attempt_answer_path else None,
            }
        )
        break

    if batch is None:
        raise ExtractionError(f"all provider attempts failed: {attempt_failures}")

    if answer_path is not None:
        answer_path.parent.mkdir(parents=True, exist_ok=True)
        answer_path.write_text(answer_text, encoding="utf-8")

    normalized_batch: list[dict[str, object]] = []
    validation_errors: list[dict[str, object]] = []
    for draft in batch:
        normalized = normalize_event(draft)
        errors = validate_event(normalized)
        if errors:
            validation_errors.append({"event_id": normalized.get("event_id"), "errors": errors, "draft": normalized})
            continue
        normalized_batch.append(normalized)

    if validation_errors:
        raise ExtractionError(f"extracted batch failed validation: {validation_errors}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(normalized_batch, ensure_ascii=False, indent=2), encoding="utf-8")
    extraction_evaluation = evaluate_extracted_batch(normalized_batch, spec if spec_path.exists() else None)

    import_result = None
    conn = None
    if args.import_events:
        paths = resolve_paths(root)
        ensure_runtime_dirs(paths)
        conn = connect(paths.db_path)
        init_db(conn)
        import_result = import_events(conn, normalized_batch)
    else:
        paths = resolve_paths(root)
        ensure_runtime_dirs(paths)
        conn = connect(paths.db_path)
        init_db(conn)

    payload = {
        "ok": True,
        "job_id": selected_job_id,
        "source_job_id": selected_source_job_id,
        "output_path": str(output_path),
        "count": len(normalized_batch),
        "import_result": import_result,
        "extraction_evaluation": extraction_evaluation,
        "provider_kind": selected_kind,
        "provider_preset": selected_preset,
        "provider_attempts": attempt_records,
        "provider_failures": attempt_failures,
        "used_extract_fallback": used_extract_fallback,
        "conversation_url": selected_conversation_url,
    }
    run_record = record_event_mining_run(
        conn,
        engine="event_mining.extraction",
        run_slug=raw_path.stem,
        schema_version="3.0",
        input_refs={
            "kind": "event_extraction",
            "raw_path": str(raw_path),
            "output_path": str(output_path),
            "spec_path": str(spec_path),
            "provider_kind": args.kind,
            "preset": args.preset,
        },
        output_ref=str(output_path),
        summary=payload,
    )
    registry_paths = resolve_paths(REPO_ROOT)
    ensure_runtime_dirs(registry_paths)
    registry_conn = connect(registry_paths.db_path)
    init_db(registry_conn)
    record_event_mining_run(
        registry_conn,
        engine="event_mining.extraction",
        run_slug=raw_path.stem,
        schema_version="3.0",
        input_refs={
            "kind": "event_extraction",
            "raw_path": str(raw_path),
            "output_path": str(output_path),
            "spec_path": str(spec_path),
            "provider_kind": args.kind,
            "preset": args.preset,
            "registry_scope": "repo_root",
        },
        output_ref=str(output_path),
        summary=payload,
    )
    payload["analysis_run_id"] = run_record["analysis_run_id"]
    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
