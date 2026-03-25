"""Two-wheeler pipeline activation script.

Modes:
  (default)    Fallback-ok pipeline proof — runs full run_research() chain
               against production DBs. Falls back to templates if real providers
               are unavailable.
  --strict     Real-provider proof — deferred dispatch to ChatgptREST advisor
               + Tavily search. Fails if any provider falls back.
  --resume-from FILE  Re-poll unfinished sessions from a previous run's ledger.

Session ledger:
  Every LLM call writes to state/activation_sessions.jsonl with:
    {session_id, stage, role, submitted_at, completed_at, status, answer_len}
  This makes long overnight runs auditable and resumable.

Provider wiring:
  LLM    → ChatgptREST advisor_agent_turn (deferred submit + poll)
  Search → Tavily Search API (TAVILY_API_KEY)

Usage:
  # 1-iteration strict proof (recommended first run)
  python scripts/activate_two_wheeler_pipeline.py --strict --iterations 1

  # Overnight full strict run
  nohup python scripts/activate_two_wheeler_pipeline.py --strict > strict.log 2>&1 &

  # Resume polling unfinished sessions from a previous run
  python scripts/activate_two_wheeler_pipeline.py --resume-from state/activation_sessions.jsonl

  # Fallback-ok pipeline proof
  python scripts/activate_two_wheeler_pipeline.py
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

# Force line-buffered stdout so nohup/redirect logs flush immediately
if not sys.stdout.line_buffering:
    sys.stdout.reconfigure(line_buffering=True)
from pathlib import Path

import requests

# ── Paths ────────────────────────────────────────────────────────────
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from finagent.agents.evidence_store import EvidenceStore
from finagent.agents.orchestrator import run_research
from finagent.graph_v2.store import GraphStore
from finagent.memory import MemoryManager
from finagent.retrieval_stack import RetrievalStack

GRAPH_DB = REPO / "finagent.db"
STATE_DB = REPO / "state" / "finagent.sqlite"
MCP_URL = "http://127.0.0.1:18712/mcp"
MCP_HEADERS = {
    "Content-Type": "application/json",
    "Accept": "application/json, text/event-stream",
}
# thinking_heavy takes 20-30 min; standard takes 5-10 min on ChatGPT Pro
ADVISOR_POLL_INTERVAL = 15   # seconds between polls
ADVISOR_MAX_WAIT = 1800      # 30 min max for a single LLM call


# ═══════════════════════════════════════════════════════════════════
# Session ledger
# ═══════════════════════════════════════════════════════════════════

class SessionLedger:
    """Append-only JSONL ledger for advisor session tracking."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, session_id: str, **fields):
        # Sanitize string values to prevent newlines breaking JSONL
        for k, v in fields.items():
            if isinstance(v, str):
                fields[k] = v.replace("\n", " ").replace("\r", "")
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "session_id": session_id,
            **fields,
        }
        with open(self.path, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def load_pending(self) -> list[dict]:
        """Load sessions that were submitted but never marked completed."""
        submitted = {}  # session_id → latest entry
        if not self.path.exists():
            return []
        for line in self.path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                sid = entry.get("session_id", "")
                if sid:
                    submitted[sid] = entry
            except json.JSONDecodeError:
                continue
        # Return those still in "submitted" / "polling" state
        return [e for e in submitted.values()
                if e.get("status") in ("submitted", "polling")]


ledger: SessionLedger | None = None


# ═══════════════════════════════════════════════════════════════════
# SSE + MCP helpers
# ═══════════════════════════════════════════════════════════════════

def _parse_sse_events(raw_text: str) -> list[dict]:
    """Parse SSE text into a list of JSON-RPC messages.

    Handles multi-line data: fields correctly per SSE spec.
    """
    events = []
    data_buf: list[str] = []

    for line in raw_text.split("\n"):
        if line.startswith("data:"):
            data_buf.append(line[5:].lstrip() if len(line) > 5 else "")
        elif line == "" and data_buf:
            full = "\n".join(data_buf)
            data_buf = []
            try:
                events.append(json.loads(full))
            except json.JSONDecodeError:
                pass

    if data_buf:
        full = "\n".join(data_buf)
        try:
            events.append(json.loads(full))
        except json.JSONDecodeError:
            pass

    return events


def _extract_inner_json(mcp_result: dict) -> dict | None:
    """Extract inner JSON from MCP tools/call result envelope."""
    if not isinstance(mcp_result, dict):
        return None
    for item in mcp_result.get("content", []):
        if isinstance(item, dict) and "text" in item:
            try:
                return json.loads(item["text"])
            except json.JSONDecodeError:
                return {"_raw": item["text"][:500]}
    return mcp_result


def _mcp_tool_call(name: str, args: dict, timeout: int = 20) -> dict | None:
    """Call an MCP tool and return the inner JSON result."""
    payload = {
        "jsonrpc": "2.0", "id": 1,
        "method": "tools/call",
        "params": {"name": name, "arguments": args},
    }
    resp = requests.post(
        MCP_URL, json=payload, headers=MCP_HEADERS,
        timeout=timeout, stream=False,
    )
    events = _parse_sse_events(resp.text)
    for ev in events:
        if "result" in ev:
            return _extract_inner_json(ev["result"])
        if "error" in ev:
            return {"_error": ev["error"]}
    return None


# ═══════════════════════════════════════════════════════════════════
# Provider tracking
# ═══════════════════════════════════════════════════════════════════

class ProviderTracker:
    def __init__(self):
        self.llm_real = 0
        self.llm_fallback = 0
        self.search_real = 0
        self.search_fallback = 0

    @property
    def all_real(self) -> bool:
        return (self.llm_real > 0 and self.llm_fallback == 0
                and self.search_real > 0 and self.search_fallback == 0)

    def summary(self) -> str:
        return (
            f"LLM {self.llm_real}r/{self.llm_fallback}f | "
            f"Search {self.search_real}r/{self.search_fallback}f"
        )

tracker = ProviderTracker()


# ═══════════════════════════════════════════════════════════════════
# LLM provider (ChatgptREST async: deferred submit → poll)
# ═══════════════════════════════════════════════════════════════════

def _advisor_submit(message: str, role: str = "unknown") -> str | None:
    """Submit a deferred task. Returns session_id or None."""
    result = _mcp_tool_call("advisor_agent_turn", {
        "message": message,
        "goal_hint": "research",
        "delivery_mode": "deferred",
        "timeout_seconds": ADVISOR_MAX_WAIT,
        "depth": "standard",
        "auto_watch": False,
        "notify_done": False,
    }, timeout=20)
    if result and result.get("ok") and result.get("session_id"):
        sid = result["session_id"]
        if ledger:
            ledger.log(sid, status="submitted", role=role,
                       message_preview=message[:200])
        return sid
    return None


def _advisor_poll(session_id: str, role: str = "unknown") -> str | None:
    """Poll until completed/failed. Returns answer text or None."""
    if ledger:
        ledger.log(session_id, status="polling", role=role)

    polls = 0
    for _ in range(ADVISOR_MAX_WAIT // ADVISOR_POLL_INTERVAL):
        time.sleep(ADVISOR_POLL_INTERVAL)
        polls += 1
        result = _mcp_tool_call(
            "advisor_agent_status",
            {"session_id": session_id},
            timeout=15,
        )
        if not result:
            continue
        status = result.get("status", "")

        if status in ("completed", "done"):
            answer = result.get("answer") or result.get("last_answer") or ""
            if ledger:
                ledger.log(session_id, status="completed", role=role,
                           answer_len=len(answer), polls=polls)
            return answer

        if status in ("failed", "error", "cancelled"):
            if ledger:
                ledger.log(session_id, status=status, role=role, polls=polls,
                           error=result.get("error", ""))
            return None

    # Timed out
    if ledger:
        ledger.log(session_id, status="timeout", role=role, polls=polls)
    return None


def _detect_role(system: str) -> str:
    """Detect which agent role this prompt is for (for ledger)."""
    if "投研图谱探索规划师" in system:
        return "planner"
    if "竞品分析助手" in system:
        return "competitive_extractor"
    if "知识图谱构建助手" in system:
        return "triple_extractor"
    if "评估" in system or "决策" in system:
        return "evaluator"
    if "sufficient" in system or "insufficient" in system or "是否足以支撑" in system:
        return "sufficiency_evaluator"
    if "判断" in system and "检索结果" in system:
        return "sufficiency_evaluator"
    return "unknown"


def _chatgptrest_llm(system: str, user: str) -> str | None:
    """Full async LLM call: submit deferred → poll → return answer."""
    role = _detect_role(system)
    combined = f"[SYSTEM]\n{system}\n\n[USER]\n{user}"
    session_id = _advisor_submit(combined, role=role)
    if not session_id:
        return None
    return _advisor_poll(session_id, role=role)


def _make_llm_fn(strict: bool):
    def llm_fn(system: str, user: str) -> str:
        result = _chatgptrest_llm(system, user)
        if result is not None and result.strip():
            tracker.llm_real += 1
            return result
        if strict:
            raise RuntimeError("strict: ChatgptREST LLM returned no answer")
        tracker.llm_fallback += 1
        return _structured_llm_fallback(system, user)
    return llm_fn


# ═══════════════════════════════════════════════════════════════════
# Search provider (Tavily)
# ═══════════════════════════════════════════════════════════════════

def _make_search_fn(strict: bool):
    api_key = os.environ.get("TAVILY_API_KEY", "")

    def search_fn(query: str) -> str:
        if api_key:
            try:
                resp = requests.post(
                    "https://api.tavily.com/search",
                    json={"query": query, "max_results": 5, "search_depth": "basic"},
                    headers={"Authorization": f"Bearer {api_key}"},
                    timeout=30,
                )
                resp.raise_for_status()
                results = resp.json().get("results", [])
                parts = [f"[{r.get('title','')}] {r.get('content','')}"
                         for r in results]
                if parts:
                    tracker.search_real += 1
                    return "\n\n".join(parts)
            except Exception as exc:
                if strict:
                    raise RuntimeError(f"strict: search failed: {exc}") from exc
                print(f"  ⚠ Tavily error: {exc}")
        if strict:
            raise RuntimeError("strict: no search provider (TAVILY_API_KEY)")
        tracker.search_fallback += 1
        return _fallback_search(query)
    return search_fn


def _fallback_search(query: str) -> str:
    return (
        f"搜索：{query}\n\n"
        "【雅迪】冠能DM6：石墨烯电池60V24Ah，800W轮毂电机，10寸铝合金轮毂，¥4999-6599。\n"
        "【九号】Fz3：锂电72V30Ah，1200W轮毂电机，14寸铝合金轮毂，¥6299-7599。\n"
        "【台铃】N9：锂电72V22Ah，14寸轮毂，¥4599-5999。\n"
        "【爱玛】A500：铅酸48V20Ah，12寸钢轮毂，¥3299-4299。\n"
        "【小牛】NQi Sport：中置电机，锂电72V35Ah，14寸铝合金轮毂，¥5599-7999。\n"
        "【金谷】为雅迪/九号/台铃供应铝合金轮毂，年产能超1000万只。"
    )


# ═══════════════════════════════════════════════════════════════════
# Structured LLM fallback (pipeline-proof only)
# ═══════════════════════════════════════════════════════════════════

def _structured_llm_fallback(system: str, user: str) -> str:
    if "投研图谱探索规划师" in system:
        return json.dumps({
            "analysis": "图谱已有品牌/零部件节点，需补充价格带/供应链",
            "missing": ["SKU定位", "轮毂供应量"], "superfluous": [],
            "queries": [
                {"query": "雅迪冠能DM6 九号Fz3 轮毂规格对比", "priority": 1,
                 "target_entity": "yadea", "expected_info": "轮毂规格"},
                {"query": "金谷铝轮毂 两轮车客户 产能", "priority": 2,
                 "target_entity": "jinggu", "expected_info": "供应链"},
            ],
            "confidence": 0.55,
        }, ensure_ascii=False)
    if "竞品分析助手" in system:
        return json.dumps({
            "image_assets": [
                {"asset_id": "img-yadi-dm6-side", "brand": "雅迪", "product_line": "冠能",
                 "category": "exterior", "source_url": "",
                 "visible_content": "冠能DM6，10寸铝合金轮毂",
                 "supports_conclusion": "雅迪冠能DM6采用10寸铝合金轮毂"},
            ],
            "sku_records": [
                {"sku_id": "sku-yadi-dm6", "brand": "雅迪", "series": "冠能",
                 "model": "DM6", "positioning": "中高端", "price_range": "4999-6599",
                 "wheel_diameter": "10寸", "frame_type": "高碳钢一体式",
                 "motor_type": "800W轮毂电机", "battery_platform": "石墨烯60V24Ah",
                 "brake_config": "前碟后鼓", "target_audience": "通勤升级",
                 "style_tags": ["石墨烯"]},
                {"sku_id": "sku-jiuhao-fz3", "brand": "九号", "series": "F系列",
                 "model": "Fz3", "positioning": "高端", "price_range": "6299-7599",
                 "wheel_diameter": "14寸", "frame_type": "双管一体式",
                 "motor_type": "1200W轮毂电机", "battery_platform": "锂电72V30Ah",
                 "brake_config": "前后碟刹", "target_audience": "运动用户",
                 "style_tags": ["机甲风"]},
            ],
        }, ensure_ascii=False)
    if "知识图谱构建助手" in system:
        return json.dumps([
            {"head": "雅迪", "head_type": "company", "relation": "manufactures",
             "tail": "冠能DM6", "tail_type": "entity",
             "exact_quote": "冠能DM6采用石墨烯电池", "confidence": 0.9,
             "valid_from": "2024"},
            {"head": "金谷", "head_type": "company", "relation": "supplies_core_part_to",
             "tail": "雅迪", "tail_type": "company",
             "exact_quote": "金谷为多家品牌供应铝合金轮毂", "confidence": 0.85,
             "valid_from": "2024"},
        ], ensure_ascii=False)
    if "评估" in system or "决策" in system:
        return json.dumps({"decision": "accept", "confidence": 0.82},
                          ensure_ascii=False)
    return "两轮车五大品牌竞争格局稳定。"


# ═══════════════════════════════════════════════════════════════════
# Resume mode
# ═══════════════════════════════════════════════════════════════════

def resume_sessions(ledger_path: Path) -> int:
    """Re-poll pending sessions from a previous run's ledger."""
    lg = SessionLedger(ledger_path)
    pending = lg.load_pending()
    if not pending:
        print("No pending sessions to resume.")
        return 0

    print(f"Found {len(pending)} pending session(s):")
    completed = 0
    for entry in pending:
        sid = entry["session_id"]
        role = entry.get("role", "?")
        print(f"\n  {sid} (role={role})...")
        answer = _advisor_poll(sid, role=role)
        if answer:
            print(f"    ✅ completed, answer_len={len(answer)}")
            print(f"    preview: {answer[:200]}")
            lg.log(sid, status="completed", role=role, answer_len=len(answer),
                   resumed=True)
            completed += 1
        else:
            result = _mcp_tool_call(
                "advisor_agent_status", {"session_id": sid}, timeout=10)
            status = result.get("status", "?") if result else "unreachable"
            print(f"    ❌ {status}")
            lg.log(sid, status=f"resume_{status}", role=role, resumed=True)

    print(f"\nResumed: {completed}/{len(pending)} completed")
    return 0 if completed == len(pending) else 1


# ═══════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Two-wheeler pipeline activation")
    parser.add_argument("--strict", action="store_true",
                        help="Fail if any provider falls back")
    parser.add_argument("--iterations", type=int, default=None,
                        help="Max research iterations (default: 1 for strict, 3 for pipeline)")
    parser.add_argument("--resume-from", type=str, default=None,
                        help="Path to ledger JSONL to resume polling pending sessions")
    parser.add_argument("--ledger", type=str,
                        default=str(REPO / "state" / "activation_sessions.jsonl"),
                        help="Path for session ledger JSONL")
    args = parser.parse_args()

    # Resume mode
    if args.resume_from:
        print(f"{'='*60}\n🔄 Resume mode: {args.resume_from}\n{'='*60}")
        return resume_sessions(Path(args.resume_from))

    # Set defaults
    iterations = args.iterations
    if iterations is None:
        iterations = 1 if args.strict else 3

    label = "STRICT" if args.strict else "PIPELINE"

    # Initialize ledger
    global ledger
    ledger = SessionLedger(Path(args.ledger))

    print(f"{'='*60}")
    print(f"🚀 Two-Wheeler Activation [{label}] iterations={iterations}")
    print(f"   Ledger: {args.ledger}")
    print(f"{'='*60}")

    # Before
    sc = sqlite3.connect(str(STATE_DB))
    ev0 = sc.execute("SELECT COUNT(*) FROM evidence_store").fetchone()[0]
    mem0 = sc.execute("SELECT COUNT(*) FROM memory_records").fetchone()[0]
    sc.close()
    gc = sqlite3.connect(str(GRAPH_DB))
    nodes0 = gc.execute("SELECT COUNT(*) FROM kg_nodes").fetchone()[0]
    gc.close()
    print(f"\n📊 BEFORE: evidence={ev0} memory={mem0} nodes={nodes0}")

    # Preflight
    mcp_ok = False
    try:
        r = _mcp_tool_call("advisor_agent_status", {"session_id": "ping"},
                           timeout=5)
        mcp_ok = r is not None
    except Exception:
        pass
    if not mcp_ok:
        try:
            resp = requests.post(
                MCP_URL,
                json={"jsonrpc": "2.0", "id": 0, "method": "tools/list",
                      "params": {}},
                headers=MCP_HEADERS, timeout=5, stream=False)
            mcp_ok = resp.status_code == 200
        except Exception:
            pass
    tavily_ok = bool(os.environ.get("TAVILY_API_KEY", ""))
    print(f"   ChatgptREST MCP: {'✅' if mcp_ok else '❌'}")
    print(f"   Tavily key: {'✅' if tavily_ok else '❌'}")

    if args.strict:
        if not mcp_ok:
            print("❌ STRICT ABORT: ChatgptREST MCP unreachable"); return 2
        if not tavily_ok:
            print("❌ STRICT ABORT: TAVILY_API_KEY not set"); return 2
        print("   🔒 Strict: all calls must use real providers")

    # Wire
    llm_fn = _make_llm_fn(args.strict)
    search_fn = _make_search_fn(args.strict)
    gs = GraphStore(str(GRAPH_DB))
    mm = MemoryManager(db_path=str(STATE_DB))
    es = EvidenceStore(str(STATE_DB))
    rs = RetrievalStack(graph_store=gs, memory=mm, evidence_store=es,
                        enable_light_rerank=True)

    # Run
    print(f"\n🔄 Starting research (iterations={iterations})...")
    t0 = time.time()
    try:
        result = run_research(
            goal="两轮车竞品车身结构与轮毂技术对标",
            context="两轮车", llm_fn=llm_fn, search_fn=search_fn,
            graph_store=gs, evidence_store=es, memory_manager=mm,
            retrieval_stack=rs, max_iterations=iterations, token_budget=30000,
            verbose=True, enable_loop_consolidation=True,
            enable_retrieval_light_rerank=True,
        )
    except RuntimeError as exc:
        if "strict:" in str(exc):
            print(f"\n❌ STRICT FAIL: {exc}")
            gs.close(); mm.close(); es.close()
            return 3
        raise
    elapsed = time.time() - t0

    # After
    sc = sqlite3.connect(str(STATE_DB))
    ev1 = sc.execute("SELECT COUNT(*) FROM evidence_store").fetchone()[0]
    mem1 = sc.execute("SELECT COUNT(*) FROM memory_records").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"📊 AFTER ({elapsed:.0f}s):")
    print(f"   evidence {ev0}→{ev1}(+{ev1-ev0})")
    print(f"   memory   {mem0}→{mem1}(+{mem1-mem0})")
    print(f"   Providers: {tracker.summary()}")
    for r in sc.execute("SELECT tier, COUNT(*) FROM memory_records GROUP BY tier"):
        print(f"   memory.{r[0]}: {r[1]}")
    sc.close()
    gs.close(); mm.close(); es.close()

    # Verdict
    print(f"\n{'='*60}")
    grew = (ev1 > ev0) and (mem1 > mem0)
    if args.strict:
        if grew and tracker.all_real:
            print("✅ STRICT PASS: real providers, real growth"); return 0
        elif grew:
            print("⚠ STRICT PARTIAL: grew but some fallbacks used"); return 1
        else:
            print("❌ STRICT FAIL: insufficient growth"); return 1
    else:
        kind = "real" if tracker.all_real else "fallback-backed"
        if grew:
            print(f"✅ PIPELINE PASS ({kind})")
        elif ev1 > ev0:
            print(f"⚠ PIPELINE PARTIAL ({kind}): evidence grew, memory didn't")
        else:
            print(f"❌ PIPELINE FAIL")
        return 0 if ev1 > ev0 else 1


if __name__ == "__main__":
    sys.exit(main())
