"""Tests for finagent.research_contracts — round-trip serialization."""
from __future__ import annotations

import json

from finagent.research_contracts import (
    EvidenceRef,
    ResearchPackage,
    ResearchRun,
    RunStatus,
    SourceTier,
    WritebackAction,
    WritebackOp,
    WritebackTarget,
)


class TestResearchRun:
    def test_create_with_defaults(self):
        run = ResearchRun(goal="test")
        assert run.run_id.startswith("run-")
        assert run.status == "queued"
        assert run.goal == "test"
        assert run.llm_backend == "mock"

    def test_round_trip(self):
        run = ResearchRun(goal="AI供应链", context="科技", llm_backend="openai")
        d = run.to_dict()
        run2 = ResearchRun.from_dict(d)
        assert run2.run_id == run.run_id
        assert run2.goal == run.goal
        assert run2.llm_backend == run.llm_backend

    def test_from_dict_ignores_unknown_keys(self):
        d = {"goal": "test", "run_id": "run-abc", "unknown_field": "ignored"}
        run = ResearchRun.from_dict(d)
        assert run.goal == "test"
        assert run.run_id == "run-abc"

    def test_json_round_trip(self):
        run = ResearchRun(goal="test", context="ctx")
        j = json.dumps(run.to_dict())
        run2 = ResearchRun.from_dict(json.loads(j))
        assert run2.goal == "test"
        assert run2.context == "ctx"


class TestEvidenceRef:
    def test_create_with_provenance(self):
        ref = EvidenceRef(
            evidence_id=42,
            query="test",
            source_tier="primary",
            source_uri="https://example.com",
            published_at="2025-01-01",
        )
        assert ref.source_tier == "primary"
        assert ref.evidence_id == 42

    def test_round_trip(self):
        ref = EvidenceRef(
            evidence_id=1,
            query="q",
            char_count=100,
            source_type="document",
            source_tier=SourceTier.SECONDARY.value,
            source_uri="file:///tmp/test.md",
            published_at="2025-06-01",
        )
        d = ref.to_dict()
        ref2 = EvidenceRef.from_dict(d)
        assert ref2.evidence_id == ref.evidence_id
        assert ref2.source_tier == "secondary"
        assert ref2.source_uri == ref.source_uri

    def test_from_dict_ignores_extras(self):
        ref = EvidenceRef.from_dict({"evidence_id": 5, "extra": "ignored"})
        assert ref.evidence_id == 5

    def test_frozen(self):
        ref = EvidenceRef(evidence_id=1)
        try:
            ref.evidence_id = 2  # type: ignore
            assert False, "should be frozen"
        except AttributeError:
            pass


class TestResearchPackage:
    def test_with_evidence_refs(self):
        refs = [
            EvidenceRef(evidence_id=1, source_tier="primary"),
            EvidenceRef(evidence_id=2, source_tier="unverified"),
        ]
        pkg = ResearchPackage(
            run_id="run-test",
            goal="test",
            triples=[{"head": "A", "edge": "R", "tail": "B"}],
            evidence_refs=refs,
        )
        assert len(pkg.evidence_refs) == 2
        assert isinstance(pkg.evidence_refs[0], EvidenceRef)

    def test_round_trip(self):
        refs = [EvidenceRef(evidence_id=1, source_tier="primary")]
        pkg = ResearchPackage(
            run_id="run-test",
            goal="test",
            triples=[{"head": "X", "edge": "Y", "tail": "Z"}],
            evidence_refs=refs,
            confidence=0.85,
        )
        d = pkg.to_dict()
        pkg2 = ResearchPackage.from_dict(d)
        assert pkg2.run_id == "run-test"
        assert len(pkg2.evidence_refs) == 1
        assert isinstance(pkg2.evidence_refs[0], EvidenceRef)
        assert pkg2.evidence_refs[0].source_tier == "primary"
        assert pkg2.confidence == 0.85

    def test_json_round_trip(self):
        pkg = ResearchPackage(
            run_id="run-x",
            evidence_refs=[EvidenceRef(evidence_id=3, source_tier="aggregated")],
        )
        j = json.dumps(pkg.to_dict())
        pkg2 = ResearchPackage.from_dict(json.loads(j))
        assert pkg2.evidence_refs[0].source_tier == "aggregated"


class TestWritebackAction:
    def test_round_trip(self):
        wa = WritebackAction(
            package_id="run-x",
            target_type=WritebackTarget.THESIS.value,
            op=WritebackOp.CREATE.value,
            confidence=0.9,
            source_evidence_ids=[1, 2, 3],
        )
        d = wa.to_dict()
        wa2 = WritebackAction.from_dict(d)
        assert wa2.package_id == "run-x"
        assert wa2.target_type == "thesis"
        assert wa2.source_evidence_ids == [1, 2, 3]
        assert wa2.applied is False

    def test_json_round_trip(self):
        wa = WritebackAction(package_id="run-y", target_type="target_case")
        j = json.dumps(wa.to_dict())
        wa2 = WritebackAction.from_dict(json.loads(j))
        assert wa2.package_id == "run-y"


class TestEnums:
    def test_run_status_values(self):
        assert RunStatus.COMPLETED.value == "completed"
        assert RunStatus.PAUSED.value == "paused"

    def test_source_tier_values(self):
        assert SourceTier.PRIMARY.value == "primary"
        assert SourceTier.UNVERIFIED.value == "unverified"

    def test_writeback_target_values(self):
        assert WritebackTarget.THESIS.value == "thesis"
        assert WritebackTarget.TARGET_CASE.value == "target_case"
