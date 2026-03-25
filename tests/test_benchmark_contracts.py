from __future__ import annotations

from finagent.sector_grammars import get_sector_grammar, list_sector_grammars
from finagent.source_policy import list_source_policies


def test_list_source_policies_includes_primary_and_tertiary_roles() -> None:
    items = list_source_policies()
    by_role = {item["source_role"]: item for item in items}
    assert by_role["company_filing"]["source_tier"] == "primary"
    assert by_role["company_filing"]["state_authority"] == "direct"
    assert by_role["media"]["source_tier"] == "tertiary"
    assert by_role["media"]["discovery_only"] is True


def test_list_sector_grammars_exposes_stage_and_signal_cues() -> None:
    items = list_sector_grammars()
    by_key = {item["key"]: item for item in items}
    grammar = by_key["silicon_photonics_cpo_progress"]
    assert "early_prototype" in grammar["stage_focus"]
    assert "CPO" in grammar["proving_cues"]
    assert "标准未定" in grammar["constraint_cues"]


def test_legacy_onsite_generation_alias_resolves() -> None:
    grammar = get_sector_grammar("ai_power_onsite_generation")
    assert grammar is not None
    assert grammar.key == "ai_power_onsite_generation"
    assert "试点" in grammar.proving_cues
