from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourcePolicy:
    source_role: str
    source_tier: str
    adapter_family: str
    state_authority: str
    interrupt_eligible: bool
    needs_corroboration: bool
    discovery_only: bool
    description: str


SOURCE_POLICY_REGISTRY: dict[str, SourcePolicy] = {
    "company_filing": SourcePolicy(
        source_role="company_filing",
        source_tier="primary",
        adapter_family="filing",
        state_authority="direct",
        interrupt_eligible=True,
        needs_corroboration=False,
        discovery_only=False,
        description="Official company disclosure. Can update tracked state directly.",
    ),
    "regulator": SourcePolicy(
        source_role="regulator",
        source_tier="primary",
        adapter_family="regulatory",
        state_authority="direct",
        interrupt_eligible=True,
        needs_corroboration=False,
        discovery_only=False,
        description="Official regulator or exchange disclosure. Can update tracked state directly.",
    ),
    "customer_signal": SourcePolicy(
        source_role="customer_signal",
        source_tier="primary",
        adapter_family="customer",
        state_authority="direct",
        interrupt_eligible=True,
        needs_corroboration=False,
        discovery_only=False,
        description="Customer purchase or deployment evidence. Can update tracked state directly.",
    ),
    "competitor_pr": SourcePolicy(
        source_role="competitor_pr",
        source_tier="secondary",
        adapter_family="newsroom",
        state_authority="corroborated",
        interrupt_eligible=True,
        needs_corroboration=True,
        discovery_only=False,
        description="Competitor announcement. Needs corroboration before state change.",
    ),
    "conference": SourcePolicy(
        source_role="conference",
        source_tier="secondary",
        adapter_family="conference",
        state_authority="corroborated",
        interrupt_eligible=True,
        needs_corroboration=True,
        discovery_only=False,
        description="Conference presentation or agenda signal. Needs corroboration before state change.",
    ),
    "patent": SourcePolicy(
        source_role="patent",
        source_tier="secondary",
        adapter_family="patent",
        state_authority="corroborated",
        interrupt_eligible=False,
        needs_corroboration=True,
        discovery_only=False,
        description="Patent filing. Useful for discovery and corroboration, not direct commercialization proof.",
    ),
    "media": SourcePolicy(
        source_role="media",
        source_tier="tertiary",
        adapter_family="media",
        state_authority="none",
        interrupt_eligible=False,
        needs_corroboration=True,
        discovery_only=True,
        description="Media or analyst coverage. Discovery input only, cannot directly update state.",
    ),
    "hiring": SourcePolicy(
        source_role="hiring",
        source_tier="tertiary",
        adapter_family="hiring",
        state_authority="none",
        interrupt_eligible=False,
        needs_corroboration=True,
        discovery_only=True,
        description="Hiring signal. Discovery input only, cannot directly update state.",
    ),
    "kol_digest": SourcePolicy(
        source_role="kol_digest",
        source_tier="tertiary",
        adapter_family="digest",
        state_authority="none",
        interrupt_eligible=False,
        needs_corroboration=True,
        discovery_only=True,
        description="Second-hand KOL digest. Exploration seed only until independently verified.",
    ),
}

SOURCE_ROLES = frozenset(SOURCE_POLICY_REGISTRY.keys())
SOURCE_TIERS = frozenset(policy.source_tier for policy in SOURCE_POLICY_REGISTRY.values())
SOURCE_ROLE_TO_TIER = {
    role: policy.source_tier for role, policy in SOURCE_POLICY_REGISTRY.items()
}


def get_source_policy(source_role: str | None) -> SourcePolicy:
    role = str(source_role or "").strip()
    return SOURCE_POLICY_REGISTRY.get(
        role,
        SourcePolicy(
            source_role=role or "unknown",
            source_tier="tertiary",
            adapter_family="unknown",
            state_authority="none",
            interrupt_eligible=False,
            needs_corroboration=True,
            discovery_only=True,
            description="Unknown source role. Treated as discovery-only tertiary input.",
        ),
    )


def source_policy_prompt_lines() -> list[str]:
    lines: list[str] = []
    for role in sorted(SOURCE_POLICY_REGISTRY):
        policy = SOURCE_POLICY_REGISTRY[role]
        authority = (
            "direct-state"
            if policy.state_authority == "direct"
            else "needs-corroboration"
            if policy.state_authority == "corroborated"
            else "discovery-only"
        )
        lines.append(
            f"- {policy.source_role}: tier={policy.source_tier}; "
            f"adapter={policy.adapter_family}; authority={authority}"
        )
    return lines


def list_source_policies() -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for role in sorted(SOURCE_POLICY_REGISTRY):
        policy = SOURCE_POLICY_REGISTRY[role]
        items.append(
            {
                "source_role": policy.source_role,
                "source_tier": policy.source_tier,
                "adapter_family": policy.adapter_family,
                "state_authority": policy.state_authority,
                "interrupt_eligible": policy.interrupt_eligible,
                "needs_corroboration": policy.needs_corroboration,
                "discovery_only": policy.discovery_only,
                "description": policy.description,
            }
        )
    return items
