"""Finagent v2 agent orchestration layer."""

from __future__ import annotations

__all__ = [
    "ResearchState",
    "SafetyGuard",
    "SafetyVerdict",
    "build_research_graph",
]


def __getattr__(name: str):
    if name == "ResearchState":
        from .state import ResearchState

        return ResearchState
    if name in {"SafetyGuard", "SafetyVerdict"}:
        from .safety import SafetyGuard, SafetyVerdict

        return {"SafetyGuard": SafetyGuard, "SafetyVerdict": SafetyVerdict}[name]
    if name == "build_research_graph":
        from .orchestrator import build_research_graph

        return build_research_graph
    raise AttributeError(name)
