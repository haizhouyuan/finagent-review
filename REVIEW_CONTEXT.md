# Finagent Review Context

## Source

- source repo: `https://github.com/haizhouyuan/finagent`
- source commit: `37d869cd352cc2a21e6a948e8071f91067096c50`
- review branch: `review-20260325-091157`

## Review Goal

Review the latest `finagent` architecture against the local external reference repos,
with emphasis on:

1. OpenBB as data-bus / provider-contract reference
2. qlib as workflow / recorder / replay / leakage-discipline reference
3. TradingAgents as committee-review / debate-loop reference

## Required Judgment

- What should `finagent` borrow directly?
- What should `finagent` explicitly avoid?
- Where should changes land incrementally in the existing architecture?
- What should be deferred because it would be dog-tail accretion rather than leverage?

## Current Finagent Focus Areas

- discovery loop / research orchestration
- graph + evidence + memory retrieval
- event / claim / chronology discipline
- thesis evolution / writeback / review discipline
- recent two-wheeler pilot closure

## Notes

- External reference repos are intentionally curated here, not mirrored in full.
- Treat them as reference parts, not replacement architectures.

## Review Instructions

Compare finagent against OpenBB / qlib / TradingAgents for architecture leverage. Prioritize systemic borrow-vs-avoid decisions over cosmetic suggestions.
