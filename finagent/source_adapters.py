from __future__ import annotations

from dataclasses import dataclass
from html import unescape
import json
import re
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse
import xml.etree.ElementTree as ET

import requests

from .adapters import (
    AdapterError,
    FetchedArtifact,
    fetch_cninfo_announcements,
    fetch_defillama_protocol,
    fetch_openalex_search,
    fetch_sec_submissions,
)
from .utils import slugify


@dataclass(frozen=True)
class SourceAdapter:
    kind: str
    label: str
    adapter_family: str
    artifact_kind: str
    auto_refresh: bool
    discovery_capable: bool
    description: str


SOURCE_ADAPTERS: dict[str, SourceAdapter] = {
    "sec_submissions": SourceAdapter(
        kind="sec_submissions",
        label="SEC submissions",
        adapter_family="filing",
        artifact_kind="json",
        auto_refresh=True,
        discovery_capable=False,
        description="Refresh official SEC submissions for a tracked ticker.",
    ),
    "cninfo_announcements": SourceAdapter(
        kind="cninfo_announcements",
        label="CNINFO announcements",
        adapter_family="filing",
        artifact_kind="json",
        auto_refresh=True,
        discovery_capable=False,
        description="Refresh official CNINFO disclosures for a tracked ticker.",
    ),
    "openalex_search": SourceAdapter(
        kind="openalex_search",
        label="OpenAlex search",
        adapter_family="research",
        artifact_kind="paper_metadata",
        auto_refresh=True,
        discovery_capable=True,
        description="Refresh research / paper search results for a tracked query.",
    ),
    "defillama_protocol": SourceAdapter(
        kind="defillama_protocol",
        label="DefiLlama protocol",
        adapter_family="dashboard",
        artifact_kind="dashboard_snapshot",
        auto_refresh=True,
        discovery_capable=False,
        description="Refresh protocol dashboard stats from DefiLlama.",
    ),
    "web_page_snapshot": SourceAdapter(
        kind="web_page_snapshot",
        label="Web page snapshot",
        adapter_family="webpage",
        artifact_kind="html",
        auto_refresh=True,
        discovery_capable=True,
        description="Refresh a tracked investor relations / newsroom / conference / hiring / patent page.",
    ),
    "rss_feed_snapshot": SourceAdapter(
        kind="rss_feed_snapshot",
        label="RSS / Atom feed snapshot",
        adapter_family="feed",
        artifact_kind="html",
        auto_refresh=True,
        discovery_capable=True,
        description="Refresh a tracked RSS or Atom feed and normalize the latest entries.",
    ),
}


def list_source_adapters() -> list[dict[str, object]]:
    items: list[dict[str, object]] = []
    for kind in sorted(SOURCE_ADAPTERS):
        item = SOURCE_ADAPTERS[kind]
        items.append(
            {
                "kind": item.kind,
                "label": item.label,
                "adapter_family": item.adapter_family,
                "artifact_kind": item.artifact_kind,
                "auto_refresh": item.auto_refresh,
                "discovery_capable": item.discovery_capable,
                "description": item.description,
            }
        )
    return items


def get_source_adapter(kind: str | None) -> SourceAdapter | None:
    if not kind:
        return None
    return SOURCE_ADAPTERS.get(str(kind).strip())


def _json_loads(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _strip_html_to_text(html: str) -> str:
    text = re.sub(r"(?is)<(script|style).*?>.*?</\\1>", " ", html)
    text = re.sub(r"(?is)<br\\s*/?>", "\n", text)
    text = re.sub(r"(?is)</p\\s*>", "\n\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\r\n?", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _fetch_url(url: str) -> requests.Response:
    try:
        response = requests.get(url, timeout=30, headers={"User-Agent": "finagent-source-adapter/1.0"})
        response.raise_for_status()
    except requests.RequestException as exc:
        raise AdapterError(f"GET failed for {url}: {exc}") from exc
    return response


def fetch_web_page_snapshot(
    *,
    url: str,
    title: str,
    published_at: str | None = None,
) -> FetchedArtifact:
    response = _fetch_url(url)
    text = response.text
    normalized = _strip_html_to_text(text)
    return FetchedArtifact(
        title=title or f"Web page snapshot {url}",
        uri=url,
        published_at=published_at,
        raw_text=text,
        normalized_text=normalized,
        metadata={
            "content_type": response.headers.get("Content-Type", ""),
            "adapter_kind": "web_page_snapshot",
            "refresh_url": url,
        },
    )


def fetch_rss_feed_snapshot(
    *,
    url: str,
    title: str,
    published_at: str | None = None,
    limit: int = 10,
) -> FetchedArtifact:
    response = _fetch_url(url)
    raw = response.text
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        raise AdapterError(f"invalid RSS/Atom XML from {url}: {exc}") from exc
    entries: list[str] = []
    for item in root.findall(".//item")[:limit] + root.findall(".//{http://www.w3.org/2005/Atom}entry")[:limit]:
        entry_title = item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title") or ""
        entry_link = item.findtext("link") or item.get("href") or ""
        entry_pub = (
            item.findtext("pubDate")
            or item.findtext("{http://www.w3.org/2005/Atom}updated")
            or item.findtext("{http://www.w3.org/2005/Atom}published")
            or ""
        )
        entries.append(f"- {entry_title.strip()} | {entry_pub.strip()} | {entry_link.strip()}".strip())
    normalized = "\n".join(entries).strip()
    return FetchedArtifact(
        title=title or f"RSS feed snapshot {url}",
        uri=url,
        published_at=published_at,
        raw_text=raw,
        normalized_text=normalized or _strip_html_to_text(raw),
        metadata={
            "content_type": response.headers.get("Content-Type", ""),
            "adapter_kind": "rss_feed_snapshot",
            "refresh_url": url,
            "entry_count": len(entries),
        },
    )


def infer_refresh_spec_from_artifact(row: dict[str, Any]) -> dict[str, Any] | None:
    metadata = _json_loads(row.get("metadata_json"))
    title = row.get("title") or ""
    source_id = row.get("source_id") or ""
    uri = row.get("uri") or ""
    refresh_kind = str(metadata.get("refresh_adapter_kind") or "").strip()
    refresh_url = str(metadata.get("refresh_url") or uri or "").strip()
    source_type = row.get("source_type") or ""
    base = {
        "source_id": source_id,
        "source_name": row.get("source_name") or "",
        "source_type": source_type,
        "primaryness": row.get("primaryness") or "",
        "jurisdiction": row.get("jurisdiction") or "",
        "language": row.get("language") or "",
        "artifact_id": row.get("artifact_id") or "",
        "title": title,
        "uri": uri,
    }
    if source_id == "src_sec_edgar":
        match = re.search(r"SEC submissions\s+([A-Za-z0-9_.-]+)", title)
        ticker = (match.group(1) if match else "").upper()
        if not ticker:
            return None
        return {
            **base,
            "kind": "sec_submissions",
            "refresh_key": ticker,
            "artifact_kind": "json",
            "artifact_label": f"sec_{slugify(ticker)}",
            "args": {"ticker": ticker},
        }
    if source_id == "src_cninfo":
        match = re.search(r"CNINFO announcements\s+([A-Za-z0-9_.-]+)", title)
        ticker = (match.group(1) if match else "").upper()
        if not ticker:
            return None
        return {
            **base,
            "kind": "cninfo_announcements",
            "refresh_key": ticker,
            "artifact_kind": "json",
            "artifact_label": f"cninfo_{slugify(ticker)}",
            "args": {
                "ticker": ticker,
                "search_key": metadata.get("search_key") or "",
                "lookback_days": int(metadata.get("lookback_days") or 45),
                "limit": int(metadata.get("limit") or max(len(metadata.get("announcements") or []), 5)),
            },
        }
    if source_id == "src_openalex":
        query = str(metadata.get("query") or "").strip()
        if not query and uri:
            parsed = urlparse(uri)
            query = parse_qs(parsed.query).get("search", [""])[0].strip()
        if not query:
            return None
        per_page = 5
        if uri:
            parsed = urlparse(uri)
            per_page_raw = parse_qs(parsed.query).get("per-page", [""])[0].strip()
            if per_page_raw.isdigit():
                per_page = int(per_page_raw)
        result_count = int((metadata.get("metrics") or {}).get("result_count") or 0)
        if result_count > 0:
            per_page = max(per_page, result_count)
        return {
            **base,
            "kind": "openalex_search",
            "refresh_key": query.lower(),
            "artifact_kind": "paper_metadata",
            "artifact_label": f"openalex_{slugify(query)}",
            "args": {"query": query, "per_page": per_page},
        }
    if source_id == "src_defillama":
        slug = str(metadata.get("slug") or "").strip().lower()
        if not slug and uri:
            slug = urlparse(uri).path.rstrip("/").split("/")[-1].strip().lower()
        if not slug:
            return None
        return {
            **base,
            "kind": "defillama_protocol",
            "refresh_key": slug,
            "artifact_kind": "dashboard_snapshot",
            "artifact_label": f"defillama_{slugify(slug)}",
            "args": {"slug": slug},
        }
    if not refresh_kind:
        parsed = urlparse(refresh_url)
        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        if refresh_url.endswith(".xml") or "rss" in path or "/feed" in path or "atom" in path:
            refresh_kind = "rss_feed_snapshot"
        elif any(token in host for token in ("investor", "news", "ir", "careers")) or any(
            token in path for token in ("newsroom", "news", "press", "careers", "jobs", "conference", "events", "patent")
        ):
            refresh_kind = "web_page_snapshot"
        elif source_type in {"news", "governance"} and refresh_url:
            refresh_kind = "web_page_snapshot"
    adapter = get_source_adapter(refresh_kind)
    if adapter is None or not refresh_url:
        return None
    return {
        **base,
        "kind": adapter.kind,
        "refresh_key": refresh_url.lower(),
        "artifact_kind": adapter.artifact_kind,
        "artifact_label": f"{adapter.kind}_{slugify(base['source_name'] or refresh_url)[:48]}",
        "args": {
            "url": refresh_url,
            "title": title or f"{base['source_name']} snapshot",
            "published_at": metadata.get("published_at") or row.get("captured_at"),
            "limit": int(metadata.get("feed_limit") or 10),
        },
    }


def execute_refresh_spec(spec: dict[str, Any]) -> FetchedArtifact:
    kind = str(spec["kind"])
    if kind == "sec_submissions":
        return fetch_sec_submissions(spec["args"]["ticker"])
    if kind == "cninfo_announcements":
        return fetch_cninfo_announcements(
            spec["args"]["ticker"],
            search_key=spec["args"]["search_key"] or None,
            lookback_days=spec["args"]["lookback_days"],
            limit=spec["args"]["limit"],
        )
    if kind == "openalex_search":
        return fetch_openalex_search(spec["args"]["query"], per_page=spec["args"]["per_page"])
    if kind == "defillama_protocol":
        return fetch_defillama_protocol(spec["args"]["slug"])
    if kind == "web_page_snapshot":
        return fetch_web_page_snapshot(
            url=spec["args"]["url"],
            title=spec["args"]["title"],
            published_at=spec["args"].get("published_at"),
        )
    if kind == "rss_feed_snapshot":
        return fetch_rss_feed_snapshot(
            url=spec["args"]["url"],
            title=spec["args"]["title"],
            published_at=spec["args"].get("published_at"),
            limit=int(spec["args"].get("limit") or 10),
        )
    raise AdapterError(f"unsupported refresh kind: {kind}")
