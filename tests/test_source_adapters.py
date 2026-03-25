from __future__ import annotations

from finagent.source_adapters import infer_refresh_spec_from_artifact, list_source_adapters


def test_list_source_adapters_exposes_generic_snapshots() -> None:
    items = list_source_adapters()
    by_kind = {item["kind"]: item for item in items}
    assert by_kind["web_page_snapshot"]["discovery_capable"] is True
    assert by_kind["rss_feed_snapshot"]["adapter_family"] == "feed"


def test_infer_refresh_spec_from_metadata_driven_web_snapshot() -> None:
    row = {
        "artifact_id": "art_web_1",
        "source_id": "src_power_newsroom",
        "title": "Power Newsroom Snapshot",
        "uri": "https://example.com/newsroom/updates",
        "captured_at": "2026-03-15T10:00:00+08:00",
        "metadata_json": '{"refresh_adapter_kind":"web_page_snapshot","refresh_url":"https://example.com/newsroom/updates"}',
        "source_name": "Example Power",
        "source_type": "news",
        "primaryness": "second_hand",
        "jurisdiction": "global",
        "language": "en",
    }
    spec = infer_refresh_spec_from_artifact(row)
    assert spec is not None
    assert spec["kind"] == "web_page_snapshot"
    assert spec["args"]["url"] == "https://example.com/newsroom/updates"


def test_infer_refresh_spec_auto_detects_rss_feed() -> None:
    row = {
        "artifact_id": "art_feed_1",
        "source_id": "src_feed",
        "title": "IR Feed",
        "uri": "https://example.com/investor/feed.xml",
        "captured_at": "2026-03-15T10:00:00+08:00",
        "metadata_json": "{}",
        "source_name": "Example IR",
        "source_type": "news",
        "primaryness": "second_hand",
        "jurisdiction": "global",
        "language": "en",
    }
    spec = infer_refresh_spec_from_artifact(row)
    assert spec is not None
    assert spec["kind"] == "rss_feed_snapshot"
    assert spec["refresh_key"] == "https://example.com/investor/feed.xml"
