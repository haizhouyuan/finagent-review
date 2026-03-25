from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
from base64 import b64encode
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from finagent.two_wheeler_feed_pull import pull_two_wheeler_raw_exports, run_two_wheeler_feed_pull


def _write_pull_inputs(root: Path) -> None:
    sku_dir = root / "sku"
    supplier_dir = root / "supplier"
    sku_dir.mkdir(parents=True)
    supplier_dir.mkdir(parents=True)
    (sku_dir / "sku_export_20260324_v1.csv").write_text(
        "sku_code,brand_name,series_name,display_name,positioning,price_min,price_max,wheel_size,frame_desc,motor_desc,battery_desc,brake_desc,audience,style_tags,evidence_channels\n"
        "sku-ninebot-fz3-120,九号,Fz,Fz3 120,中高端,6499,7799,14寸,双管一体,轮毂电机 1300W,72V32Ah 锂电,前碟后碟,年轻男性/通勤,运动|智能,官网|门店\n",
        encoding="utf-8",
    )
    (sku_dir / "sku_export_20260325_v2.csv").write_text(
        "sku_code,brand_name,series_name,display_name,positioning,price_min,price_max,wheel_size,frame_desc,motor_desc,battery_desc,brake_desc,audience,style_tags,evidence_channels\n"
        "sku-ninebot-fz3-130,九号,Fz,Fz3 130,中高端,6699,7999,14寸,双管一体,轮毂电机 1400W,72V35Ah 锂电,前碟后碟,年轻男性/通勤,运动|智能,官网|门店\n",
        encoding="utf-8",
    )
    os.utime(sku_dir / "sku_export_20260324_v1.csv", (1, 1))
    os.utime(sku_dir / "sku_export_20260325_v2.csv", (2, 2))
    (supplier_dir / "supplier_latest.csv").write_text(
        "supplier_name,customer_name,relation_type,observed_on,confidence_score,evidence_note,source\n"
        "金谷,九号,supplies_core_part_to,2025-04-01,0.96,4月轮毂配套观察,field_export\n",
        encoding="utf-8",
    )
    (root / "alias_map.csv").write_text(
        "alias,canonical_name\n九号Fz3 130,Fz系列\n",
        encoding="utf-8",
    )


def _start_http_server(
    directory: Path,
    *,
    expected_headers_by_path: dict[str, dict[str, str]] | None = None,
):
    class QuietHandler(SimpleHTTPRequestHandler):
        def log_message(self, format, *args):  # noqa: A003
            return

        def do_GET(self):
            expected = (expected_headers_by_path or {}).get(self.path, {})
            for key, value in expected.items():
                if self.headers.get(key) != value:
                    self.send_response(401)
                    self.end_headers()
                    self.wfile.write(b"unauthorized")
                    return
            super().do_GET()

    handler = lambda *args, **kwargs: QuietHandler(*args, directory=str(directory), **kwargs)  # noqa: E731
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_pull_two_wheeler_raw_exports_supports_glob_file_and_url(tmp_path):
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True)
    _write_pull_inputs(source_root)
    http_root = tmp_path / "http"
    http_root.mkdir(parents=True)
    (http_root / "meta_patch.json").write_text(
        json.dumps({"confidence": 0.88, "report_md": "pulled"}, ensure_ascii=False),
        encoding="utf-8",
    )
    server, thread = _start_http_server(http_root)
    manifest_path = tmp_path / "manifest.json"
    raw_dir = tmp_path / "raw"
    try:
        manifest_path.write_text(
            json.dumps(
                {
                    "sources": [
                        {
                            "name": "sku_glob",
                            "mode": "glob",
                            "pattern": str(source_root / "sku" / "*.csv"),
                            "target_name": "sku_backoffice.csv",
                        },
                        {
                            "name": "supplier_file",
                            "mode": "file",
                            "path": str(source_root / "supplier" / "supplier_latest.csv"),
                            "target_name": "supplier_observations.csv",
                        },
                        {
                            "name": "alias_file",
                            "mode": "file",
                            "path": str(source_root / "alias_map.csv"),
                            "target_name": "alias_map.csv",
                        },
                        {
                            "name": "meta_url",
                            "mode": "url",
                            "url": f"http://127.0.0.1:{server.server_port}/meta_patch.json",
                            "target_name": "meta_patch.json",
                        },
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        summary = pull_two_wheeler_raw_exports(manifest_path, raw_dir)
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert len(summary["pulled"]) == 4
    assert (raw_dir / "sku_backoffice.csv").read_text(encoding="utf-8").count("sku-ninebot-fz3-130") == 1
    assert json.loads((raw_dir / "meta_patch.json").read_text(encoding="utf-8")) == {
        "confidence": 0.88,
        "report_md": "pulled",
    }


def test_pull_two_wheeler_raw_exports_supports_authenticated_url_sources(tmp_path, monkeypatch):
    http_root = tmp_path / "http"
    http_root.mkdir(parents=True)
    (http_root / "bearer.json").write_text(json.dumps({"auth": "bearer"}), encoding="utf-8")
    (http_root / "basic.json").write_text(json.dumps({"auth": "basic"}), encoding="utf-8")
    (http_root / "api.json").write_text(json.dumps({"auth": "api"}), encoding="utf-8")

    expected_basic = b64encode("feed-user:feed-pass".encode("utf-8")).decode("ascii")
    server, thread = _start_http_server(
        http_root,
        expected_headers_by_path={
            "/bearer.json": {"Authorization": "Bearer bearer-secret"},
            "/basic.json": {"Authorization": f"Basic {expected_basic}"},
            "/api.json": {"X-Feed-Key": "api-secret"},
        },
    )
    manifest_path = tmp_path / "manifest.json"
    raw_dir = tmp_path / "raw"
    monkeypatch.setenv("TEST_FEED_PULL_BEARER", "bearer-secret")
    monkeypatch.setenv("TEST_FEED_PULL_API_VALUE", "api-secret")
    try:
        manifest_path.write_text(
            json.dumps(
                {
                    "sources": [
                        {
                            "name": "bearer_url",
                            "mode": "url",
                            "url": f"http://127.0.0.1:{server.server_port}/bearer.json",
                            "target_name": "bearer.json",
                            "auth": {
                                "type": "bearer",
                                "token_env": "TEST_FEED_PULL_BEARER",
                            },
                        },
                        {
                            "name": "basic_url",
                            "mode": "url",
                            "url": f"http://127.0.0.1:{server.server_port}/basic.json",
                            "target_name": "basic.json",
                            "auth": {
                                "type": "basic",
                                "username": "feed-user",
                                "password": "feed-pass",
                            },
                        },
                        {
                            "name": "api_url",
                            "mode": "url",
                            "url": f"http://127.0.0.1:{server.server_port}/api.json",
                            "target_name": "api.json",
                            "auth": {
                                "type": "api_key",
                                "header": "X-Feed-Key",
                                "value_env": "TEST_FEED_PULL_API_VALUE",
                            },
                        },
                    ]
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        summary = pull_two_wheeler_raw_exports(manifest_path, raw_dir)
    finally:
        server.shutdown()
        thread.join(timeout=5)

    assert len(summary["pulled"]) == 3
    assert json.loads((raw_dir / "bearer.json").read_text(encoding="utf-8")) == {"auth": "bearer"}
    assert json.loads((raw_dir / "basic.json").read_text(encoding="utf-8")) == {"auth": "basic"}
    assert json.loads((raw_dir / "api.json").read_text(encoding="utf-8")) == {"auth": "api"}


def test_run_two_wheeler_feed_pull_chains_into_connector(tmp_path):
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True)
    _write_pull_inputs(source_root)
    manifest_path = tmp_path / "manifest.json"
    raw_dir = tmp_path / "raw"
    bundle_dir = tmp_path / "bundle"
    delta_path = tmp_path / "delta.json"

    manifest_path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "name": "sku_glob",
                        "mode": "glob",
                        "pattern": str(source_root / "sku" / "*.csv"),
                        "target_name": "sku_backoffice.csv",
                    },
                    {
                        "name": "supplier_file",
                        "mode": "file",
                        "path": str(source_root / "supplier" / "supplier_latest.csv"),
                        "target_name": "supplier_observations.csv",
                    },
                    {
                        "name": "alias_file",
                        "mode": "file",
                        "path": str(source_root / "alias_map.csv"),
                        "target_name": "alias_map.csv",
                    },
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    summary = run_two_wheeler_feed_pull(
        manifest_path,
        raw_dir,
        bundle_dir,
        run_id="pull-run",
        delta_path=delta_path,
    )

    assert summary["connector"] is not None
    payload = json.loads(delta_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "pull-run"
    assert payload["sku_records"][0]["sku_id"] == "sku-ninebot-fz3-130"
    assert payload["graph"]["aliases"] == {"九号Fz3 130": "ninebot_fz"}


def test_pull_two_wheeler_feeds_cli_dry_run_writes_changelog(tmp_path):
    source_root = tmp_path / "source"
    source_root.mkdir(parents=True)
    _write_pull_inputs(source_root)
    manifest_path = tmp_path / "manifest.json"
    raw_dir = tmp_path / "raw"
    bundle_dir = tmp_path / "bundle"
    changelog_path = tmp_path / "pull.json"

    manifest_path.write_text(
        json.dumps(
            {
                "sources": [
                    {
                        "name": "sku_glob",
                        "mode": "glob",
                        "pattern": str(source_root / "sku" / "*.csv"),
                        "target_name": "sku_backoffice.csv",
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/pull_two_wheeler_feeds.py",
            "--manifest-path",
            str(manifest_path),
            str(raw_dir),
            str(bundle_dir),
            "--run-id",
            "pull-cli",
            "--dry-run",
            "--changelog-path",
            str(changelog_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout
    assert "Dry run: True" in result.stdout
    payload = json.loads(changelog_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == "pull-cli"
    assert payload["pull"]["dry_run"] is True
    assert len(payload["pull"]["pulled"]) == 1
