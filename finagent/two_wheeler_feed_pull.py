"""Pull raw two-wheeler export files and optionally run the connector chain."""

from __future__ import annotations

import glob
import json
import os
import shutil
from base64 import b64encode
from pathlib import Path
from typing import Any

import requests

from finagent.two_wheeler_catalog import DEFAULT_SOURCE_DIR, REPO_ROOT, _write_json
from finagent.two_wheeler_feed_connector import connect_two_wheeler_feeds

DEFAULT_PULL_MANIFEST = REPO_ROOT / "data" / "two_wheeler" / "feed_pull_manifest.template.json"
DEFAULT_PULL_CHANGELOG_DIR = REPO_ROOT / "state" / "two_wheeler_feed_pull"
REQUEST_TIMEOUT_SEC = 30


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"invalid pull manifest payload: {path}")
    return payload


def _resolve_value(spec: dict[str, Any], inline_key: str, env_key: str) -> str:
    inline = str(spec.get(inline_key, "") or "").strip()
    if inline:
        return inline
    env_name = str(spec.get(env_key, "") or "").strip()
    if env_name:
        resolved = os.environ.get(env_name, "").strip()
        if resolved:
            return resolved
    raise ValueError(f"missing {inline_key} / {env_key} in source spec: {spec}")


def _resolve_headers(spec: dict[str, Any]) -> dict[str, str]:
    headers = spec.get("headers")
    resolved: dict[str, str] = {}
    if isinstance(headers, dict):
        resolved.update({str(key): str(value) for key, value in headers.items()})
    env_name = str(spec.get("headers_env", "") or "").strip()
    if env_name:
        raw_value = os.environ.get(env_name, "").strip()
        if raw_value:
            payload = json.loads(raw_value)
            if not isinstance(payload, dict):
                raise ValueError(f"invalid headers payload from env {env_name}")
            resolved.update({str(key): str(value) for key, value in payload.items()})
    resolved.update(_resolve_auth_headers(spec))
    return resolved


def _resolve_auth_value(auth_spec: dict[str, Any], inline_key: str, env_key: str) -> str:
    inline = str(auth_spec.get(inline_key, "") or "").strip()
    if inline:
        return inline
    env_name = str(auth_spec.get(env_key, "") or "").strip()
    if env_name:
        resolved = os.environ.get(env_name, "").strip()
        if resolved:
            return resolved
    raise ValueError(f"missing {inline_key} / {env_key} in auth spec: {auth_spec}")


def _resolve_auth_headers(spec: dict[str, Any]) -> dict[str, str]:
    auth_spec = spec.get("auth")
    if not isinstance(auth_spec, dict):
        return {}
    auth_type = str(auth_spec.get("type", "") or "").strip().lower()
    if auth_type == "bearer":
        token = _resolve_auth_value(auth_spec, "token", "token_env")
        return {"Authorization": f"Bearer {token}"}
    if auth_type == "basic":
        username = _resolve_auth_value(auth_spec, "username", "username_env")
        password = _resolve_auth_value(auth_spec, "password", "password_env")
        encoded = b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
        return {"Authorization": f"Basic {encoded}"}
    if auth_type == "api_key":
        header_name = _resolve_auth_value(auth_spec, "header", "header_env")
        value = _resolve_auth_value(auth_spec, "value", "value_env")
        return {header_name: value}
    raise ValueError(f"unsupported auth type: {auth_type}")


def _latest_matching_file(pattern: str) -> Path:
    matches = [Path(path) for path in glob.glob(pattern)]
    if not matches:
        raise FileNotFoundError(f"no files matched pattern: {pattern}")
    return max(matches, key=lambda path: path.stat().st_mtime)


def _pull_source_to_target(spec: dict[str, Any], target_path: Path, *, dry_run: bool) -> dict[str, Any]:
    mode = str(spec.get("mode", "") or "").strip().lower()
    optional = bool(spec.get("optional", False))
    summary: dict[str, Any] = {
        "name": str(spec.get("name", target_path.name)),
        "mode": mode,
        "target": str(target_path),
        "optional": optional,
    }
    try:
        if mode == "file":
            source_path = Path(_resolve_value(spec, "path", "path_env"))
            summary["source"] = str(source_path)
            if not dry_run:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, target_path)
                summary["bytes"] = target_path.stat().st_size
            return summary
        if mode == "glob":
            pattern = _resolve_value(spec, "pattern", "pattern_env")
            source_path = _latest_matching_file(pattern)
            summary["source"] = str(source_path)
            summary["pattern"] = pattern
            if not dry_run:
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source_path, target_path)
                summary["bytes"] = target_path.stat().st_size
            return summary
        if mode == "url":
            url = _resolve_value(spec, "url", "url_env")
            summary["source"] = url
            if not dry_run:
                response = requests.get(url, headers=_resolve_headers(spec), timeout=REQUEST_TIMEOUT_SEC)
                response.raise_for_status()
                target_path.parent.mkdir(parents=True, exist_ok=True)
                target_path.write_bytes(response.content)
                summary["bytes"] = len(response.content)
            return summary
        raise ValueError(f"unsupported pull mode: {mode}")
    except Exception as exc:
        if optional:
            summary["skipped"] = True
            summary["reason"] = str(exc)
            return summary
        raise


def pull_two_wheeler_raw_exports(
    manifest_path: str | Path,
    raw_dir: str | Path,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    manifest_path = Path(manifest_path)
    raw_dir = Path(raw_dir)
    manifest = _read_json(manifest_path)
    source_specs = list(manifest.get("sources", []))
    if not isinstance(source_specs, list):
        raise ValueError(f"invalid sources list in manifest: {manifest_path}")

    pulled: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for spec in source_specs:
        if not isinstance(spec, dict):
            raise ValueError(f"invalid source spec in manifest: {spec}")
        target_name = str(spec.get("target_name", "") or "").strip()
        if not target_name:
            raise ValueError(f"missing target_name in source spec: {spec}")
        result = _pull_source_to_target(spec, raw_dir / target_name, dry_run=dry_run)
        if result.get("skipped"):
            skipped.append(result)
        else:
            pulled.append(result)

    return {
        "manifest_path": str(manifest_path),
        "raw_dir": str(raw_dir),
        "dry_run": dry_run,
        "pulled": pulled,
        "skipped": skipped,
    }


def run_two_wheeler_feed_pull(
    manifest_path: str | Path,
    raw_dir: str | Path,
    bundle_dir: str | Path,
    *,
    run_id: str | None = None,
    delta_path: str | Path | None = None,
    source_dir: str | Path = DEFAULT_SOURCE_DIR,
    dry_run: bool = False,
) -> dict[str, Any]:
    pull_summary = pull_two_wheeler_raw_exports(manifest_path, raw_dir, dry_run=dry_run)
    connector_summary: dict[str, Any] | None = None
    if not dry_run:
        connector_summary = connect_two_wheeler_feeds(
            raw_dir,
            bundle_dir,
            run_id=run_id,
            delta_path=delta_path,
            source_dir=source_dir,
        )
    return {
        "run_id": run_id or Path(raw_dir).name,
        "manifest_path": str(Path(manifest_path)),
        "raw_dir": str(Path(raw_dir)),
        "bundle_dir": str(Path(bundle_dir)),
        "source_dir": str(Path(source_dir)),
        "delta_path": str(Path(delta_path)) if delta_path is not None else "",
        "dry_run": dry_run,
        "pull": pull_summary,
        "connector": connector_summary,
    }


def write_feed_pull_changelog(summary: dict[str, Any], output_path: str | Path) -> Path:
    return _write_json(Path(output_path), summary)
