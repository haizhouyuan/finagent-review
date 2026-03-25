from __future__ import annotations

import json
import os
import re
import shlex
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


def _build_default_headers() -> dict[str, str]:
    contact = os.environ.get("FINAGENT_CONTACT_EMAIL", "").strip()
    user_agent = os.environ.get("FINAGENT_USER_AGENT", "").strip()
    host = socket.gethostname()
    user = os.environ.get("USER", "local")
    contact_value = contact or f"{user}@{host}.local"
    if not user_agent:
        user_agent = f"finagent-p0/0.1 ({contact_value})"
    headers = {
        "User-Agent": user_agent,
        "From": contact_value,
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
    }
    return headers


DEFAULT_HEADERS = _build_default_headers()


class AdapterError(RuntimeError):
    pass


@dataclass
class FetchedArtifact:
    title: str
    uri: str
    published_at: str | None
    raw_text: str
    normalized_text: str
    metadata: dict[str, Any]


@dataclass
class HomePCTranscription:
    title: str
    raw_text: str
    transcript_text: str
    metadata: dict[str, Any]


def _get_json(url: str, headers: dict[str, str] | None = None) -> Any:
    merged = dict(DEFAULT_HEADERS)
    merged.update(headers or {})
    try:
        response = requests.get(url, headers=merged, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise AdapterError(f"GET failed for {url}: {exc}") from exc
    try:
        return response.json()
    except ValueError as exc:
        raise AdapterError(f"invalid json from {url}: {exc}") from exc


def _run_local_command(argv: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout,
    )


def transcribe_audio_with_homepc_funasr(
    audio_path: str | Path,
    *,
    host: str = "yuanhaizhou@192.168.1.17",
    env_name: str = "soulxpodcast",
    modelscope_cache: str = "/home/yuanhaizhou/funasr_models",
    remote_root_base: str = "/home/yuanhaizhou/finagent-runtime/homepc-funasr",
    device: str = "cpu",
    timeout_seconds: int = 180,
    cleanup_remote: bool = False,
) -> HomePCTranscription:
    local_audio = Path(audio_path).resolve()
    if not local_audio.exists():
        raise AdapterError(f"audio path not found: {local_audio}")
    if not local_audio.is_file():
        raise AdapterError(f"audio path is not a file: {local_audio}")

    remote_job = f"{int(time.time())}_{local_audio.stem}"
    remote_root = f"{remote_root_base.rstrip('/')}/{remote_job}"
    remote_audio = f"{remote_root}/input{local_audio.suffix.lower()}"
    remote_wav = f"{remote_root}/input_16k.wav"

    mkdir_proc = _run_local_command(
        [
            "timeout",
            str(timeout_seconds),
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={min(10, timeout_seconds)}",
            host,
            f"mkdir -p {shlex.quote(remote_root)}",
        ],
        timeout=timeout_seconds + 5,
    )
    if mkdir_proc.returncode != 0:
        raise AdapterError(f"failed to create remote dir: {mkdir_proc.stderr.strip() or mkdir_proc.stdout.strip()}")

    scp_proc = _run_local_command(
        [
            "timeout",
            str(timeout_seconds),
            "scp",
            "-q",
            str(local_audio),
            f"{host}:{remote_audio}",
        ],
        timeout=timeout_seconds + 5,
    )
    if scp_proc.returncode != 0:
        raise AdapterError(f"failed to copy audio to homepc: {scp_proc.stderr.strip() or scp_proc.stdout.strip()}")

    remote_lines = [
        "set -euo pipefail",
        'source "$HOME/miniconda3/etc/profile.d/conda.sh"',
        f"conda activate {shlex.quote(env_name)}",
        f"export MODELSCOPE_CACHE={shlex.quote(modelscope_cache)}",
    ]
    if device == "cpu":
        remote_lines.append("export CUDA_VISIBLE_DEVICES=''")
    remote_lines.extend(
        [
            f"export FINAGENT_AUDIO_PATH={shlex.quote(remote_audio)}",
            f"export FINAGENT_WAV_PATH={shlex.quote(remote_wav)}",
            f"export FINAGENT_DEVICE={shlex.quote(device)}",
            "python - <<'PY'",
            "import contextlib",
            "import io",
            "import json",
            "import os",
            "import subprocess",
            "from funasr import AutoModel",
            "audio = os.environ['FINAGENT_AUDIO_PATH']",
            "wav = os.environ['FINAGENT_WAV_PATH']",
            "device = os.environ.get('FINAGENT_DEVICE', 'cpu')",
            "ffmpeg_proc = subprocess.run([",
            "    'ffmpeg', '-y', '-i', audio, '-ar', '16000', '-ac', '1', '-c:a', 'pcm_s16le', wav,",
            "], check=False, capture_output=True, text=True)",
            "if ffmpeg_proc.returncode != 0:",
            "    raise RuntimeError(f'ffmpeg failed: {ffmpeg_proc.stderr[-800:]}')",
            "buf = io.StringIO()",
            "with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):",
            "    model = AutoModel(",
            "        model='paraformer-zh',",
            "        vad_model='fsmn-vad',",
            "        punc_model='ct-punc',",
            "        disable_update=True,",
            "        device=device,",
            "    )",
            "    res = model.generate(input=wav)",
            "text = res[0].get('text', '') if res else ''",
            "out = {",
            "    'audio': audio,",
            "    'wav': wav,",
            "    'text': text,",
            "    'char_count': len(text),",
            "    'engine': f'homepc_funasr_{device}',",
            "    'env': os.environ.get('CONDA_DEFAULT_ENV', ''),",
            "    'modelscope_cache': os.environ.get('MODELSCOPE_CACHE', ''),",
            "}",
            "print(json.dumps(out, ensure_ascii=False))",
            "PY",
        ]
    )
    remote_script = "\n".join(remote_lines)
    run_proc = subprocess.run(
        [
            "timeout",
            str(timeout_seconds),
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={min(10, timeout_seconds)}",
            host,
            "bash",
        ],
        input=remote_script,
        text=True,
        capture_output=True,
        check=False,
        timeout=timeout_seconds + 5,
    )
    if cleanup_remote:
        _run_local_command(
            [
                "timeout",
                str(timeout_seconds),
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                f"ConnectTimeout={min(10, timeout_seconds)}",
                host,
                f"rm -rf {shlex.quote(remote_root)}",
            ],
            timeout=timeout_seconds + 5,
        )
    if run_proc.returncode != 0:
        raise AdapterError(
            "homepc funasr failed: "
            f"returncode={run_proc.returncode} "
            f"stdout={run_proc.stdout.strip()!r} "
            f"stderr={run_proc.stderr.strip()!r}"
        )
    try:
        payload = json.loads((run_proc.stdout or "").strip())
    except json.JSONDecodeError as exc:
        raise AdapterError(f"invalid homepc funasr json output: {exc}") from exc
    transcript_text = str(payload.get("text", "")).strip()
    return HomePCTranscription(
        title=f"Home PC FunASR transcript {local_audio.stem}",
        raw_text=json.dumps(payload, ensure_ascii=False, indent=2),
        transcript_text=transcript_text,
        metadata={
            "local_audio_path": str(local_audio),
            "remote_root": remote_root,
            "host": host,
            **payload,
        },
    )


def _post_form_json(url: str, data: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
    merged = dict(DEFAULT_HEADERS)
    merged.update(
        {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://www.cninfo.com.cn/",
        }
    )
    merged.update(headers or {})
    try:
        response = requests.post(url, headers=merged, data=data, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise AdapterError(f"POST failed for {url}: {exc}") from exc
    try:
        return response.json()
    except ValueError as exc:
        raise AdapterError(f"invalid json from {url}: {exc}") from exc


_CN_TICKER_RE = re.compile(r"^(?P<code>\d{6})\.(?P<ex>SH|SZ)$", re.I)


def _parse_cn_ticker(ticker: str) -> tuple[str, str]:
    match = _CN_TICKER_RE.match((ticker or "").strip())
    if not match:
        raise AdapterError(f"invalid CN ticker: {ticker!r}; expected 000001.SZ or 600000.SH")
    return match.group("code"), match.group("ex").upper()


def _cninfo_column(exchange: str) -> str:
    return "sse_main" if exchange == "SH" else "szse_main"


def _cninfo_full_url(adjunct_url: str) -> str:
    clean = (adjunct_url or "").strip()
    if not clean:
        return ""
    if clean.startswith("http://") or clean.startswith("https://"):
        return clean
    return "https://static.cninfo.com.cn/" + clean.lstrip("/")


def _cninfo_doc_type(title: str) -> str:
    text = (title or "").strip()
    if not text:
        return "other"
    if any(token in text for token in ("年度报告", "年报", "年度报告摘要")):
        return "annual"
    if any(token in text for token in ("半年度报告", "季度报告", "业绩快报", "业绩预告")):
        return "quarter"
    if any(token in text for token in ("减持", "增持", "质押", "股东", "权益变动", "解禁")):
        return "ownership"
    if any(
        token in text
        for token in ("风险提示", "异常波动", "回购", "中标", "合同", "问询函", "监管函", "重大", "停牌", "复牌")
    ):
        return "material"
    return "other"


def fetch_sec_submissions(ticker: str) -> FetchedArtifact:
    tickers = _get_json("https://www.sec.gov/files/company_tickers.json")
    ticker_upper = ticker.upper()
    match = None
    for value in tickers.values():
        if str(value.get("ticker", "")).upper() == ticker_upper:
            match = value
            break
    if match is None:
        raise AdapterError(f"ticker not found in SEC mapping: {ticker}")
    cik = str(match["cik_str"]).zfill(10)
    submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    data = _get_json(submissions_url)
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])[:5]
    dates = recent.get("filingDate", [])[:5]
    accession = recent.get("accessionNumber", [])[:5]
    summary_lines = [
        f"SEC submissions summary for {data.get('name', match['title'])} ({ticker_upper}, CIK {cik}).",
        "Recent official filings:",
    ]
    recent_rows: list[dict[str, str]] = []
    for idx, form in enumerate(forms):
        filing_date = dates[idx] if idx < len(dates) else ""
        accession_number = accession[idx] if idx < len(accession) else ""
        summary_lines.append(f"- {form} filed on {filing_date} accession {accession_number}")
        recent_rows.append(
            {
                "form": form,
                "filing_date": filing_date,
                "accession_number": accession_number,
            }
        )
    published_at = dates[0] if dates else None
    return FetchedArtifact(
        title=f"SEC submissions {ticker_upper}",
        uri=submissions_url,
        published_at=published_at,
        raw_text=json.dumps(data, ensure_ascii=False, indent=2),
        normalized_text="\n".join(summary_lines),
        metadata={
            "ticker": ticker_upper,
            "cik": cik,
            "company_name": data.get("name", match["title"]),
            "recent_filings": recent_rows,
            "metrics": {
                "recent_filing_count": len(recent_rows),
            },
        },
    )


def fetch_openalex_search(query: str, per_page: int = 5) -> FetchedArtifact:
    url = f"https://api.openalex.org/works?search={requests.utils.quote(query)}&per-page={per_page}"
    data = _get_json(url)
    results = data.get("results", [])
    summary_lines = [f"OpenAlex search results for query: {query}"]
    works: list[dict[str, Any]] = []
    for item in results:
        title = item.get("display_name") or item.get("title") or "Untitled"
        year = item.get("publication_year")
        cited = item.get("cited_by_count")
        summary_lines.append(f"- {title} ({year}), cited_by_count={cited}")
        works.append(
            {
                "title": title,
                "year": year,
                "cited_by_count": cited,
                "openalex_id": item.get("id"),
                "doi": item.get("doi"),
            }
        )
    published_at = None
    if results:
        published_at = results[0].get("publication_date")
    return FetchedArtifact(
        title=f"OpenAlex search {query}",
        uri=url,
        published_at=published_at,
        raw_text=json.dumps(data, ensure_ascii=False, indent=2),
        normalized_text="\n".join(summary_lines),
        metadata={
            "query": query,
            "works": works,
            "metrics": {
                "result_count": len(works),
            },
        },
    )


def fetch_defillama_protocol(slug: str) -> FetchedArtifact:
    url = f"https://api.llama.fi/protocol/{slug}"
    data = _get_json(url)
    metrics: dict[str, Any] = {}
    if isinstance(data.get("currentChainTvls"), dict):
        tvl_values = [value for value in data["currentChainTvls"].values() if isinstance(value, (int, float))]
        if tvl_values:
            metrics["max_chain_tvl"] = max(tvl_values)
            metrics["chain_count"] = len(tvl_values)
    if isinstance(data.get("mcap"), (int, float)):
        metrics["mcap"] = data["mcap"]
    summary_lines = [
        f"DefiLlama protocol snapshot for {data.get('name', slug)}.",
        f"Category: {data.get('category', 'unknown')}",
        f"Symbol: {data.get('symbol', 'unknown')}",
    ]
    if "max_chain_tvl" in metrics:
        summary_lines.append(f"Max current chain TVL: {metrics['max_chain_tvl']}")
    if "chain_count" in metrics:
        summary_lines.append(f"Chain count with current TVL values: {metrics['chain_count']}")
    published_at = None
    if isinstance(data.get("chainTvls"), dict):
        metrics["history_chain_count"] = len(data["chainTvls"])
    return FetchedArtifact(
        title=f"DefiLlama protocol {slug}",
        uri=url,
        published_at=published_at,
        raw_text=json.dumps(data, ensure_ascii=False, indent=2),
        normalized_text="\n".join(summary_lines),
        metadata={
            "slug": slug,
            "name": data.get("name"),
            "symbol": data.get("symbol"),
            "category": data.get("category"),
            "url": data.get("url"),
            "metrics": metrics,
        },
    )


def fetch_cninfo_announcements(
    ticker: str,
    *,
    search_key: str | None = None,
    lookback_days: int = 45,
    limit: int = 5,
) -> FetchedArtifact:
    from datetime import datetime, timedelta, timezone

    code, exchange = _parse_cn_ticker(ticker)
    url = "https://www.cninfo.com.cn/new/hisAnnouncement/query"
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=max(1, int(lookback_days)))
    payload = {
        "pageNum": 1,
        "pageSize": max(limit, 10),
        "tabName": "fulltext",
        "column": _cninfo_column(exchange),
        "stock": "",
        "searchkey": (search_key or code).strip() or code,
        "secid": "",
        "category": "",
        "trade": "",
        "seDate": f"{start.isoformat()}~{today.isoformat()}",
        "sortName": "",
        "sortType": "",
    }
    data = _post_form_json(url, payload)
    rows = []
    summary_lines = [f"CNINFO announcement summary for {ticker}."]
    announcements = data.get("announcements", [])
    for item in announcements:
        if not isinstance(item, dict):
            continue
        if str(item.get("secCode") or "").strip() != code:
            continue
        title = str(item.get("announcementTitle") or "").strip()
        adjunct_url = str(item.get("adjunctUrl") or "").strip()
        full_url = _cninfo_full_url(adjunct_url)
        if not full_url:
            continue
        published_ms = int(item.get("announcementTime") or 0)
        published_at = ""
        if published_ms > 0:
            published_at = datetime.fromtimestamp(published_ms / 1000.0, tz=timezone.utc).replace(microsecond=0).isoformat()
        rows.append(
            {
                "announcement_id": str(item.get("announcementId") or item.get("id") or "").strip(),
                "sec_code": code,
                "sec_name": str(item.get("secName") or "").strip(),
                "title": title,
                "doc_type": _cninfo_doc_type(title),
                "published_at": published_at,
                "url": full_url,
            }
        )
    rows = rows[: max(1, int(limit))]
    if not rows:
        raise AdapterError(f"no CNINFO announcements found for {ticker}")
    for row in rows:
        summary_lines.append(f"- {row['published_at'][:10]} {row['title']} ({row['doc_type']})")
    return FetchedArtifact(
        title=f"CNINFO announcements {ticker}",
        uri=url,
        published_at=rows[0]["published_at"] or None,
        raw_text=json.dumps(data, ensure_ascii=False, indent=2),
        normalized_text="\n".join(summary_lines),
        metadata={
            "ticker": ticker.upper(),
            "sec_code": code,
            "exchange": exchange,
            "company_name": rows[0]["sec_name"],
            "announcements": rows,
            "metrics": {
                "recent_announcement_count": len(rows),
            },
        },
    )
