"""Document parser — orchestrates the full ingestion pipeline.

Pipeline: raw input → TextCleaner → SemanticChunker → EvidenceStore

Supports:
  - URL (fetches via curl, cleans HTML)
  - Local file (reads text/markdown/PDF stub)
  - Raw text string

This is the single entry point for all data going into the research
system.  Nothing reaches the Extractor without passing through here.
"""

from __future__ import annotations

import logging
import subprocess
import os
from pathlib import Path
from typing import Any

from .text_cleaner import TextCleaner
from .chunker import SemanticChunker

logger = logging.getLogger(__name__)


class DocumentParser:
    """Full document ingestion pipeline.

    Usage:
        parser = DocumentParser(evidence_store=my_store)
        refs = parser.parse_url("https://example.com/research-report.html")
        refs = parser.parse_file("/path/to/report.md")
        refs = parser.parse_text(raw_text, query="蓝箭航天 供应链")
    """

    def __init__(
        self,
        *,
        evidence_store: Any | None = None,
        cleaner: TextCleaner | None = None,
        chunker: SemanticChunker | None = None,
        max_chars_per_chunk: int = 4000,
        overlap_chars: int = 200,
        quality_threshold: float = 0.05,
    ):
        """
        Args:
            evidence_store: EvidenceStore instance for persistence.
            cleaner: TextCleaner (uses default if None).
            chunker: SemanticChunker (uses default if None).
            max_chars_per_chunk: Max characters per chunk.
            overlap_chars: Overlap between consecutive chunks.
            quality_threshold: Minimum quality_score to accept a document.
        """
        self.evidence_store = evidence_store
        self.cleaner = cleaner or TextCleaner()
        self.chunker = chunker or SemanticChunker(
            max_chars=max_chars_per_chunk,
            overlap_chars=overlap_chars,
        )
        self.quality_threshold = quality_threshold

    def parse_text(
        self,
        raw_text: str,
        *,
        query: str = "",
        source_type: str = "raw_text",
    ) -> list[dict[str, Any]]:
        """Parse raw text through the full pipeline.

        Returns list of evidence references (for LangGraph state).
        """
        # Step 1: Clean
        cleaned = self.cleaner.clean_for_evidence(raw_text, query=query)

        if cleaned["quality_score"] < self.quality_threshold:
            logger.warning(
                "document quality too low (%.3f < %.3f), skipping",
                cleaned["quality_score"], self.quality_threshold,
            )
            return []

        clean_text = cleaned["clean_text"]
        if not clean_text or len(clean_text) < 50:
            return []

        # Step 2: Chunk
        chunk_dicts = self.chunker.chunk_for_evidence(
            clean_text, query=query, source_type=source_type,
        )

        # Step 3: Store to EvidenceStore (if available)
        refs = []
        for chunk in chunk_dicts:
            if self.evidence_store:
                ref = self.evidence_store.store(
                    query=query,
                    raw_text=chunk["text"],
                    source_type=source_type,
                )
                ref["heading"] = chunk.get("heading", "")
                ref["chunk_index"] = chunk.get("chunk_index", 0)
            else:
                # Inline fallback (for testing)
                ref = {
                    "evidence_id": None,
                    "query": query,
                    "char_count": chunk["char_count"],
                    "source_type": source_type,
                    "heading": chunk.get("heading", ""),
                    "chunk_index": chunk.get("chunk_index", 0),
                    "_text": chunk["text"],
                }
            refs.append(ref)

        logger.info(
            "parse_text: %d chars → clean %d chars (quality=%.3f) → %d chunks",
            len(raw_text), len(clean_text), cleaned["quality_score"], len(refs),
        )
        return refs

    def parse_url(
        self,
        url: str,
        *,
        query: str = "",
        timeout: int = 15,
    ) -> list[dict[str, Any]]:
        """Fetch URL content and parse through the pipeline."""
        try:
            env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
            env["PATH"] = os.environ.get("PATH", "/usr/bin")

            result = subprocess.run(
                ["curl", "-s", "--noproxy", "*",
                 "--connect-timeout", "8", "-m", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                 "-H", "Accept-Language: zh-CN,zh;q=0.9,en;q=0.8",
                 url],
                capture_output=True, timeout=timeout + 5, env=env,
            )
            raw_html = result.stdout.decode("utf-8", errors="replace")

            if not raw_html or len(raw_html) < 100:
                logger.warning("parse_url: empty or very short response from %s", url)
                return []

            return self.parse_text(
                raw_html,
                query=query or url,
                source_type="web_page",
            )

        except Exception as exc:
            logger.error("parse_url failed for '%s': %s", url, exc)
            return []

    def parse_file(
        self,
        path: str | Path,
        *,
        query: str = "",
        encoding: str = "utf-8",
    ) -> list[dict[str, Any]]:
        """Read a local file and parse through the pipeline."""
        path = Path(path)
        if not path.exists():
            logger.error("parse_file: file not found: %s", path)
            return []

        suffix = path.suffix.lower()

        if suffix == ".pdf":
            # PDF stub — would integrate pdfplumber or pymupdf
            logger.warning("PDF parsing not yet implemented, attempting raw text read")
            try:
                raw = path.read_text(encoding=encoding, errors="replace")
            except Exception:
                raw = path.read_bytes().decode("utf-8", errors="replace")
        else:
            raw = path.read_text(encoding=encoding, errors="replace")

        return self.parse_text(
            raw,
            query=query or path.name,
            source_type=f"local_file:{suffix}",
        )


# ── Convenience function ────────────────────────────────────────────


def parse_and_store(
    content: str,
    *,
    evidence_store: Any | None = None,
    query: str = "",
    source_type: str = "raw_text",
    max_chars_per_chunk: int = 4000,
) -> list[dict[str, Any]]:
    """One-shot parse + store."""
    parser = DocumentParser(
        evidence_store=evidence_store,
        max_chars_per_chunk=max_chars_per_chunk,
    )
    return parser.parse_text(content, query=query, source_type=source_type)
