"""Semantic chunker — splits cleaned text into LLM-friendly chunks.

Why chunking matters:
  - LLM context windows have limits (4K-128K tokens)
  - Long documents dilute extraction quality
  - exact_quote validation needs a bounded source text per extraction

Chunking strategy:
  1. Prefer natural boundaries (## headers, double newlines)
  2. Fallback to sentence boundaries for oversized paragraphs
  3. Overlap between chunks ensures no relationship is split across borders
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class Chunk:
    """A chunk of text with metadata."""
    text: str
    index: int
    start_char: int
    end_char: int
    heading: str = ""        # Nearest heading context
    char_count: int = 0

    def __post_init__(self):
        self.char_count = len(self.text)


class SemanticChunker:
    """Splits clean text into overlapping semantic chunks.

    Each chunk is designed to be a self-contained unit of meaning
    that can be independently fed to an LLM for triple extraction.

    Usage:
        chunker = SemanticChunker(max_chars=4000, overlap_chars=200)
        chunks = chunker.chunk(clean_text)
    """

    def __init__(
        self,
        *,
        max_chars: int = 4000,
        overlap_chars: int = 200,
        min_chunk_chars: int = 100,
    ):
        """Initialize the chunker.

        Args:
            max_chars: Maximum characters per chunk (target, not hard limit).
            overlap_chars: Characters to overlap between consecutive chunks.
            min_chunk_chars: Minimum characters for a chunk to be kept.
        """
        self.max_chars = max_chars
        self.overlap_chars = overlap_chars
        self.min_chunk_chars = min_chunk_chars

    def chunk(self, text: str) -> list[Chunk]:
        """Split text into semantic chunks.

        Strategy:
          1. Split by headers (## level 2+) — these are strongest boundaries
          2. Within each section, split by double newlines (paragraphs)
          3. If a section is still too long, split by sentence boundaries
          4. Apply overlap between chunks
        """
        if not text or len(text) < self.min_chunk_chars:
            if text and text.strip():
                return [Chunk(text=text.strip(), index=0, start_char=0, end_char=len(text))]
            return []

        # Step 1: Split by headers
        sections = self._split_by_headers(text)

        # Step 2: Split oversized sections by paragraphs/sentences
        raw_chunks: list[str] = []
        headings: list[str] = []
        for heading, section_text in sections:
            if len(section_text) <= self.max_chars:
                raw_chunks.append(section_text)
                headings.append(heading)
            else:
                # Split further by paragraphs
                sub_chunks = self._split_long_section(section_text)
                raw_chunks.extend(sub_chunks)
                headings.extend([heading] * len(sub_chunks))

        # Step 3: Merge tiny trailing chunks
        merged_chunks, merged_headings = self._merge_small_chunks(raw_chunks, headings)

        # Step 4: Build Chunk objects with overlap
        chunks = self._apply_overlap(merged_chunks, merged_headings, text)

        logger.info(
            "chunker: %d chars → %d chunks (max=%d, overlap=%d)",
            len(text), len(chunks), self.max_chars, self.overlap_chars,
        )

        return chunks

    def chunk_for_evidence(
        self,
        text: str,
        *,
        query: str = "",
        source_type: str = "document",
    ) -> list[dict[str, Any]]:
        """Chunk text and return metadata dicts ready for EvidenceStore.

        Each dict contains 'text', 'chunk_index', 'heading', 'char_count'.
        """
        chunks = self.chunk(text)
        return [
            {
                "text": c.text,
                "chunk_index": c.index,
                "heading": c.heading,
                "char_count": c.char_count,
                "query": query,
                "source_type": source_type,
            }
            for c in chunks
        ]

    # ── Internal splitting methods ────────────────────────────────

    @staticmethod
    def _split_by_headers(text: str) -> list[tuple[str, str]]:
        """Split text by Markdown headers, preserving heading context.

        Returns: [(heading, section_text), ...]
        """
        # Match ## or deeper headers
        header_pattern = re.compile(r"^(#{2,6}\s+.+)$", re.MULTILINE)
        parts = header_pattern.split(text)

        sections: list[tuple[str, str]] = []
        current_heading = ""

        for i, part in enumerate(parts):
            stripped = part.strip()
            if not stripped:
                continue
            if header_pattern.match(stripped):
                current_heading = stripped
            else:
                # Prepend heading to section for context
                section_text = f"{current_heading}\n{stripped}" if current_heading else stripped
                sections.append((current_heading, section_text.strip()))

        if not sections and text.strip():
            sections = [("", text.strip())]

        return sections

    def _split_long_section(self, text: str) -> list[str]:
        """Split an oversized section by paragraph boundaries, then sentences."""
        # Try paragraph split first (double newline)
        paragraphs = re.split(r"\n\n+", text)

        # If single paragraph is still too long, split by sentences
        result: list[str] = []
        current_block = ""

        for para in paragraphs:
            if len(current_block) + len(para) + 2 <= self.max_chars:
                current_block = f"{current_block}\n\n{para}".strip()
            else:
                if current_block:
                    result.append(current_block)
                if len(para) > self.max_chars:
                    # Split by Chinese/English sentence boundaries
                    sentences = self._split_by_sentences(para)
                    sent_block = ""
                    for sent in sentences:
                        if len(sent_block) + len(sent) + 1 <= self.max_chars:
                            sent_block = f"{sent_block} {sent}".strip()
                        else:
                            if sent_block:
                                result.append(sent_block)
                            sent_block = sent
                    if sent_block:
                        result.append(sent_block)
                    current_block = ""
                else:
                    current_block = para

        if current_block:
            result.append(current_block)

        return result if result else [text]

    @staticmethod
    def _split_by_sentences(text: str) -> list[str]:
        """Split text by sentence boundaries (Chinese and English)."""
        # Chinese sentence endings: 。！？；
        # English sentence endings: . ! ? ;
        sentences = re.split(r"(?<=[。！？；.!?;])\s*", text)
        return [s.strip() for s in sentences if s.strip()]

    def _merge_small_chunks(
        self,
        chunks: list[str],
        headings: list[str],
    ) -> tuple[list[str], list[str]]:
        """Merge chunks that are too small to standalone."""
        if len(chunks) <= 1:
            return chunks, headings

        merged: list[str] = []
        merged_headings: list[str] = []
        current = ""
        current_heading = ""

        for chunk, heading in zip(chunks, headings):
            if len(current) + len(chunk) + 2 <= self.max_chars and len(chunk) < self.min_chunk_chars:
                current = f"{current}\n\n{chunk}".strip()
                if not current_heading:
                    current_heading = heading
            else:
                if current:
                    merged.append(current)
                    merged_headings.append(current_heading)
                current = chunk
                current_heading = heading

        if current:
            merged.append(current)
            merged_headings.append(current_heading)

        return merged, merged_headings

    def _apply_overlap(
        self,
        chunks: list[str],
        headings: list[str],
        original: str,
    ) -> list[Chunk]:
        """Create Chunk objects with overlap between consecutive chunks."""
        result: list[Chunk] = []
        char_offset = 0

        for i, (text, heading) in enumerate(zip(chunks, headings)):
            # Find actual position in original text
            pos = original.find(text[:50], max(0, char_offset - 100))
            start_char = pos if pos >= 0 else char_offset

            # Add overlap from next chunk (if not last)
            chunk_text = text
            if i < len(chunks) - 1 and self.overlap_chars > 0:
                next_chunk = chunks[i + 1]
                overlap_text = next_chunk[:self.overlap_chars]
                # Only add if it doesn't make the chunk too large
                if len(chunk_text) + len(overlap_text) + 4 <= self.max_chars * 1.1:
                    chunk_text = f"{chunk_text}\n\n{overlap_text}"

            result.append(Chunk(
                text=chunk_text,
                index=i,
                start_char=start_char,
                end_char=start_char + len(text),
                heading=heading,
            ))

            char_offset = start_char + len(text)

        return result
