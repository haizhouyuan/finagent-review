"""Text cleaner — the "净水器" for finagent.

Transforms raw HTML / web scraping / PDF extraction output into
clean, normalized Markdown text that is compatible with exact_quote
substring matching.

Design principle: every transformation must preserve the SEMANTIC
content while eliminating formatting noise that would cause
exact_quote validation to fail.

Pipeline:
  1. Strip HTML tags (preserve semantic structure as Markdown)
  2. Decode HTML entities
  3. Normalize Unicode (NFKC)
  4. Remove noise (ads, navbars, boilerplate)
  5. Normalize whitespace (collapse runs, standardize newlines)
  6. Remove invisible characters
  7. Strip URLs and image references (optional)
"""

from __future__ import annotations

import html
import logging
import re
import unicodedata
from typing import Any

logger = logging.getLogger(__name__)

# Noise patterns commonly found in web scraping
_NOISE_PATTERNS = [
    # Cookie/GDPR notices
    re.compile(r"(?:cookie|privacy|隐私|免责声明).*?(?:\n|$)", re.IGNORECASE),
    # Navigation elements
    re.compile(r"(?:首页|登录|注册|关于我们|联系我们|Copyright).*?(?:\n|$)", re.IGNORECASE),
    # Share buttons
    re.compile(r"(?:分享到|转发|微信|微博|QQ)\s*"),
    # "Read more" links
    re.compile(r"(?:阅读全文|查看更多|展开全部|点击展开)"),
    # Ad markers
    re.compile(r"(?:广告|推广|赞助|Sponsored)\s*"),
]

# Block-level HTML tags that should become Markdown structure
_BLOCK_TAGS = {
    "h1", "h2", "h3", "h4", "h5", "h6",
    "p", "div", "section", "article",
    "li", "ul", "ol",
    "blockquote",
    "table", "tr", "td", "th",
    "pre", "code",
}


class TextCleaner:
    """High-purity text cleaner for exact_quote compatibility.

    Usage:
        cleaner = TextCleaner()
        clean_text = cleaner.clean(raw_html_or_text)
    """

    def __init__(
        self,
        *,
        strip_urls: bool = True,
        strip_images: bool = True,
        remove_noise: bool = True,
        min_line_length: int = 5,
        max_consecutive_newlines: int = 2,
    ):
        self.strip_urls = strip_urls
        self.strip_images = strip_images
        self.remove_noise = remove_noise
        self.min_line_length = min_line_length
        self.max_consecutive_newlines = max_consecutive_newlines

    def clean(self, raw: str) -> str:
        """Full cleaning pipeline.

        Args:
            raw: Raw HTML, web scraping output, or PDF text extraction.

        Returns:
            Clean, normalized text suitable for exact_quote matching.
        """
        if not raw or not raw.strip():
            return ""

        text = raw

        # Step 1: If it contains HTML tags, convert to text
        if self._looks_like_html(text):
            text = self._html_to_text(text)
        else:
            # Still decode any stray HTML entities
            text = html.unescape(text)

        # Step 2: Unicode normalization (NFKC for CJK compatibility)
        text = unicodedata.normalize("NFKC", text)

        # Step 3: Remove invisible/control characters
        text = self._strip_invisible(text)

        # Step 4: Strip URLs if configured
        if self.strip_urls:
            text = self._strip_urls(text)

        # Step 5: Strip image references
        if self.strip_images:
            text = re.sub(r"!\[.*?\]\(.*?\)", "", text)

        # Step 6: Remove noise patterns
        if self.remove_noise:
            text = self._remove_noise(text)

        # Step 7: Normalize whitespace
        text = self._normalize_whitespace(text)

        # Step 8: Filter short lines
        text = self._filter_short_lines(text)

        return text.strip()

    def clean_for_evidence(self, raw: str, *, query: str = "") -> dict[str, Any]:
        """Clean text and return metadata for EvidenceStore.

        Returns:
            Dict with 'clean_text', 'original_chars', 'clean_chars', 'quality_score'.
        """
        original_chars = len(raw)
        clean_text = self.clean(raw)
        clean_chars = len(clean_text)

        # Quality score: ratio of clean to original (too low = mostly noise)
        quality_score = clean_chars / max(original_chars, 1)

        return {
            "clean_text": clean_text,
            "original_chars": original_chars,
            "clean_chars": clean_chars,
            "quality_score": round(quality_score, 3),
            "query": query,
        }

    # ── Internal pipeline stages ──────────────────────────────────

    @staticmethod
    def _looks_like_html(text: str) -> bool:
        """Heuristic: does this look like HTML content?"""
        tag_count = len(re.findall(r"<[a-zA-Z/]", text))
        return tag_count > 3

    @staticmethod
    def _html_to_text(raw: str) -> str:
        """Convert HTML to clean text, preserving semantic structure."""
        # Remove scripts and styles entirely
        text = re.sub(r"<script[^>]*>.*?</script>", "", raw, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<noscript[^>]*>.*?</noscript>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL)

        # Convert headers to Markdown
        for i in range(1, 7):
            text = re.sub(
                rf"<h{i}[^>]*>(.*?)</h{i}>",
                lambda m: f"\n{'#' * i} {m.group(1).strip()}\n",
                text, flags=re.DOTALL | re.IGNORECASE,
            )

        # Convert paragraphs to double newlines
        text = re.sub(r"</?p[^>]*>", "\n\n", text, flags=re.IGNORECASE)

        # Convert list items
        text = re.sub(r"<li[^>]*>(.*?)</li>", r"\n- \1", text, flags=re.DOTALL | re.IGNORECASE)

        # Convert <br> to newlines
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)

        # Convert <td>/<th> to tab separation for tables
        text = re.sub(r"</?(td|th)[^>]*>", " | ", text, flags=re.IGNORECASE)
        text = re.sub(r"</?tr[^>]*>", "\n", text, flags=re.IGNORECASE)

        # Bold/italic preservation (use separate patterns to avoid backreference case issues)
        text = re.sub(r"<b[^>]*>(.*?)</b>", r"**\1**", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<strong[^>]*>(.*?)</strong>", r"**\1**", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<i[^>]*>(.*?)</i>", r"*\1*", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<em[^>]*>(.*?)</em>", r"*\1*", text, flags=re.DOTALL | re.IGNORECASE)

        # Extract link text
        text = re.sub(r"<a[^>]*>(.*?)</a>", r"\1", text, flags=re.DOTALL | re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r"<[^>]+>", " ", text)

        # Decode HTML entities
        text = html.unescape(text)

        return text

    @staticmethod
    def _strip_invisible(text: str) -> str:
        """Remove invisible Unicode characters that break string matching."""
        # Remove zero-width characters
        text = re.sub(r"[\u200b\u200c\u200d\u200e\u200f\ufeff\u00ad]", "", text)
        # Remove other control characters (keep tabs and newlines)
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)
        return text

    @staticmethod
    def _strip_urls(text: str) -> str:
        """Remove URLs while preserving surrounding context."""
        # Full URLs
        text = re.sub(r"https?://\S+", "", text)
        # Markdown-style links: keep text, drop URL
        text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)
        return text

    def _remove_noise(self, text: str) -> str:
        """Remove common web scraping noise."""
        for pattern in _NOISE_PATTERNS:
            text = pattern.sub("", text)
        return text

    def _normalize_whitespace(self, text: str) -> str:
        """Normalize whitespace for consistent substring matching.

        This is THE critical operation for exact_quote compatibility:
        all whitespace variations (tabs, multiple spaces, \r\n vs \n)
        are collapsed to single spaces within lines and standardized
        newlines between lines.
        """
        # Convert tabs to spaces
        text = text.replace("\t", " ")
        # Normalize line endings
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        # Collapse multiple spaces within lines
        text = re.sub(r"[ ]+", " ", text)
        # Strip leading/trailing space per line
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)
        # Collapse excessive blank lines (max_consecutive_newlines=2 means at most 2 blank lines)
        limit = self.max_consecutive_newlines
        # Replace 3+ consecutive newlines with exactly `limit` newlines
        pattern = r"\n{" + str(limit + 1) + r",}"
        text = re.sub(pattern, "\n" * limit, text)
        return text

    def _filter_short_lines(self, text: str) -> str:
        """Remove lines that are too short to be meaningful content."""
        lines = text.split("\n")
        filtered = []
        for line in lines:
            stripped = line.strip()
            # Keep headers, keep meaningful content, skip noise
            if (
                stripped.startswith("#")
                or stripped.startswith("-")
                or stripped.startswith("|")
                or len(stripped) >= self.min_line_length
                or not stripped  # Keep blank lines for paragraph separation
            ):
                filtered.append(line)
        return "\n".join(filtered)


# ── Convenience function ────────────────────────────────────────────


def clean_text(raw: str, **kwargs) -> str:
    """One-shot text cleaning."""
    return TextCleaner(**kwargs).clean(raw)
