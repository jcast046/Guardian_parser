"""Text cleaning functions for PDF extraction before LLM processing.

Provides utilities to normalize, clean, and preprocess extracted PDF text
for improved LLM extraction accuracy.
"""
from __future__ import annotations
import re
import unicodedata
from collections import Counter
from typing import List, Optional


LIGATURES = {
    "\uFB00": "ff", "\uFB01": "fi", "\uFB02": "fl", "\uFB03": "ffi", "\uFB04": "ffl",
    "\u2010": "-", "\u2011": "-", "\u2012": "-", "\u2013": "-", "\u2014": "-", "\u2212": "-",
}
LIG_RE = re.compile("|".join(map(re.escape, LIGATURES.keys())))


def _replace_ligatures(s: str) -> str:
    """Replace Unicode ligatures with ASCII equivalents.

    Args:
        s: Input string potentially containing ligatures.

    Returns:
        String with ligatures replaced by ASCII characters.
    """
    return LIG_RE.sub(lambda m: LIGATURES[m.group(0)], s)


def _dehyphenate(s: str) -> str:
    """Join words split across line breaks.

    Args:
        s: Input string with potential hyphenated line breaks.

    Returns:
        String with hyphenated line breaks joined.

    Example:
        'invest-\\nigation' becomes 'investigation'
    """
    return re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", s)


def _collapse_ws(s: str) -> str:
    """Collapse whitespace to single spaces and normalize line breaks.

    Args:
        s: Input string with variable whitespace.

    Returns:
        String with normalized whitespace.
    """
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def _strip_common_headers_footers(pages: List[str]) -> str:
    """Remove common headers and footers from page text.

    Removes lines appearing on more than 60% of pages at top or bottom.

    Args:
        pages: List of page text strings.

    Returns:
        Cleaned pages joined with double newlines.
    """
    if not pages:
        return ""
    
    tops, bottoms = [], []
    for p in pages:
        lines = [ln.strip() for ln in p.splitlines() if ln.strip()]
        if not lines:
            continue
        tops.append(lines[0][:120])
        bottoms.append(lines[-1][:120])
    
    if not tops or not bottoms:
        return "\n\n".join(pages)
    
    top_counts = Counter(tops)
    bottom_counts = Counter(bottoms)
    top_common = {t for t, c in top_counts.items() if c / len(pages) > 0.6}
    bottom_common = {b for b, c in bottom_counts.items() if c / len(pages) > 0.6}
    
    cleaned_pages = []
    for p in pages:
        lines = [ln for ln in p.splitlines()]
        if lines:
            if lines[0].strip()[:120] in top_common:
                lines = lines[1:]
            if lines and lines[-1].strip()[:120] in bottom_common:
                lines = lines[:-1]
        cleaned_pages.append("\n".join(lines))
    
    return "\n\n".join(cleaned_pages)


def clean_pdf_text(raw_text: str, pages_text: Optional[List[str]] = None) -> str:
    """Clean PDF text before LLM extraction.

    Applies normalization, ligature replacement, header/footer removal,
    dehyphenation, and whitespace collapse.

    Args:
        raw_text: Raw extracted text from PDF.
        pages_text: Optional list of per-page text strings for header/footer
            removal. If provided and multiple pages exist, removes common
            headers/footers.

    Returns:
        Cleaned text string ready for LLM processing.
    """
    # 1) Unicode normalize and fix ligatures/dashes
    s = unicodedata.normalize("NFKC", raw_text)
    s = _replace_ligatures(s)
    
    # 2) Optional header/footer stripping if per-page text is available
    if pages_text and len(pages_text) > 1:
        s = _strip_common_headers_footers(pages_text)
    elif pages_text and len(pages_text) == 1:
        s = pages_text[0]
    
    # 3) Dehyphenate across line breaks
    s = _dehyphenate(s)
    
    # 4) Remove page numbers like "Page 3 of 12" (common)
    s = re.sub(r"\bPage\s+\d+\s+(of|/)\s+\d+\b", "", s, flags=re.I)
    
    # 5) Collapse whitespace
    s = _collapse_ws(s)
    
    return s

