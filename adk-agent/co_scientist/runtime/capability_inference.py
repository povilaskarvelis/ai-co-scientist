"""
Lightweight capability keyword inference helpers.
"""
from __future__ import annotations

import re


_CAPABILITY_STOPWORDS: set[str] = {
    "about",
    "across",
    "after",
    "also",
    "analysis",
    "answer",
    "based",
    "before",
    "between",
    "build",
    "collect",
    "compare",
    "current",
    "data",
    "details",
    "evidence",
    "final",
    "find",
    "from",
    "high",
    "include",
    "into",
    "latest",
    "list",
    "many",
    "method",
    "more",
    "most",
    "need",
    "only",
    "output",
    "query",
    "report",
    "results",
    "review",
    "score",
    "search",
    "show",
    "step",
    "steps",
    "summary",
    "synthesize",
    "this",
    "tool",
    "tools",
    "using",
    "with",
}


def infer_capabilities_from_text(text: str) -> set[str]:
    value = re.sub(r"[-/]+", " ", str(text or "").lower())
    tokens = re.findall(r"\b[a-z][a-z0-9_]{2,}\b", value)
    found: set[str] = set()
    for token in tokens:
        if token in _CAPABILITY_STOPWORDS:
            continue
        if len(token) < 4:
            continue
        normalized = token.rstrip("s") if len(token) > 5 else token
        if normalized in _CAPABILITY_STOPWORDS:
            continue
        found.add(normalized)
        if len(found) >= 24:
            break
    return found
