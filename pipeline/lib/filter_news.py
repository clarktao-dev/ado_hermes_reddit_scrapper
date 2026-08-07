"""Filter news items by include/exclude keywords."""
import re

from .config_loader import get_keywords


def _normalize(text):
    return (text or "").lower()


def matches_keywords(text, words):
    """Returns True if any word (case-insensitive) appears in text."""
    if not words:
        return True
    t = _normalize(text)
    return any(w.lower() in t for w in words)


def filter_items(items):
    """Apply include/exclude keywords. Returns filtered list."""
    include, exclude = get_keywords()
    out = []
    for it in items:
        text = (it.get("title", "") or "") + " " + (it.get("summary", "") or "")
        if not matches_keywords(text, include):
            continue
        if matches_keywords(text, exclude):
            continue
        out.append(it)
    return out


def detect_branch_targets(text, cfg):
    """Detect laws/institutions/programs mentioned in text. Returns dict."""
    text_l = (text or "").lower()
    found = {"laws": [], "institutions": [], "programs": []}
    for w in cfg.get("branch_laws", []):
        if w.lower() in text_l:
            found["laws"].append(w)
    for w in cfg.get("branch_institutions", []):
        if w.lower() in text_l:
            found["institutions"].append(w)
    for w in cfg.get("branch_programs", []):
        if w.lower() in text_l:
            found["programs"].append(w)
    return found
