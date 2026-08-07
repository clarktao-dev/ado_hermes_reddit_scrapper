"""Deduplicate news items by URL and fuzzy title match."""
from rapidfuzz import fuzz


def dedup_items(items, title_threshold=0.85):
    """Returns deduped list. Order preserved; later duplicates dropped.
    When duplicates found, the item with lowest priority value (most authoritative) wins.
    """
    seen_urls = set()
    out = []
    for it in items:
        url = it.get("url", "").strip()
        if url and url in seen_urls:
            continue
        title = it.get("title", "").strip()
        # Fuzzy match against existing titles
        is_dup = False
        for kept in out:
            t_ratio = fuzz.ratio(title.lower(), kept.get("title", "").lower()) / 100.0
            if t_ratio >= title_threshold:
                # Keep the lower priority number
                if it.get("priority", 99) < kept.get("priority", 99):
                    out.remove(kept)
                    if kept.get("url"):
                        seen_urls.discard(kept["url"])
                    out.append(it)
                    if url:
                        seen_urls.add(url)
                is_dup = True
                break
        if is_dup:
            continue
        if url:
            seen_urls.add(url)
        out.append(it)
    return out
