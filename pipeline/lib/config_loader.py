"""Config loader for immobilien-pipeline.

Single entry point: load_config() returns a dict with all settings merged
from config/*.json files. Caches the result so repeat reads are cheap.
"""
import json
import os
from functools import lru_cache

_CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config")


@lru_cache(maxsize=1)
def load_config():
    """Load and merge all config/*.json files. Returns a dict."""
    cfg = {}
    for fname in os.listdir(_CONFIG_DIR):
        if not fname.endswith(".json"):
            continue
        # destatis_sources.json is read directly by destatis_daily.py and its
        # "sources" key would clobber sources.json via cfg.update below.
        # Skip it here so news_daily.py sees only the RSS news sources.
        if fname.startswith("destatis_"):
            continue
        with open(os.path.join(_CONFIG_DIR, fname), encoding="utf-8") as f:
            data = json.load(f)
        cfg.update(data)
    return cfg


def get_keywords():
    return load_config().get("include", []), load_config().get("exclude", [])


def get_sources():
    return [s for s in load_config().get("sources", []) if s.get("enabled", True)]


def get_channels():
    return [c for c in load_config().get("channels", []) if c.get("enabled", True)]
