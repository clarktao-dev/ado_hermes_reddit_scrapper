"""Central path helpers for the Hermes pipeline ↔ vault split.

Environment variables
---------------------
PIPELINE_ROOT
    Code repository root (``pipeline/``, ``push_to_github.py``,
    ``immobilien-kb/tools/``, ``podcast-kb/state.json``, etc.).
    Defaults to the parent of ``pipeline/``.

HERMES_VAULT_ROOT
    Git root of the vault collection repo (``immobilien-kb/vault/``,
    ``podcast-kb/vault/``). Defaults to ``PIPELINE_ROOT`` so the
    mono-repo layout keeps working until Phase 3.

Notes
-----
- ``podcast-kb/content/`` lives under ``PIPELINE_ROOT`` and is **not**
  part of the vault; never stage or push it.
- YouTube vault has two historical directory schemas under
  ``immobilien-kb/vault/YouTube/``; this module does not normalize them.
"""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

_LIB_DIR = Path(__file__).resolve().parent
_PIPELINE_DIR = _LIB_DIR.parent


@lru_cache(maxsize=1)
def get_pipeline_root() -> Path:
    env = os.environ.get("PIPELINE_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    return _PIPELINE_DIR.parent


@lru_cache(maxsize=1)
def get_vault_root() -> Path:
    env = os.environ.get("HERMES_VAULT_ROOT")
    if env:
        return Path(env).expanduser().resolve()
    return get_pipeline_root()


def reset_path_cache() -> None:
    """Clear cached roots (for tests that mutate env mid-process)."""
    get_pipeline_root.cache_clear()
    get_vault_root.cache_clear()


def _refresh_module_constants() -> None:
    global PIPELINE_ROOT, VAULT_ROOT, IMMO_VAULT, PODCAST_VAULT
    global PODCAST_DAILY_VAULT, LONGFORM_ROOT, YOUTUBE_VAULT
    global DISCORD_SENDER, PUSH_TO_GITHUB_SCRIPT, CHANNELS_CONFIG
    global DESTATIS_SOURCES_CONFIG, PODCAST_STATE_PATH
    global IMMO_VAULT_GIT_PATH, PODCAST_VAULT_GIT_PATH

    PIPELINE_ROOT = get_pipeline_root()
    VAULT_ROOT = get_vault_root()
    IMMO_VAULT = VAULT_ROOT / "immobilien-kb" / "vault"
    PODCAST_VAULT = VAULT_ROOT / "podcast-kb" / "vault"
    PODCAST_DAILY_VAULT = PODCAST_VAULT / "Daily"
    LONGFORM_ROOT = IMMO_VAULT / "Longform"
    YOUTUBE_VAULT = IMMO_VAULT / "YouTube"
    DISCORD_SENDER = PIPELINE_ROOT / "immobilien-kb" / "tools" / "discord_sender.py"
    PUSH_TO_GITHUB_SCRIPT = PIPELINE_ROOT / "push_to_github.py"
    CHANNELS_CONFIG = PIPELINE_ROOT / "pipeline" / "config" / "channels.json"
    DESTATIS_SOURCES_CONFIG = PIPELINE_ROOT / "pipeline" / "config" / "destatis_sources.json"
    PODCAST_STATE_PATH = PIPELINE_ROOT / "podcast-kb" / "state.json"
    IMMO_VAULT_GIT_PATH = "immobilien-kb/vault"
    PODCAST_VAULT_GIT_PATH = "podcast-kb/vault"


_refresh_module_constants()


def vault_relative(path: Path | str) -> str:
    """Return a path relative to ``VAULT_ROOT`` (e.g. ``immobilien-kb/vault/...``)."""
    return str(Path(path).resolve().relative_to(VAULT_ROOT))


def github_vault_repo() -> str:
    """GitHub repo name for vault pushes."""
    if os.environ.get("HERMES_VAULT_ROOT"):
        return os.environ.get("HERMES_VAULT_GITHUB_REPO", "hermes_vault_collection")
    return os.environ.get("HERMES_PIPELINE_GITHUB_REPO", "ado_hermes_reddit_scrapper")


def github_vault_repo_dir() -> Path:
    """Directory whose ``.git`` is pushed by ``push_to_github.py``."""
    return VAULT_ROOT
