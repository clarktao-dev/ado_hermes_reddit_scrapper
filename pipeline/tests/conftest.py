"""Shared pytest fixtures / path setup for pipeline tests.

Adds a minimal ``reddit_safe`` stub when the real package (normally at
``/root/reddit-safe/src``) is absent — Cloud Agent / CI sandboxes.
"""
from __future__ import annotations

import sys
from pathlib import Path

_STUBS = Path(__file__).resolve().parent / "stubs"
if _STUBS.is_dir() and str(_STUBS) not in sys.path:
    # Prefer real reddit_safe if already importable; otherwise use stub.
    try:
        import reddit_safe  # noqa: F401
    except ImportError:
        sys.path.insert(0, str(_STUBS))
