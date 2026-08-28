"""Tests for pipeline.lib.reaction_store."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

THIS_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(THIS_DIR.parent.parent))

from pipeline.lib.reaction_store import ReactionStore


@pytest.fixture
def reaction_store() -> ReactionStore:
    return ReactionStore(_memory={})


def test_create_and_find(reaction_store: ReactionStore) -> None:
    fields = {
        "reaction_id": "u1-ch1-msg1-✅",
        "reaction_date": "2026-08-17T10:00:00.000Z",
        "title": "Test",
        "message_kind": "news",
    }
    doc_id = reaction_store.create_reaction_pick(fields)
    assert doc_id == "u1-ch1-msg1-✅"
    assert reaction_store.find_by_reaction_id("u1-ch1-msg1-✅") == doc_id
    assert reaction_store.find_by_reaction_id("missing") is None


def test_query_recent(reaction_store: ReactionStore) -> None:
    reaction_store.create_reaction_pick({
        "reaction_id": "recent-1",
        "reaction_date": "2026-08-27T10:00:00.000Z",
        "title": "Recent",
        "message_kind": "podcast",
    })
    records = reaction_store.query_recent(days=30)
    assert len(records) == 1
    assert records[0]["fields"]["title"] == "Recent"
