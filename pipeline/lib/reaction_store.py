"""Firestore-backed store for Discord ReactionPicks (Plan 4).

Replaces the inline Airtable HTTP helpers in ``discord_picks.py`` and
``weekly_recap.py``. Documents use ``reaction_id`` as the document id
for idempotent dedup.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pipeline.lib.firestore_client import (
    FirestoreConfigError,
    collection_name_for_table,
    get_firestore_client,
)

logger = logging.getLogger(__name__)

DEFAULT_TABLE = "ReactionPicks"


class ReactionStoreError(RuntimeError):
    pass


class ReactionStore:
    """Firestore store for user ✅ reactions on Discord digest messages."""

    def __init__(
        self,
        *,
        collection_name: Optional[str] = None,
        _memory: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> None:
        self.collection_name = collection_name or collection_name_for_table(
            DEFAULT_TABLE
        )
        self._memory = _memory

    def _collection(self):
        if self._memory is not None:
            return None
        try:
            return get_firestore_client().collection(self.collection_name)
        except FirestoreConfigError as e:
            raise ReactionStoreError(str(e)) from e

    @staticmethod
    def _as_record(doc_id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Airtable-shaped record for callers that expect ``fields``."""
        return {"id": doc_id, "fields": dict(data)}

    def find_by_reaction_id(self, reaction_id: str) -> Optional[str]:
        """Return document id if ``reaction_id`` already exists."""
        if self._memory is not None:
            for doc_id, fields in self._memory.items():
                if fields.get("reaction_id") == reaction_id:
                    return doc_id
            return None

        doc = self._collection().document(reaction_id).get()
        if doc.exists:
            return doc.id
        return None

    def create_reaction_pick(self, fields: Dict[str, Any]) -> str:
        """Create a reaction pick. ``reaction_id`` must be in ``fields``."""
        reaction_id = fields.get("reaction_id")
        if not reaction_id:
            raise ReactionStoreError("reaction_id is required")

        if self._memory is not None:
            self._memory[reaction_id] = dict(fields)
            return reaction_id

        ref = self._collection().document(reaction_id)
        ref.set(fields, merge=True)
        logger.info("reaction recorded: %s", reaction_id)
        return reaction_id

    def query_recent(self, days: int) -> List[Dict[str, Any]]:
        """Return Airtable-shaped records newer than ``days`` ago (newest first)."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        records: List[Dict[str, Any]] = []

        if self._memory is not None:
            for doc_id, fields in self._memory.items():
                raw = fields.get("reaction_date", "")
                if not raw:
                    continue
                try:
                    when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except ValueError:
                    continue
                if when >= cutoff:
                    records.append(self._as_record(doc_id, fields))
        else:
            # Small collection — stream + client-side date filter keeps indexes simple.
            for doc in self._collection().stream():
                data = doc.to_dict() or {}
                raw = data.get("reaction_date", "")
                if not raw:
                    continue
                try:
                    when = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                except ValueError:
                    continue
                if when >= cutoff:
                    records.append(self._as_record(doc.id, data))

        records.sort(
            key=lambda r: r.get("fields", {}).get("reaction_date", ""),
            reverse=True,
        )
        return records


def get_reaction_store() -> ReactionStore:
    return ReactionStore()
