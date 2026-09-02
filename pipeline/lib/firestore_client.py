"""Shared Firestore client factory for pipeline stores.

Authentication (first match wins):
  1. ``GOOGLE_APPLICATION_CREDENTIALS`` — path to a service-account JSON key.
  2. ``FIRESTORE_CREDENTIALS_JSON`` — inline JSON string (useful in CI).
  3. **Auto-discovered service-account JSON** at one of the well-known host
     paths (covers cron jobs whose prompt forgot to ``source .env``).
  4. Application Default Credentials (``gcloud auth application-default login``).

Auto-discovered paths (checked in order, first hit wins):
  - ``$HOME/.hermes/firestore-sa.json``
  - ``$HOME/.config/gcloud/application_default_credentials.json``

Environment:
  - ``FIRESTORE_PROJECT_ID`` — GCP project id (required unless embedded in creds).
  - ``FIRESTORE_DATABASE_ID`` — database id, default ``(default)``.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from typing import Any, Optional

logger = None  # lazy — avoid import cycles at module load


def _log():
    global logger
    if logger is None:
        import logging

        logger = logging.getLogger(__name__)
    return logger


class FirestoreConfigError(RuntimeError):
    """Missing or invalid Firestore configuration."""


def get_project_id() -> str:
    """Resolve the GCP project id from env or credentials."""
    explicit = os.environ.get("FIRESTORE_PROJECT_ID", "").strip()
    if explicit:
        return explicit

    creds_json = os.environ.get("FIRESTORE_CREDENTIALS_JSON", "").strip()
    if creds_json:
        try:
            data = json.loads(creds_json)
            project = data.get("project_id", "")
            if project:
                return project
        except json.JSONDecodeError as e:
            raise FirestoreConfigError(
                "FIRESTORE_CREDENTIALS_JSON is not valid JSON"
            ) from e

    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if creds_path and os.path.isfile(creds_path):
        try:
            with open(creds_path, encoding="utf-8") as fh:
                data = json.load(fh)
            project = data.get("project_id", "")
            if project:
                return project
        except (OSError, json.JSONDecodeError):
            pass

    # Last resort for project_id inference: auto-discovered SA on host.
    discovered = _discover_sa_path()
    if discovered:
        try:
            with open(discovered, encoding="utf-8") as fh:
                data = json.load(fh)
            project = data.get("project_id", "")
            if project:
                return project
        except (OSError, json.JSONDecodeError):
            pass

    raise FirestoreConfigError(
        "FIRESTORE_PROJECT_ID not set and could not infer project_id from "
        "credentials. Set FIRESTORE_PROJECT_ID or provide a service-account "
        "JSON via GOOGLE_APPLICATION_CREDENTIALS / FIRESTORE_CREDENTIALS_JSON, "
        "or place a service-account JSON at $HOME/.hermes/firestore-sa.json."
    )


def get_database_id() -> str:
    return os.environ.get("FIRESTORE_DATABASE_ID", "(default)").strip() or "(default)"


def collection_name_for_table(table_name: str) -> str:
    """Map legacy Airtable table names to Firestore collection ids."""
    mapping = {
        "ProcessedContent": os.environ.get(
            "FIRESTORE_PROCESSED_COLLECTION", "processed"
        ),
        "ReactionPicks": os.environ.get(
            "FIRESTORE_REACTIONS_COLLECTION", "reactions"
        ),
        "DailyDigestPicks": os.environ.get(
            "FIRESTORE_DIGEST_PICKS_COLLECTION", "digest_picks"
        ),
    }
    return mapping.get(table_name, table_name)


def _candidate_sa_paths() -> list[str]:
    """Return well-known host paths where a Firestore SA key may live.

    Order matters — first hit wins. Kept conservative: only the hermes
    host config and the standard gcloud ADC location.
    """
    paths: list[str] = []
    home = os.path.expanduser("~")
    if home and home != "~":
        paths.append(os.path.join(home, ".hermes", "firestore-sa.json"))
        paths.append(os.path.join(home, ".config", "gcloud", "application_default_credentials.json"))
    return paths


def _discover_sa_path() -> Optional[str]:
    """First existing path from :func:`_candidate_sa_paths`, or ``None``."""
    for p in _candidate_sa_paths():
        if os.path.isfile(p):
            return p
    return None


@lru_cache(maxsize=1)
def get_firestore_client() -> Any:
    """Return a cached :class:`google.cloud.firestore.Client`."""
    try:
        from google.cloud import firestore
        from google.oauth2 import service_account
    except ImportError as e:
        raise FirestoreConfigError(
            "google-cloud-firestore is not installed. "
            "Run: pip install google-cloud-firestore"
        ) from e

    project_id = get_project_id()
    database_id = get_database_id()

    creds_json = os.environ.get("FIRESTORE_CREDENTIALS_JSON", "").strip()
    if creds_json:
        info = json.loads(creds_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        client = firestore.Client(
            project=project_id,
            credentials=credentials,
            database=database_id,
        )
        _log().info(
            "Firestore client ready: project=%s database=%s (inline creds)",
            project_id,
            database_id,
        )
        return client

    # 1) Explicit GOOGLE_APPLICATION_CREDENTIALS
    # 2) Auto-discovered SA on host (covers cron jobs that forgot to source .env)
    gac_explicit = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    sa_path = gac_explicit if gac_explicit else _discover_sa_path()
    if sa_path:
        credentials = service_account.Credentials.from_service_account_file(sa_path)
        client = firestore.Client(
            project=project_id,
            credentials=credentials,
            database=database_id,
        )
        _log().info(
            "Firestore client ready: project=%s database=%s (sa=%s)",
            project_id,
            database_id,
            sa_path,
        )
        return client

    # Last resort: rely on ambient ADC (gcloud auth application-default login).
    client = firestore.Client(project=project_id, database=database_id)
    _log().info(
        "Firestore client ready: project=%s database=%s (ambient ADC)",
        project_id,
        database_id,
    )
    return client


def clear_client_cache() -> None:
    """Reset the cached client (tests only)."""
    get_firestore_client.cache_clear()
