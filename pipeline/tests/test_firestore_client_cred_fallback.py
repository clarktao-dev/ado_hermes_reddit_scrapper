"""Unit tests for pipeline.lib.firestore_client credential resolution.

Background
----------
The Firestore client supports four credential sources (first match wins):

1. ``GOOGLE_APPLICATION_CREDENTIALS`` env var (path to SA JSON).
2. ``FIRESTORE_CREDENTIALS_JSON`` env var (inline JSON string).
3. **Auto-discovered service-account JSON** in a well-known host path
   (new — covers cron jobs whose prompt forgot to ``source .env``).
4. Application Default Credentials (last resort, e.g. ``gcloud auth
   application-default login``).

The previous code stopped at #1/#2 and fell through to #4, which fails
silently in fresh cron shells with ``Your default credentials were not
found``. This test file pins the resolution order so a future refactor
can't regress the fallback without breaking CI.

All tests use ``monkeypatch`` + a temp-dir fixture so they run without
GCP credentials and without mutating real env state.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

import pytest

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))

from pipeline.lib import firestore_client  # noqa: E402


SAMPLE_SA: Dict[str, Any] = {
    "type": "service_account",
    "project_id": "test-project-from-sa-json",
    "private_key_id": "abc123",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...\n-----END PRIVATE KEY-----\n",
    "client_email": "test@test-project.iam.gserviceaccount.com",
    "client_id": "1234567890",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
}


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Auto-strip every cred-related env var so each test starts from scratch.

    Without this, the parent pytest process inherits host env (e.g.
    ``FIRESTORE_PROJECT_ID``) and tests that intend to exercise the
    "missing env" path can't actually do so.
    """
    for k in (
        "GOOGLE_APPLICATION_CREDENTIALS",
        "FIRESTORE_CREDENTIALS_JSON",
        "FIRESTORE_PROJECT_ID",
        "FIRESTORE_DATABASE_ID",
    ):
        monkeypatch.delenv(k, raising=False)
    firestore_client.clear_client_cache()


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip every cred-related env var so each test starts from scratch."""
    for k in (
        "GOOGLE_APPLICATION_CREDENTIALS",
        "FIRESTORE_CREDENTIALS_JSON",
        "FIRESTORE_PROJECT_ID",
        "FIRESTORE_DATABASE_ID",
    ):
        monkeypatch.delenv(k, raising=False)
    firestore_client.clear_client_cache()


# ---------------------------------------------------------------------------
# get_project_id — pure function, easy to test
# ---------------------------------------------------------------------------

class TestGetProjectId:
    def test_explicit_env_wins(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FIRESTORE_PROJECT_ID", "from-env")
        monkeypatch.setenv("FIRESTORE_CREDENTIALS_JSON", json.dumps(SAMPLE_SA))
        assert firestore_client.get_project_id() == "from-env"

    def test_inline_json_project(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FIRESTORE_CREDENTIALS_JSON", json.dumps(SAMPLE_SA))
        assert firestore_client.get_project_id() == "test-project-from-sa-json"

    def test_sa_file_project(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        sa_path = tmp_path / "sa.json"
        sa_path.write_text(json.dumps(SAMPLE_SA), encoding="utf-8")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(sa_path))
        assert firestore_client.get_project_id() == "test-project-from-sa-json"

    def test_missing_all_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # No env, no auto-discover file in tmp HOME → must raise
        monkeypatch.setenv("HOME", str(tmp_path := __import__("pathlib").Path("/tmp/empty-home-xyz")))
        with pytest.raises(firestore_client.FirestoreConfigError):
            firestore_client.get_project_id()


# ---------------------------------------------------------------------------
# get_firestore_client — picks the right credential source
# ---------------------------------------------------------------------------

class TestGetFirestoreClient:
    """Verify credential resolution order. Mocks google.cloud.firestore so
    tests run without a real GCP project."""

    def _import_mocks(self, monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
        """Install fake google.cloud modules and return the captures dict."""
        captures: Dict[str, Any] = {"client_kwargs": []}

        class FakeClient:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                captures["client_kwargs"].append(kwargs)

        fake_firestore = type(sys)("google.cloud.firestore")
        fake_firestore.Client = FakeClient  # type: ignore[attr-defined]

        fake_google_cloud = type(sys)("google.cloud")
        fake_google_cloud.firestore = fake_firestore  # type: ignore[attr-defined]

        fake_google_oauth2 = type(sys)("google.oauth2")
        fake_service_account = type(sys)("google.oauth2.service_account")

        class FakeCredentials:
            @staticmethod
            def from_service_account_info(info: Any) -> str:
                return f"inline-cred:{info['project_id']}"

            @staticmethod
            def from_service_account_file(path: str) -> str:
                return f"file-cred:{path}"

        fake_service_account.Credentials = FakeCredentials  # type: ignore[attr-defined]

        monkeypatch.setitem(sys.modules, "google", type(sys)("google"))
        monkeypatch.setitem(sys.modules, "google.cloud", fake_google_cloud)
        monkeypatch.setitem(sys.modules, "google.cloud.firestore", fake_firestore)
        monkeypatch.setitem(sys.modules, "google.oauth2", fake_google_oauth2)
        monkeypatch.setitem(
            sys.modules, "google.oauth2.service_account", fake_service_account
        )
        return captures

    def test_inline_json_creds_used(self, monkeypatch: pytest.MonkeyPatch) -> None:
        firestore_client.clear_client_cache()
        captures = self._import_mocks(monkeypatch)
        monkeypatch.setenv("FIRESTORE_CREDENTIALS_JSON", json.dumps(SAMPLE_SA))
        firestore_client.get_firestore_client()
        kwargs = captures["client_kwargs"][0]
        assert "credentials" in kwargs
        assert kwargs["credentials"] == "inline-cred:test-project-from-sa-json"
        assert kwargs["project"] == "test-project-from-sa-json"

    def test_explicit_gac_env_used(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        firestore_client.clear_client_cache()
        captures = self._import_mocks(monkeypatch)
        sa_path = tmp_path / "sa.json"
        sa_path.write_text(json.dumps(SAMPLE_SA), encoding="utf-8")
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(sa_path))
        firestore_client.get_firestore_client()
        kwargs = captures["client_kwargs"][0]
        assert kwargs["credentials"] == f"file-cred:{sa_path}"

    def test_auto_discover_home_hermes_sa(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """NEW: when env vars are missing, look in $HOME/.hermes/firestore-sa.json.

        This is the fix for cron jobs whose prompt forgot to source .env —
        the host has the SA key at /root/.hermes/firestore-sa.json.
        """
        firestore_client.clear_client_cache()
        captures = self._import_mocks(monkeypatch)
        # Point HOME at a temp dir; place the SA inside $HOME/.hermes/
        monkeypatch.setenv("HOME", str(tmp_path))
        hermes_dir = tmp_path / ".hermes"
        hermes_dir.mkdir()
        sa_path = hermes_dir / "firestore-sa.json"
        sa_path.write_text(json.dumps(SAMPLE_SA), encoding="utf-8")
        # No FIRESTORE_PROJECT_ID, no GAC, no inline JSON
        firestore_client.get_firestore_client()
        kwargs = captures["client_kwargs"][0]
        assert kwargs["credentials"] == f"file-cred:{sa_path}"
        assert kwargs["project"] == "test-project-from-sa-json"

    def test_explicit_env_wins_over_auto_discover(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """If GAC env is set to a different file, that one wins — don't silently
        fall back to the auto-discovered one."""
        firestore_client.clear_client_cache()
        captures = self._import_mocks(monkeypatch)
        explicit_sa = tmp_path / "explicit.json"
        explicit_sa.write_text(
            json.dumps({**SAMPLE_SA, "project_id": "explicit-project"}),
            encoding="utf-8",
        )
        hermes_dir = tmp_path / ".hermes"
        hermes_dir.mkdir()
        (hermes_dir / "firestore-sa.json").write_text(
            json.dumps({**SAMPLE_SA, "project_id": "auto-project"}),
            encoding="utf-8",
        )
        monkeypatch.setenv("HOME", str(tmp_path))
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(explicit_sa))
        firestore_client.get_firestore_client()
        kwargs = captures["client_kwargs"][0]
        assert kwargs["project"] == "explicit-project"
