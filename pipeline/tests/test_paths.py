"""Tests for pipeline.lib.paths vault/pipeline root resolution."""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def _reload_paths(monkeypatch: pytest.MonkeyPatch, **env: str) -> object:
    for key in ("PIPELINE_ROOT", "HERMES_VAULT_ROOT"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    import pipeline.lib.paths as paths

    importlib.reload(paths)
    return paths


def test_defaults_to_mono_repo_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _reload_paths(monkeypatch)
    assert paths.VAULT_ROOT == paths.PIPELINE_ROOT
    assert paths.IMMO_VAULT == paths.VAULT_ROOT / "immobilien-kb" / "vault"
    assert paths.PODCAST_VAULT == paths.VAULT_ROOT / "podcast-kb" / "vault"


def test_hermes_vault_root_splits_from_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline = tmp_path / "code"
    vault = tmp_path / "vault-collection"
    pipeline.mkdir()
    vault.mkdir()
    paths = _reload_paths(
        monkeypatch,
        PIPELINE_ROOT=str(pipeline),
        HERMES_VAULT_ROOT=str(vault),
    )
    assert paths.PIPELINE_ROOT == pipeline.resolve()
    assert paths.VAULT_ROOT == vault.resolve()
    assert paths.IMMO_VAULT == (vault / "immobilien-kb" / "vault").resolve()
    assert paths.PODCAST_DAILY_VAULT == (
        vault / "podcast-kb" / "vault" / "Daily"
    ).resolve()
    assert paths.DISCORD_SENDER == (
        pipeline / "immobilien-kb" / "tools" / "discord_sender.py"
    ).resolve()
    assert paths.PODCAST_STATE_PATH == (pipeline / "podcast-kb" / "state.json").resolve()


def test_vault_relative(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    vault = tmp_path / "vault"
    vault.mkdir()
    paths = _reload_paths(monkeypatch, HERMES_VAULT_ROOT=str(vault))
    md = vault / "immobilien-kb" / "vault" / "Daily" / "2026-08-28" / "item.md"
    md.parent.mkdir(parents=True)
    md.write_text("x", encoding="utf-8")
    assert paths.vault_relative(md) == "immobilien-kb/vault/Daily/2026-08-28/item.md"


def test_github_vault_repo_switches_with_env(monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _reload_paths(monkeypatch)
    assert paths.github_vault_repo() == "ado_hermes_reddit_scrapper"
    monkeypatch.setenv("HERMES_VAULT_ROOT", "/tmp/vault")
    assert paths.github_vault_repo() == "hermes_vault_collection"
    monkeypatch.setenv("HERMES_VAULT_GITHUB_REPO", "custom_vault")
    assert paths.github_vault_repo() == "custom_vault"
