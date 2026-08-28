"""Tests for push_to_github push strategy and git CLI helper."""
from __future__ import annotations

from unittest import mock

import pytest

import push_to_github as ptg


def test_push_via_git_success() -> None:
    proc = mock.Mock(returncode=0, stdout="ok\n", stderr="")
    with mock.patch.object(ptg, "_ssh_key_path", return_value=""):
        with mock.patch.object(ptg.subprocess, "run", return_value=proc) as run_mock:
            ok, output = ptg.push_via_git("/tmp/vault", branch="main")
    assert ok is True
    assert "ok" in output
    run_mock.assert_called_once()
    assert run_mock.call_args.args[0] == [
        "git", "-C", "/tmp/vault", "push", "origin", "main",
    ]


def test_push_vault_git_first_then_paramiko() -> None:
    with mock.patch.object(ptg, "push_via_git", return_value=(False, "git failed")) as git_mock:
        with mock.patch.object(
            ptg, "push_via_paramiko", return_value=(True, "paramiko ok"),
        ) as paramiko_mock:
            ok, output = ptg.push_vault(
                "/tmp/vault", "owner", "repo", prefer_git=True,
            )
    assert ok is True
    git_mock.assert_called_once()
    paramiko_mock.assert_called_once()
    assert "paramiko succeeded" in output


def test_push_vault_paramiko_first_then_git() -> None:
    with mock.patch.object(
        ptg, "push_via_paramiko", return_value=(False, "paramiko failed"),
    ) as paramiko_mock:
        with mock.patch.object(
            ptg, "push_via_git", return_value=(True, "git ok"),
        ) as git_mock:
            ok, output = ptg.push_vault(
                "/tmp/vault", "owner", "repo", prefer_git=False,
            )
    assert ok is True
    paramiko_mock.assert_called_once()
    git_mock.assert_called_once()
    assert "git succeeded" in output


def test_push_vault_returns_false_when_both_fail() -> None:
    with mock.patch.object(ptg, "push_via_git", return_value=(False, "git failed")):
        with mock.patch.object(
            ptg, "push_via_paramiko", return_value=(False, "paramiko failed"),
        ):
            ok, output = ptg.push_vault(
                "/tmp/vault", "owner", "repo", prefer_git=True,
            )
    assert ok is False
    assert "git failed" in output
    assert "paramiko failed" in output


def test_ssh_key_path_explicit_vault_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HERMES_VAULT_GITHUB_KEY_PATH", "/custom/vault-key")
    assert ptg._ssh_key_path() == "/custom/vault-key"


def test_ssh_key_path_user_key_when_vault_split(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HERMES_VAULT_GITHUB_KEY_PATH", raising=False)
    monkeypatch.delenv("HERMES_PIPELINE_GITHUB_KEY_PATH", raising=False)
    monkeypatch.setenv("HERMES_VAULT_ROOT", "/root/projects/hermes_vault_collection")
    assert ptg._ssh_key_path() == "/root/.ssh/github_deploy_key"


def test_ssh_key_path_mono_repo_deploy_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HERMES_VAULT_GITHUB_KEY_PATH", raising=False)
    monkeypatch.delenv("HERMES_PIPELINE_GITHUB_KEY_PATH", raising=False)
    monkeypatch.delenv("HERMES_VAULT_ROOT", raising=False)
    assert ptg._ssh_key_path() == "/root/.ssh/ado_reddit_deploy"
