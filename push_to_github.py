#!/usr/bin/env python3
"""Push vault markdown to GitHub.

Primary path: ``git push`` from ``HERMES_VAULT_ROOT`` (SSH deploy key).
Fallback: dulwich + paramiko wire protocol (legacy @-mangle bypass).

Only the vault collection repo is pushed. Stage ``podcast-kb/vault/`` or
``immobilien-kb/vault/`` in the daily pipelines before calling this script —
never ``podcast-kb/content/``.

Usage:
    python3 push_to_github.py --scope podcast
    python3 push_to_github.py --scope immobilien

Environment:
    HERMES_PUSH_PREFER_GIT=1   Try ``git push`` first (default).
    HERMES_PUSH_PREFER_GIT=0   Try paramiko first, then ``git push``.
"""
from __future__ import annotations

import argparse
import io
import os
import subprocess
import sys
from pathlib import Path
from typing import Callable

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.lib.paths import (  # noqa: E402
    IMMO_VAULT_GIT_PATH,
    PODCAST_VAULT_GIT_PATH,
    github_vault_repo,
    github_vault_repo_dir,
)

SCOPES = {
    "podcast": PODCAST_VAULT_GIT_PATH,
    "immobilien": IMMO_VAULT_GIT_PATH,
}


def _ssh_key_path() -> str:
    """SSH private key for vault-repo pushes.

    Priority:
      1. ``HERMES_VAULT_GITHUB_KEY_PATH`` (explicit vault key)
      2. ``HERMES_PIPELINE_GITHUB_KEY_PATH`` (shared override)
      3. When ``HERMES_VAULT_ROOT`` is set: user-level ``github_deploy_key``
         (``ado_reddit_deploy`` is scoped to the code repo deploy key only)
      4. Mono-repo fallback: ``ado_reddit_deploy``
    """
    explicit = os.environ.get("HERMES_VAULT_GITHUB_KEY_PATH")
    if explicit:
        return explicit
    pipeline_key = os.environ.get("HERMES_PIPELINE_GITHUB_KEY_PATH")
    if pipeline_key:
        return pipeline_key
    if os.environ.get("HERMES_VAULT_ROOT"):
        return "/root/.ssh/github_deploy_key"
    return "/root/.ssh/ado_reddit_deploy"


def push_via_git(
    repo_dir: str,
    branch: str = "main",
    remote: str = "origin",
    timeout: int = 120,
) -> tuple[bool, str]:
    """Push using the system git CLI and SSH deploy key."""
    env = os.environ.copy()
    key_path = _ssh_key_path()
    if key_path and Path(key_path).exists():
        env["GIT_SSH_COMMAND"] = (
            f"ssh -i {key_path} -o IdentitiesOnly=yes "
            f"-o StrictHostKeyChecking=accept-new"
        )
    proc = subprocess.run(
        ["git", "-C", repo_dir, "push", remote, branch],
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    output = "".join(part for part in (proc.stdout, proc.stderr) if part)
    return proc.returncode == 0, output.strip()


def recv_exact(sock, n, timeout=30):
    sock.settimeout(timeout)
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError(f"got {len(buf)}/{n} bytes before EOF")
        buf += chunk
    return buf


def read_pkt_line(chan):
    raw = recv_exact(chan, 4)
    if len(raw) < 4:
        return b""
    ln = int(raw, 16)
    if ln == 0:
        return b""
    if ln == 1:
        return recv_exact(chan, 1)
    return recv_exact(chan, ln - 4)


def build_pack(repo_dir):
    sys.path.insert(0, "/root/.hermes/hermes-agent/venv/lib/python3.11/site-packages")
    from dulwich.pack import write_pack_objects
    from dulwich.repo import Repo

    repo = Repo(repo_dir)
    head_sha = repo.refs[b"refs/heads/main"]
    seen = set()
    objs = []

    def queue(sha):
        if sha in seen:
            return
        seen.add(sha)
        try:
            obj = repo[sha]
        except KeyError:
            return
        objs.append(obj)
        if obj.type_name == b"commit":
            for child in list(obj.parents) + [obj.tree]:
                queue(child)
        elif obj.type_name == b"tree":
            for _m, _n, child in obj.items():
                queue(child)

    queue(head_sha)
    buf = io.BytesIO()
    try:
        write_pack_objects(buf, objs, object_format=repo.object_format)
    except TypeError:
        buf = io.BytesIO()
        write_pack_objects(buf, objs)
    return buf.getvalue(), head_sha


def push_via_paramiko(
    repo_dir: str,
    owner: str,
    repo: str,
    branch: str = "main",
) -> tuple[bool, str]:
    """Legacy dulwich + paramiko push (bypasses @-mangle on some hosts)."""
    sys.path.insert(0, "/root/.hermes/hermes-agent/venv/lib/python3.11/site-packages")
    import paramiko

    key_path = _ssh_key_path()
    pack, head_sha = build_pack(repo_dir)
    lines = [f"[pack] {len(pack)} bytes, head={head_sha.decode()[:12]}"]

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.Ed25519Key.from_private_key_file(key_path)
    client.connect(
        "github.com", 22, "git", pkey=pkey,
        look_for_keys=False, allow_agent=False,
    )
    lines.append("[ssh] connected to github.com:22 as git")

    chan = client.get_transport().open_session()
    chan.settimeout(60)
    cmd = f"git-receive-pack '{owner}/{repo}'"
    chan.exec_command(cmd)
    lines.append(f"[ssh] exec: {cmd}")

    all_lines = []
    init = read_pkt_line(chan)
    all_lines.append(init)
    while True:
        line = read_pkt_line(chan)
        if line == b"":
            break
        all_lines.append(line)

    remote_refs = {}
    for i, line in enumerate(all_lines):
        if len(line) < 40:
            continue
        sha = line[:40].decode()
        rest = line[40:]
        if i == 0 and b"\x00" in rest:
            name_part, _, _ = rest.partition(b"\x00")
            name = name_part.strip().decode()
        else:
            name = rest.rstrip(b"\n").strip().decode()
        if name:
            remote_refs[name] = sha
    lines.append(f"[refs] {remote_refs}")

    old_sha = remote_refs.get(f"refs/heads/{branch}", "0" * 40)
    new_sha = head_sha.decode()
    newline = f"{old_sha} {new_sha} refs/heads/{branch}\x00 report-status\n".encode()
    pkt_len = len(newline) + 4
    chan.sendall(("%04x" % pkt_len).encode() + newline)
    chan.sendall(b"0000")
    lines.append(f"[push] {old_sha[:7]} -> {new_sha[:7]} on refs/heads/{branch}")

    chan.sendall(pack)
    chan.sendall(b"0000")
    lines.append("[push] pack streamed")

    full = b""
    try:
        while True:
            data = chan.recv(65536)
            if not data:
                break
            full += data
    except Exception:
        pass

    chan.close()
    client.close()

    response = full.decode(errors="replace")
    lines.append(f"[result]\n{response}")
    ok = "unpack ok" in response and f"ok refs/heads/{branch}" in response
    return ok, "\n".join(lines)


def push_vault(
    repo_dir: str,
    owner: str,
    repo: str,
    branch: str = "main",
    *,
    prefer_git: bool | None = None,
) -> tuple[bool, str]:
    """Try git CLI and/or paramiko until one succeeds."""
    if prefer_git is None:
        prefer_git = os.environ.get("HERMES_PUSH_PREFER_GIT", "1") != "0"

    methods: list[tuple[str, Callable[[], tuple[bool, str]]]] = []
    if prefer_git:
        methods.append(("git", lambda: push_via_git(repo_dir, branch)))
        methods.append(("paramiko", lambda: push_via_paramiko(repo_dir, owner, repo, branch)))
    else:
        methods.append(("paramiko", lambda: push_via_paramiko(repo_dir, owner, repo, branch)))
        methods.append(("git", lambda: push_via_git(repo_dir, branch)))

    log_parts: list[str] = []
    for name, fn in methods:
        log_parts.append(f"[push] trying {name}...")
        try:
            ok, output = fn()
        except Exception as exc:  # noqa: BLE001 — collect and try fallback
            ok, output = False, f"{type(exc).__name__}: {exc}"
        log_parts.append(output)
        if ok:
            log_parts.append(f"[push] {name} succeeded")
            return True, "\n".join(log_parts)
        log_parts.append(f"[push] {name} failed")

    return False, "\n".join(log_parts)


def main():
    parser = argparse.ArgumentParser(description="Push vault repo to GitHub")
    parser.add_argument(
        "--scope",
        choices=sorted(SCOPES),
        required=True,
        help="Vault subtree that was staged (podcast-kb/vault or immobilien-kb/vault)",
    )
    args = parser.parse_args()

    repo_dir = str(github_vault_repo_dir())
    owner = os.environ.get("HERMES_VAULT_GITHUB_OWNER", "clarktao-dev")
    repo = github_vault_repo()
    branch = os.environ.get("HERMES_VAULT_GITHUB_BRANCH", "main")
    vault_subpath = SCOPES[args.scope]

    print(
        f"[config] repo_dir={repo_dir} remote={owner}/{repo} "
        f"scope={vault_subpath} branch={branch}"
    )

    ok, output = push_vault(repo_dir, owner, repo, branch)
    print(output)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
