#!/usr/bin/env python3
"""Push vault markdown to GitHub via paramiko (bypasses @-mangle).

Only the vault collection repo is pushed (``HERMES_VAULT_ROOT``). Stage
``podcast-kb/vault/`` or ``immobilien-kb/vault/`` in the daily pipelines
before calling this script — never ``podcast-kb/content/``.

Usage:
    python3 push_to_github.py --scope podcast
    python3 push_to_github.py --scope immobilien
"""
from __future__ import annotations

import argparse
import io
import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

sys.path.insert(0, "/root/.hermes/hermes-agent/venv/lib/python3.11/site-packages")

import paramiko  # noqa: E402
from dulwich.pack import write_pack_objects  # noqa: E402
from dulwich.repo import Repo  # noqa: E402

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
    key_path = os.environ.get(
        "HERMES_VAULT_GITHUB_KEY_PATH",
        os.environ.get("HERMES_PIPELINE_GITHUB_KEY_PATH", "/root/.ssh/ado_reddit_deploy"),
    )
    username = "git"
    branch = os.environ.get("HERMES_VAULT_GITHUB_BRANCH", "main")
    vault_subpath = SCOPES[args.scope]

    print(f"[config] repo_dir={repo_dir} remote={owner}/{repo} scope={vault_subpath}")

    pack, head_sha = build_pack(repo_dir)
    print(f"[pack] {len(pack)} bytes, head={head_sha.decode()[:12]}")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.Ed25519Key.from_private_key_file(key_path)
    client.connect("github.com", 22, username, pkey=pkey,
                   look_for_keys=False, allow_agent=False)
    print(f"[ssh] connected to github.com:22 as {username}")

    chan = client.get_transport().open_session()
    chan.settimeout(60)
    cmd = f"git-receive-pack '{owner}/{repo}'"
    chan.exec_command(cmd)
    print(f"[ssh] exec: {cmd}")

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
    print(f"[refs] {remote_refs}")

    old_sha = remote_refs.get(f"refs/heads/{branch}", "0" * 40)
    new_sha = head_sha.decode()
    newline = f"{old_sha} {new_sha} refs/heads/{branch}\x00 report-status\n".encode()
    pkt_len = len(newline) + 4
    chan.sendall(("%04x" % pkt_len).encode() + newline)
    chan.sendall(b"0000")
    print(f"[push] {old_sha[:7]} -> {new_sha[:7]} on refs/heads/{branch}")

    chan.sendall(pack)
    chan.sendall(b"0000")
    print("[push] pack streamed")

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
    print(f"[result]\n{response}")

    ok = "unpack ok" in response and f"ok refs/heads/{branch}" in response
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
