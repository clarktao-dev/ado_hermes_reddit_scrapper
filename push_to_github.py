#!/usr/bin/env python3
"""Push immobilien-kb to GitHub via paramiko (bypasses @-mangle)."""
import sys
sys.path.insert(0, "/root/.hermes/hermes-agent/venv/lib/python3.11/site-packages")

import io
import paramiko
from dulwich.pack import write_pack_objects
from dulwich.repo import Repo


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
    REPO_DIR = "/root/projects/ado_hermes_reddit_scrapper"
    OWNER = "clarktao-dev"
    REPO = "ado_hermes_reddit_scrapper"
    KEY_PATH = "/root/.ssh/ado_reddit_deploy"
    USERNAME = "git"  # separate arg, no @ string
    BRANCH = "main"

    pack, head_sha = build_pack(REPO_DIR)
    print(f"[pack] {len(pack)} bytes, head={head_sha.decode()[:12]}")

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = paramiko.Ed25519Key.from_private_key_file(KEY_PATH)
    client.connect("github.com", 22, USERNAME, pkey=pkey,
                   look_for_keys=False, allow_agent=False)
    print(f"[ssh] connected to github.com:22 as {USERNAME}")

    chan = client.get_transport().open_session()
    chan.settimeout(60)
    cmd = f"git-receive-pack '{OWNER}/{REPO}'"
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

    old_sha = remote_refs.get(f"refs/heads/{BRANCH}", "0" * 40)
    new_sha = head_sha.decode()
    newline = f"{old_sha} {new_sha} refs/heads/{BRANCH}\x00 report-status\n".encode()
    pkt_len = len(newline) + 4
    chan.sendall(("%04x" % pkt_len).encode() + newline)
    chan.sendall(b"0000")
    print(f"[push] {old_sha[:7]} -> {new_sha[:7]} on refs/heads/{BRANCH}")

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

    ok = "unpack ok" in response and f"ok refs/heads/{BRANCH}" in response
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
