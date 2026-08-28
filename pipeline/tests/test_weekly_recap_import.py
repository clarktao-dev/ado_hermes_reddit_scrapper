"""weekly_recap.py must import regardless of cwd (cron may run from /tmp)."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SCRIPT = REPO_ROOT / "pipeline" / "scripts" / "weekly_recap.py"


def test_weekly_recap_imports_from_arbitrary_cwd() -> None:
    """Regression: sys.path must point at repo root, not pipeline/."""
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd="/tmp",
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert proc.returncode == 0, proc.stderr
    assert "usage" in proc.stdout.lower()
