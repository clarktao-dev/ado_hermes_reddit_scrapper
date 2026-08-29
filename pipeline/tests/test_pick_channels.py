"""Unit tests for day-of-year round-robin channel selection."""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pipeline.youtube_daily import pick_channels


def _channels(n: int) -> list[dict]:
    return [{"id": f"ch{i}", "name": f"Channel {i}"} for i in range(n)]


def test_pick_channels_day_of_year_rotation():
    channels = _channels(19)
    doy = 241  # 2026-08-29 UTC
    with patch("pipeline.youtube_daily.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 8, 29, tzinfo=timezone.utc)
        picked = pick_channels(channels, n=4)
    start = doy % 19  # 241 % 19 = 13
    expected = [channels[(start + i) % 19] for i in range(4)]
    assert [c["id"] for c in picked] == [c["id"] for c in expected]


def test_pick_channels_same_day_is_idempotent():
    channels = _channels(9)
    with patch("pipeline.youtube_daily.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 8, 29, tzinfo=timezone.utc)
        first = pick_channels(channels, n=4)
        second = pick_channels(channels, n=4)
    assert [c["id"] for c in first] == [c["id"] for c in second]


def test_pick_channels_n_ge_len_returns_all():
    channels = _channels(5)
    with patch("pipeline.youtube_daily.datetime") as mock_dt:
        mock_dt.now.return_value = datetime(2026, 8, 29, tzinfo=timezone.utc)
        picked = pick_channels(channels, n=10)
    assert len(picked) == 5


if __name__ == "__main__":
    test_pick_channels_day_of_year_rotation()
    test_pick_channels_same_day_is_idempotent()
    test_pick_channels_n_ge_len_returns_all()
    print("pick_channels tests OK")