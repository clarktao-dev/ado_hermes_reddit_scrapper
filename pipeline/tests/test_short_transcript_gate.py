"""Plan 11 — short-transcript gate (incident 2026-09-04).

When Podigee (or kome.ai) returns a short non-premium preview transcript,
the short-mode LLM emits placeholder stubs that pollute the vault and lock
the ProcessedStore ledger. These tests assert we divert to
``_pending_review/ShortTranscript/`` instead — no LLM, no mark_processed,
Discord warning only when cases exist.
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

THIS_DIR = Path(__file__).resolve().parent
PIPELINE_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(PIPELINE_ROOT))


def _meta(video_id: str = "6b46483eec0c20c1448b0b71daf2a8c7",
          title: str = "Die Zeiten haben sich geändert",
          channel_id: str = "limmo",
          channel_name: str = "L'Immo") -> object:
    from pipeline.lib.youtube_fetch import VideoMeta
    return VideoMeta(
        id=video_id,
        title=title,
        duration_sec=1800,
        epoch=int(datetime(2026, 8, 10, tzinfo=timezone.utc).timestamp()),
        url="https://example.com/audio.mp3",
        channel_id=channel_id,
        channel_name=channel_name,
    )


def _tr(text: str, *, is_premium: bool = False, video=None):
    from pipeline.lib.youtube_fetch import TranscriptResult
    v = video or _meta()
    return TranscriptResult(
        video=v, language="de", text=text,
        n_chars=len(text), is_premium=is_premium,
    )


def _args(**kwargs):
    base = dict(
        dry_run=True, mode="short", channels="limmo",
        n_channels=1, skip_store=True, pipeline_run_id="test-run",
        video_id="", force=False,
    )
    base.update(kwargs)
    return SimpleNamespace(**base)


def _ch_limmo() -> dict:
    return {
        "id": "limmo",
        "name": "L'Immo",
        "source_type": "podcast",
        "url": "https://haufe-immobilienpodcast.podigee.io/feed/mp3",
        "enabled": True,
    }


class TestIsShortNonPremiumTranscript:
    def test_299_chars_premium_false_is_short(self) -> None:
        from pipeline import youtube_daily as yd
        assert yd._is_short_non_premium_transcript(_tr("x" * 299)) is True

    def test_302_chars_premium_false_still_short_at_500_threshold(self) -> None:
        """Incident observed 302→digest, but Plan 11 default threshold is 500."""
        from pipeline import youtube_daily as yd
        assert yd._is_short_non_premium_transcript(_tr("x" * 302)) is True

    def test_500_chars_premium_false_passes(self) -> None:
        from pipeline import youtube_daily as yd
        assert yd._is_short_non_premium_transcript(_tr("x" * 500)) is False

    def test_499_chars_premium_false_is_short(self) -> None:
        from pipeline import youtube_daily as yd
        assert yd._is_short_non_premium_transcript(_tr("x" * 499)) is True

    def test_299_chars_premium_true_bypasses_gate(self) -> None:
        from pipeline import youtube_daily as yd
        assert yd._is_short_non_premium_transcript(
            _tr("x" * 299, is_premium=True),
        ) is False

    def test_empty_chars_premium_false_is_short(self) -> None:
        from pipeline import youtube_daily as yd
        assert yd._is_short_non_premium_transcript(_tr("")) is True


class TestProcessOneChannelShortGate:
    def test_299_premium_false_pending_no_digest_no_llm(self) -> None:
        from pipeline import youtube_daily as yd

        ch = _ch_limmo()
        meta = _meta()
        pending: list = []
        store = MagicMock()
        store.is_processed.return_value = False
        state = MagicMock()
        args = _args()

        with patch.object(yd, "pick_video_for_channel", return_value=[meta]), \
             patch.object(
                 yd, "_fetch_transcript_for_channel",
                 return_value=_tr("a" * 299, video=meta),
             ), \
             patch.object(yd, "_translate_only") as translate, \
             patch.object(yd, "_build_short_digest") as build, \
             patch.object(yd, "step_structure_short") as structure:
            result = yd._process_one_channel(
                ch, store=store, state=state, args=args,
                short_transcript_pending=pending,
            )

        assert result is None
        assert len(pending) == 1
        assert pending[0]["video_id"] == meta.id
        assert pending[0]["n_chars"] == 299
        assert pending[0]["is_premium"] is False
        translate.assert_not_called()
        build.assert_not_called()
        structure.assert_not_called()
        store.mark_processed.assert_not_called()

    def test_long_enough_transcript_runs_llm(self) -> None:
        from pipeline import youtube_daily as yd
        from pipeline.lib.youtube_translate import VideoDigest

        ch = _ch_limmo()
        meta = _meta()
        pending: list = []
        store = MagicMock()
        store.is_processed.return_value = False
        state = MagicMock()
        args = _args()
        digest = VideoDigest(
            video_id=meta.id, title=meta.title, channel_name=meta.channel_name,
            url=meta.url, published_epoch=meta.epoch, duration_sec=meta.duration_sec,
            source_language="de", n_chars=600,
            summary_zh="摘要", analyst_zh="- a", producer_zh="觀點",
            vocab_zh="", map_calls=0, reduce_calls=1, elapsed_sec=0.1,
        )

        with patch.object(yd, "pick_video_for_channel", return_value=[meta]), \
             patch.object(
                 yd, "_fetch_transcript_for_channel",
                 return_value=_tr("b" * 600, video=meta),
             ), \
             patch.object(yd, "_translate_only", return_value="譯文"), \
             patch.object(yd, "_build_short_digest", return_value=digest):
            result = yd._process_one_channel(
                ch, store=store, state=state, args=args,
                short_transcript_pending=pending,
            )

        assert result is not None
        assert result[2] is digest
        assert pending == []

    def test_299_premium_true_runs_llm(self) -> None:
        from pipeline import youtube_daily as yd
        from pipeline.lib.youtube_translate import VideoDigest

        ch = _ch_limmo()
        meta = _meta()
        pending: list = []
        store = MagicMock()
        store.is_processed.return_value = False
        state = MagicMock()
        args = _args()
        digest = VideoDigest(
            video_id=meta.id, title=meta.title, channel_name=meta.channel_name,
            url=meta.url, published_epoch=meta.epoch, duration_sec=meta.duration_sec,
            source_language="de", n_chars=299,
            summary_zh="摘要", analyst_zh="- a", producer_zh="觀點",
            vocab_zh="", map_calls=0, reduce_calls=1, elapsed_sec=0.1,
        )

        with patch.object(yd, "pick_video_for_channel", return_value=[meta]), \
             patch.object(
                 yd, "_fetch_transcript_for_channel",
                 return_value=_tr("c" * 299, is_premium=True, video=meta),
             ), \
             patch.object(yd, "_translate_only", return_value="譯文"), \
             patch.object(yd, "_build_short_digest", return_value=digest):
            result = yd._process_one_channel(
                ch, store=store, state=state, args=args,
                short_transcript_pending=pending,
            )

        assert result is not None
        assert pending == []

    def test_empty_transcript_exhausted_goes_pending(self) -> None:
        from pipeline import youtube_daily as yd

        ch = _ch_limmo()
        meta = _meta()
        pending: list = []
        store = MagicMock()
        store.is_processed.return_value = False
        state = MagicMock()
        args = _args()

        with patch.object(yd, "pick_video_for_channel", return_value=[meta]), \
             patch.object(
                 yd, "_fetch_transcript_for_channel",
                 return_value=_tr("", video=meta),
             ), \
             patch.object(yd, "_translate_only") as translate:
            result = yd._process_one_channel(
                ch, store=store, state=state, args=args,
                short_transcript_pending=pending,
            )

        assert result is None
        assert len(pending) == 1
        assert pending[0]["n_chars"] == 0
        translate.assert_not_called()
        store.mark_processed.assert_not_called()


class TestWriteShortTranscriptPending:
    def test_writes_raw_transcript_not_llm_digest(self, tmp_path: Path) -> None:
        from pipeline.lib import youtube_obsidian as yo

        pending = [{
            "channel_id": "limmo",
            "channel_name": "L'Immo",
            "video_id": "6b46483eec0c20c1448b0b71daf2a8c7",
            "title": "Die Zeiten haben sich geändert",
            "n_chars": 299,
            "is_premium": False,
            "transcript": "RAW_PREVIEW_" + ("x" * 280),
            "url": "https://example.com/a.mp3",
        }]
        summary = yo.step_write_short_transcript_pending(
            pending, repo_root=str(tmp_path), date_str="2026-09-04",
        )
        assert summary["n_files"] == 1
        assert summary["n_errors"] == 0
        written = Path(tmp_path) / summary["written"][0]
        assert "_pending_review/ShortTranscript/" in summary["written"][0]
        assert "Daily/" not in summary["written"][0]
        text = written.read_text(encoding="utf-8")
        assert "RAW_PREVIEW_" in text
        assert "awaiting review" in text
        assert "skipped LLM digest" in text
        assert "一句話摘要" not in text


class TestDiscordShortTranscriptWarning:
    def test_empty_pending_is_silent(self) -> None:
        from pipeline.lib import youtube_discord as yd
        out = yd.step_send_short_transcript_warning([], dry_run=False)
        assert out.get("skipped") is True
        assert out["n_embeds"] == 0

    def test_dry_run_preview_without_send(self) -> None:
        from pipeline.lib import youtube_discord as yd
        pending = [{
            "channel_name": "L'Immo",
            "video_id": "6b46483eec0c20c1448b0b71daf2a8c7",
            "title": "Die Zeiten haben sich geändert",
            "n_chars": 299,
        }]
        with patch.object(yd, "_send") as send:
            out = yd.step_send_short_transcript_warning(
                pending, dry_run=True,
            )
        send.assert_not_called()
        assert out["n_embeds"] == 1
        assert out["pending_count"] == 1


class TestMainMixedShortAndNormal:
    def test_one_short_one_normal_discord_digest_plus_warning(
        self, tmp_path: Path,
    ) -> None:
        from pipeline import youtube_daily as yd
        from pipeline.lib.youtube_fetch import VideoMeta
        from pipeline.lib.youtube_translate import VideoDigest

        ch_short = _ch_limmo()
        ch_ok = {
            "id": "insightsimmo", "name": "Insights Immo",
            "url": "https://www.youtube.com/@insightsimmo",
            "channel_id": "UCxxx", "enabled": True,
        }
        short_meta = _meta()
        ok_meta = VideoMeta(
            id="GOODVIDEO01", title="Good", duration_sec=120, epoch=1,
            url="https://www.youtube.com/watch?v=GOODVIDEO01",
            channel_id="UCxxx", channel_name="Insights Immo",
        )
        ok_digest = VideoDigest(
            video_id="GOODVIDEO01", title="Good", channel_name="Insights Immo",
            url=ok_meta.url, published_epoch=1, duration_sec=120,
            source_language="de", n_chars=800,
            summary_zh="摘要", analyst_zh="- a", producer_zh="觀點",
            vocab_zh="", map_calls=0, reduce_calls=1, elapsed_sec=0.1,
        )

        def fake_process(ch, **kwargs):
            pending = kwargs.get("short_transcript_pending")
            if ch["id"] == "limmo":
                if pending is not None:
                    pending.append({
                        "channel_id": "limmo",
                        "channel_name": "L'Immo",
                        "video_id": short_meta.id,
                        "title": short_meta.title,
                        "n_chars": 299,
                        "is_premium": False,
                        "transcript": "x" * 299,
                        "url": short_meta.url,
                        "epoch": short_meta.epoch,
                        "source_type": "podcast",
                    })
                return None
            return (ch, ok_meta, ok_digest)

        vault_calls: list = []
        pending_calls: list = []
        discord_calls: list = []
        warn_calls: list = []

        def capture_vault(*a, **k):
            vault_calls.append((a, k))
            return {"n_files": 1, "n_errors": 0, "content_kind": "short-summary"}

        def capture_pending(items, **k):
            pending_calls.append(items)
            return {"n_files": 1, "n_errors": 0, "written": ["ok"]}

        def capture_discord(digests, **k):
            discord_calls.append(digests)
            return {"n_embeds": 1, "errors": [], "per_video": [
                {"video_id": "GOODVIDEO01", "message_ids": ["1"]},
            ]}

        def capture_warn(items, **k):
            warn_calls.append(items)
            return {"n_embeds": 1, "errors": []}

        with patch.object(yd, "load_channels", return_value=[ch_short, ch_ok]), \
             patch.object(yd, "_process_one_channel", side_effect=fake_process), \
             patch.object(yd, "ProcessedStore"), \
             patch.object(yd.youtube_state, "StateStore"), \
             patch.object(yd.youtube_obsidian, "step_write_vault",
                          side_effect=capture_vault), \
             patch.object(yd.youtube_obsidian, "step_write_short_transcript_pending",
                          side_effect=capture_pending), \
             patch.object(yd.youtube_discord, "step_send_discord",
                          side_effect=capture_discord), \
             patch.object(yd.youtube_discord, "step_send_short_transcript_warning",
                          side_effect=capture_warn), \
             patch.object(yd, "push_to_github",
                          return_value={"pushed": False, "commit_sha": None,
                                        "dry_run": False}), \
             patch.object(yd, "VAULT_ROOT", tmp_path), \
             patch.object(yd.time, "sleep"), \
             patch("sys.argv", [
                 "youtube_daily.py",
                 "--channels", "limmo,insightsimmo",
                 "--skip-store",
             ]):
            rc = yd.main()

        assert rc == 0
        assert len(vault_calls) == 1
        assert len(discord_calls) == 1
        assert len(discord_calls[0]) == 1
        assert discord_calls[0][0].video_id == "GOODVIDEO01"
        assert len(pending_calls) == 1
        assert pending_calls[0][0]["n_chars"] == 299
        assert len(warn_calls) == 1
        assert len(warn_calls[0]) == 1

    def test_only_short_pending_no_formal_vault_or_mark(
        self, tmp_path: Path,
    ) -> None:
        from pipeline import youtube_daily as yd

        ch = _ch_limmo()
        meta = _meta()

        def fake_process(ch, **kwargs):
            pending = kwargs.get("short_transcript_pending")
            if pending is not None:
                pending.append({
                    "channel_id": "limmo",
                    "channel_name": "L'Immo",
                    "video_id": meta.id,
                    "title": meta.title,
                    "n_chars": 299,
                    "is_premium": False,
                    "transcript": "y" * 299,
                    "url": meta.url,
                    "epoch": meta.epoch,
                    "source_type": "podcast",
                })
            return None

        store = MagicMock()
        with patch.object(yd, "load_channels", return_value=[ch]), \
             patch.object(yd, "_process_one_channel", side_effect=fake_process), \
             patch.object(yd, "ProcessedStore", return_value=store), \
             patch.object(yd.youtube_state, "StateStore"), \
             patch.object(yd.youtube_obsidian, "step_write_vault") as write_vault, \
             patch.object(
                 yd.youtube_obsidian, "step_write_short_transcript_pending",
                 return_value={"n_files": 1, "n_errors": 0, "written": ["p"]},
             ) as write_pending, \
             patch.object(yd.youtube_discord, "step_send_discord") as send_discord, \
             patch.object(
                 yd.youtube_discord, "step_send_short_transcript_warning",
                 return_value={"n_embeds": 1, "errors": []},
             ) as send_warn, \
             patch.object(yd, "push_to_github") as push, \
             patch.object(yd, "VAULT_ROOT", tmp_path), \
             patch.object(yd.time, "sleep"), \
             patch("sys.argv", [
                 "youtube_daily.py", "--channels", "limmo",
             ]):
            rc = yd.main()

        assert rc == 0
        write_pending.assert_called_once()
        send_warn.assert_called_once()
        write_vault.assert_not_called()
        send_discord.assert_not_called()
        push.assert_not_called()
        store.mark_processed.assert_not_called()
