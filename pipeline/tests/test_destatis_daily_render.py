"""Unit tests for pipeline.destatis_daily vault rendering (Stage 1 T3.1).

Pure-logic tests — no network, no Discord, no Airtable. Fixtures build
``DestatisDataset`` instances directly so we can exercise:

  - :func:`_is_time_series_first_col` — keyword matching
  - :func:`_render_dataset_md` — time-series vs cross-section branches
  - :func:`step_write_vault` — pulls ``_source_page`` from config into
    the rendered .md and picks the right filename

These cover the three fixes for Stage 1 T3.1:
  1. ``source_page`` is now written to the per-dataset .md
  2. Non-time-series CSVs (e.g. ``investments_construction``) no longer
     mis-label their first column as "最新月份"
  3. ``DEFAULT_DISCORD_CHANNEL = "tao"`` (verified in module attrs)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make pipeline package importable when running pytest from repo root.
THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline import destatis_daily  # noqa: E402
from pipeline.destatis_daily import (  # noqa: E402
    DEFAULT_DISCORD_CHANNEL,
    _is_time_series_first_col,
    _render_dataset_md,
    _render_index_md,
    step_write_vault,
)
from pipeline.lib.destatis_csv import DestatisDataset  # noqa: E402


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

def _make_dataset(
    *,
    source_id: str,
    header: list[str],
    rows: list[list[str]],
    name_zh: str = "測試資料",
    name_de: str = "Test Daten",
    url: str = "https://example.invalid/test.csv",
) -> DestatisDataset:
    """Build a DestatisDataset with sensible defaults for render tests."""
    return DestatisDataset(
        source_id=source_id,
        name=name_de,
        name_de=name_de,
        name_zh=name_zh,
        reference_period="latest",
        fetched_at="2026-08-13T00:00:00+00:00",
        encoding="utf-8",
        raw_text="",
        rows=[header] + rows,
        header=header,
        file_path="",
        url=url,
    )


@pytest.fixture
def ts_dataset() -> DestatisDataset:
    """A time-series dataset: first column is `Monat`, 3 monthly rows."""
    return _make_dataset(
        source_id="auftragseingang_bauhauptgewerbe",
        header=["Monat", "Wert-A", "Wert-B"],
        rows=[
            ["2026/01/01", "90,0", "85,0"],
            ["2026/02/01", "92,0", "87,0"],
            ["2026/03/01", "95,0", "89,0"],
        ],
    )


@pytest.fixture
def cross_section_dataset() -> DestatisDataset:
    """A cross-section dataset: first column is `Kategorie`, 6 categories."""
    return _make_dataset(
        source_id="investments_construction",
        name_zh="營建業投資",
        name_de="Investitionen im Baugewerbe",
        header=["Kategorie", "Anteil (%)"],
        rows=[
            ["Bau von Gebäuden", "20"],
            ["Bauinstallation", "18"],
            ["Bau von Straßen und Bahnverkehrsstrecken", "17"],
            ["Sonstige spezialisierte Bautätigkeiten", "15"],
            ["Leitungstiefbau und Kläranlagenbau", "11"],
            ["Übrige Wirtschaftszweige", "19"],
        ],
    )


# --------------------------------------------------------------------------- #
# _is_time_series_first_col
# --------------------------------------------------------------------------- #

class TestIsTimeSeriesFirstCol:
    @pytest.mark.parametrize("name", [
        "Monat", "monat", "Berichtsmonat (YYYY-MM)", "MONAT",
        "Jahr", "Jahr/Quartal", "Year",
        "Zeit", "Datum", "Date", "Period", "Periode",
        "Quartal", "Quarter", "Stichtag",
    ])
    def test_recognises_time_series_keys(self, name: str):
        assert _is_time_series_first_col([name]) is True, name

    @pytest.mark.parametrize("name", [
        "Kategorie", "Anteil (%)", "Wirtschaftszweig",
        "Bundesland", "Geschlecht", "Altersgruppe",
    ])
    def test_rejects_non_time_series_keys(self, name: str):
        assert _is_time_series_first_col([name]) is False, name

    def test_empty_header_is_false(self):
        assert _is_time_series_first_col([]) is False

    def test_empty_first_col_is_false(self):
        assert _is_time_series_first_col([""]) is False


# --------------------------------------------------------------------------- #
# _render_dataset_md — time-series branch
# --------------------------------------------------------------------------- #

class TestRenderDatasetMdTimeSeries:
    def test_uses_latest_month_label(self, ts_dataset: DestatisDataset):
        md = _render_dataset_md(ts_dataset, "2026-08-13")
        assert "**最新月份**：2026/03/01" in md

    def test_shows_latest_row_values(self, ts_dataset: DestatisDataset):
        md = _render_dataset_md(ts_dataset, "2026-08-13")
        # Last data row is 2026/03/01 with Wert-A=95,0 / Wert-B=89,0
        assert "Wert-A=95,0" in md
        assert "Wert-B=89,0" in md

    def test_table_title_is_months(self, ts_dataset: DestatisDataset):
        md = _render_dataset_md(ts_dataset, "2026-08-13")
        assert "## 完整資料表（前 12 個月，最新在上）" in md

    def test_range_labels_are_start_end(self, ts_dataset: DestatisDataset):
        md = _render_dataset_md(ts_dataset, "2026-08-13")
        assert "**起**：2026/01/01" in md
        assert "**迄**：2026/03/01" in md
        assert "**月資料筆數**：3 筆" in md

    def test_source_page_placeholder_when_missing(
        self, ts_dataset: DestatisDataset,
    ):
        md = _render_dataset_md(ts_dataset, "2026-08-13")
        assert "來源網頁：（見 config 內 `_source_page`）" in md

    def test_source_page_appears_when_provided(
        self, ts_dataset: DestatisDataset,
    ):
        page = "https://www.destatis.de/DE/Themen/x/y.html"
        md = _render_dataset_md(ts_dataset, "2026-08-13",
                                source_page=page)
        assert f"來源網頁：{page}" in md
        # Placeholder gone
        assert "（見 config 內" not in md

    def test_yaml_frontmatter_has_required_fields(
        self, ts_dataset: DestatisDataset,
    ):
        md = _render_dataset_md(ts_dataset, "2026-08-13")
        assert "source_type: destatis_csv" in md
        assert "source_id: auftragseingang_bauhauptgewerbe" in md
        assert "reference_period: latest" in md
        assert "n_rows: 4" in md  # 1 header + 3 data
        assert "n_cols: 3" in md


# --------------------------------------------------------------------------- #
# _render_dataset_md — cross-section branch
# --------------------------------------------------------------------------- #

class TestRenderDatasetMdCrossSection:
    def test_uses_cross_section_labels(
        self, cross_section_dataset: DestatisDataset,
    ):
        md = _render_dataset_md(cross_section_dataset, "2026-08-13")
        # Must NOT label the first column as "最新月份"
        assert "**最新月份**" not in md
        # Cross-section wording present
        assert "**資料類型**：橫斷面（cross-section）" in md
        assert "**首筆**：Bau von Gebäuden" in md
        assert "**末筆**：Übrige Wirtschaftszweige" in md
        assert "**橫斷面資料筆數**：6 筆" in md

    def test_summary_says_non_time_series(
        self, cross_section_dataset: DestatisDataset,
    ):
        md = _render_dataset_md(cross_section_dataset, "2026-08-13")
        assert "非時間序列資料,共 6 筆橫斷面資料" in md

    def test_does_not_mislabel_last_category_as_month(
        self, cross_section_dataset: DestatisDataset,
    ):
        """Regression: the bug was Übrige Wirtschaftszweige appearing
        as "最新月份". With the fix it should appear only as "末筆"."""
        md = _render_dataset_md(cross_section_dataset, "2026-08-13")
        # The category text should still be in the file (it's a real
        # data point), but never paired with the "最新月份" label.
        assert "Übrige Wirtschaftszweige" in md
        # Specifically: must not be adjacent to the latest-month label
        # The bad line used to be: "- **最新月份**：Übrige Wirtschaftszweige"
        assert "**最新月份**：Übrige Wirtschaftszweige" not in md

    def test_table_title_is_not_months(
        self, cross_section_dataset: DestatisDataset,
    ):
        md = _render_dataset_md(cross_section_dataset, "2026-08-13")
        assert "## 完整資料表（最多顯示 12 筆）" in md
        assert "前 12 個月" not in md


# --------------------------------------------------------------------------- #
# _render_index_md
# --------------------------------------------------------------------------- #

class TestRenderIndexMd:
    def test_index_labels_cross_section_correctly(
        self, ts_dataset, cross_section_dataset,
    ):
        md = _render_index_md([ts_dataset, cross_section_dataset],
                              "2026-08-13")
        # TS source uses 月份 wording
        assert "**最新月份**：2026/03/01" in md
        assert "197" not in md  # not part of the 3-row fixture
        # Cross-section source: must NOT mislabel last category as a
        # "最新分類" (the bug). Instead show "資料類型: 橫斷面" +
        # row count, consistent with the single-dataset .md.
        assert "**最新分類**" not in md
        assert "**最新月份**：Übrige Wirtschaftszweige" not in md
        assert "**資料類型**：橫斷面（cross-section）" in md
        assert "6 筆（橫斷面）" in md

    def test_index_empty_when_no_datasets(self):
        md = _render_index_md([], "2026-08-13")
        assert "（本日沒有新資料）" in md


# --------------------------------------------------------------------------- #
# step_write_vault — source_page wiring
# --------------------------------------------------------------------------- #

class TestStepWriteVaultSourcePage:
    def test_writes_source_page_into_per_dataset_md(self, tmp_path: Path):
        cfg = {
            "id": "auftragseingang_bauhauptgewerbe",
            "name_de": "Auftragseingang im Bauhauptgewerbe",
            "name_zh": "主承攬業新訂單",
            "url": "https://www.destatis.de/example.csv",
            "vault_filename": "auftragseingang_bauhauptgewerbe.md",
            "_source_page": "https://www.destatis.de/DE/Themen/Bauen/x.html",
        }
        ds = _make_dataset(
            source_id="auftragseingang_bauhauptgewerbe",
            header=["Monat", "Wert-A"],
            rows=[["2026/01/01", "90,0"], ["2026/02/01", "92,0"]],
        )
        results, index_path = step_write_vault(
            [ds], [cfg],
            repo_root=str(tmp_path),
            date_str="2026-08-13",
            wipe=True,
        )
        assert len(results) == 1
        vault_path = tmp_path / "immobilien-kb" / "vault" / "Stat" / \
            "2026-08-13" / "auftragseingang_bauhauptgewerbe.md"
        assert vault_path.exists()
        content = vault_path.read_text(encoding="utf-8")
        assert "https://www.destatis.de/DE/Themen/Bauen/x.html" in content
        assert "（見 config 內" not in content

    def test_falls_back_to_source_page_alias(
        self, tmp_path: Path,
    ):
        """If only the public ``source_page`` key is set, it still works."""
        cfg = {
            "id": "x",
            "name_de": "X",
            "name_zh": "X",
            "url": "https://x.example/csv",
            "vault_filename": "x.md",
            # No _source_page, but plain source_page is provided.
            "source_page": "https://x.example/page.html",
        }
        ds = _make_dataset(
            source_id="x",
            header=["Monat", "Wert"],
            rows=[["2026/01/01", "1"]],
        )
        step_write_vault([ds], [cfg],
                         repo_root=str(tmp_path), date_str="2026-08-13",
                         wipe=True)
        out = tmp_path / "immobilien-kb" / "vault" / "Stat" / \
            "2026-08-13" / "x.md"
        assert out.read_text(encoding="utf-8").endswith("")  # exists
        assert "https://x.example/page.html" in out.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Module-level defaults
# --------------------------------------------------------------------------- #

class TestModuleDefaults:
    def test_default_discord_channel_is_tao(self):
        # Stage 1 T3.1 fix: switch from "home" to "tao" alias
        # (channel id 1495562787685011616) for Destatis digests.
        assert DEFAULT_DISCORD_CHANNEL == "tao"

    def test_default_discord_channel_helper(self):
        # Sanity check that the alias resolves to the expected id when
        # sent through discord_sender._resolve_channel. We don't import
        # discord_sender directly (it has side effects on .env), so we
        # duplicate the lookup table check via a tiny inlined call.
        import importlib.util as _ilu
        spec = _ilu.spec_from_file_location(
            "discord_sender",
            "/root/projects/ado_hermes_reddit_scrapper/"
            "immobilien-kb/tools/discord_sender.py",
        )
        assert spec and spec.loader
        mod = _ilu.module_from_spec(spec)
        spec.loader.exec_module(mod)
        assert mod._resolve_channel("tao") == "1495562787685011616"
        # Existing aliases still work
        assert mod._resolve_channel("home") == "1495548848183967916"
        assert mod._resolve_channel("headlines") == "1520791894995501106"
        assert mod._resolve_channel("podcast") == "1535461574460968960"
