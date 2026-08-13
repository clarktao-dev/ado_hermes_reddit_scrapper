"""Unit tests for pipeline.lib.destatis_csv (pure logic only, no network).

Covers:
  - detect_encoding: utf-8-sig / utf-8 / latin-1 fallback
  - parse_csv: header on row 0, all data rows, summary fields populated

Network-dependent code (fetch_csv, fetch_and_parse) is exercised by the
``__main__`` smoke test in destatis_csv.py — we keep the test suite fully
offline so CI doesn't need internet.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Make pipeline package importable when running pytest from repo root.
THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline.lib import destatis_csv  # noqa: E402
from pipeline.lib.destatis_csv import (  # noqa: E402
    DestatisDataset,
    detect_encoding,
    parse_csv,
)


# --------------------------------------------------------------------------- #
# detect_encoding
# --------------------------------------------------------------------------- #

class TestDetectEncoding:
    def test_utf8_sig_with_bom(self):
        raw = "\ufeffMonat;Wert\n2026-01;100".encode("utf-8-sig")
        # encode with utf-8-sig will prepend BOM if the string starts with one,
        # but to be safe we add it explicitly:
        raw = b"\xef\xbb\xbf" + "Monat;Wert\n2026-01;100".encode("utf-8")
        assert detect_encoding(raw) == "utf-8-sig"

    def test_plain_utf8(self):
        raw = "Monat;Wert\n2026-01;100".encode("utf-8")
        assert detect_encoding(raw) == "utf-8"

    def test_latin1_fallback(self):
        # 0xe4 is "ä" in latin-1, invalid as standalone utf-8 byte
        raw = b"Monat;Wert\n2026-01;M\xe4rz"
        assert detect_encoding(raw) == "latin-1"

    def test_empty_bytes(self):
        # Empty bytes: utf-8-sig check fails (no BOM), utf-8 decode succeeds
        assert detect_encoding(b"") == "utf-8"


# --------------------------------------------------------------------------- #
# parse_csv — using tmp files with realistic Destatis-shaped content
# --------------------------------------------------------------------------- #

class TestParseCsv:
    @pytest.fixture
    def sample_csv(self, tmp_path: Path) -> Path:
        """Write a 3-line CSV with semicolon delimiter + double-quote quoting."""
        content = (
            '"Monat";"Kalender- und saisonbereinigt";"Trend-Konjunktur-Komponente"\n'
            '2010/01/01;65,4;71,2\n'
            '2010/02/01;71,5;71,1\n'
            '2010/03/01;72,0;71,0\n'
        )
        p = tmp_path / "sample.csv"
        p.write_text(content, encoding="utf-8")
        return p

    def test_header_is_row_zero(self, sample_csv: Path):
        ds = parse_csv(sample_csv, "utf-8")
        assert ds.header == [
            "Monat",
            "Kalender- und saisonbereinigt",
            "Trend-Konjunktur-Komponente",
        ]

    def test_rows_count(self, sample_csv: Path):
        ds = parse_csv(sample_csv, "utf-8")
        # 1 header + 3 data
        assert len(ds.rows) == 4
        assert len(ds.rows) - 1 == 3  # data rows

    def test_first_data_row(self, sample_csv: Path):
        ds = parse_csv(sample_csv, "utf-8")
        assert ds.rows[1] == ["2010/01/01", "65,4", "71,2"]

    def test_metadata_fields_default(self, sample_csv: Path):
        ds = parse_csv(sample_csv, "utf-8")
        # parse_csv 預設把 source_id/name/reference_period 留空 / "latest"
        assert ds.source_id == ""
        assert ds.name == ""
        assert ds.reference_period == "latest"
        assert ds.encoding == "utf-8"
        assert ds.file_path == str(sample_csv)
        assert ds.fetched_at  # ISO timestamp 非空

    def test_empty_csv_raises(self, tmp_path: Path):
        p = tmp_path / "empty.csv"
        p.write_text("", encoding="utf-8")
        with pytest.raises(ValueError, match="empty or unparseable"):
            parse_csv(p, "utf-8")

    def test_summary_shape(self, sample_csv: Path):
        ds = parse_csv(sample_csv, "utf-8")
        s = ds.summary()
        assert s["n_rows"] == 4
        assert s["n_cols"] == 3
        assert s["first_data_row"] == ["2010/01/01", "65,4", "71,2"]
        assert s["last_data_row"] == ["2010/03/01", "72,0", "71,0"]
        assert s["encoding"] == "utf-8"
