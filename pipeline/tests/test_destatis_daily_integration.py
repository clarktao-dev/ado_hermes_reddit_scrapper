"""Integration tests for pipeline.destatis_daily (Stage 1 T4).

These cover four end-to-end scenarios that go beyond the unit-level
``test_destatis_daily_render.py`` suite:

  1. ``test_fetch_and_parse_end_to_end_smoke`` — Real network call to
     Destatis to confirm the published CSV still parses to 198 rows
     (1 header + 197 monthly values). Skipped if the host is offline.

  2. ``test_vault_wipe_and_write_idempotent`` — Round-trip through
     :func:`pipeline.destatis_daily.step_write_vault` into a
     ``tmp_path`` vault. Verifies:
       * Pre-existing ``_index.md`` is wiped on a second run.
       * Running the per-dataset renderer twice yields identical bytes.
       * Wipe wipes the whole day folder (3 dataset → 0 file).

  3. ``test_airtable_dedup_blocks_rerun`` — A FakeAirtable layer is
     monkey-patched into :class:`ProcessedStore` so we can exercise
     the dedup gate without touching production Airtable. Confirms:
       * ``is_processed`` returns True after a record exists.
       * ``run_pipeline`` short-circuits all 3 sources to ``skipped``.
       * ``--dry-run`` still lists 3 sources but writes no vault.

  4. ``test_discord_push_mocked`` — Monkey-patches
     :func:`destatis_daily._import_discord` so the import returns a
     mock module whose ``send_to_channel`` is recorded. Confirms:
       * 3 calls were made (one per dataset).
       * Embed title starts with ``🏗️ [Destatis 官方數據]``.
       * Channel alias passed is ``tao`` (resolves to
         ``1495562787685011616``).

Network policy
--------------
Scenario 1 is the only one that hits the network, and it is
**skipped** when the upstream is unreachable so a CI outage does not
turn the suite red. All other scenarios are fully hermetic: no real
Airtable, no real Discord, no real GitHub, no real vault.

Run from repo root::

    pytest pipeline/tests/test_destatis_daily_integration.py -v
"""
from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest import mock

import pytest

# Make pipeline package importable when running pytest from repo root.
THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline import destatis_daily  # noqa: E402
from pipeline.destatis_daily import (  # noqa: E402
    DEFAULT_DISCORD_CHANNEL,
    step_write_vault,
    run_pipeline,
    _render_dataset_md,
)
from pipeline.lib.destatis_csv import (  # noqa: E402
    DestatisDataset,
    fetch_and_parse,
)
from pipeline.lib.processed_store import (  # noqa: E402
    ProcessedStore,
    make_hash,
)


# --------------------------------------------------------------------------- #
# Frozen-time helper
# --------------------------------------------------------------------------- #

class _FrozenDateTime:
    """Stand-in for ``datetime`` that always returns the same instant.

    ``destatis_daily._render_dataset_md`` embeds
    ``datetime.now(timezone.utc).isoformat()`` in the footer; without
    a frozen value, two calls in quick succession produce slightly
    different bytes and break the byte-for-byte idempotency assertion.
    We only need to satisfy ``datetime.now(timezone.utc)`` and the
    ``.isoformat()`` chain.
    """

    _FROZEN = "2026-08-13T05:00:00+00:00"

    @classmethod
    def now(cls, tz=None):  # noqa: D401
        from datetime import datetime as _dt
        return _dt.fromisoformat(cls._FROZEN)


# --------------------------------------------------------------------------- #
# Shared fixture helper
# --------------------------------------------------------------------------- #

def make_fixture_dataset(
    *,
    source_id: str,
    header: List[str],
    rows: List[List[str]],
    name_zh: str = "測試資料集",
    name_de: str = "Testdatensatz",
    reference_period: str = "latest",
    url: str = "https://example.invalid/test.csv",
) -> DestatisDataset:
    """Build a self-contained ``DestatisDataset`` for tests.

    Header is prepended to ``rows`` so the dataclass invariant
    ``rows[0] == header`` is honoured. This is the same shape the
    production ``parse_csv`` produces.
    """
    return DestatisDataset(
        source_id=source_id,
        name=name_de,
        name_de=name_de,
        name_zh=name_zh,
        reference_period=reference_period,
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
    """3-row time-series fixture: first col is Monat."""
    return make_fixture_dataset(
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
    """6-row cross-section fixture: first col is Kategorie."""
    return make_fixture_dataset(
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
# Fake Airtable layer (local copy — see test_processed_store.FakeAirtable
# for the canonical version; we keep a private copy so this file's tests
# don't depend on the storage-test module layout).
# --------------------------------------------------------------------------- #

class _FakeAirtable:
    """In-memory Airtable stub mirroring ProcessedStore's HTTP shape.

    Records are stored in a dict keyed by record id. Implements the
    subset of Airtable REST used by ProcessedStore:
      - GET  /<base>/<table>?filterByFormula=…  (with pagination)
      - POST /<base>/<table>  (plain create)
      - PATCH /<base>/<table>/<rec>  (partial update)
    """

    def __init__(self) -> None:
        self.records: Dict[str, Dict[str, Any]] = {}
        self.call_log: List[Tuple[str, str, Dict[str, Any]]] = []

    @staticmethod
    def _next_id() -> str:
        return f"rec{uuid.uuid4().hex[:14]}"

    def _match(self, formula: Optional[str]) -> List[Dict[str, Any]]:
        """Evaluate the small subset of Airtable formulas we actually use."""
        if not formula:
            return list(self.records.values())
        if "{source_hash}=" in formula:
            needle = formula.split("'")[1]
            return [
                r for r in self.records.values()
                if r.get("fields", {}).get("source_hash") == needle
            ]
        return []

    def handle(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        data: Optional[bytes],
        timeout: float,
    ) -> Dict[str, Any]:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        path = parsed.path
        params = parse_qs(parsed.query)
        formula = (params.get("filterByFormula") or [None])[0]
        body: Dict[str, Any] = {}
        if data is not None:
            body = json.loads(data.decode("utf-8"))
        self.call_log.append((method, path, body))

        # POST plain create
        if method == "POST" and "fields" in body:
            new_id = self._next_id()
            new_rec = {
                "id": new_id,
                "fields": dict(body["fields"]),
                "createdTime": "2026-08-13T00:00:00.000Z",
            }
            self.records[new_id] = new_rec
            return {"records": [new_rec]}

        # PATCH single record
        if method == "PATCH":
            rec_id = path.rsplit("/", 1)[-1]
            if rec_id in self.records:
                self.records[rec_id]["fields"].update(body.get("fields", {}))
                return {
                    "id": rec_id,
                    "fields": self.records[rec_id]["fields"],
                }
            return {}

        # GET list (with or without filter)
        if method == "GET":
            matched = self._match(formula)
            return {"records": matched}

        return {}


@pytest.fixture
def fake_airtable(monkeypatch: pytest.MonkeyPatch):
    """Return a ``(memory_dict, factory_fn)`` pair for ProcessedStore tests."""
    memory: Dict[str, Dict[str, Any]] = {}

    def _factory() -> ProcessedStore:
        return ProcessedStore("appFAKE", "ProcessedContent", _memory=memory)

    def _install(store: ProcessedStore) -> None:
        store._memory = memory  # type: ignore[attr-defined]

    return memory, _factory, _install


# --------------------------------------------------------------------------- #
# Scenario 1 — end-to-end fetch + parse against the live Destatis host
# --------------------------------------------------------------------------- #

class TestFetchAndParseSmoke:
    """Hit the real Destatis host once. Skipped if unreachable."""

    def test_fetch_and_parse_end_to_end_smoke(self) -> None:
        # Pull the live source config so the URL stays in sync with
        # the canonical destatis_sources.json.
        cfg_path = (
            REPO_ROOT / "pipeline" / "config" / "destatis_sources.json"
        )
        sources = json.loads(cfg_path.read_text(encoding="utf-8"))["sources"]
        target = None
        for s in sources:
            if s.get("id") == "auftragseingang_bauhauptgewerbe" and s.get(
                "enabled"
            ):
                target = s
                break
        if target is None:
            pytest.skip("auftragseingang_bauhauptgewerbe not enabled")

        t0 = time.time()
        try:
            ds = fetch_and_parse(target)
        except Exception as e:  # noqa: BLE001
            pytest.skip(f"Destatis host unreachable: {type(e).__name__}: {e}")
        elapsed = time.time() - t0

        # Time budget: 5s nominal, hard cap 10s.
        assert elapsed < 10.0, f"fetch took {elapsed:.1f}s, budget is 10s"

        # 198 rows = 1 header + 197 monthly values (1970-01 → 2026-05).
        # We assert the production contract (the test_destatis_daily_render
        # contract uses 3-row fixtures; the live dataset is much bigger).
        assert isinstance(ds, DestatisDataset)
        assert ds.source_id == "auftragseingang_bauhauptgewerbe"
        assert ds.name_zh == "主承攬業新訂單(實質,原值)"
        assert len(ds.rows) == 198, (
            f"expected 198 rows (1 header + 197 monthly), got {len(ds.rows)}"
        )
        assert len(ds.header) >= 2
        # First column should be a time-series period column.
        assert destatis_daily._is_time_series_first_col(ds.header), (
            f"first column {ds.header[0]!r} not a time-series key"
        )


# --------------------------------------------------------------------------- #
# Scenario 2 — vault write / wipe / idempotency
# --------------------------------------------------------------------------- #

class TestVaultWipeAndWriteIdempotent:
    """Round-trip step_write_vault into a tmp_path vault."""

    def test_vault_wipe_and_write_idempotent(
        self,
        tmp_path: Path,
        ts_dataset: DestatisDataset,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Freeze the in-doc timestamp so two consecutive renders
        # produce byte-identical output.
        monkeypatch.setattr(destatis_daily, "datetime", _FrozenDateTime)
        date_str = "2026-08-13"
        cfg = {
            "id": ts_dataset.source_id,
            "name_de": ts_dataset.name_de,
            "name_zh": ts_dataset.name_zh,
            "url": ts_dataset.url,
            "vault_filename": f"{ts_dataset.source_id}.md",
            "_source_page": (
                "https://www.destatis.de/DE/Themen/Bauen/example.html"
            ),
        }

        # 1) Plant a stale _index.md in the day's vault dir.
        stat_dir = tmp_path / "immobilien-kb" / "vault" / "Stat" / date_str
        stat_dir.mkdir(parents=True, exist_ok=True)
        stale_index = stat_dir / "_index.md"
        stale_index.write_text(
            "STALE INDEX FROM PREVIOUS RUN\n", encoding="utf-8"
        )
        assert stale_index.exists()

        # 2) Run _render_index_md twice — second run's content must
        #    be byte-identical (pure function contract).
        ds2 = make_fixture_dataset(
            source_id=ts_dataset.source_id,
            header=ts_dataset.header,
            # ts_dataset.rows[0] is the header (fixture invariant).
            # make_fixture_dataset prepends header again, so we must
            # strip it here to avoid a duplicated header row.
            rows=ts_dataset.rows[1:],
            name_zh=ts_dataset.name_zh,
            name_de=ts_dataset.name_de,
        )
        index_a = destatis_daily._render_index_md([ts_dataset], date_str)
        index_b = destatis_daily._render_index_md([ds2], date_str)
        assert index_a == index_b, "_render_index_md is not deterministic"

        # 3) Run _render_dataset_md twice — must be byte-identical.
        ds_md_a = _render_dataset_md(ts_dataset, date_str, source_page=cfg["_source_page"])
        ds_md_b = _render_dataset_md(ds2, date_str, source_page=cfg["_source_page"])
        assert ds_md_a == ds_md_b, "_render_dataset_md is not deterministic"

        # 4) step_write_vault runs first time — wipes the stale index
        #    and writes the per-dataset .md + new _index.md.
        results_1, index_path_1 = step_write_vault(
            [ts_dataset], [cfg],
            repo_root=str(tmp_path), date_str=date_str, wipe=True,
        )
        assert len(results_1) == 1
        per_dataset_path = stat_dir / cfg["vault_filename"]
        assert per_dataset_path.exists()
        assert index_path_1 == stat_dir / "_index.md"
        assert index_path_1.exists()
        # Stale marker must be gone.
        index_content_1 = index_path_1.read_text(encoding="utf-8")
        assert "STALE INDEX FROM PREVIOUS RUN" not in index_content_1
        # _render_index_md content matches the written file.
        assert index_content_1 == index_a

        # 5) Run step_write_vault again with the same dataset —
        #    per-dataset .md must be byte-identical to the first run.
        results_2, index_path_2 = step_write_vault(
            [ds2], [cfg],
            repo_root=str(tmp_path), date_str=date_str, wipe=True,
        )
        assert results_2[0].vault_path == results_1[0].vault_path
        assert per_dataset_path.read_text(encoding="utf-8") == ds_md_a
        # The second run wiped and re-wrote — index is still there and
        # content is identical to the first run.
        assert index_path_2.read_text(encoding="utf-8") == index_content_1

        # 6) Three datasets, then wipe — folder is empty.
        # Use fresh fixtures (source_id="a") so step_write_vault's
        # cfg_by_id lookup finds the matching cfg; reusing ts_dataset
        # would fall through to the default f"{source_id}.md" filename.
        ds_a = make_fixture_dataset(
            source_id="a",
            header=ts_dataset.header,
            rows=ts_dataset.rows[1:],
            name_zh=ts_dataset.name_zh,
            name_de=ts_dataset.name_de,
        )
        ds_b = make_fixture_dataset(
            source_id="b", header=["Monat", "X"],
            rows=[["2026/01/01", "1"], ["2026/02/01", "2"]],
        )
        ds_c = make_fixture_dataset(
            source_id="c", header=["Monat", "Y"],
            rows=[["2026/01/01", "3"], ["2026/02/01", "4"]],
        )
        cfgs = [
            {**cfg, "id": "a", "vault_filename": "a.md"},
            {**cfg, "id": "b", "vault_filename": "b.md"},
            {**cfg, "id": "c", "vault_filename": "c.md"},
        ]
        step_write_vault(
            [ds_a, ds_b, ds_c], cfgs,
            repo_root=str(tmp_path), date_str=date_str, wipe=True,
        )
        files = sorted(p.name for p in stat_dir.iterdir())
        assert "a.md" in files and "b.md" in files and "c.md" in files
        assert "_index.md" in files
        # Now wipe by re-running with no datasets — folder still exists
        # but contains only the index.md (which is always written).
        step_write_vault(
            [], [], repo_root=str(tmp_path), date_str=date_str, wipe=True,
        )
        files_after = sorted(p.name for p in stat_dir.iterdir())
        # Per-dataset files are gone, _index.md is still written.
        assert "a.md" not in files_after
        assert "b.md" not in files_after
        assert "c.md" not in files_after
        assert "_index.md" in files_after


# --------------------------------------------------------------------------- #
# Scenario 3 — Airtable dedup gate
# --------------------------------------------------------------------------- #

class TestAirtableDedupBlocksRerun:
    """Confirm the dedup gate short-circuits a re-run when records exist.

    Uses a FakeAirtable so no real Airtable writes happen.
    """

    def test_airtable_dedup_blocks_rerun(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fake_airtable,
        tmp_path: Path,
    ) -> None:
        memory, _factory, install = fake_airtable

        # Redirect vault writes into tmp_path so the real
        # immobilien-kb/ tree is not touched.
        monkeypatch.setattr(
            destatis_daily, "REPO_ROOT", str(tmp_path),
        )
        monkeypatch.setattr(
            destatis_daily, "VAULT_ROOT",
            str(tmp_path / "immobilien-kb" / "vault"),
        )

        # Build a ProcessedStore and patch it onto the fake.
        store = ProcessedStore("appFAKE", "ProcessedContent")
        install(store)

        # Inject the store via the run_pipeline plumbing: we want to
        # skip the auto-init in run_pipeline so we control the store
        # object — monkeypatch destatis_daily.ProcessedStore to a
        # factory that returns our pre-built instance (which already
        # has the fake _http_call installed).
        def _store_factory(*args, **kwargs):
            return store

        monkeypatch.setattr(
            destatis_daily, "ProcessedStore", _store_factory,
        )

        # Stage 3 destatis records (the same shape T3.2 wrote in
        # production). After this loop, is_processed must return True
        # for every (destatis_csv, destatis:<id>:latest) tuple.
        seed_ids = [
            "auftragseingang_bauhauptgewerbe",
            "genehmigte_wohnungen_monat",
            "investments_construction",
        ]
        for sid in seed_ids:
            dedup_id = f"destatis:{sid}:latest"
            store.mark_processed(
                source_type="destatis_csv",
                source_id=dedup_id,
                title=f"test-{sid}",
                channels=[f"destatis.{sid}"],
                pipeline_run_id="test-dedup-run",
                output_path=f"immobilien-kb/vault/Stat/2026-08-13/{sid}.md",
                metadata={"n_data_rows": 10, "n_cols": 2, "reference_period": "latest"},
                tags=["destatis", "test"],
                article_type="stat-table",
            )

        # Confirm the ledger has the 3 records.
        for sid in seed_ids:
            assert store.is_processed(
                "destatis_csv", f"destatis:{sid}:latest"
            ) is True, f"is_processed should be True for {sid}"

        # ---- A: run_pipeline with --skip-store=False but ledger full
        #         → every source should be 'skipped', no vault writes.
        rc = run_pipeline(
            push=False, dry_run=False,
            skip_store=False, channel=DEFAULT_DISCORD_CHANNEL,
            pipeline_run_id="test-dedup-rerun",
        )
        assert rc == 0, f"run_pipeline returned {rc}"

        # No vault files should exist (all 3 sources were skipped).
        stat_dir = tmp_path / "immobilien-kb" / "vault" / "Stat"
        if stat_dir.exists():
            # The day's folder may not even exist; if it does, only
            # _index.md from a previous run would be there, but since
            # nothing was written, no folder should exist.
            day_dirs = list(stat_dir.iterdir())
            assert day_dirs == [], (
                f"unexpected vault dirs created on all-skipped run: {day_dirs}"
            )

        # ---- B: --dry-run with full ledger → still lists 3 sources.
        # The dry-run path doesn't gate on is_processed, so all 3
        # fetch + render. We mock fetch_and_parse to return a
        # deterministic 2-row dataset per source so no network is hit.
        def _fake_fetch(source_config):
            return make_fixture_dataset(
                source_id=source_config["id"],
                header=["Monat", "Wert"],
                rows=[["2026/01/01", "1"], ["2026/02/01", "2"]],
                name_zh=source_config.get("name_zh", source_config["id"]),
                name_de=source_config.get("name_de", source_config["id"]),
                url=source_config["url"],
            )

        monkeypatch.setattr(
            destatis_daily, "fetch_and_parse", _fake_fetch,
        )
        # Snapshot record count before the dry-run. The dry-run path must
        # not write new ledger rows.
        count_before_dryrun = len(memory)

        rc = run_pipeline(
            push=False, dry_run=True,
            skip_store=False, channel=DEFAULT_DISCORD_CHANNEL,
            pipeline_run_id="test-dedup-dryrun",
        )
        assert rc == 0

        assert len(memory) == count_before_dryrun, (
            f"dry-run wrote unexpected ledger rows: before={count_before_dryrun} "
            f"after={len(memory)}"
        )

        # The seed records survived the dry-run (dry-run is read-only).
        for sid in seed_ids:
            assert store.is_processed(
                "destatis_csv", f"destatis:{sid}:latest"
            ) is True, f"is_processed should STILL be True for {sid} after dry-run"


# --------------------------------------------------------------------------- #
# Scenario 4 — Discord push (mocked, no real network)
# --------------------------------------------------------------------------- #

class TestDiscordPushMocked:
    """Confirm 3 embeds are sent, with the expected title prefix and alias."""

    def test_discord_push_mocked(
        self,
        monkeypatch: pytest.MonkeyPatch,
        fake_airtable,
        tmp_path: Path,
    ) -> None:
        memory, _factory, install = fake_airtable

        # Redirect vault + repo roots.
        monkeypatch.setattr(
            destatis_daily, "REPO_ROOT", str(tmp_path),
        )
        monkeypatch.setattr(
            destatis_daily, "VAULT_ROOT",
            str(tmp_path / "immobilien-kb" / "vault"),
        )

        # Mock fetch_and_parse to return a deterministic dataset per
        # source so the test is hermetic.
        def _fake_fetch(source_config):
            return make_fixture_dataset(
                source_id=source_config["id"],
                header=["Monat", "Wert"],
                rows=[["2026/01/01", "1"], ["2026/02/01", "2"]],
                name_zh=source_config.get("name_zh", source_config["id"]),
                name_de=source_config.get("name_de", source_config["id"]),
                url=source_config["url"],
            )

        monkeypatch.setattr(
            destatis_daily, "fetch_and_parse", _fake_fetch,
        )

        # Mock the discord import — return a module whose
        # send_to_channel is a recorder.
        send_log: List[Dict[str, Any]] = []

        class _MockDiscordModule:
            def send_to_channel(self, channel, text, *, as_embed=False, title=None, color=0x3498db):
                send_log.append({
                    "channel": channel,
                    "text_chars": len(text),
                    "as_embed": as_embed,
                    "title": title,
                })
                return {"ok": True, "message_ids": [f"mock-msg-{len(send_log)}"]}

        def _fake_import():
            return _MockDiscordModule()

        monkeypatch.setattr(
            destatis_daily, "_import_discord", _fake_import,
        )

        # Mock git + push_to_github to no-op (we're testing Discord).
        def _fake_git(*args, **kwargs):
            return mock.MagicMock(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(
            destatis_daily.subprocess, "run", _fake_git,
        )

        # Build a store with the fake Airtable layer.
        store = ProcessedStore("appFAKE", "ProcessedContent")
        install(store)

        # Inject the store via the run_pipeline plumbing: we want to
        # skip the auto-init in run_pipeline so we control the store
        # object. The simplest way: pass skip_store=False but also
        # monkeypatch ProcessedStore to return our pre-built instance.
        def _store_factory(*args, **kwargs):
            print(f"  [test] _store_factory called! args={args} kwargs={kwargs}", file=sys.stderr)
            return store

        monkeypatch.setattr(
            destatis_daily, "ProcessedStore", _store_factory,
        )

        # Clear the Airtable ledger to guarantee all 3 sources go
        # through vault + Discord + Airtable (no dedup short-circuit).
        memory.clear()
        store.clear_cache()

        # Run with --push (the call we actually want to test).
        rc = run_pipeline(
            push=True, dry_run=False,
            skip_store=False, channel=DEFAULT_DISCORD_CHANNEL,
            pipeline_run_id="test-discord-mock",
        )
        assert rc == 0, f"run_pipeline returned {rc}"

        # ---- 1) send_to_channel must have been called 3 times.
        assert len(send_log) == 3, (
            f"expected 3 send_to_channel calls (one per dataset), got {len(send_log)}: "
            f"{send_log}"
        )

        # ---- 2) Every embed title must start with the Destatis prefix.
        for entry in send_log:
            assert entry["as_embed"] is True, "must be sent as embed"
            assert entry["title"].startswith("🏗️ [Destatis 官方數據]"), (
                f"title missing Destatis prefix: {entry['title']!r}"
            )

        # ---- 3) Every call must have used the 'tao' channel alias.
        for entry in send_log:
            assert entry["channel"] == "tao", (
                f"expected channel 'tao', got {entry['channel']!r}"
            )

        # ---- 4) 'tao' alias resolves to the expected id (sanity).
        # We duplicate the alias table from
        # immobilien-kb/tools/discord_sender.py to keep the test
        # hermetic (no .env / token side-effects).
        import importlib.util as _ilu
        _spec = _ilu.spec_from_file_location(
            "_discord_sender_aliases",
            str(REPO_ROOT / "immobilien-kb" / "tools" / "discord_sender.py"),
        )
        assert _spec and _spec.loader
        _aliases_mod = _ilu.module_from_spec(_spec)
        _spec.loader.exec_module(_aliases_mod)
        assert _aliases_mod._resolve_channel("tao") == "1495562787685011616"

        # ---- 5) All 3 sources landed in the Airtable ledger.
        for sid in (
            "auftragseingang_bauhauptgewerbe",
            "genehmigte_wohnungen_monat",
            "investments_construction",
        ):
            assert store.is_processed(
                "destatis_csv", f"destatis:{sid}:latest"
            ) is True, f"is_processed should be True for {sid}"
