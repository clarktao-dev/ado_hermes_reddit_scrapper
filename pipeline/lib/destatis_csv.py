"""Destatis CSV fetcher + parser — 通用下載 + 解析 3 個主站公開的 highchart CSV。

策略
----
Destatis 主站把 highchart 圖表用的 CSV 公開放在
``/DE/Themen/Branchen-Unternehmen/Bauen/_Grafik/_Interaktiv/_Daten/*.csv``。
這些檔案:
  - 直接 GET 200,``Content-Type: text/csv;charset=UTF-8``
  - 沒有 metadata rows,row 0 就是 header
  - 通常 197 行(196 個月資料)或更少(分類資料)

GENESIS-Online 雖然資料更完整,但需要帳密;這條線以後再接。
現在先用主站公開 CSV 讓 pipeline 跑得起來。

公開 API
--------
- :class:`DestatisDataset` — 統一結果容器
- :func:`fetch_csv` — 下載到 /tmp,回傳 (Path, encoding)
- :func:`detect_encoding` — BOM-aware 編碼偵測
- :func:`parse_csv` — 把檔案切成 2D list
- :func:`fetch_and_parse` — 高階介面,直接吃 config dict

風格
----
- 函式小、單一職責
- 中文 log + inline docstring
- type hints 用 ``from __future__ import annotations`` + ``Optional[X]``
- network call 有 retry + 指數 backoff(2s, 4s, 8s)
"""
from __future__ import annotations

import csv
import io
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


logger = logging.getLogger(__name__)


_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# 指數 backoff 秒數,共 3 次重試(共 4 次嘗試:第 0 次 + 3 retries)
_RETRY_BACKOFFS_SEC = (2.0, 4.0, 8.0)

# 下載暫存根目錄
_DOWNLOAD_ROOT = Path("/tmp/destatis_csv")


# --------------------------------------------------------------------------- #
# Data class
# --------------------------------------------------------------------------- #

@dataclass
class DestatisDataset:
    """單一 Destatis CSV 來源的完整解析結果。

    Attributes:
        source_id: pipeline 內部 id,例 ``"auftragseingang_bauhauptgewerbe"``
        name: 英文 / 對外顯示名(這版直接用 name_de)
        name_de: 德文原名
        name_zh: 中文翻譯(vault 標題用)
        reference_period: 最新資料期間,例 ``"2026-06"`` 或 ``"latest"``
        fetched_at: ISO 8601 UTC timestamp
        encoding: 偵測到的編碼(``"utf-8-sig"`` / ``"utf-8"`` / ``"latin-1"``)
        raw_text: 完整原始 CSV 文字
        rows: 2D list,header 在 index 0
        header: 欄位名(row 0 copy)
        file_path: 下載到本地的暫存路徑
        url: 來源 URL
    """
    source_id: str
    name: str
    name_de: str
    name_zh: str
    reference_period: str
    fetched_at: str
    encoding: str
    raw_text: str
    rows: List[List[str]]
    header: List[str]
    file_path: str
    url: str

    def summary(self) -> Dict[str, Any]:
        """回傳給 log / JSON 用的摘要 dict。"""
        return {
            "source_id": self.source_id,
            "name_de": self.name_de,
            "name_zh": self.name_zh,
            "url": self.url,
            "encoding": self.encoding,
            "fetched_at": self.fetched_at,
            "reference_period": self.reference_period,
            "n_rows": len(self.rows),
            "n_cols": len(self.header),
            "header": self.header,
            "first_data_row": self.rows[1] if len(self.rows) > 1 else [],
            "last_data_row": self.rows[-1] if len(self.rows) > 1 else [],
            "file_path": self.file_path,
        }


# --------------------------------------------------------------------------- #
# Encoding detection
# --------------------------------------------------------------------------- #

def detect_encoding(raw_bytes: bytes) -> str:
    """自動偵測 CSV 的編碼。

    嘗試順序:
      1. ``utf-8-sig`` — 有 BOM (``\\xef\\xbb\\xbf``) 的 UTF-8
      2. ``utf-8`` — 純 UTF-8
      3. ``latin-1`` — 萬國碼,任何 byte sequence 都能 decode,一定會成功

    Args:
        raw_bytes: 檔案原始 bytes(不要先 decode)。

    Returns:
        第一個能 round-trip decode 的編碼名。
    """
    # utf-8-sig: 開頭 BOM
    if raw_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    # 純 utf-8
    try:
        raw_bytes.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        pass
    # 最後退路:latin-1 一定可以
    return "latin-1"


# --------------------------------------------------------------------------- #
# Network fetch
# --------------------------------------------------------------------------- #

def fetch_csv(
    url: str,
    *,
    max_retries: int = 3,
    timeout: int = 60,
    dest_dir: Optional[Path] = None,
) -> Tuple[Path, str]:
    """下載 Destatis CSV 到本地,回傳 ``(file_path, encoding)``。

    重試策略:最多 ``max_retries`` 次,指數 backoff(預設 2s / 4s / 8s)。
    觸發重試的條件:任何 ``requests.RequestException`` 或 HTTP status >= 500。

    Args:
        url: CSV 完整 URL。
        max_retries: 重試次數(預設 3)。
        timeout: 單次 request timeout(秒,預設 60)。
        dest_dir: 覆寫預設下載目錄;預設 ``/tmp/destatis_csv/``。

    Returns:
        ``(file_path, encoding)`` tuple。

    Raises:
        RuntimeError: 全部重試都失敗時。
    """
    target_dir = Path(dest_dir) if dest_dir else _DOWNLOAD_ROOT
    target_dir.mkdir(parents=True, exist_ok=True)

    last_error: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            logger.info(
                "[destatis] GET %s (attempt %d/%d, timeout=%ds)",
                url, attempt + 1, max_retries + 1, timeout,
            )
            resp = requests.get(
                url,
                headers={"User-Agent": _USER_AGENT, "Accept": "text/csv,*/*;q=0.8"},
                timeout=timeout,
                allow_redirects=True,
            )
            # 5xx 視為暫時錯誤,丟出去觸發重試
            if resp.status_code >= 500:
                raise RuntimeError(f"HTTP {resp.status_code} from {url}")
            # 4xx 不重試,直接 fail
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"HTTP {resp.status_code} from {url} (non-retriable)"
                )

            raw_bytes = resp.content
            encoding = detect_encoding(raw_bytes)
            text = raw_bytes.decode(encoding)

            # 從 URL 抽個安全的檔名
            slug = _slug_from_url(url)
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            file_path = target_dir / f"{slug}_{stamp}.csv"
            file_path.write_text(text, encoding="utf-8")

            logger.info(
                "[destatis] saved %s (%d bytes, encoding=%s)",
                file_path, len(raw_bytes), encoding,
            )
            return file_path, encoding

        except (requests.RequestException, RuntimeError) as e:
            last_error = e
            if attempt < max_retries:
                backoff = _RETRY_BACKOFFS_SEC[attempt]
                logger.warning(
                    "[destatis] attempt %d failed: %s — retry in %.1fs",
                    attempt + 1, e, backoff,
                )
                time.sleep(backoff)
            else:
                logger.error(
                    "[destatis] all %d attempts failed for %s: %s",
                    max_retries + 1, url, e,
                )

    raise RuntimeError(
        f"fetch_csv failed after {max_retries + 1} attempts for {url}: {last_error}"
    )


def _slug_from_url(url: str) -> str:
    """從 URL 抽檔名 slug。

    例: ``.../_Daten/auftragseingang-bauhauptgewerbe.csv?__blob=value&v=70``
    → ``auftragseingang-bauhauptgewerbe``
    """
    # 找最後一個 ``/`` 到第一個 ``?`` 或 ``.csv`` 之間
    path = url.split("?", 1)[0]
    fname = path.rsplit("/", 1)[-1]
    if fname.endswith(".csv"):
        fname = fname[:-4]
    # 把不合法字元換成底線
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in fname) or "destatis"


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

def parse_csv(file_path: Path, encoding: str) -> DestatisDataset:
    """把已下載的 CSV 解析為 :class:`DestatisDataset`。

    重要:Destatis 主站 highchart 用的 CSV **沒有 metadata rows**,
    row 0 直接就是 header。``csv.reader`` 預設逗號分隔,但這些 CSV 用
    ``;`` 分隔 + 雙引號包欄位,所以走 :class:`csv.DictReader` 走
    ``delimiter=";"`` + ``quoting=csv.QUOTE_ALL``。

    Args:
        file_path: 已下載的 CSV 檔路徑。
        encoding: 該檔案使用的編碼(由 :func:`detect_encoding` 給)。

    Returns:
        填好 header / rows / raw_text 的 :class:`DestatisDataset`
        (其餘欄位由 caller 從 source_config 補上)。
    """
    raw_text = Path(file_path).read_text(encoding=encoding)
    # 用 io.StringIO 餵給 csv module
    reader = csv.reader(
        io.StringIO(raw_text),
        delimiter=";",
        quotechar='"',
        quoting=csv.QUOTE_ALL,
    )
    rows: List[List[str]] = [row for row in reader if row]
    if not rows:
        raise ValueError(f"CSV is empty or unparseable: {file_path}")
    header = rows[0]
    return DestatisDataset(
        source_id="",
        name="",
        name_de="",
        name_zh="",
        reference_period="latest",
        fetched_at=datetime.now(timezone.utc).isoformat(),
        encoding=encoding,
        raw_text=raw_text,
        rows=rows,
        header=header,
        file_path=str(file_path),
        url="",
    )


# --------------------------------------------------------------------------- #
# High-level: source_config → DestatisDataset
# --------------------------------------------------------------------------- #

def fetch_and_parse(source_config: Dict[str, Any]) -> DestatisDataset:
    """從 source config dict 跑完整流程:下載 + 解析 + 補上 metadata。

    ``source_config`` 必含欄位:
      - ``id``: pipeline 內部 id(例 ``"auftragseingang_bauhauptgewerbe"``)
      - ``name_de``: 德文原名
      - ``name_zh``: 中文翻譯
      - ``url``: 完整 CSV URL

    選用欄位:
      - ``reference_period``: 預設 ``"latest"``
      - ``name``: 預設 == ``name_de``

    Returns:
        完整 :class:`DestatisDataset`,可直接餵給 renderer / vault writer。
    """
    source_id = source_config["id"]
    name_de = source_config["name_de"]
    name_zh = source_config.get("name_zh", name_de)
    url = source_config["url"]
    name = source_config.get("name", name_de)
    reference_period = source_config.get("reference_period", "latest")

    logger.info("[destatis] 開始抓取 %s (%s)", name_zh, source_id)
    file_path, encoding = fetch_csv(url)
    ds = parse_csv(file_path, encoding)

    # 補上 config 來的 metadata
    ds.source_id = source_id
    ds.name = name
    ds.name_de = name_de
    ds.name_zh = name_zh
    ds.reference_period = reference_period
    ds.url = url
    logger.info(
        "[destatis] 完成 %s: %d rows × %d cols, encoding=%s",
        source_id, len(ds.rows), len(ds.header), encoding,
    )
    return ds


# --------------------------------------------------------------------------- #
# CLI smoke test
# --------------------------------------------------------------------------- #

def _load_first_enabled_source() -> Optional[Dict[str, Any]]:
    """讀 ``pipeline/config/destatis_sources.json``,回傳第一個 enabled source。"""
    import json
    config_path = Path(__file__).resolve().parent.parent / "config" / "destatis_sources.json"
    if not config_path.exists():
        print(f"[destatis] config 不存在: {config_path}", flush=True)
        return None
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    for src in cfg.get("sources", []):
        if src.get("enabled", False):
            return src
    return None


if __name__ == "__main__":
    import json as _json

    src = _load_first_enabled_source()
    if src is None:
        raise SystemExit("no enabled source in destatis_sources.json")

    print(f"[destatis] 測試來源: {src['id']} — {src['name_zh']}")
    ds = fetch_and_parse(src)
    print("[destatis] 摘要:")
    print(_json.dumps(ds.summary(), ensure_ascii=False, indent=2))
