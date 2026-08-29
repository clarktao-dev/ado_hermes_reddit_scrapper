"""Mock-test the anti-stub guard in step_structure_short (no unittest.mock)."""
import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[2]))

from pipeline.youtube_daily import step_structure_short
from pipeline.lib.youtube_fetch import VideoMeta


def make_video():
    return VideoMeta(
        id="3CSs8GoVnOk",
        title="Darauf kommt es bei Immos an",
        duration_sec=2017,
        epoch=1756272000,
        url="https://www.youtube.com/watch?v=3CSs8GoVnOk",
        channel_id="UCv7QkpJ5IAYMe7ws_jX4tyQ",
        channel_name="Finanztip",
    )


GOOD_INPUT = (
    "本影片探討購買德國房地產時最關鍵的指標:除了購買價格外,買家更應該看「乘數」與「淨收益率」。"
    "影片舉例 80 平方公尺、30 萬歐元的公寓在不同地區的差異,說明為何同一個價格在慕尼黑可能是超值、"
    "在鄉村卻可能是昂貴。"
)

STUB_SUMMARY = (
    "## 一句話摘要\n\n"
    "影片內容因伺服器錯誤無法取得,無法提供實質摘要。\n\n"
    "## 重點 bullets\n\n- 無\n\n"
    "## 觀點\n\n此影片目前無法觀看,建議待平臺修復後再行評估。\n\n"
    "## 重點詞彙\n\n"
)

HEALTHY_SUMMARY = (
    "## 一句話摘要\n\n"
    "影片指出德國買房除了看購買價格外,更應關注乘數與淨收益率,以 80 平方公尺 30 萬歐元的公寓"
    "在不同地區差異為例,強調位置比價格更重要。\n\n"
    "## 重點 bullets\n\n"
    "- 買房應看乘數與淨收益率,而非單純購買價\n"
    "- 同一價格在慕尼黑可能是超值、在鄉村可能是昂貴\n"
    "- 投資前應先計算年租金收入與維護成本\n\n"
    "## 觀點\n\n對台灣投資人而言,德國租金收益穩定但需注意地區差異。\n\n"
    "## 重點詞彙\n\n"
    "- **Multiplikator(乘數)**:買價除以年淨租金\n"
)

# A *realistic* stub marker (verbatim from 8/27 alexanderschmid vault)
REAL_STUB_MARKER = "因影片伺服器錯誤,未取得實際內容,無法提供具體摘要。"
STUB_SUMMARY_REAL = (
    f"## 一句話摘要\n\n{REAL_STUB_MARKER}\n\n"
    "## 重點 bullets\n\n- 無法取得影片文字稿,內容未知。\n\n"
    "## 觀點\n\n建議等待影片正常上傳或尋找其他來源的完整講稿。\n\n"
    "## 重點詞彙\n\n"
)


class MockLLM:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def __call__(self, user, llm_timeout):
        self.calls.append(user)
        if not self.responses:
            raise RuntimeError("MockLLM: no more responses queued")
        return self.responses.pop(0)


class _PatchStep:
    """Context manager that patches pipeline.youtube_daily._call_llm_short."""
    def __init__(self, mock_llm):
        import pipeline.youtube_daily as m
        self.mod = m
        self.replacement = mock_llm
        self.orig = None

    def __enter__(self):
        self.orig = self.mod._call_llm_short
        self.mod._call_llm_short = self.replacement
        return self

    def __exit__(self, *exc):
        self.mod._call_llm_short = self.orig


def case_1_healthy_first_try():
    print("Case 1: healthy first try → no retry, return as-is")
    mock = MockLLM([HEALTHY_SUMMARY])
    with _PatchStep(mock):
        out = step_structure_short(make_video(), GOOD_INPUT, llm_timeout=10)
    print(f"  call count: {len(mock.calls)}")
    print(f"  summary[:80]: {out['summary_zh'][:80]}...")
    assert len(mock.calls) == 1, f"expected 1 call, got {len(mock.calls)}"
    assert "位置比價格更重要" in out["summary_zh"], f"healthy summary not returned: {out['summary_zh']!r}"
    print("  ✓ PASS\n")


def case_2_stub_then_healthy():
    print("Case 2: stub first → retry → healthy → return retry")
    mock = MockLLM([STUB_SUMMARY, HEALTHY_SUMMARY])
    with _PatchStep(mock):
        out = step_structure_short(make_video(), GOOD_INPUT, llm_timeout=10)
    print(f"  call count: {len(mock.calls)}")
    print(f"  summary[:80]: {out['summary_zh'][:80]}...")
    assert len(mock.calls) == 2, f"expected 2 calls, got {len(mock.calls)}"
    assert "位置比價格更重要" in out["summary_zh"], f"retry not used: {out['summary_zh']!r}"
    print("  ✓ PASS\n")


def case_3_triple_stub_downgrades():
    print("Case 3: stub + stub + stub → downgrade to (內容待補) after 3 calls")
    mock = MockLLM([STUB_SUMMARY, STUB_SUMMARY, STUB_SUMMARY])
    with _PatchStep(mock):
        out = step_structure_short(make_video(), GOOD_INPUT, llm_timeout=10)
    print(f"  call count: {len(mock.calls)}")
    print(f"  summary: {out['summary_zh']}")
    assert len(mock.calls) == 3
    assert "內容待補" in out["summary_zh"], f"expected downgrade: {out['summary_zh']!r}"
    assert "三次" in out["summary_zh"]
    assert out["analyst_zh"] == "(無)"
    print("  ✓ PASS\n")


def case_4_refuse_marker():
    print("Case 4: stub → __REFUSE__ → downgrade")
    mock = MockLLM([STUB_SUMMARY, "__REFUSE__"])
    with _PatchStep(mock):
        out = step_structure_short(make_video(), GOOD_INPUT, llm_timeout=10)
    print(f"  call count: {len(mock.calls)}")
    print(f"  summary: {out['summary_zh']}")
    assert len(mock.calls) == 2
    assert "內容待補" in out["summary_zh"], f"expected downgrade on refuse: {out['summary_zh']!r}"
    print("  ✓ PASS\n")


def case_5_empty_input_no_retry():
    print("Case 5: empty input + stub → no retry (gate only fires with real content)")
    mock = MockLLM([STUB_SUMMARY])
    with _PatchStep(mock):
        out = step_structure_short(make_video(), "", llm_timeout=10)
    print(f"  call count: {len(mock.calls)}")
    print(f"  summary: {out['summary_zh'][:80]}")
    assert len(mock.calls) == 1, f"expected 1 call (no retry on empty), got {len(mock.calls)}"
    print("  ✓ PASS\n")


def case_6_llm_error():
    print("Case 6: LLM call returns None (simulating timeout/connection failure) → return empty")
    def return_none(user, llm_timeout):
        # _call_llm_short returns None on failure; simulate that boundary
        return None
    with _PatchStep(return_none):
        out = step_structure_short(make_video(), GOOD_INPUT, llm_timeout=10)
    print(f"  summary: {out['summary_zh']!r}")
    assert out["summary_zh"] == ""
    print("  ✓ PASS\n")


def case_7_real_stub_marker():
    """Use the *exact* stub string from the 8/27 buggy cron output."""
    print("Case 7: realistic stub marker (verbatim from 8/27 vault)")
    mock = MockLLM([STUB_SUMMARY_REAL, HEALTHY_SUMMARY])
    with _PatchStep(mock):
        out = step_structure_short(make_video(), GOOD_INPUT, llm_timeout=10)
    print(f"  call count: {len(mock.calls)}")
    print(f"  summary: {out['summary_zh'][:80]}")
    assert len(mock.calls) == 2, "stub not detected, no retry triggered"
    assert "位置比價格更重要" in out["summary_zh"]
    print("  ✓ PASS\n")


def case_8_stub_stub_then_healthy():
    print("Case 8: stub + stub + healthy → third attempt succeeds")
    mock = MockLLM([STUB_SUMMARY, STUB_SUMMARY, HEALTHY_SUMMARY])
    with _PatchStep(mock):
        out = step_structure_short(make_video(), GOOD_INPUT, llm_timeout=10)
    print(f"  call count: {len(mock.calls)}")
    assert len(mock.calls) == 3
    assert "位置比價格更重要" in out["summary_zh"]
    print("  ✓ PASS\n")


if __name__ == "__main__":
    case_1_healthy_first_try()
    case_2_stub_then_healthy()
    case_3_triple_stub_downgrades()
    case_4_refuse_marker()
    case_5_empty_input_no_retry()
    case_6_llm_error()
    case_7_real_stub_marker()
    case_8_stub_stub_then_healthy()
    print("=" * 60)
    print("ALL 8 CASES PASSED")
