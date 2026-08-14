"""Shared LLM-call throttle for ollama-cloud.

Context manager that enforces a minimum interval between sequential LLM calls
and applies an adaptive longer pause once a high call volume is reached.

Verified 2026-08-07 against ollama-cloud: 3s pre-call #8 keeps latency
predictable; 8s from call #8 onward prevents the N+7-hang pattern documented
in skill `batch-llm-agent-step` pitfall 7.

Public API:
    LLMCooldown            context manager (preferred)
    _MIN_INTERVAL_S, _ADAPTIVE_AFTER, _ADAPTIVE_INTERVAL_S  exported for
                            back-compat with translate.py callers that read
                            the constants directly.

Usage:
    from pipeline.lib.llm_cooldown import LLMCooldown

    with LLMCooldown():
        parsed, usage = call_json(messages, temperature=0.0)
"""
from __future__ import annotations

import time

_MIN_INTERVAL_S: float = 3.0
_ADAPTIVE_AFTER: int = 8
_ADAPTIVE_INTERVAL_S: float = 8.0

# Shared rolling log of recent call timestamps. Bounded to avoid memory growth
# across long-lived processes (matches translate.py's original behavior).
_call_log: list[float] = []


class LLMCooldown:
    """Context manager that paces sequential LLM calls.

    Sleeps on entry if the previous call was too recent, then records this
    call's timestamp on exit. After `_ADAPTIVE_AFTER` cumulative calls the
    interval jumps from `_MIN_INTERVAL_S` to `_ADAPTIVE_INTERVAL_S` to give
    ollama-cloud room to drain its request queue.
    """

    def __enter__(self) -> "LLMCooldown":
        if _call_log:
            target = (
                _ADAPTIVE_INTERVAL_S
                if len(_call_log) >= _ADAPTIVE_AFTER
                else _MIN_INTERVAL_S
            )
            elapsed = time.time() - _call_log[-1]
            if elapsed < target:
                time.sleep(target - elapsed)
        return self

    def __exit__(self, *exc: object) -> None:
        _call_log.append(time.time())
        if len(_call_log) > 64:
            del _call_log[:32]


__all__ = ["LLMCooldown", "_MIN_INTERVAL_S", "_ADAPTIVE_AFTER", "_ADAPTIVE_INTERVAL_S"]
