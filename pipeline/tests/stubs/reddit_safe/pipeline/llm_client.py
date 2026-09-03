"""Minimal stub of reddit_safe.pipeline.llm_client for unit tests."""


class LLMError(Exception):
    pass


def call_json(messages, timeout=60, temperature=0.3):
    raise LLMError("stub llm_client: no backend in test env")


def call_text(messages, timeout=60, temperature=0.3):
    raise LLMError("stub llm_client: no backend in test env")
