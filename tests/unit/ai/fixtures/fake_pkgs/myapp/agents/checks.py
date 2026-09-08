"""Output checks referenced by ``output-check-agent/agent.yaml`` and the phase tests."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

NOT_CALLABLE: int = 42
"""Resolves fine, but is not callable, so it cannot satisfy ``OutputCheck``."""


def report_is_complete(answer: Mapping[str, Any]) -> str | None:
    """Reject a report that claims resolution without naming the cause.

    Args:
        answer: The answer as the engine parsed it, before loom decodes it.

    Returns:
        ``None`` to accept, or the text the model must read to correct itself.
    """
    if answer.get("resolved") and answer.get("root_cause_id") is None:
        return "resolved reports must name a root cause; query it and answer again"
    return None


async def report_is_complete_async(answer: Mapping[str, Any]) -> str | None:
    """Same rule as a coroutine function: the contract refuses it at compile time."""
    return report_is_complete(answer)
