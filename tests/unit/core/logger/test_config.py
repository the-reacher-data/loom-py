from __future__ import annotations

from typing import Any

import pytest

from loom.core.logger.config import configure_logging_from_values


def test_configure_logging_from_values_forwards_extra_processors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_configure(config: Any) -> None:
        captured["config"] = config

    sentinel = object()
    monkeypatch.setattr("loom.core.logger.config.configure_logging", _fake_configure)

    configure_logging_from_values(extra_processors=(sentinel,))

    config = captured["config"]
    assert config.extra_processors == (sentinel,)
