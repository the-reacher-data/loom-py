from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.logger import ColorLogger, StdLogger, configure_logger_factory, get_logger
from loom.rest.fastapi.auto import _configure_logger, _LoggerConfig


@pytest.fixture(autouse=True)
def reset_logger_factory() -> Iterator[None]:
    configure_logger_factory(None)
    yield
    configure_logger_factory(None)


def test_configure_logger_uses_color_backend_by_default() -> None:
    _configure_logger(_LoggerConfig())
    logger = get_logger("loom.color")
    assert isinstance(logger, ColorLogger)


def test_configure_logger_can_use_std_backend() -> None:
    _configure_logger(_LoggerConfig(backend="std"))
    logger = get_logger("loom.std")
    assert isinstance(logger, StdLogger)


def test_configure_logger_rejects_unknown_backend() -> None:
    with pytest.raises(ValueError, match="Unsupported logger backend"):
        _configure_logger(_LoggerConfig(backend="unknown"))
