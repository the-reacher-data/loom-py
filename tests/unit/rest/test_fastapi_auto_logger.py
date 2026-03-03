from __future__ import annotations

from collections.abc import Iterator

import pytest

from loom.core.logger import (
    StructLogger,
    configure_logging_from_values,
    get_logger,
    reset_logger_factory,
)


@pytest.fixture(autouse=True)
def reset_factory() -> Iterator[None]:
    reset_logger_factory()
    yield
    reset_logger_factory()


def test_configure_logger_uses_structlog_backend_by_default() -> None:
    configure_logging_from_values()
    logger = get_logger("loom.struct")
    assert isinstance(logger, StructLogger)


def test_configure_logger_uses_json_renderer() -> None:
    configure_logging_from_values(renderer="json", colors=False, level="DEBUG")
    logger = get_logger("loom.struct.json")
    assert isinstance(logger, StructLogger)


def test_configure_logger_rejects_unknown_renderer() -> None:
    with pytest.raises(ValueError, match="Unsupported renderer"):
        configure_logging_from_values(renderer="unknown")


def test_configure_logger_prod_environment_defaults_to_json() -> None:
    configure_logging_from_values(environment="prod")
    logger = get_logger("loom.struct.prod")
    assert isinstance(logger, StructLogger)


def test_configure_logger_unknown_environment_falls_back_to_dev() -> None:
    configure_logging_from_values(environment="staging")
    logger = get_logger("loom.struct.staging")
    assert isinstance(logger, StructLogger)
