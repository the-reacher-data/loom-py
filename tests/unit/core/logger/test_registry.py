from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from pytest import fixture

from loom.core.logger import configure_logger_factory, get_logger, reset_logger_factory
from loom.core.logger.abc import LoggerPort
from loom.core.logger.structlogger import StructLogger


class _FakeLogger(LoggerPort):
    def __init__(self, name: str) -> None:
        self.name = name
        self.events: list[tuple[str, str, dict[str, Any]]] = []

    def bind(self, **fields: Any) -> LoggerPort:
        _ = fields
        return self

    def debug(self, event: str, **fields: Any) -> None:
        self.events.append(("debug", event, fields))

    def info(self, event: str, **fields: Any) -> None:
        self.events.append(("info", event, fields))

    def warning(self, event: str, **fields: Any) -> None:
        self.events.append(("warning", event, fields))

    def error(self, event: str, **fields: Any) -> None:
        self.events.append(("error", event, fields))

    def exception(self, event: str, **fields: Any) -> None:
        self.events.append(("exception", event, fields))


@fixture(autouse=True)
def reset_factory() -> Iterator[None]:
    reset_logger_factory()
    yield
    reset_logger_factory()


class TestLoggerRegistry:
    def test_returns_struct_logger_by_default(self) -> None:
        logger = get_logger("loom.default")
        assert isinstance(logger, StructLogger)

    def test_uses_custom_factory(self) -> None:
        def factory(name: str) -> LoggerPort:
            return _FakeLogger(name)

        configure_logger_factory(factory)
        logger = get_logger("loom.custom")

        assert isinstance(logger, _FakeLogger)
        assert logger.name == "loom.custom"
