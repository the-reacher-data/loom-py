from __future__ import annotations

from collections.abc import Iterator

import pytest
from fastapi import FastAPI

from loom.core.logger import (
    StructLogger,
    configure_logging_from_values,
    get_logger,
    reset_logger_factory,
)
from loom.rest.fastapi.auto import (
    _DatabaseConfig,
    _resolve_effective_echo,
    _TraceConfig,
)
from loom.rest.middleware import TraceIdMiddleware


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


# ---------------------------------------------------------------------------
# _TraceConfig defaults
# ---------------------------------------------------------------------------


def test_trace_default_enabled() -> None:
    cfg = _TraceConfig()
    assert cfg.enabled is True
    assert cfg.header == "x-request-id"


def test_trace_explicit_disabled() -> None:
    cfg = _TraceConfig(enabled=False)
    assert cfg.enabled is False


# ---------------------------------------------------------------------------
# _resolve_effective_echo
# ---------------------------------------------------------------------------


def test_trace_enabled_inherits_echo() -> None:
    trace_cfg = _TraceConfig(enabled=True)
    db_cfg = _DatabaseConfig(url="sqlite+aiosqlite:///")
    assert _resolve_effective_echo(db_cfg, trace_cfg) is True


def test_trace_disabled_inherits_echo() -> None:
    trace_cfg = _TraceConfig(enabled=False)
    db_cfg = _DatabaseConfig(url="sqlite+aiosqlite:///")
    assert _resolve_effective_echo(db_cfg, trace_cfg) is False


def test_database_echo_explicit_false_overrides_trace() -> None:
    trace_cfg = _TraceConfig(enabled=True)
    db_cfg = _DatabaseConfig(url="sqlite+aiosqlite:///", echo=False)
    assert _resolve_effective_echo(db_cfg, trace_cfg) is False


def test_database_echo_explicit_true_overrides_trace_disabled() -> None:
    trace_cfg = _TraceConfig(enabled=False)
    db_cfg = _DatabaseConfig(url="sqlite+aiosqlite:///", echo=True)
    assert _resolve_effective_echo(db_cfg, trace_cfg) is True


def test_database_echo_default_is_none() -> None:
    db_cfg = _DatabaseConfig(url="sqlite+aiosqlite:///")
    assert db_cfg.echo is None


# ---------------------------------------------------------------------------
# TraceIdMiddleware presence based on trace config
# ---------------------------------------------------------------------------


def test_trace_enabled_adds_trace_middleware() -> None:
    trace_cfg = _TraceConfig(enabled=True, header="x-request-id")
    app = FastAPI()
    if trace_cfg.enabled:
        app.add_middleware(TraceIdMiddleware, header=trace_cfg.header)
    classes = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware in classes


def test_trace_disabled_no_trace_middleware() -> None:
    trace_cfg = _TraceConfig(enabled=False)
    app = FastAPI()
    if trace_cfg.enabled:
        app.add_middleware(TraceIdMiddleware, header=trace_cfg.header)
    classes = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware not in classes
