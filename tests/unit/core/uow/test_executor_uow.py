"""Unit tests for RuntimeExecutor UoW lifecycle integration."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.uow.abc import UnitOfWork, UnitOfWorkFactory
from loom.core.uow.context import _active_uow
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Helpers — mock UoW
# ---------------------------------------------------------------------------


def _make_uow_factory() -> tuple[MagicMock, MagicMock]:
    """Return (factory_mock, uow_mock) with async begin/commit/rollback."""
    uow = MagicMock(spec=UnitOfWork)
    uow.begin = AsyncMock()
    uow.commit = AsyncMock()
    uow.rollback = AsyncMock()
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=None)

    factory = MagicMock(spec=UnitOfWorkFactory)
    factory.create.return_value = uow

    return factory, uow


class _SimpleUC(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "ok"


class _FailingUC(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:
        raise RuntimeError("intentional failure")


def _compiler(*uc_types: type[UseCase[Any, Any]]) -> UseCaseCompiler:
    c = UseCaseCompiler()
    for t in uc_types:
        c.compile(t)
    return c


# ---------------------------------------------------------------------------
# No UoW factory — backward compatible path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_uow_factory_runs_without_transaction() -> None:
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler)  # no uow_factory
    uc = _SimpleUC()
    result = await executor.execute(uc)
    assert result == "ok"


# ---------------------------------------------------------------------------
# UoW factory present — lifecycle on success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uow_factory_creates_uow_per_execution() -> None:
    factory, uow = _make_uow_factory()
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    uc = _SimpleUC()
    await executor.execute(uc)

    factory.create.assert_called_once()


@pytest.mark.asyncio
async def test_uow_aenter_called_on_execution() -> None:
    factory, uow = _make_uow_factory()
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    await executor.execute(_SimpleUC())

    uow.__aenter__.assert_awaited_once()


@pytest.mark.asyncio
async def test_uow_aexit_called_with_no_exception_on_success() -> None:
    factory, uow = _make_uow_factory()
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    await executor.execute(_SimpleUC())

    uow.__aexit__.assert_awaited_once_with(None, None, None)


# ---------------------------------------------------------------------------
# UoW lifecycle on exception
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_uow_aexit_called_with_exception_on_failure() -> None:
    factory, uow = _make_uow_factory()
    # Make __aexit__ actually propagate the exception
    uow.__aexit__ = AsyncMock(return_value=None)
    compiler = _compiler(_FailingUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    with pytest.raises(RuntimeError, match="intentional"):
        await executor.execute(_FailingUC())

    # __aexit__ should have been called with the exception type
    call_args = uow.__aexit__.await_args
    assert call_args is not None
    exc_type = call_args.args[0]
    assert exc_type is RuntimeError


# ---------------------------------------------------------------------------
# Nesting: inner execute reuses outer UoW
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_nested_execution_does_not_open_second_uow() -> None:
    factory, uow = _make_uow_factory()
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    # Simulate nested: manually set the _active_uow ContextVar before calling
    token = _active_uow.set(uow)
    try:
        await executor.execute(_SimpleUC())
    finally:
        _active_uow.reset(token)

    # factory.create should NOT have been called — nested reuse
    factory.create.assert_not_called()


@pytest.mark.asyncio
async def test_active_uow_reset_after_execution() -> None:
    factory, uow = _make_uow_factory()
    compiler = _compiler(_SimpleUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    assert _active_uow.get() is None
    await executor.execute(_SimpleUC())
    assert _active_uow.get() is None


@pytest.mark.asyncio
async def test_active_uow_reset_after_failed_execution() -> None:
    factory, uow = _make_uow_factory()
    compiler = _compiler(_FailingUC)
    executor = RuntimeExecutor(compiler, uow_factory=factory)

    assert _active_uow.get() is None
    with pytest.raises(RuntimeError):
        await executor.execute(_FailingUC())
    assert _active_uow.get() is None


# ---------------------------------------------------------------------------
# 4.5 — LoadById with profile
# ---------------------------------------------------------------------------


class _EntityUC(UseCase[Any, str]):
    async def execute(self, entity_id: str, **kwargs: Any) -> str:
        return f"loaded:{entity_id}"


@pytest.mark.asyncio
async def test_load_default_profile_passed_to_repo() -> None:
    from loom.core.use_case.markers import LoadById
    from loom.core.use_case.use_case import UseCase

    class MyEntity:
        pass

    class LoadUC(UseCase[Any, str]):
        async def execute(
            self,
            entity_id: str,
            entity: MyEntity = LoadById(MyEntity, by="entity_id"),
        ) -> str:
            return "ok"

    repo = MagicMock()
    repo.get_by_id = AsyncMock(return_value=MyEntity())

    compiler = _compiler(LoadUC)
    executor = RuntimeExecutor(compiler)
    await executor.execute(
        LoadUC(),
        params={"entity_id": "42"},
        dependencies={MyEntity: repo},
    )
    repo.get_by_id.assert_awaited_once_with("42", profile="default")


@pytest.mark.asyncio
async def test_load_custom_profile_passed_to_repo() -> None:
    from loom.core.use_case.markers import LoadById
    from loom.core.use_case.use_case import UseCase

    class MyEntity:
        pass

    class LoadUC(UseCase[Any, str]):
        async def execute(
            self,
            entity_id: str,
            entity: MyEntity = LoadById(MyEntity, by="entity_id", profile="detail"),
        ) -> str:
            return "ok"

    repo = MagicMock()
    repo.get_by_id = AsyncMock(return_value=MyEntity())

    compiler = _compiler(LoadUC)
    executor = RuntimeExecutor(compiler)
    await executor.execute(
        LoadUC(),
        params={"entity_id": "7"},
        dependencies={MyEntity: repo},
    )
    repo.get_by_id.assert_awaited_once_with("7", profile="detail")
