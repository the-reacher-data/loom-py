"""Unit tests for ApplicationInvoker and UseCaseRegistry."""

from __future__ import annotations

from typing import Any

import msgspec
import pytest

from loom.core.bootstrap import create_kernel
from loom.core.command import Command
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case import Input, UseCase
from loom.core.use_case.invoker import ApplicationInvoker
from loom.core.use_case.keys import use_case_key
from loom.core.use_case.registry import UseCaseRegistry
from loom.rest.autocrud import _get_or_create


class _Item(BaseModel):
    __tablename__ = "test_invoker_item"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField()


class _EchoInput(Command):
    value: str


class _Repo:
    def __init__(self) -> None:
        self._next = 1
        self._data: dict[int, _Item] = {}

    async def create(self, cmd: Any) -> _Item:
        data = msgspec.to_builtins(cmd)
        item = _Item(id=self._next, name=str(data.get("name", "")))
        self._data[item.id] = item
        self._next += 1
        return item

    async def get_by_id(self, id_value: Any, profile: str = "default") -> _Item | None:
        del profile
        return self._data.get(int(id_value))

    async def update(self, id_value: Any, cmd: Any) -> _Item | None:
        item = self._data.get(int(id_value))
        if item is None:
            return None
        data = msgspec.to_builtins(cmd)
        name = data.get("name", item.name)
        updated = _Item(id=item.id, name=name)
        self._data[item.id] = updated
        return updated

    async def delete(self, id_value: Any) -> bool:
        return self._data.pop(int(id_value), None) is not None

    async def exists_by(self, field: str, value: Any) -> bool:
        if field != "id":
            return False
        return int(value) in self._data


@use_case_key("demo.echo")
class _EchoUseCase(UseCase[Any, str]):
    async def execute(self, cmd: _EchoInput = Input()) -> str:
        return cmd.value


@use_case_key("demo.dup")
class _DupA(UseCase[Any, str]):
    async def execute(self) -> str:
        return "a"


@use_case_key("demo.dup")
class _DupB(UseCase[Any, str]):
    async def execute(self) -> str:
        return "b"


def _module_for(repo: _Repo) -> Any:
    def _module(container: LoomContainer) -> None:
        container.register(_Repo, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(_Item, _Repo)

    return _module


def test_registry_build_raises_on_duplicate_keys() -> None:
    with pytest.raises(ValueError, match="Duplicate use-case key"):
        UseCaseRegistry.build([_DupA, _DupB])


@pytest.mark.asyncio
async def test_invoke_and_invoke_name_work_with_registry() -> None:
    repo = _Repo()
    runtime = create_kernel(
        config=object(),
        use_cases=[_EchoUseCase],
        modules=[_module_for(repo)],
    )

    result_by_type = await runtime.app.invoke(_EchoUseCase, payload={"value": "ok"})
    result_by_name = await runtime.app.invoke_name("demo.echo", payload={"value": "ok2"})

    assert result_by_type == "ok"
    assert result_by_name == "ok2"
    assert runtime.registry.has("demo.echo")


@pytest.mark.asyncio
async def test_entity_facade_invokes_autocrud_create_and_update() -> None:
    repo = _Repo()
    auto = _get_or_create(_Item)
    runtime = create_kernel(
        config=object(),
        use_cases=[auto["create"], auto["update"], auto["get"]],
        modules=[_module_for(repo)],
    )

    app = runtime.container.resolve(ApplicationInvoker)
    entity = app.entity(_Item)

    created = await entity.create(payload={"name": "first"})
    assert created.name == "first"

    updated = await entity.update(params={"id": created.id}, payload={"name": "second"})
    assert updated.name == "second"

    loaded = await entity.get(params={"id": created.id})
    assert loaded.name == "second"


@pytest.mark.asyncio
async def test_invoke_name_raises_for_missing_key() -> None:
    runtime = create_kernel(config=object(), use_cases=[_EchoUseCase])
    with pytest.raises(KeyError, match="not registered"):
        await runtime.app.invoke_name("missing:key", payload={})
