"""The ``Mcp()`` marker at execution time: fail-loud, not silently bound.

No MCP resolver exists on the executor yet — resolving a binding into a
live ``McpHandle`` is future work (T301). Without a guard, ``execute``
would receive Python's own default-argument value for the unbound
parameter: the raw ``_McpMarker`` object, which only fails once the use
case calls a handle method on it, with an opaque ``AttributeError`` that
never mentions loom. This mirrors ``test_agent_marker.py``'s
``test_no_resolver_bound_fails_with_a_named_error``, one level earlier:
``Agent()`` has a resolver to fail without; ``Mcp()`` has none to bind at
all yet, so it always fails.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.identity import Identity
from loom.core.use_case.markers import Caller, Mcp
from loom.core.use_case.use_case import UseCase

_ALICE = Identity(subject="alice", mechanism="test")


class DocsSearchUseCase(UseCase[object, str]):
    """Declares one Mcp() marker alongside the caller."""

    async def execute(
        self,
        caller: Identity = Caller(),
        docs: Any = Mcp("docs-server", include=["search"]),
    ) -> str:
        return docs.server


class TwoServersUseCase(UseCase[object, str]):
    """Two distinct named MCP servers in one signature."""

    async def execute(
        self,
        first: Any = Mcp("docs-server", include=["search"]),
        second: Any = Mcp("billing-server", include=["lookup"]),
    ) -> str:
        return f"{first.server}:{second.server}"


class NoMcpUseCase(UseCase[object, str]):
    """Declares no Mcp() marker at all."""

    async def execute(self) -> str:
        return "no servers here"


async def test_a_declared_mcp_marker_fails_loud_with_a_named_error() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(DocsSearchUseCase)
    executor = RuntimeExecutor(compiler)

    with pytest.raises(RuntimeError, match="DocsSearchUseCase.*docs.*remove the marker"):
        await executor.execute(DocsSearchUseCase(), identity=_ALICE)


async def test_the_error_names_every_declared_mcp_parameter() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(TwoServersUseCase)
    executor = RuntimeExecutor(compiler)

    with pytest.raises(RuntimeError, match="first, second"):
        await executor.execute(TwoServersUseCase(), identity=_ALICE)


async def test_a_use_case_with_no_mcp_marker_is_unaffected() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(NoMcpUseCase)
    executor = RuntimeExecutor(compiler)

    result = await executor.execute(NoMcpUseCase(), identity=_ALICE)

    assert result == "no servers here"
