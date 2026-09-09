"""The ``Mcp()`` marker: resolved to a handle closed over the verified caller.

Mirrors ``test_agent_marker.py`` — same shape of coverage, one level over:
a use case declares ``Mcp("server", include=[...])``, the compiler records
where it landed, and the executor resolves it through a callable the AI
pillar supplies, never through a parameter the caller could pass. Unlike
``Agent()``, ``bind_mcp_resolver`` is a second, differently-typed method
(T301): the agent resolver is ``(name, identity)``, the MCP one is
``(server, include, identity)``, so they cannot share one callable without
a discriminant argument the project's rules forbid.
"""

from __future__ import annotations

from typing import Any

import pytest

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import Unauthenticated
from loom.core.identity import ANONYMOUS, Identity
from loom.core.use_case.markers import Caller, Mcp
from loom.core.use_case.use_case import UseCase

_ALICE = Identity(subject="alice", mechanism="test")


class _FakeHandle:
    """Stands in for an ``McpHandle`` without pulling in the AI pillar."""

    def __init__(self, server: str, include: tuple[str, ...], identity: Identity) -> None:
        self.server = server
        self.include = include
        self.identity = identity


class DocsSearchUseCase(UseCase[object, tuple[str, str]]):
    """Declares one Mcp() marker alongside the caller."""

    async def execute(
        self,
        caller: Identity = Caller(),
        docs: Any = Mcp("docs-server", include=["search"]),
    ) -> tuple[str, str]:
        return caller.subject, docs.server


class TwoServersUseCase(UseCase[object, str]):
    """Two distinct named MCP servers in one signature."""

    async def execute(
        self,
        first: Any = Mcp("docs-server", include=["search"]),
        second: Any = Mcp("billing-server", include=["lookup"]),
    ) -> str:
        return f"{first.server}:{second.server}"


class McpOnlyUseCase(UseCase[object, str]):
    """Declares Mcp() and no Caller(): nothing shadows ``_bind_mcp``'s own guard.

    ``DocsSearchUseCase`` above declares both markers, so its own missing-identity
    test is refused by ``_bind_caller`` before ``_bind_mcp`` is ever reached — this
    use case is the one whose refusal can only come from ``_bind_mcp`` itself.
    """

    async def execute(self, docs: Any = Mcp("docs-server", include=["search"])) -> str:
        return docs.server


class NoMcpUseCase(UseCase[object, str]):
    """Declares no Mcp() marker at all."""

    async def execute(self) -> str:
        return "no servers here"


def _resolver(calls: list[tuple[str, tuple[str, ...], Identity]]) -> Any:
    def _resolve(server: str, include: tuple[str, ...], identity: Identity) -> _FakeHandle:
        calls.append((server, include, identity))
        return _FakeHandle(server, include, identity)

    return _resolve


# ---------------------------------------------------------------------------
# Compilation
# ---------------------------------------------------------------------------


def test_the_marker_compiles_into_an_mcp_binding() -> None:
    plan = UseCaseCompiler().compile(DocsSearchUseCase)
    assert [mb.name for mb in plan.mcp_bindings] == ["docs"]
    assert plan.mcp_bindings[0].server == "docs-server"
    assert plan.mcp_bindings[0].include == ("search",)


def test_a_use_case_without_the_marker_has_no_mcp_bindings() -> None:
    plan = UseCaseCompiler().compile(NoMcpUseCase)
    assert plan.mcp_bindings == ()


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


async def test_the_resolved_handle_is_bound_to_the_verified_caller() -> None:
    calls: list[tuple[str, tuple[str, ...], Identity]] = []
    compiler = UseCaseCompiler()
    compiler.compile(DocsSearchUseCase)
    executor = RuntimeExecutor(compiler)
    executor.bind_mcp_resolver(_resolver(calls))

    subject, server = await executor.execute(DocsSearchUseCase(), identity=_ALICE)

    assert subject == "alice"
    assert server == "docs-server"
    assert calls == [("docs-server", ("search",), _ALICE)]


async def test_several_mcp_markers_each_resolve_their_own_server() -> None:
    calls: list[tuple[str, tuple[str, ...], Identity]] = []
    compiler = UseCaseCompiler()
    compiler.compile(TwoServersUseCase)
    executor = RuntimeExecutor(compiler)
    executor.bind_mcp_resolver(_resolver(calls))

    result = await executor.execute(TwoServersUseCase(), identity=_ALICE)

    assert result == "docs-server:billing-server"
    assert calls == [
        ("docs-server", ("search",), _ALICE),
        ("billing-server", ("lookup",), _ALICE),
    ]


async def test_an_anonymous_identity_still_resolves_a_handle() -> None:
    """Binding succeeds even for ANONYMOUS: the handle itself refuses it, not the executor."""
    calls: list[tuple[str, tuple[str, ...], Identity]] = []
    compiler = UseCaseCompiler()
    compiler.compile(DocsSearchUseCase)
    executor = RuntimeExecutor(compiler)
    executor.bind_mcp_resolver(_resolver(calls))

    subject, _ = await executor.execute(DocsSearchUseCase(), identity=ANONYMOUS)

    assert subject == ""
    assert calls == [("docs-server", ("search",), ANONYMOUS)]


async def test_a_missing_identity_is_refused_instead_of_defaulted() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(DocsSearchUseCase)
    executor = RuntimeExecutor(compiler)
    executor.bind_mcp_resolver(_resolver([]))

    with pytest.raises(Unauthenticated, match="DocsSearchUseCase"):
        await executor.execute(DocsSearchUseCase())


async def test_a_missing_identity_is_refused_by_bind_mcp_itself() -> None:
    """Pins ``_bind_mcp``'s own guard, unshadowed by ``_bind_caller``'s.

    ``DocsSearchUseCase`` also declares ``Caller()``, so its own missing-identity
    test above is refused before ``_bind_mcp`` runs at all; mutating away
    ``_bind_mcp``'s guard would not turn that test red. ``McpOnlyUseCase``
    declares no ``Caller()``, so this is the one that reaches it.
    """
    compiler = UseCaseCompiler()
    compiler.compile(McpOnlyUseCase)
    executor = RuntimeExecutor(compiler)
    calls: list[tuple[str, tuple[str, ...], Identity]] = []
    executor.bind_mcp_resolver(_resolver(calls))

    with pytest.raises(Unauthenticated, match=r"McpOnlyUseCase.*Mcp\(\) parameter"):
        await executor.execute(McpOnlyUseCase())

    assert calls == []


async def test_no_resolver_bound_fails_with_a_named_error() -> None:
    compiler = UseCaseCompiler()
    compiler.compile(DocsSearchUseCase)
    executor = RuntimeExecutor(compiler)

    with pytest.raises(RuntimeError, match="bind_mcp_resolver"):
        await executor.execute(DocsSearchUseCase(), identity=_ALICE)


async def test_binding_a_resolver_twice_is_refused() -> None:
    executor = RuntimeExecutor(UseCaseCompiler())
    executor.bind_mcp_resolver(_resolver([]))

    with pytest.raises(RuntimeError):
        executor.bind_mcp_resolver(_resolver([]))


async def test_a_use_case_with_no_mcp_marker_never_calls_the_resolver() -> None:
    calls: list[tuple[str, tuple[str, ...], Identity]] = []
    compiler = UseCaseCompiler()
    compiler.compile(NoMcpUseCase)
    executor = RuntimeExecutor(compiler)
    executor.bind_mcp_resolver(_resolver(calls))

    result = await executor.execute(NoMcpUseCase(), identity=_ALICE)

    assert result == "no servers here"
    assert calls == []


async def test_a_use_case_with_no_mcp_marker_needs_no_identity_either() -> None:
    """The 'no marker' guard must run before the identity check, not after it.

    Mutating ``_bind_mcp`` to drop this guard (or to reorder it below the
    identity check) turns a plan with no ``Mcp()`` at all into one that
    demands an identity it never declared a use for.
    """
    compiler = UseCaseCompiler()
    compiler.compile(NoMcpUseCase)
    executor = RuntimeExecutor(compiler)
    executor.bind_mcp_resolver(_resolver([]))

    result = await executor.execute(NoMcpUseCase())

    assert result == "no servers here"
