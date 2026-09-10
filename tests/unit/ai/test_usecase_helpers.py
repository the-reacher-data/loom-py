"""Contracts of the engine-free use-case helpers in ``loom.ai._usecase``.

``require_invoker`` reads the bound invoker structurally from a dependency
bundle and refuses a bundle that carries none; ``invoke_as`` runs a use case
as a given caller, installing the ambient identity only for the duration of
the call and forwarding ``params``/``payload`` untouched.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, cast

import pytest

from loom.ai._usecase import invoke_as, require_invoker
from loom.ai.errors import AgentRunError, AgentRunErrorCode
from loom.core.engine.compilable import Compilable
from loom.core.identity import ANONYMOUS, Identity, current_identity
from loom.core.use_case.invoker import ApplicationInvoker, EntityInvoker


@dataclass
class _RecordingInvoker:
    """Fake invoker that records what it saw, including the ambient identity."""

    calls: list[dict[str, Any]] = field(default_factory=list)

    async def invoke(
        self,
        use_case: type[Compilable],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        self.calls.append(
            {
                "use_case": use_case,
                "params": params,
                "payload": payload,
                "identity": current_identity(),
            }
        )
        return "result"

    async def invoke_name(
        self,
        key: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        raise NotImplementedError

    def entity(self, model: type[Any]) -> EntityInvoker:
        raise NotImplementedError


@dataclass(frozen=True)
class _Bundle:
    invoker: object


class _UseCase:
    """Stand-in use case type; ``invoke_as`` forwards it without inspecting it."""


_USE_CASE = cast("type[Compilable]", _UseCase)


def test_require_invoker_returns_the_invoker_when_the_bundle_exposes_one() -> None:
    invoker = _RecordingInvoker()

    assert require_invoker(_Bundle(invoker=invoker), "tool") is invoker


def test_require_invoker_rejects_a_bundle_without_an_invoker() -> None:
    with pytest.raises(AgentRunError) as excinfo:
        require_invoker({"identity": ANONYMOUS}, "tool 'lookup'")

    assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED
    assert str(excinfo.value).startswith("tool 'lookup' requires ")


def test_require_invoker_uses_the_label_verbatim_for_a_hook_caller() -> None:
    """The label names the caller verbatim: a hook is not reported as a tool."""
    with pytest.raises(AgentRunError) as excinfo:
        require_invoker({"identity": ANONYMOUS}, "on_output hook 'incidents.record_triage'")

    assert str(excinfo.value).startswith("on_output hook 'incidents.record_triage' requires ")


def test_require_invoker_rejects_an_attribute_that_is_not_an_invoker() -> None:
    bundle = _Bundle(invoker=object())

    with pytest.raises(AgentRunError) as excinfo:
        require_invoker(bundle, "tool")

    assert excinfo.value.code is AgentRunErrorCode.UNAUTHORIZED


async def test_invoke_as_installs_the_ambient_identity_before_calling_the_invoker() -> None:
    invoker = _RecordingInvoker()
    identity = Identity(subject="caller")

    await invoke_as(invoker, _USE_CASE, identity, params=None, payload=None)

    assert invoker.calls[0]["identity"] is identity


async def test_invoke_as_restores_the_previous_identity_on_completion() -> None:
    invoker = _RecordingInvoker()

    await invoke_as(invoker, _USE_CASE, Identity(subject="caller"), params=None, payload=None)

    assert current_identity() is ANONYMOUS


async def test_invoke_as_restores_the_previous_identity_when_the_invoker_fails() -> None:
    class _Failing(_RecordingInvoker):
        async def invoke(self, use_case: type[Compilable], **_: Any) -> Any:
            raise RuntimeError("boom")

    invoker = _Failing()
    identity = Identity(subject="caller")

    with pytest.raises(RuntimeError):
        await invoke_as(invoker, _USE_CASE, identity, params=None, payload=None)

    assert current_identity() is ANONYMOUS


async def test_invoke_as_forwards_params_and_payload_to_the_invoker() -> None:
    invoker = _RecordingInvoker()
    params = {"id": "1"}
    payload = {"value": 2}

    result = await invoke_as(
        invoker, _USE_CASE, Identity(subject="caller"), params=params, payload=payload
    )

    assert result == "result"
    assert invoker.calls[0]["use_case"] is _UseCase
    assert invoker.calls[0]["params"] == params
    assert invoker.calls[0]["payload"] == payload


def test_the_fake_satisfies_the_invoker_protocol() -> None:
    """Guards the fake itself: ``require_invoker`` checks the protocol structurally."""
    assert isinstance(_RecordingInvoker(), ApplicationInvoker)
