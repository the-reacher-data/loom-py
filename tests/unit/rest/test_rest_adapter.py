from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pytest
from fastapi import HTTPException

from loom.core.errors import (
    Conflict,
    Forbidden,
    LoomError,
    NotFound,
    RuleViolation,
    RuleViolations,
)
from loom.core.errors.codes import ErrorCode
from loom.core.transport.adapter import AdapterRequest, LoomAdapter
from loom.core.use_case.use_case import UseCase
from loom.rest.errors import HttpErrorMapper
from loom.rest.rest_adapter import LoomRestAdapter

# ---------------------------------------------------------------------------
# Domain fixtures
# ---------------------------------------------------------------------------


class _ResultStruct(msgspec.Struct):
    id: int
    name: str


class _SimpleUseCase(UseCase[Any, str]):
    async def execute(self) -> str:
        return "ok"


# ---------------------------------------------------------------------------
# AdapterRequest
# ---------------------------------------------------------------------------


class TestAdapterRequest:
    def test_params_stored(self) -> None:
        req = AdapterRequest(params={"user_id": 1})
        assert req.params == {"user_id": 1}

    def test_payload_default_none(self) -> None:
        req = AdapterRequest(params={})
        assert req.payload is None

    def test_payload_stored(self) -> None:
        req = AdapterRequest(params={}, payload={"email": "x@corp.com"})
        assert req.payload == {"email": "x@corp.com"}

    def test_dependencies_default_empty(self) -> None:
        req = AdapterRequest(params={})
        assert req.dependencies == {}

    def test_dependencies_stored(self) -> None:
        class Repo:
            pass

        repo = Repo()
        req = AdapterRequest(params={}, dependencies={Repo: repo})
        assert req.dependencies[Repo] is repo


# ---------------------------------------------------------------------------
# LoomAdapter Protocol (duck-typing conformance)
# ---------------------------------------------------------------------------


class TestLoomAdapterProtocol:
    def test_rest_adapter_satisfies_loom_adapter(self) -> None:
        executor = MagicMock(spec=["execute"])
        adapter = LoomRestAdapter(executor)
        assert callable(adapter.handle)

    def test_loom_adapter_is_protocol(self) -> None:
        class _CustomAdapter:
            async def handle(
                self,
                use_case: UseCase[Any, Any],
                request: AdapterRequest,
            ) -> Any:
                return None

        adapter = _CustomAdapter()
        assert isinstance(adapter, LoomAdapter)


# ---------------------------------------------------------------------------
# HttpErrorMapper
# ---------------------------------------------------------------------------


class TestHttpErrorMapper:
    def _mapper(self) -> HttpErrorMapper:
        return HttpErrorMapper()

    def test_not_found_maps_to_404(self) -> None:
        exc = self._mapper().to_http(NotFound("User", id=1))
        assert exc.status_code == 404

    def test_forbidden_maps_to_403(self) -> None:
        exc = self._mapper().to_http(Forbidden())
        assert exc.status_code == 403

    def test_conflict_maps_to_409(self) -> None:
        exc = self._mapper().to_http(Conflict("duplicate"))
        assert exc.status_code == 409

    def test_rule_violations_maps_to_422(self) -> None:
        violations = RuleViolations([RuleViolation("email", "invalid")])
        exc = self._mapper().to_http(violations)
        assert exc.status_code == 422

    def test_rule_violations_detail_includes_violations(self) -> None:
        violations = RuleViolations(
            [RuleViolation("email", "invalid"), RuleViolation("name", "too short")]
        )
        exc = self._mapper().to_http(violations)
        assert isinstance(exc.detail, dict)
        assert "violations" in exc.detail
        assert len(exc.detail["violations"]) == 2
        assert exc.detail["violations"][0] == {"field": "email", "message": "invalid"}

    def test_unknown_code_defaults_to_500(self) -> None:
        class _CustomError(LoomError):
            pass

        err = _CustomError("oops", code="unknown_custom")
        exc = self._mapper().to_http(err)
        assert exc.status_code == 500

    def test_detail_contains_code_and_message(self) -> None:
        exc = self._mapper().to_http(NotFound("Order", id=42))
        assert isinstance(exc.detail, dict)
        assert exc.detail["code"] == ErrorCode.NOT_FOUND
        assert "42" in exc.detail["message"]


# ---------------------------------------------------------------------------
# LoomRestAdapter — success paths
# ---------------------------------------------------------------------------


class TestLoomRestAdapterSuccess:
    def _make_executor(self, return_value: Any) -> Any:
        executor = MagicMock()
        executor.execute = AsyncMock(return_value=return_value)
        return executor

    async def test_returns_plain_result(self) -> None:
        adapter = LoomRestAdapter(self._make_executor("ok"))
        result = await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert result == "ok"

    async def test_struct_result_serialized_to_builtins(self) -> None:
        struct = _ResultStruct(id=1, name="Alice")
        adapter = LoomRestAdapter(self._make_executor(struct))
        result = await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert result == {"id": 1, "name": "Alice"}

    async def test_non_struct_result_returned_as_is(self) -> None:
        payload = {"key": "value"}
        adapter = LoomRestAdapter(self._make_executor(payload))
        result = await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert result is payload

    async def test_params_forwarded_to_executor(self) -> None:
        executor = self._make_executor("ok")
        adapter = LoomRestAdapter(executor)
        req = AdapterRequest(params={"user_id": 7})
        await adapter.handle(_SimpleUseCase(), req)
        _, kwargs = executor.execute.call_args
        assert kwargs["params"] == {"user_id": 7}

    async def test_payload_forwarded_to_executor(self) -> None:
        executor = self._make_executor("ok")
        adapter = LoomRestAdapter(executor)
        req = AdapterRequest(params={}, payload={"email": "x@corp.com"})
        await adapter.handle(_SimpleUseCase(), req)
        _, kwargs = executor.execute.call_args
        assert kwargs["payload"] == {"email": "x@corp.com"}

    async def test_empty_dependencies_passed_as_none(self) -> None:
        executor = self._make_executor("ok")
        adapter = LoomRestAdapter(executor)
        req = AdapterRequest(params={})
        await adapter.handle(_SimpleUseCase(), req)
        _, kwargs = executor.execute.call_args
        assert kwargs["dependencies"] is None

    async def test_non_empty_dependencies_forwarded(self) -> None:
        class Repo:
            pass

        repo = Repo()
        executor = self._make_executor("ok")
        adapter = LoomRestAdapter(executor)
        req = AdapterRequest(params={}, dependencies={Repo: repo})
        await adapter.handle(_SimpleUseCase(), req)
        _, kwargs = executor.execute.call_args
        assert kwargs["dependencies"] == {Repo: repo}


# ---------------------------------------------------------------------------
# LoomRestAdapter — error mapping
# ---------------------------------------------------------------------------


class TestLoomRestAdapterErrors:
    def _make_failing_executor(self, error: Exception) -> Any:
        executor = MagicMock()
        executor.execute = AsyncMock(side_effect=error)
        return executor

    async def test_loom_error_mapped_to_http_exception(self) -> None:
        executor = self._make_failing_executor(NotFound("Item", id=99))
        adapter = LoomRestAdapter(executor)
        with pytest.raises(HTTPException) as exc_info:
            await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert exc_info.value.status_code == 404

    async def test_rule_violations_mapped_to_422(self) -> None:
        err = RuleViolations([RuleViolation("email", "bad")])
        executor = self._make_failing_executor(err)
        adapter = LoomRestAdapter(executor)
        with pytest.raises(HTTPException) as exc_info:
            await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert exc_info.value.status_code == 422

    async def test_non_loom_error_not_caught(self) -> None:
        executor = self._make_failing_executor(RuntimeError("unexpected"))
        adapter = LoomRestAdapter(executor)
        with pytest.raises(RuntimeError, match="unexpected"):
            await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))

    async def test_custom_error_mapper_used(self) -> None:
        class _AlwaysTeapot(HttpErrorMapper):
            def to_http(self, error: LoomError) -> HTTPException:
                return HTTPException(status_code=418, detail="teapot")

        executor = self._make_failing_executor(NotFound("X", id=1))
        adapter = LoomRestAdapter(executor, error_mapper=_AlwaysTeapot())
        with pytest.raises(HTTPException) as exc_info:
            await adapter.handle(_SimpleUseCase(), AdapterRequest(params={}))
        assert exc_info.value.status_code == 418
