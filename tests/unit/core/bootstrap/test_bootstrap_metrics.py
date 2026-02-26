"""Tests for metrics wiring through bootstrap → create_fastapi_app."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import msgspec

from loom.core.bootstrap import BootstrapResult, bootstrap_app
from loom.core.engine.events import RuntimeEvent
from loom.core.engine.metrics import MetricsAdapter
from loom.core.use_case.use_case import UseCase


class _Model(msgspec.Struct):
    id: int


class _SimpleUseCase(UseCase[_Model, None]):
    async def execute(self) -> None:  # type: ignore[override]
        return None


class TestBootstrapResultCarriesMetrics:
    def test_metrics_none_by_default(self) -> None:
        result = bootstrap_app(config=object(), use_cases=[_SimpleUseCase])
        assert result.metrics is None

    def test_metrics_stored_when_provided(self) -> None:
        adapter = MagicMock(spec=MetricsAdapter)
        result = bootstrap_app(
            config=object(),
            use_cases=[_SimpleUseCase],
            metrics=adapter,
        )
        assert result.metrics is adapter


class TestCreateFastapiAppWiresMetrics:
    def test_executor_receives_metrics_adapter(self) -> None:
        """create_fastapi_app must wire result.metrics into the executor."""
        pytest.importorskip("fastapi")

        from unittest.mock import patch

        from loom.core.engine.executor import RuntimeExecutor
        from loom.rest.fastapi.app import create_fastapi_app
        from loom.rest.model import RestInterface, RestRoute

        class _Adapter:
            def on_event(self, event: RuntimeEvent) -> None:
                pass

        adapter = _Adapter()
        result = bootstrap_app(
            config=object(),
            use_cases=[_SimpleUseCase],
            metrics=adapter,
        )

        class _Iface(RestInterface[_Model]):
            prefix = "/test"
            routes = (RestRoute(use_case=_SimpleUseCase, method="GET", path="/"),)

        created_executors: list[RuntimeExecutor] = []
        original_init = RuntimeExecutor.__init__

        def _capture_init(self: RuntimeExecutor, *args: object, **kwargs: object) -> None:
            original_init(self, *args, **kwargs)
            created_executors.append(self)

        with patch.object(RuntimeExecutor, "__init__", _capture_init):
            create_fastapi_app(result, interfaces=[_Iface])

        assert len(created_executors) == 1
        assert created_executors[0]._metrics is adapter
