"""Tests for metrics wiring through bootstrap → create_fastapi_app."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from loom.core.bootstrap import bootstrap_app
from loom.core.engine.events import RuntimeEvent
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.metrics import MetricsAdapter
from loom.core.model import LoomStruct
from loom.core.use_case.use_case import UseCase


class _Model(LoomStruct):
    id: int


class _SimpleUseCase(UseCase[_Model, None]):
    async def execute(self) -> None:
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
        """bootstrap_app must register RuntimeExecutor with the metrics adapter."""
        pytest.importorskip("fastapi")

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

        executor = result.container.resolve(RuntimeExecutor)
        assert executor._metrics is adapter
        app = create_fastapi_app(result, interfaces=[_Iface])
        assert app is not None

    def test_reuses_registered_executor_when_present(self) -> None:
        pytest.importorskip("fastapi")

        from loom.rest.fastapi.app import create_fastapi_app
        from loom.rest.model import RestInterface, RestRoute

        result = bootstrap_app(config=object(), use_cases=[_SimpleUseCase])
        shared_executor = result.container.resolve(RuntimeExecutor)

        class _Iface(RestInterface[_Model]):
            prefix = "/test"
            routes = (RestRoute(use_case=_SimpleUseCase, method="GET", path="/"),)

        app = create_fastapi_app(result, interfaces=[_Iface])
        assert app is not None
        assert result.container.resolve(RuntimeExecutor) is shared_executor
