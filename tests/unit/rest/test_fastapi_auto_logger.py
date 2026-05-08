from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest
from fastapi import FastAPI
from omegaconf import OmegaConf

import loom.rest.fastapi.auto as auto
from loom.core.observability.config import ObservabilityConfig
from loom.core.observability.runtime import ObservabilityRuntime
from loom.prometheus.middleware import PrometheusMiddleware
from loom.rest.fastapi.auto import _MetricsConfig, _mount_optional_middlewares, create_app
from loom.rest.middleware import TraceIdMiddleware

_AUTO = cast(Any, auto)


def test_mount_optional_middlewares_always_adds_trace_middleware() -> None:
    app = FastAPI()

    _mount_optional_middlewares(app, _MetricsConfig(enabled=False), None)

    classes: list[Any] = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware in classes


def test_mount_optional_middlewares_adds_prometheus_when_enabled() -> None:
    app = FastAPI()

    _mount_optional_middlewares(app, _MetricsConfig(enabled=True), None)

    classes: list[Any] = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware in classes
    assert PrometheusMiddleware in classes


def test_create_app_uses_observability_section(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = OmegaConf.create(
        {
            "app": {"name": "demo"},
            "database": {"url": "sqlite+aiosqlite:///"},
            "observability": {
                "log": {"enabled": False},
                "prometheus": {"enabled": False, "config": {"path": "/metrics"}},
            },
        }
    )
    captured: dict[str, Any] = {}
    runtime = ObservabilityRuntime.noop()

    def _fake_load_config(*config_paths: str) -> Any:
        del config_paths
        return raw

    def _fake_from_config(
        cls: type[ObservabilityRuntime], config: ObservabilityConfig
    ) -> ObservabilityRuntime:
        captured["observability_config"] = config
        return runtime

    def _fake_build_bootstrap(
        app_cfg: Any,
        db_cfg: Any,
        echo: bool,
        metrics: Any | None = None,
    ) -> tuple[Any, Any, Any]:
        del app_cfg, db_cfg, echo, metrics
        result = SimpleNamespace(
            compiler=object(),
            factory=object(),
            container=SimpleNamespace(),
        )
        session_manager = SimpleNamespace(engine=SimpleNamespace())
        discovered = SimpleNamespace(
            use_cases=(object(),),
            interfaces=(type("DummyRestInterface", (), {}),),
            models=(object(),),
        )
        return result, session_manager, discovered

    def _fake_configure_job_service(
        raw_cfg: Any, result: Any, observability_runtime: ObservabilityRuntime | None
    ) -> None:
        del raw_cfg, result, observability_runtime

    def _fake_create_fastapi_app(
        result: Any,
        interfaces: Any,
        *,
        observability_runtime: ObservabilityRuntime | None = None,
        **kwargs: Any,
    ) -> FastAPI:
        del result, interfaces, kwargs
        captured["runtime"] = observability_runtime
        return FastAPI()

    monkeypatch.setattr(auto, "load_config", _fake_load_config)
    monkeypatch.setattr(
        _AUTO.ObservabilityRuntime,
        "from_config",
        classmethod(_fake_from_config),
    )
    monkeypatch.setattr(auto, "_build_bootstrap", _fake_build_bootstrap)
    monkeypatch.setattr(auto, "_configure_job_service", _fake_configure_job_service)
    monkeypatch.setattr(auto, "create_fastapi_app", _fake_create_fastapi_app)
    monkeypatch.setattr(auto, "_ensure_code_path", lambda code_path: None)

    app = create_app("config.yaml")

    assert captured["observability_config"].log.enabled is False
    assert captured["runtime"] is runtime
    classes: list[Any] = [m.cls for m in app.user_middleware]
    assert TraceIdMiddleware in classes
    assert PrometheusMiddleware not in classes
