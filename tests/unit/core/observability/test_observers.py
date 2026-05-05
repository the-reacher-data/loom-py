"""Unit tests for built-in lifecycle observers."""

from __future__ import annotations

import pytest

from loom.core.observability.event import EventKind, LifecycleEvent
from loom.core.observability.observer.noop import NoopObserver
from loom.core.observability.observer.structlog import StructlogLifecycleObserver


class TestNoopObserver:
    def test_accepts_any_event_without_error(self) -> None:
        obs = NoopObserver()
        for kind in EventKind:
            obs.on_event(LifecycleEvent(scope="node", name="x", kind=kind))


class TestStructlogLifecycleObserver:
    @pytest.fixture()
    def observer(self) -> StructlogLifecycleObserver:
        return StructlogLifecycleObserver()

    def test_start_event_calls_debug(
        self, observer: StructlogLifecycleObserver, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **_: object) -> None:
                calls.append(event)

            def info(self, event: str, **_: object) -> None:
                calls.append(event)

            def error(self, event: str, **_: object) -> None:
                calls.append(event)

        monkeypatch.setattr("loom.core.observability.observer.structlog._log", _FakeLogger())

        observer.on_event(LifecycleEvent(scope="node", name="x", kind=EventKind.START))
        assert calls == ["lifecycle_start"]

    def test_end_event_calls_info(
        self, observer: StructlogLifecycleObserver, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **_: object) -> None:
                calls.append(event)

            def info(self, event: str, **_: object) -> None:
                calls.append(event)

            def error(self, event: str, **_: object) -> None:
                calls.append(event)

        monkeypatch.setattr("loom.core.observability.observer.structlog._log", _FakeLogger())

        observer.on_event(
            LifecycleEvent(scope="use_case", name="x", kind=EventKind.END, duration_ms=12.5)
        )
        assert calls == ["lifecycle_end"]

    def test_error_event_calls_error(
        self, observer: StructlogLifecycleObserver, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        calls: list[str] = []

        class _FakeLogger:
            def bind(self, **_: object) -> _FakeLogger:
                return self

            def debug(self, event: str, **_: object) -> None:
                calls.append(event)

            def info(self, event: str, **_: object) -> None:
                calls.append(event)

            def error(self, event: str, **_: object) -> None:
                calls.append(event)

        monkeypatch.setattr("loom.core.observability.observer.structlog._log", _FakeLogger())

        observer.on_event(
            LifecycleEvent(scope="node", name="x", kind=EventKind.ERROR, error="boom")
        )
        assert calls == ["lifecycle_error"]


class TestPrometheusLifecycleAdapter:
    pytest.importorskip("prometheus_client")

    def test_records_duration_on_end_event(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleAdapter

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleAdapter(registry=registry)

        adapter.on_event(
            LifecycleEvent(
                scope="use_case", name="CreateOrder", kind=EventKind.END, duration_ms=42.0
            )
        )

        assert (
            registry.get_sample_value(
                "loom_lifecycle_duration_seconds_count",
                {"scope": "use_case", "name": "CreateOrder"},
            )
            == 1.0
        )

    def test_increments_errors_on_error_event(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleAdapter

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleAdapter(registry=registry)

        adapter.on_event(LifecycleEvent(scope="node", name="transform", kind=EventKind.ERROR))

        assert (
            registry.get_sample_value(
                "loom_lifecycle_errors_total",
                {"scope": "node", "name": "transform"},
            )
            == 1.0
        )

    def test_start_event_is_ignored(self) -> None:
        from prometheus_client import CollectorRegistry

        from loom.prometheus.lifecycle import PrometheusLifecycleAdapter

        registry = CollectorRegistry()
        adapter = PrometheusLifecycleAdapter(registry=registry)

        adapter.on_event(LifecycleEvent(scope="node", name="x", kind=EventKind.START))
