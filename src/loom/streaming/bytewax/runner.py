"""StreamingRunner — production Bytewax runtime entry point for Loom flows."""

from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from time import perf_counter
from typing import Any

import msgspec
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.run import cli_main
from omegaconf import DictConfig, OmegaConf

from loom.core.async_bridge import AsyncBridge
from loom.core.config import load_config, section
from loom.streaming.bytewax._adapter import build_dataflow_with_shutdown
from loom.streaming.bytewax._runtime_io import (
    build_runtime_error_sinks,
    build_runtime_sink,
    build_runtime_source,
    build_runtime_terminal_sinks,
)
from loom.streaming.compiler import CompiledPlan, compile_flow
from loom.streaming.core._errors import ErrorKind
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.observability.config import StreamingObservabilityConfig
from loom.streaming.observability.factory import make_flow_observers
from loom.streaming.observability.observers.protocol import StreamingFlowObserver

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _PreparedStreamingRun:
    """Prepared Bytewax execution bundle.

    Args:
        dataflow: Built Bytewax dataflow ready for execution.
        shutdown: Callback that releases adapter-owned resources.
    """

    dataflow: Dataflow
    shutdown: Callable[[], None]


class BytewaxRecoverySettings(msgspec.Struct, frozen=True, kw_only=True):
    """Recovery settings for the Bytewax runtime.

    Args:
        db_dir: Local recovery directory shared by this execution.
        backup_interval_ms: Optional snapshot backup interval in milliseconds.
    """

    db_dir: str
    backup_interval_ms: int | None = None


class BytewaxRuntimeConfig(msgspec.Struct, frozen=True, kw_only=True):
    """Runtime settings for executing a Bytewax dataflow.

    **Delivery guarantee:** The runtime currently provides **at-most-once**
    delivery. Kafka offsets are committed by the consumer's auto-commit policy
    and are not tied to successful downstream processing. To achieve
    at-least-once delivery, configure Bytewax recovery (see
    :class:`BytewaxRecoverySettings`) and disable ``enable.auto.commit`` in
    the consumer settings; offset commits must then be wired to epoch
    snapshots via the Bytewax recovery mechanism.

    Args:
        workers_per_process: Number of worker threads in this process.
        process_id: Optional cluster process id.
        addresses: Optional cluster address list, including this process.
        epoch_interval_ms: Optional epoch duration in milliseconds.
        recovery: Optional recovery settings.
        async_backend: Anyio backend used for :class:`AsyncBridge` when the
            flow contains :class:`~loom.streaming.WithAsync` nodes.
            Accepted values: ``"asyncio"`` (default) or ``"trio"``.
        use_uvloop: Replace the asyncio event loop with uvloop for lower
            latency async I/O.  Only effective when *async_backend* is
            ``"asyncio"`` and the platform is not Windows.
        force_shutdown_timeout_ms: Maximum milliseconds to wait for in-flight
            async tasks to finish during shutdown.  When exceeded, the portal
            thread is abandoned and a warning is logged.  ``None`` waits
            indefinitely (default).
    """

    workers_per_process: int = 1
    process_id: int | None = None
    addresses: tuple[str, ...] | None = None
    epoch_interval_ms: int | None = None
    recovery: BytewaxRecoverySettings | None = None
    async_backend: str = "asyncio"
    use_uvloop: bool = False
    force_shutdown_timeout_ms: int | None = None


class StreamingRunner:
    """Wire a :class:`StreamFlow` declaration into the real Bytewax runtime.

    Args:
        flow: User-declared streaming flow.
        plan: Compiled plan produced by the compiler.
        runtime: Resolved Bytewax runtime settings.
        observer: Optional flow observer for lifecycle events.
    """

    def __init__(
        self,
        flow: StreamFlow[Any, Any],
        plan: CompiledPlan,
        runtime: BytewaxRuntimeConfig | None = None,
        observer: StreamingFlowObserver | None = None,
    ) -> None:
        self._flow = flow
        self._plan = plan
        self._runtime = runtime or BytewaxRuntimeConfig()
        self._observer = observer
        self._shutdown: Callable[[], None] | None = None

    @classmethod
    def from_config(
        cls,
        flow: StreamFlow[Any, Any],
        *,
        runtime_config: DictConfig,
        observer: StreamingFlowObserver | None = None,
    ) -> StreamingRunner:
        """Build a runner from a resolved OmegaConf config."""
        plan = compile_flow(flow, runtime_config=runtime_config)
        runtime = _load_runtime_config(runtime_config)
        resolved_observer = observer or _load_observability_observer(runtime_config)
        return cls(flow, plan, runtime, observer=resolved_observer)

    @classmethod
    def from_yaml(
        cls,
        flow: StreamFlow[Any, Any],
        path: str,
        *,
        observer: StreamingFlowObserver | None = None,
    ) -> StreamingRunner:
        """Load config from YAML and build a runner."""
        return cls.from_config(flow, runtime_config=load_config(path), observer=observer)

    @classmethod
    def from_dict(
        cls,
        flow: StreamFlow[Any, Any],
        config: dict[str, Any],
        *,
        observer: StreamingFlowObserver | None = None,
    ) -> StreamingRunner:
        """Build a runner from a plain Python config mapping."""
        return cls.from_config(flow, runtime_config=_dict_config(config), observer=observer)

    def build_dataflow(self) -> Dataflow:
        """Assemble the Bytewax Dataflow and keep shutdown state."""
        prepared = self.prepare_run()
        return prepared.dataflow

    def prepare_run(self) -> _PreparedStreamingRun:
        """Prepare one executable dataflow and its shutdown callback."""
        prepared = _prepare_run(self._plan, observer=self._observer, runtime=self._runtime)
        self._shutdown = prepared.shutdown
        return prepared

    def run(self, *, runtime: BytewaxRuntimeConfig | None = None) -> None:
        """Execute the compiled dataflow with the real Bytewax runtime.

        Args:
            runtime: Optional runtime override. When omitted, uses the runner's
                config-loaded runtime settings.
        """

        resolved_runtime = runtime or self._runtime
        observer = self._observer
        if observer is not None:
            observer.on_flow_start(self._plan.name, node_count=len(self._plan.nodes))
        started_at = perf_counter()
        status = "failed"
        prepared = self.prepare_run()
        try:
            cli_main(prepared.dataflow, **_runtime_kwargs(resolved_runtime))  # type: ignore[no-untyped-call]
            status = "success"
        except Exception:
            status = "failed"
            raise
        finally:
            if observer is not None:
                duration_ms = int((perf_counter() - started_at) * 1000)
                observer.on_flow_end(self._plan.name, status=status, duration_ms=duration_ms)
            self.shutdown()

    def shutdown(self) -> None:
        """Release adapter resources after a run or failed build."""
        if self._shutdown is not None:
            logger.debug("shutting_down")
            self._shutdown()
            self._shutdown = None


def _dict_config(raw: dict[str, Any]) -> DictConfig:
    config = OmegaConf.create(raw)
    if not isinstance(config, DictConfig):
        raise TypeError("Streaming runner config must resolve to a mapping")
    return config


def _load_runtime_config(cfg: DictConfig) -> BytewaxRuntimeConfig:
    if not _has_section(cfg, "streaming.runtime"):
        return BytewaxRuntimeConfig()
    return section(cfg, "streaming.runtime", BytewaxRuntimeConfig)


def _load_observability_observer(cfg: DictConfig) -> StreamingFlowObserver | None:
    if not _has_section(cfg, "streaming.observability"):
        return make_flow_observers()
    observability = section(cfg, "streaming.observability", StreamingObservabilityConfig)
    return make_flow_observers(observability)


def _has_section(cfg: DictConfig, path: str) -> bool:
    node: Any = cfg
    for part in path.split("."):
        try:
            node = node[part]
        except Exception:
            return False
    return True


def _runtime_kwargs(runtime: BytewaxRuntimeConfig) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "workers_per_process": runtime.workers_per_process,
        "process_id": runtime.process_id,
        "addresses": list(runtime.addresses) if runtime.addresses is not None else None,
        "epoch_interval": _to_timedelta(runtime.epoch_interval_ms),
        "recovery_config": _recovery_config(runtime.recovery),
    }
    return kwargs


def _recovery_config(recovery: BytewaxRecoverySettings | None) -> RecoveryConfig | None:
    if recovery is None:
        return None
    return RecoveryConfig(  # type: ignore[no-untyped-call]
        Path(recovery.db_dir),
        backup_interval=_to_timedelta(recovery.backup_interval_ms),
    )


def _to_timedelta(value_ms: int | None) -> timedelta | None:
    if value_ms is None:
        return None
    return timedelta(milliseconds=value_ms)


def _prepare_run(
    plan: CompiledPlan,
    *,
    observer: StreamingFlowObserver | None = None,
    source: Any | None = None,
    sink: Any | None = None,
    terminal_sinks: Mapping[tuple[int, ...], Any] | None = None,
    error_sinks: Mapping[ErrorKind, Any] | None = None,
    runtime: BytewaxRuntimeConfig | None = None,
) -> _PreparedStreamingRun:
    if source is None:
        source = build_runtime_source(plan.source)
    if sink is None and plan.output is not None:
        sink = build_runtime_sink(plan.output)
    if terminal_sinks is None:
        terminal_sinks = build_runtime_terminal_sinks(plan.terminal_sinks)
    if error_sinks is None:
        error_sinks = build_runtime_error_sinks(plan.error_routes)
    bridge = _create_bridge(plan, runtime or BytewaxRuntimeConfig())
    built = build_dataflow_with_shutdown(
        plan=plan,
        flow_observer=observer,
        source=source,
        sink=sink,
        terminal_sinks=terminal_sinks,
        error_sinks=error_sinks,
        bridge=bridge,
    )
    return _PreparedStreamingRun(dataflow=built.dataflow, shutdown=built.shutdown)


def _create_bridge(plan: CompiledPlan, runtime: BytewaxRuntimeConfig) -> AsyncBridge | None:
    """Create an AsyncBridge configured from runtime settings, or None if not needed."""
    if not plan.needs_async_bridge:
        return None
    return AsyncBridge(
        backend=runtime.async_backend,
        backend_options=_build_backend_options(runtime.async_backend, runtime.use_uvloop),
        shutdown_timeout_ms=runtime.force_shutdown_timeout_ms,
    )


def _build_backend_options(backend: str, use_uvloop: bool) -> dict[str, Any]:
    """Build anyio backend options dict from runtime config."""
    if backend == "asyncio" and use_uvloop and sys.platform != "win32":
        import uvloop  # guarded by sys_platform marker in pyproject.toml

        return {"loop_factory": uvloop.new_event_loop}
    return {}


__all__ = [
    "BytewaxRecoverySettings",
    "BytewaxRuntimeConfig",
    "StreamingRunner",
    "make_flow_observers",
]
