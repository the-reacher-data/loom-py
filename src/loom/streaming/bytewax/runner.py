"""StreamingRunner — production Bytewax runtime entry point for Loom flows."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import msgspec
from bytewax.dataflow import Dataflow
from bytewax.recovery import RecoveryConfig
from bytewax.run import cli_main
from omegaconf import DictConfig, OmegaConf

from loom.core.config import load_config, section
from loom.streaming.bytewax._adapter import build_dataflow_with_shutdown
from loom.streaming.compiler import CompiledPlan, compile_flow
from loom.streaming.graph._flow import StreamFlow
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

    Args:
        workers_per_process: Number of worker threads in this process.
        process_id: Optional cluster process id.
        addresses: Optional cluster address list, including this process.
        epoch_interval_ms: Optional epoch duration in milliseconds.
        recovery: Optional recovery settings.
    """

    workers_per_process: int = 1
    process_id: int | None = None
    addresses: tuple[str, ...] | None = None
    epoch_interval_ms: int | None = None
    recovery: BytewaxRecoverySettings | None = None


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
        return cls(flow, plan, runtime, observer=observer)

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
        prepared = _prepare_run(self._plan, observer=self._observer)
        self._shutdown = prepared.shutdown
        return prepared

    def run(self, *, runtime: BytewaxRuntimeConfig | None = None) -> None:
        """Execute the compiled dataflow with the real Bytewax runtime.

        Args:
            runtime: Optional runtime override. When omitted, uses the runner's
                config-loaded runtime settings.
        """

        resolved_runtime = runtime or self._runtime
        prepared = self.prepare_run()
        try:
            cli_main(prepared.dataflow, **_runtime_kwargs(resolved_runtime))  # type: ignore[no-untyped-call]
        finally:
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
    error_sinks: Any | None = None,
) -> _PreparedStreamingRun:
    built = build_dataflow_with_shutdown(
        plan=plan,
        flow_observer=observer,
        source=source,
        sink=sink,
        error_sinks=error_sinks,
    )
    return _PreparedStreamingRun(dataflow=built.dataflow, shutdown=built.shutdown)


__all__ = [
    "BytewaxRecoverySettings",
    "BytewaxRuntimeConfig",
    "StreamingRunner",
    "make_flow_observers",
]
