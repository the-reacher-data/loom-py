"""StreamingRunner — single entry point that wires config, compiler, and Bytewax runtime.

Mirrors the pattern used by :class:`~loom.etl.runner.core.ETLRunner`.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from bytewax.dataflow import Dataflow
from bytewax.inputs import Source
from bytewax.outputs import Sink
from bytewax.testing import run_main
from omegaconf import DictConfig, OmegaConf

from loom.streaming.bytewax._adapter import (
    _assemble_dataflow,
    _BuildContext,
    _maybe_create_bridge,
)
from loom.streaming.compiler import CompiledPlan, compile_flow
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.observability.factory import make_flow_observers
from loom.streaming.observability.observers.protocol import StreamingFlowObserver

logger = logging.getLogger(__name__)


class StreamingRunner:
    """Wire a :class:`StreamFlow` declaration into a runnable Bytewax dataflow.

    Lifecycle
    ---------
    1. Load config (``from_yaml`` / ``from_dict`` / ``from_config``).
    2. Compile the flow into a :class:`CompiledPlan`.
    3. Build a Bytewax :class:`Dataflow`.
    4. Run until EOF / interrupt.
    5. ``shutdown()`` closes resources and the async bridge.

    Args:
        flow: User-declared streaming flow.
        plan: Compiled plan produced by the compiler.
        observer: Optional flow observer for lifecycle events.
    """

    def __init__(
        self,
        flow: StreamFlow[Any, Any],
        plan: CompiledPlan,
        observer: StreamingFlowObserver | None = None,
    ) -> None:
        self._flow = flow
        self._plan = plan
        self._observer = observer
        self._bridge: Any | None = None
        self._ctx: _BuildContext | None = None

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

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
        return cls(flow, plan, observer=observer)

    @classmethod
    def from_yaml(
        cls,
        flow: StreamFlow[Any, Any],
        path: str,
        *,
        observer: StreamingFlowObserver | None = None,
    ) -> StreamingRunner:
        """Load runtime config from a YAML file and build a runner."""
        raw_path = Path(path)
        if not raw_path.exists():
            raise FileNotFoundError(f"Config file not found: {path}")
        with raw_path.open("r", encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        return cls.from_config(flow, runtime_config=_dict_config(raw), observer=observer)

    @classmethod
    def from_dict(
        cls,
        flow: StreamFlow[Any, Any],
        config: dict[str, Any],
        *,
        observer: StreamingFlowObserver | None = None,
    ) -> StreamingRunner:
        """Build a runner from a plain Python dict."""
        return cls.from_config(flow, runtime_config=_dict_config(config), observer=observer)

    # ------------------------------------------------------------------
    # Build & run
    # ------------------------------------------------------------------

    def build_dataflow(
        self,
        source: Source[Any] | None = None,
        sink: Sink[Any] | None = None,
    ) -> Dataflow:
        """Assemble the Bytewax Dataflow, keeping the build context for shutdown."""
        self._bridge = _maybe_create_bridge(self._plan)
        self._ctx = _BuildContext(
            plan=self._plan,
            bridge=self._bridge,
            flow_observer=self._observer,
            source=source,
            sink=sink,
        )
        return _assemble_dataflow(self._plan, self._ctx)

    def run(
        self,
        source: Source[Any] | None = None,
        sink: Sink[Any] | None = None,
    ) -> None:
        """Compile, build, and execute the streaming dataflow.

        Blocks until the dataflow reaches EOF (e.g. ``TestingSource`` is
        exhausted) or the process is interrupted.
        """
        dataflow = self.build_dataflow(source=source, sink=sink)
        try:
            run_main(dataflow)  # type: ignore[no-untyped-call]
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Close all resource managers and the async bridge."""
        if self._ctx is not None:
            logger.debug("shutting_down_resource_managers")
            self._ctx.shutdown_all()
        if self._bridge is not None:
            logger.debug("closing_async_bridge")
            self._bridge.close()
            self._bridge = None


def _dict_config(raw: Any) -> DictConfig:
    config = OmegaConf.create(raw)
    if not isinstance(config, DictConfig):
        raise TypeError("Streaming runner config must resolve to a mapping")
    return config


__all__ = ["StreamingRunner", "make_flow_observers"]
