"""StreamingRunner — single entry point that wires config, compiler, and Bytewax runtime.

Mirrors the pattern used by :class:`~loom.etl.runner.core.ETLRunner`.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

from bytewax.dataflow import Dataflow
from bytewax.inputs import Source
from bytewax.outputs import Sink
from bytewax.testing import run_main
from omegaconf import DictConfig, OmegaConf

from loom.core.config import load_config
from loom.streaming.bytewax._adapter import build_dataflow_with_shutdown
from loom.streaming.compiler import CompiledPlan, compile_flow
from loom.streaming.core._errors import ErrorKind
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
        self._shutdown: Callable[[], None] | None = None

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
        return cls.from_config(flow, runtime_config=load_config(path), observer=observer)

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
        error_sinks: Mapping[ErrorKind, Sink[Any]] | None = None,
    ) -> Dataflow:
        """Assemble the Bytewax Dataflow, keeping shutdown state."""
        built = build_dataflow_with_shutdown(
            plan=self._plan,
            flow_observer=self._observer,
            source=source,
            sink=sink,
            error_sinks=error_sinks,
        )
        self._shutdown = built.shutdown
        return built.dataflow

    def run(
        self,
        source: Source[Any] | None = None,
        sink: Sink[Any] | None = None,
        error_sinks: Mapping[ErrorKind, Sink[Any]] | None = None,
    ) -> None:
        """Compile, build, and execute the streaming dataflow.

        Blocks until the dataflow reaches EOF (e.g. ``TestingSource`` is
        exhausted) or the process is interrupted.

        .. note::

            This method is intended for **local development and tests**.
            For production use the Bytewax CLI (``python -m bytewax.run``
            or ``waxctl``) directly.
        """
        dataflow = self.build_dataflow(source=source, sink=sink, error_sinks=error_sinks)
        try:
            run_main(dataflow)  # type: ignore[no-untyped-call]
        finally:
            self.shutdown()

    def shutdown(self) -> None:
        """Close all resource managers and the async bridge."""
        if self._shutdown is not None:
            logger.debug("shutting_down")
            self._shutdown()
            self._shutdown = None


def _dict_config(raw: dict[str, Any]) -> DictConfig:
    config = OmegaConf.create(raw)
    if not isinstance(config, DictConfig):
        raise TypeError("Streaming runner config must resolve to a mapping")
    return config


__all__ = ["StreamingRunner", "make_flow_observers"]
