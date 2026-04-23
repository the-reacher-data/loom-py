"""Streaming testing helpers built on top of Bytewax testing sinks and sources.

Public API
----------
* :class:`StreamingTestRunner` — test-oriented runner with injectable input,
  captured output, and explicit error branch capture.
"""

from __future__ import annotations

import logging
from typing import Any

from bytewax.outputs import Sink
from bytewax.testing import TestingSink, TestingSource, run_main
from omegaconf import DictConfig, OmegaConf

from loom.core.config import load_config
from loom.streaming.bytewax._adapter import build_dataflow_with_shutdown
from loom.streaming.compiler import CompiledPlan, compile_flow
from loom.streaming.core._errors import ErrorKind
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.observability.observers.protocol import StreamingFlowObserver

logger = logging.getLogger(__name__)


class StreamingTestRunner:
    """Run a streaming flow with test doubles for input and output.

    The runner mirrors the production runner's dataflow assembly, but swaps
    the source and sinks for Bytewax testing primitives. Tests can seed input
    records, append more events, opt into capturing explicit error branches,
    and inspect captured outputs after execution.

    Args:
        plan: Compiled plan produced by the compiler.
        observer: Optional flow observer.
    """

    def __init__(
        self,
        plan: CompiledPlan,
        observer: StreamingFlowObserver | None = None,
    ) -> None:
        self._plan = plan
        self._observer = observer
        self._input: list[Any] = []
        self._output: list[Any] = []
        self._errors: dict[ErrorKind, list[Any]] = {}

    @classmethod
    def from_flow(
        cls,
        flow: StreamFlow[Any, Any],
        *,
        runtime_config: DictConfig,
        observer: StreamingFlowObserver | None = None,
    ) -> StreamingTestRunner:
        """Compile a flow and build a test runner from resolved config."""
        plan = compile_flow(flow, runtime_config=runtime_config)
        return cls(plan, observer=observer)

    @classmethod
    def from_yaml(
        cls,
        flow: StreamFlow[Any, Any],
        path: str,
        *,
        observer: StreamingFlowObserver | None = None,
    ) -> StreamingTestRunner:
        """Load YAML config, compile the flow, and build a test runner."""
        return cls.from_flow(flow, runtime_config=load_config(path), observer=observer)

    @classmethod
    def from_dict(
        cls,
        flow: StreamFlow[Any, Any],
        config: dict[str, Any],
        *,
        observer: StreamingFlowObserver | None = None,
    ) -> StreamingTestRunner:
        """Build a test runner from a plain Python config mapping."""
        runtime_config = OmegaConf.create(config)
        if not isinstance(runtime_config, DictConfig):
            raise TypeError("Streaming test config must resolve to a mapping")
        return cls.from_flow(flow, runtime_config=runtime_config, observer=observer)

    def seed(self, items: list[Any]) -> StreamingTestRunner:
        """Replace the current input buffer with *items*."""
        self._input = list(items)
        return self

    def append_input(self, *items: Any) -> StreamingTestRunner:
        """Append input records that the next run will consume."""
        self._input.extend(items)
        return self

    def capture_errors(self, *kinds: ErrorKind) -> StreamingTestRunner:
        """Enable capture for explicit error branches."""
        for kind in kinds:
            self._errors.setdefault(kind, [])
        return self

    def reset(self) -> StreamingTestRunner:
        """Clear captured input, output, and error buffers."""
        self._input.clear()
        self._output.clear()
        self._errors.clear()
        return self

    @property
    def output(self) -> list[Any]:
        """Captured items written to the main testing sink."""
        return self._output

    @property
    def errors(self) -> dict[ErrorKind, list[Any]]:
        """Captured items written to configured error sinks."""
        return self._errors

    def run(self) -> None:
        """Execute the compiled dataflow with testing source and sinks."""
        error_sinks: dict[ErrorKind, Sink[Any]] = {
            kind: TestingSink(items) for kind, items in self._errors.items()
        }
        built = build_dataflow_with_shutdown(
            plan=self._plan,
            flow_observer=self._observer,
            source=TestingSource(list(self._input)),
            sink=TestingSink(self._output),
            error_sinks=error_sinks,
        )
        try:
            run_main(built.dataflow)  # type: ignore[no-untyped-call]
        finally:
            logger.debug("shutting_down_test_runner")
            built.shutdown()


__all__ = ["StreamingTestRunner"]
