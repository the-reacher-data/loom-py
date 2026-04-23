"""Streaming testing helpers built on top of Bytewax testing sinks and sources.

Public API
----------
* :class:`StreamingTestRunner` — test-oriented runner with injectable input,
  captured output, and explicit error branch capture.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from omegaconf import DictConfig, OmegaConf

from loom.core.config import load_config
from loom.streaming.compiler import CompiledPlan, compile_flow
from loom.streaming.core._errors import ErrorKind
from loom.streaming.graph._flow import StreamFlow
from loom.streaming.observability.observers.protocol import StreamingFlowObserver

_bytewax_testing: Any | None = None
_bytewax_adapter: Any | None = None

try:
    import bytewax.testing as _loaded_bytewax_testing

    import loom.streaming.bytewax._adapter as _loaded_bytewax_adapter
except ModuleNotFoundError:  # pragma: no cover - exercised only without streaming extra
    pass
else:
    _bytewax_testing = _loaded_bytewax_testing
    _bytewax_adapter = _loaded_bytewax_adapter

if TYPE_CHECKING:
    from bytewax.outputs import Sink
else:
    Sink = Any

logger = logging.getLogger(__name__)


class StreamingTestRunner:
    """Run a streaming flow with test doubles for input and output.

    The runner mirrors the production runner's dataflow assembly, but swaps
    the source and sinks for Bytewax testing primitives. Tests can provide
    payloads or fully formed messages, opt into capturing explicit error
    branches, and inspect captured outputs after execution.

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

    def with_payloads(self, items: list[Any]) -> StreamingTestRunner:
        """Replace the current input buffer with payload-derived messages."""
        topic = self._plan.source.topics[0]
        self._input = [_test_message(topic, idx, item) for idx, item in enumerate(items)]
        return self

    def with_messages(self, items: list[Any]) -> StreamingTestRunner:
        """Replace the current input buffer with fully formed runtime messages."""
        self._input = list(items)
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
        _require_bytewax_testing()
        assert _bytewax_testing is not None
        assert _bytewax_adapter is not None

        error_sinks: dict[ErrorKind, Sink[Any]] = {
            kind: _bytewax_testing.TestingSink(items) for kind, items in self._errors.items()
        }
        built = _bytewax_adapter.build_dataflow_with_shutdown(
            plan=self._plan,
            flow_observer=self._observer,
            source=_bytewax_testing.TestingSource(list(self._input)),
            sink=_bytewax_testing.TestingSink(self._output),
            error_sinks=error_sinks,
        )
        try:
            _bytewax_testing.run_main(built.dataflow)
        finally:
            logger.debug("shutting_down_test_runner")
            built.shutdown()


__all__ = ["StreamingTestRunner"]


def _test_message(topic: str, idx: int, payload: Any) -> Any:
    from loom.streaming import Message, MessageMeta

    return Message(
        payload=payload,
        meta=MessageMeta(
            message_id=f"test-{idx}",
            topic=topic,
        ),
    )


def _require_bytewax_testing() -> None:
    if _bytewax_testing is None or _bytewax_adapter is None:
        raise RuntimeError(
            "StreamingTestRunner requires the optional streaming extra with Bytewax installed."
        )
