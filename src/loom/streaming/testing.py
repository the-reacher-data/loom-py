"""Streaming testing helpers built on top of Bytewax testing sinks and sources.

Public API
----------
* :class:`StreamingTestRunner` — test-oriented runner with injectable input,
  captured output, and explicit error branch capture.

Typical usage::

    runner = StreamingTestRunner.from_flow(flow, runtime_config=cfg)
    runner.with_payloads([OrderPlaced(order_id="o-1")])
    runner.capture_errors(ErrorKind.WIRE)
    runner.run()
    assert len(runner.output) == 1
"""

from __future__ import annotations

import logging
from datetime import timedelta
from time import perf_counter
from typing import Any

import bytewax.testing as bytewax_testing
from bytewax.outputs import Sink
from omegaconf import DictConfig, OmegaConf

from loom.core.config import load_config
from loom.core.observability.event import LifecycleEvent, LifecycleStatus, Scope
from loom.core.observability.runtime import ObservabilityRuntime
from loom.streaming import Message, MessageMeta
from loom.streaming.bytewax.runner import _prepare_run
from loom.streaming.compiler import CompiledPlan, compile_flow
from loom.streaming.core._errors import ErrorKind
from loom.streaming.graph._flow import StreamFlow

logger = logging.getLogger(__name__)


class StreamingTestRunner:
    """Run a streaming flow with test doubles for input and output.

    The runner mirrors the production runner's dataflow preparation and swaps
    only the source and sinks for Bytewax testing primitives. Use
    :meth:`with_payloads` for ergonomic flow-author tests and
    :meth:`with_messages` when metadata must be controlled explicitly.

    Args:
        plan: Compiled plan produced by the compiler.
        observer: Optional flow observer.
    """

    def __init__(
        self,
        plan: CompiledPlan,
        observability_runtime: ObservabilityRuntime | None = None,
    ) -> None:
        self._plan = plan
        self._observability_runtime = observability_runtime or ObservabilityRuntime.noop()
        self._input: list[Any] = []
        self._output: list[Any] = []
        self._errors: dict[ErrorKind, list[Any]] = {}

    @classmethod
    def from_flow(
        cls,
        flow: StreamFlow[Any, Any],
        *,
        runtime_config: DictConfig,
        observability_runtime: ObservabilityRuntime | None = None,
    ) -> StreamingTestRunner:
        """Compile a flow and build a test runner from resolved config."""
        plan = compile_flow(flow, runtime_config=runtime_config)
        return cls(plan, observability_runtime=observability_runtime)

    @classmethod
    def from_yaml(
        cls,
        flow: StreamFlow[Any, Any],
        path: str,
        *,
        observability_runtime: ObservabilityRuntime | None = None,
    ) -> StreamingTestRunner:
        """Load YAML config, compile the flow, and build a test runner."""
        return cls.from_flow(
            flow,
            runtime_config=load_config(path),
            observability_runtime=observability_runtime,
        )

    @classmethod
    def from_dict(
        cls,
        flow: StreamFlow[Any, Any],
        config: dict[str, Any],
        *,
        observability_runtime: ObservabilityRuntime | None = None,
    ) -> StreamingTestRunner:
        """Build a test runner from a plain Python config mapping."""
        runtime_config = OmegaConf.create(config)
        if not isinstance(runtime_config, DictConfig):
            raise TypeError("Streaming test config must resolve to a mapping")
        return cls.from_flow(
            flow,
            runtime_config=runtime_config,
            observability_runtime=observability_runtime,
        )

    def with_payloads(self, items: list[Any]) -> StreamingTestRunner:
        """Replace input with payload-derived test messages.

        Payloads are wrapped into :class:`loom.streaming.Message` values using
        deterministic test metadata:

        - ``message_id``: ``test-<index>``
        - ``topic``: first source topic declared by the flow
        """
        topic = _first_source_topic(self._plan)
        self._input = [_test_message(topic, idx, item) for idx, item in enumerate(items)]
        return self

    def with_messages(self, items: list[Any]) -> StreamingTestRunner:
        """Replace input with fully formed runtime messages or raw test records."""
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
        error_sinks: dict[ErrorKind, Sink[Any]] = {
            kind: bytewax_testing.TestingSink(items) for kind, items in self._errors.items()
        }
        terminal_sinks: dict[tuple[int, ...], Sink[Any]] = {
            path: bytewax_testing.TestingSink(self._output) for path in self._plan.terminal_sinks
        }
        prepared = _prepare_run(
            plan=self._plan,
            observability_runtime=self._observability_runtime,
            source=bytewax_testing.TestingSource(list(self._input)),
            sink=(
                bytewax_testing.TestingSink(self._output)
                if self._plan.output is not None or not self._plan.terminal_sinks
                else None
            ),
            terminal_sinks=terminal_sinks,
            error_sinks=error_sinks,
        )
        self._observability_runtime.emit(
            LifecycleEvent.start(
                scope=Scope.POLL_CYCLE,
                name=self._plan.name,
                meta={"node_count": len(self._plan.nodes)},
            )
        )
        started_at = perf_counter()
        status = LifecycleStatus.FAILURE
        try:
            bytewax_testing.run_main(
                prepared.dataflow,
                epoch_interval=timedelta(milliseconds=1),
            )  # type: ignore[no-untyped-call]
            status = LifecycleStatus.SUCCESS
        except Exception:
            status = LifecycleStatus.FAILURE
            raise
        finally:
            duration_ms = int((perf_counter() - started_at) * 1000)
            self._observability_runtime.emit(
                LifecycleEvent.end(
                    scope=Scope.POLL_CYCLE,
                    name=self._plan.name,
                    duration_ms=duration_ms,
                    status=status,
                )
            )
            logger.debug("shutting_down_test_runner")
            prepared.shutdown()


__all__ = ["StreamingTestRunner"]


def _test_message(topic: str, idx: int, payload: Any) -> Any:
    return Message(
        payload=payload,
        meta=MessageMeta(
            message_id=f"test-{idx}",
            topic=topic,
        ),
    )


def _first_source_topic(plan: CompiledPlan) -> str:
    """Return the first declared source topic or fail with a clear error."""
    topic = next(iter(plan.source.topics), None)
    if topic is None:
        raise RuntimeError("StreamingTestRunner requires at least one configured source topic.")
    return topic
