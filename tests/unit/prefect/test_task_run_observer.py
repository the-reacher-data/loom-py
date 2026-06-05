"""Tests for ``loom.prefect.observer._task_run.PrefectTaskRunObserver``.

These verify two contract properties of the observer:

- It binds the active TaskRun id to the logging-bridge ``ContextVar`` on
  STEP_START and resets it on STEP_END / STEP_ERROR, so logs emitted
  during the step land under the right TaskRun in the Prefect UI.
- The ``@prefect.task`` decorator used as a Prefect-side marker is
  cached per step name, not re-defined on every STEP_START.
- ``Scope.PROCESS`` events are recorded in an internal map (Fase 4
  preparation for subgraph rendering).

We stub out the actual Prefect orchestration calls via patching so the
tests run without a live Prefect server.
"""

from __future__ import annotations

import uuid
from unittest.mock import patch

from loom.core.observability.event import LifecycleEvent, LifecycleStatus, Scope
from loom.prefect.observer import PrefectTaskRunObserver
from loom.prefect.observer._logging_bridge import current_task_run_id


def _start(name: str, step_run_id: str) -> LifecycleEvent:
    event = LifecycleEvent.start(scope=Scope.STEP, name=name)
    return event.__class__(  # rebuild with deterministic id
        scope=event.scope,
        name=event.name,
        kind=event.kind,
        id=step_run_id,
        trace_id=event.trace_id,
        correlation_id=event.correlation_id,
        meta=event.meta,
    )


def _end(name: str, step_run_id: str) -> LifecycleEvent:
    event = LifecycleEvent.end(
        scope=Scope.STEP,
        name=name,
        duration_ms=1.0,
        status=LifecycleStatus.SUCCESS,
    )
    return event.__class__(
        scope=event.scope,
        name=event.name,
        kind=event.kind,
        id=step_run_id,
        trace_id=event.trace_id,
        correlation_id=event.correlation_id,
        meta=event.meta,
        duration_ms=event.duration_ms,
        status=event.status,
    )


def test_step_start_binds_contextvar_step_end_resets() -> None:
    observer = PrefectTaskRunObserver(flow_run_id=uuid.uuid4())
    fake_task_run = uuid.uuid4()

    with (
        patch.object(observer, "_create_task_run", return_value=fake_task_run),
        patch.object(observer, "_set_state"),
    ):
        assert current_task_run_id() is None
        observer.on_event(_start("MyStep", "step-1"))
        assert current_task_run_id() == fake_task_run
        observer.on_event(_end("MyStep", "step-1"))
        assert current_task_run_id() is None


def test_step_marker_is_cached_per_name() -> None:
    observer = PrefectTaskRunObserver(flow_run_id=uuid.uuid4())

    first = observer._step_marker("StepA")
    second = observer._step_marker("StepA")
    third = observer._step_marker("StepB")

    assert first is second  # same name → same marker
    assert first is not third  # different name → different marker


def test_process_scope_events_are_recorded() -> None:
    observer = PrefectTaskRunObserver(flow_run_id=uuid.uuid4())
    process_id = str(uuid.uuid4())
    event = LifecycleEvent.start(scope=Scope.PROCESS, name="MyProcess")
    event = event.__class__(
        scope=event.scope,
        name=event.name,
        kind=event.kind,
        id=process_id,
        trace_id=event.trace_id,
        correlation_id=event.correlation_id,
        meta=event.meta,
    )

    observer.on_event(event)

    # The Scope.PROCESS event lands in the internal map but does NOT
    # create a TaskRun yet (Fase 4 preparation).
    assert uuid.UUID(process_id) in observer._process_events
    assert not observer._task_runs
