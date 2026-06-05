"""PrefectTaskRunObserver — synthesise per-step Prefect TaskRuns from loom events.

The ETL Prefect factory invokes ``runner.run(pipeline, params)`` exactly ONCE
per flow run (preserving every loom invariant — single Prometheus push, single
lineage record, RUN-scope temps in memory, single ``cleanup_correlation()``).

For per-step visibility in the Prefect UI we attach this observer to the
runner. Each ``STEP`` lifecycle event coming out of loom is translated into
a Prefect ``TaskRun`` state transition via the orchestration client:

* ``START`` → create a TaskRun bound to the active flow run, mark Running.
* ``END``   → transition that TaskRun to Completed.
* ``ERROR`` → transition to Failed with the loom error message.

The TaskRuns appear in the Prefect UI as standard rows under the flow run.
This is the only externally-visible side effect; the observer never blocks
or alters runner execution.

Failure isolation: the observer's ``on_event`` swallows any exception so a
Prefect API hiccup never aborts the loom pipeline. State will simply be
missing in the UI for the affected step.
"""

from __future__ import annotations

import contextlib
import logging
import uuid
from contextvars import Token
from typing import Any

from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.prefect._async import run_sync
from loom.prefect.observer._logging_bridge import _current_task_run_id

_log = logging.getLogger(__name__)


class PrefectTaskRunObserver:
    """``LifecycleObserver`` that creates Prefect TaskRuns per loom step.

    Args:
        flow_run_id: UUID of the active Prefect flow run. Passed explicitly
            (instead of looked up from ``prefect.runtime``) so the observer
            can be unit-tested in isolation.

    Example::

        from loom.prefect import PrefectTaskRunObserver
        from prefect.runtime import flow_run

        observer = PrefectTaskRunObserver(flow_run_id=flow_run.id)
        runner = ETLRunner.from_yaml(path, extra_observers=[observer])
        runner.run(pipeline, params)
    """

    def __init__(self, flow_run_id: uuid.UUID | str) -> None:
        self._flow_run_id = (
            flow_run_id if isinstance(flow_run_id, uuid.UUID) else uuid.UUID(str(flow_run_id))
        )
        # step_run_id (from loom) → task_run_id (from Prefect)
        self._task_runs: dict[str, uuid.UUID] = {}
        # step_run_id → ContextVar token, kept so STEP_END/ERROR can
        # reset the logging-bridge binding for this step.
        self._tokens: dict[str, Token[uuid.UUID | None]] = {}
        # Cache @task decorators by step name to avoid re-defining one
        # on every STEP_START (was an anti-pattern flagged in review).
        self._task_markers: dict[str, Any] = {}
        # Pre-bind the ``Scope.PROCESS`` map (Fase 4): events arrive
        # already so future subflow integration only needs to read this.
        self._process_events: dict[uuid.UUID, LifecycleEvent] = {}

    def on_event(self, event: LifecycleEvent) -> None:
        """Handle one lifecycle event from the ``ObservabilityRuntime``.

        Args:
            event: Immutable lifecycle event emitted by the runtime.
        """
        if event.scope is Scope.PROCESS:
            self._record_process_event(event)
            return
        if event.scope is not Scope.STEP:
            return
        _log.info(
            "PrefectTaskRunObserver event step=%s kind=%s step_run_id=%s",
            event.name,
            event.kind,
            event.id,
        )
        try:
            match event.kind:
                case EventKind.START:
                    self._on_start(event)
                case EventKind.END:
                    self._on_end(event)
                case EventKind.ERROR:
                    self._on_error(event)
        except Exception:  # noqa: BLE001
            _log.warning(
                "PrefectTaskRunObserver swallowed exception (step=%s, kind=%s)",
                event.name,
                event.kind,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Internal handlers
    # ------------------------------------------------------------------

    def _on_start(self, event: LifecycleEvent) -> None:
        step_run_id = event.id
        if step_run_id is None:
            return
        task_run_id = self._create_task_run(event)
        if task_run_id is not None:
            self._task_runs[step_run_id] = task_run_id
            # Bind the TaskRun for any logger.* calls inside the step so
            # the logging bridge ships them under this TaskRun in the UI.
            self._tokens[step_run_id] = _current_task_run_id.set(task_run_id)

    def _on_end(self, event: LifecycleEvent) -> None:
        self._reset_log_binding(event.id)
        task_run_id = self._task_runs.pop(event.id, None) if event.id else None
        if task_run_id is None:
            return
        self._set_state(task_run_id, completed=True)

    def _on_error(self, event: LifecycleEvent) -> None:
        self._reset_log_binding(event.id)
        task_run_id = self._task_runs.pop(event.id, None) if event.id else None
        if task_run_id is None:
            return
        self._set_state(task_run_id, completed=False, message=event.error)

    def _reset_log_binding(self, step_run_id: str | None) -> None:
        if step_run_id is None:
            return
        token = self._tokens.pop(step_run_id, None)
        if token is None:
            return
        # Token may have been created in a different async context — best effort.
        with contextlib.suppress(ValueError):
            _current_task_run_id.reset(token)

    def _record_process_event(self, event: LifecycleEvent) -> None:
        # TODO(subflow): wire Scope.PROCESS into a Prefect subflow / artifact
        # so processes render as subgraphs in the UI. For now we only
        # observe them so the integration point exists and is testable.
        if event.id is None:
            return
        self._process_events[uuid.UUID(str(event.id))] = event

    # ------------------------------------------------------------------
    # Prefect orchestration calls (sync, best-effort)
    # ------------------------------------------------------------------

    def _create_task_run(self, event: LifecycleEvent) -> uuid.UUID | None:
        # Lazy import: keep observer importable in environments without Prefect.
        from prefect.client.orchestration import get_client  # noqa: PLC0415
        from prefect.states import Running  # noqa: PLC0415

        marker = self._step_marker(event.name)

        async def _create() -> uuid.UUID:
            async with get_client() as client:
                created = await client.create_task_run(
                    task=marker,
                    flow_run_id=self._flow_run_id,
                    dynamic_key=str(event.id),
                    name=event.name,
                    state=Running(),
                    extra_tags=["loom-step"],
                )
                return uuid.UUID(str(created.id))

        result = run_sync(_create())
        return result if isinstance(result, uuid.UUID) else None

    def _step_marker(self, name: str) -> Any:
        """Return a cached no-op ``@task`` for *name*.

        The orchestration plane only uses the Task object for naming /
        ``task_key`` — it is never invoked. Caching by step name avoids
        re-defining a decorated function on every STEP_START.
        """
        marker = self._task_markers.get(name)
        if marker is not None:
            return marker
        from prefect import task  # noqa: PLC0415

        @task(name=name)
        def _step_marker() -> None:  # pragma: no cover - never called
            return None

        self._task_markers[name] = _step_marker
        return _step_marker

    def _set_state(
        self, task_run_id: uuid.UUID, *, completed: bool, message: str | None = None
    ) -> None:
        from prefect.client.orchestration import get_client  # noqa: PLC0415
        from prefect.states import Completed, Failed  # noqa: PLC0415

        state: Any = Completed() if completed else Failed(message=message or "loom step failed")

        async def _set() -> None:
            async with get_client() as client:
                await client.set_task_run_state(task_run_id, state, force=True)

        run_sync(_set())


__all__ = ["PrefectTaskRunObserver"]
