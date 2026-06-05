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

import logging
import uuid
from typing import Any

from loom.core.observability.event import EventKind, LifecycleEvent, Scope
from loom.prefect._async import run_sync

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

    def on_event(self, event: LifecycleEvent) -> None:
        """Handle one lifecycle event from the ``ObservabilityRuntime``.

        Args:
            event: Immutable lifecycle event emitted by the runtime.
        """
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

    def _on_end(self, event: LifecycleEvent) -> None:
        task_run_id = self._task_runs.pop(event.id, None) if event.id else None
        if task_run_id is None:
            return
        self._set_state(task_run_id, completed=True)

    def _on_error(self, event: LifecycleEvent) -> None:
        task_run_id = self._task_runs.pop(event.id, None) if event.id else None
        if task_run_id is None:
            return
        self._set_state(task_run_id, completed=False, message=event.error)

    # ------------------------------------------------------------------
    # Prefect orchestration calls (sync, best-effort)
    # ------------------------------------------------------------------

    def _create_task_run(self, event: LifecycleEvent) -> uuid.UUID | None:
        # Lazy import: keep observer importable in environments without Prefect.
        from prefect import task  # noqa: PLC0415
        from prefect.client.orchestration import get_client  # noqa: PLC0415
        from prefect.states import Running  # noqa: PLC0415

        # The client requires a Prefect Task object even though we never
        # invoke it — the orchestration plane uses it only for naming /
        # task_key. We synthesise a no-op task per step name so the UI
        # shows a meaningful row.
        @task(name=event.name)
        def _step_marker() -> None:  # pragma: no cover - never called
            return None

        async def _create() -> uuid.UUID:
            async with get_client() as client:
                created = await client.create_task_run(
                    task=_step_marker,
                    flow_run_id=self._flow_run_id,
                    dynamic_key=str(event.id),
                    name=event.name,
                    state=Running(),
                    extra_tags=["loom-step"],
                )
                return uuid.UUID(str(created.id))

        result = run_sync(_create())
        return result if isinstance(result, uuid.UUID) else None

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
