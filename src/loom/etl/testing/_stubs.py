"""In-memory stub implementations for ETL testing.

Provides stub I/O and observer implementations that allow step and pipeline
tests to run without real storage dependencies.
"""

from __future__ import annotations

from typing import Any

from loom.etl._schema import ColumnSchema, LoomDtype
from loom.etl._source import SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import TargetSpec
from loom.etl.executor.observer._events import EventName, RunContext, RunStatus

_UNKNOWN_DTYPE = LoomDtype.NULL


class StubCatalog:
    """In-memory :class:`~loom.etl._io.TableDiscovery` for tests.

    Accepts two optional seed dictionaries:

    * ``tables``  — column name tuples (for quick existence + column checks)
    * ``schemas`` — full :class:`~loom.etl._schema.ColumnSchema` tuples
                    (for type-aware writer tests)

    When ``schemas`` is provided, ``columns`` and ``exists`` are also derived
    from it — no need to populate both.  ``update_schema`` persists changes in
    memory so later steps in the same test see the evolved schema.

    Args:
        tables:  Mapping of ``"schema.table"`` → ``("col1", "col2", ...)``.
        schemas: Mapping of ``"schema.table"`` → ``tuple[ColumnSchema, ...]``.

    Example::

        catalog = StubCatalog(
            schemas={
                "raw.orders": (
                    ColumnSchema("id",     LoomDtype.INT64,   nullable=False),
                    ColumnSchema("amount", LoomDtype.FLOAT64),
                    ColumnSchema("year",   LoomDtype.INT32,   nullable=False),
                ),
                "staging.out": (),
            }
        )
        assert catalog.exists(TableRef("raw.orders"))
        assert catalog.schema(TableRef("raw.orders"))[0].name == "id"
    """

    def __init__(
        self,
        tables: dict[str, tuple[str, ...]] | None = None,
        schemas: dict[str, tuple[ColumnSchema, ...]] | None = None,
    ) -> None:
        self._schemas: dict[str, tuple[ColumnSchema, ...]] = dict(schemas or {})
        for ref, cols in (tables or {}).items():
            if ref not in self._schemas:
                self._schemas[ref] = tuple(ColumnSchema(name=c, dtype=_UNKNOWN_DTYPE) for c in cols)

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` if the ref is registered in this stub catalog."""
        return ref.ref in self._schemas

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return registered column names, or ``()`` for unknown tables."""
        return tuple(c.name for c in self._schemas.get(ref.ref, ()))

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return the full schema, or ``None`` if the table is not yet registered."""
        return self._schemas.get(ref.ref)

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        """Persist a schema update in memory."""
        self._schemas[ref.ref] = schema


class StubSourceReader:
    """In-memory :class:`~loom.etl._io.SourceReader` for tests.

    Returns pre-seeded frames keyed by source alias.  Any alias not found
    returns ``None`` — tests should seed all expected aliases.

    Args:
        frames: Mapping of source alias → frame object.

    Example::

        reader = StubSourceReader({"orders": pl.LazyFrame({"id": [1, 2]})})
        frame = reader.read(orders_spec, params)
        assert frame is not None
    """

    def __init__(self, frames: dict[str, Any] | None = None) -> None:
        self._frames: dict[str, Any] = dict(frames or {})

    def read(self, spec: SourceSpec, _params_instance: Any) -> Any:
        """Return the pre-seeded frame for ``spec.alias``, or ``None``."""
        return self._frames.get(spec.alias)


class StubTargetWriter:
    """In-memory :class:`~loom.etl._io.TargetWriter` for tests.

    Captures all write calls in :attr:`written` for post-execution assertion.

    Attributes:
        written: List of ``(frame, spec)`` pairs in write order.

    Example::

        writer = StubTargetWriter()
        writer.write(frame, spec, params)
        assert len(writer.written) == 1
        frame_out, spec_out = writer.written[0]
        assert spec_out.mode == WriteMode.REPLACE
    """

    def __init__(self) -> None:
        self.written: list[tuple[Any, TargetSpec]] = []

    def write(self, frame: Any, spec: TargetSpec, _params_instance: Any) -> None:
        """Capture the ``(frame, spec)`` pair for later assertion."""
        self.written.append((frame, spec))


class StubRunObserver:
    """In-memory :class:`~loom.etl.executor.ETLRunObserver` for tests.

    Captures all lifecycle events as a flat list of ``(event_name, data)``
    tuples.  Helper properties provide quick access to common assertions.

    Attributes:
        events: All events in order — ``(event_name, data_dict)`` tuples.

    Example::

        observer = StubRunObserver()
        executor = ETLExecutor(reader, writer, observers=[observer])
        executor.run_step(plan, params)

        assert observer.step_statuses == ["success"]
        assert "step_start" in observer.event_names
    """

    def __init__(self) -> None:
        self.events: list[tuple[EventName, dict[str, Any]]] = []

    def on_pipeline_start(self, _plan: Any, _params: Any, ctx: RunContext) -> None:
        self.events.append((EventName.PIPELINE_START, {"run_id": ctx.run_id, "ctx": ctx}))

    def on_pipeline_end(self, ctx: RunContext, status: RunStatus, _duration_ms: int) -> None:
        self.events.append((EventName.PIPELINE_END, {"run_id": ctx.run_id, "status": status}))

    def on_process_start(self, _plan: Any, ctx: RunContext, process_run_id: str) -> None:
        self.events.append(
            (EventName.PROCESS_START, {"run_id": ctx.run_id, "process_run_id": process_run_id})
        )

    def on_process_end(self, process_run_id: str, status: RunStatus, _duration_ms: int) -> None:
        self.events.append(
            (EventName.PROCESS_END, {"process_run_id": process_run_id, "status": status})
        )

    def on_step_start(self, plan: Any, ctx: RunContext, step_run_id: str) -> None:
        self.events.append(
            (
                EventName.STEP_START,
                {
                    "step": plan.step_type.__name__,
                    "run_id": ctx.run_id,
                    "step_run_id": step_run_id,
                },
            )
        )

    def on_step_end(self, step_run_id: str, status: RunStatus, _duration_ms: int) -> None:
        self.events.append((EventName.STEP_END, {"step_run_id": step_run_id, "status": status}))

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        self.events.append((EventName.STEP_ERROR, {"step_run_id": step_run_id, "error": repr(exc)}))

    @property
    def event_names(self) -> list[EventName]:
        """Ordered list of event names."""
        return [name for name, _ in self.events]

    @property
    def step_statuses(self) -> list[RunStatus]:
        """Statuses from all ``step_end`` events in order."""
        return [d["status"] for name, d in self.events if name is EventName.STEP_END]

    @property
    def pipeline_statuses(self) -> list[RunStatus]:
        """Statuses from all ``pipeline_end`` events in order."""
        return [d["status"] for name, d in self.events if name is EventName.PIPELINE_END]
