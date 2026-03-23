"""ETL testing utilities — stub I/O, observer, and unified step runner.

Provides in-memory implementations of :class:`~loom.etl._io.TableDiscovery`,
:class:`~loom.etl._io.SourceReader`, and :class:`~loom.etl._io.TargetWriter`
that allow step and pipeline tests to run without real storage dependencies.

Also provides :class:`StepRunner` — a unified in-memory harness typed on the
frame type — and :class:`ETLScenario` for reusable backend-agnostic seed datasets.

Example::

    from loom.etl.testing import StepRunner, ETLScenario

    ORDERS = ETLScenario().with_table("raw.orders", [(1, 10.0)], ["id", "amount"])

    # Polars — result is pl.DataFrame, no cast needed
    runner = StepRunner.polars()
    ORDERS.apply(runner).run(MyPolarsStep, params)
    assert runner.result["amount"].to_list() == [20.0]

    # Spark — result is pyspark.sql.DataFrame, no cast needed
    runner = StepRunner.spark(spark_session)
    ORDERS.apply(runner).run(MySparkStep, params)
    assert_df_equality(runner.result, expected)
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    import polars as pl
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession

from loom.etl._schema import ColumnSchema, LoomDtype
from loom.etl._source import SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import TargetSpec
from loom.etl.executor.observer._events import EventName, RunStatus

# TypeVar for the frame type produced by a StepRunner backend.
FrameT = TypeVar("FrameT")

# TypeVar for preserving the concrete runner type through ETLScenario.apply().
_RunnerT = TypeVar("_RunnerT", bound="StepRunnerProto")


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
        # Merge explicit tables into _schemas as name-only entries when absent
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


# Sentinel dtype used when a table is registered by column names only.
_UNKNOWN_DTYPE = LoomDtype.NULL


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

    # --- protocol implementation ---

    def on_pipeline_start(self, plan: Any, _params: Any, run_id: str) -> None:
        self.events.append((EventName.PIPELINE_START, {"run_id": run_id}))

    def on_pipeline_end(self, run_id: str, status: RunStatus, duration_ms: int) -> None:
        self.events.append((EventName.PIPELINE_END, {"run_id": run_id, "status": status}))

    def on_process_start(self, plan: Any, run_id: str, process_run_id: str) -> None:
        self.events.append(
            (EventName.PROCESS_START, {"run_id": run_id, "process_run_id": process_run_id})
        )

    def on_process_end(self, process_run_id: str, status: RunStatus, duration_ms: int) -> None:
        self.events.append(
            (EventName.PROCESS_END, {"process_run_id": process_run_id, "status": status})
        )

    def on_step_start(self, plan: Any, run_id: str, step_run_id: str) -> None:
        self.events.append(
            (
                EventName.STEP_START,
                {
                    "step": plan.step_type.__name__,
                    "run_id": run_id,
                    "step_run_id": step_run_id,
                },
            )
        )

    def on_step_end(self, step_run_id: str, status: RunStatus, duration_ms: int) -> None:
        self.events.append((EventName.STEP_END, {"step_run_id": step_run_id, "status": status}))

    def on_step_error(self, step_run_id: str, exc: Exception) -> None:
        self.events.append((EventName.STEP_ERROR, {"step_run_id": step_run_id, "error": repr(exc)}))

    # --- convenience accessors ---

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


# ---------------------------------------------------------------------------
# StepRunner — unified in-memory harness, typed on the backend frame type
# ---------------------------------------------------------------------------


class _BackendKind(Enum):
    POLARS = "polars"
    SPARK = "spark"


class _RunnerCapturingWriter:
    frame: Any = None
    spec: TargetSpec | None = None

    def write(self, frame: Any, spec: TargetSpec, params_instance: Any) -> None:  # noqa: ARG002
        self.frame = frame
        self.spec = spec


class _RunnerStubReader:
    def __init__(self, frames: dict[str, Any]) -> None:
        self._frames = frames

    def read(self, spec: SourceSpec, params_instance: Any) -> Any:  # noqa: ARG002
        key = spec.table_ref.ref if spec.table_ref is not None else spec.alias
        return self._frames[key]


class StepRunner(Generic[FrameT]):
    """Unified in-memory test harness for :class:`~loom.etl.ETLStep` subclasses.

    Generic on the frame type — the factory method fixes ``FrameT`` so all
    result accessors are fully typed without casts:

    * :meth:`polars` → ``StepRunner[pl.DataFrame]``
    * :meth:`spark`  → ``StepRunner[pyspark.sql.DataFrame]``

    No Delta I/O — all reads and writes are captured in memory.

    Example::

        # Polars — result is pl.DataFrame
        runner = StepRunner.polars()
        runner.seed("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])
        runner.run(DoubleAmountStep, NoParams())
        assert runner.result["amount"].to_list() == [20.0, 40.0]

        # Spark — result is pyspark.sql.DataFrame
        runner = StepRunner.spark(session)
        runner.seed("raw.orders", [(1, 10.0)], ["id", "amount"])
        runner.run(DoubleAmountStep, NoParams())
        assert_df_equality(runner.result, expected)
    """

    def __init__(self, backend: _BackendKind, spark: SparkSession | None = None) -> None:
        self._backend = backend
        self._spark = spark
        self._seeds: dict[str, tuple[list[tuple[Any, ...]], list[str]]] = {}
        self._writer = _RunnerCapturingWriter()
        self._last_params: Any = None

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def polars(cls) -> StepRunner[pl.DataFrame]:
        """Return a runner that materialises seed frames as ``polars.DataFrame``."""
        return cls(_BackendKind.POLARS)  # type: ignore[return-value]

    @classmethod
    def spark(cls, session: SparkSession) -> StepRunner[SparkDataFrame]:
        """Return a runner that materialises seed frames as ``pyspark.sql.DataFrame``.

        Args:
            session: Active :class:`pyspark.sql.SparkSession`.
        """
        return cls(_BackendKind.SPARK, spark=session)  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Seeding
    # ------------------------------------------------------------------

    def seed(
        self,
        ref: str,
        data: list[tuple[Any, ...]],
        columns: list[str],
    ) -> StepRunner[FrameT]:
        """Register raw data under the logical table reference *ref*.

        Args:
            ref:     Logical table name, e.g. ``"raw.orders"``.
            data:    Row data as a list of tuples.
            columns: Column names aligned with the tuple positions.

        Returns:
            ``self`` for fluent chaining.
        """
        self._seeds[ref] = (list(data), list(columns))
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self, step_cls: type[Any], params: Any) -> StepRunner[FrameT]:
        """Compile and execute *step_cls* against the seeded tables.

        Args:
            step_cls: :class:`~loom.etl.ETLStep` subclass to execute.
            params:   Concrete params instance for this run.

        Returns:
            ``self`` for fluent chaining.

        Raises:
            KeyError:     If a source table was not seeded before calling ``run()``.
            RuntimeError: If ``StepRunner.spark(session)`` was not used for a Spark step.
        """
        from loom.etl.compiler import ETLCompiler
        from loom.etl.executor import ETLExecutor

        self._last_params = params
        frames = {ref: self._materialize(data, cols) for ref, (data, cols) in self._seeds.items()}
        plan = ETLCompiler().compile_step(step_cls)
        self._writer = _RunnerCapturingWriter()
        ETLExecutor(_RunnerStubReader(frames), self._writer).run_step(plan, params)
        return self

    # ------------------------------------------------------------------
    # Result accessors
    # ------------------------------------------------------------------

    @property
    def result(self) -> FrameT:
        """Frame produced by the last :meth:`run` call.

        The return type matches the backend: ``pl.DataFrame`` for Polars,
        ``pyspark.sql.DataFrame`` for Spark — no cast required.

        Raises:
            RuntimeError: If :meth:`run` has not been called yet.
        """
        if self._writer.frame is None:
            raise RuntimeError("No result — call run() first.")
        return self._writer.frame  # type: ignore[no-any-return]

    @property
    def target_spec(self) -> TargetSpec:
        """Target spec from the last :meth:`run` call.

        Useful for asserting write mode, partition columns, and predicates.

        Raises:
            RuntimeError: If :meth:`run` has not been called yet.
        """
        if self._writer.spec is None:
            raise RuntimeError("No spec — call run() first.")
        return self._writer.spec

    @property
    def resolved_predicate(self) -> str | None:
        """SQL predicate resolved from the last run's target spec and params.

        Returns ``None`` when the write mode has no predicate
        (e.g. ``APPEND``, ``REPLACE``).  Useful for asserting
        ``replace_where`` / ``replace_partitions(values=…)`` SQL without
        touching Delta.

        Example::

            runner.run(BackfillStep, params)
            assert runner.resolved_predicate == "date >= '2024-01-01' AND date <= '2024-01-31'"
        """
        spec = self._writer.spec
        if spec is None or spec.replace_predicate is None:
            return None
        from loom.etl._predicate_sql import predicate_to_sql

        return predicate_to_sql(spec.replace_predicate, self._last_params)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _materialize(self, data: list[tuple[Any, ...]], columns: list[str]) -> Any:
        if self._backend is _BackendKind.POLARS:
            return _to_polars_frame(data, columns)
        if self._spark is None:
            raise RuntimeError(
                "Spark backend requires a SparkSession — use StepRunner.spark(session)."
            )
        return self._spark.createDataFrame(data, columns)


def _to_polars_frame(data: list[tuple[Any, ...]], columns: list[str]) -> Any:
    import polars as pl

    if not data:
        return pl.DataFrame(schema=columns)
    return pl.DataFrame(
        {col: list(vals) for col, vals in zip(columns, zip(*data, strict=True), strict=True)}
    )


# ---------------------------------------------------------------------------
# ETLScenario — backend-agnostic reusable seed dataset
# ---------------------------------------------------------------------------


@runtime_checkable
class StepRunnerProto(Protocol):
    """Protocol for runners compatible with :class:`ETLScenario`.

    Any object implementing :meth:`seed` satisfies this protocol.
    """

    def seed(self, ref: str, data: list[tuple[Any, ...]], columns: list[str]) -> Any:
        """Register raw data under the logical table reference *ref*."""
        ...


class ETLScenario:
    """Named, reusable seed dataset for step runner tests.

    Stores input data as plain Python tuples — no backend dependency at
    definition time.  Data is passed directly to the runner's :meth:`seed`
    when :meth:`apply` is called, so any :class:`StepRunner` backend works.

    :meth:`apply` is generic — it returns the same concrete runner type it
    receives, preserving ``FrameT`` for typed result access after the call.

    Example::

        ORDERS = (
            ETLScenario()
            .with_table("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])
        )

        def test_double_amount(step_runner):
            ORDERS.apply(step_runner).run(DoubleAmountStep, NoParams())
            assert step_runner.result["amount"].to_list() == [20.0, 40.0]
    """

    def __init__(self) -> None:
        self._seeds: list[tuple[str, list[tuple[Any, ...]], list[str]]] = []

    def with_table(
        self,
        ref: str,
        data: list[tuple[Any, ...]],
        columns: list[str],
    ) -> ETLScenario:
        """Add a table seed to this scenario.

        Args:
            ref:     Logical table reference, e.g. ``"raw.orders"``.
            data:    Row data as a list of tuples.
            columns: Column names aligned with the tuple positions.

        Returns:
            ``self`` for fluent chaining.
        """
        self._seeds.append((ref, list(data), list(columns)))
        return self

    def apply(self, runner: _RunnerT) -> _RunnerT:
        """Seed all tables into *runner* and return it unchanged.

        The return type preserves the concrete runner type — a
        ``StepRunner[pl.DataFrame]`` in, a ``StepRunner[pl.DataFrame]`` out.

        Args:
            runner: Any :class:`StepRunnerProto`-compatible runner.

        Returns:
            The same *runner* for fluent chaining.
        """
        for ref, data, columns in self._seeds:
            runner.seed(ref, data, columns)
        return runner

    def __repr__(self) -> str:
        tables = [ref for ref, *_ in self._seeds]
        return f"ETLScenario(tables={tables})"
