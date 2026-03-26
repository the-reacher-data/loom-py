"""PolarsStepRunner — in-memory Polars step test harness."""

from __future__ import annotations

from typing import Any

import polars as pl

from loom.etl._predicate_sql import predicate_to_sql
from loom.etl._source import SourceSpec
from loom.etl._target import TargetSpec
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor
from loom.etl.testing._result import StepResult


class _PolarsCapturingWriter:
    def __init__(self) -> None:
        self.frame: pl.LazyFrame | pl.DataFrame | None = None
        self.spec: TargetSpec | None = None
        self._last_params: Any = None

    def write(self, frame: Any, spec: TargetSpec, params_instance: Any) -> None:
        self.frame = frame
        self.spec = spec
        self._last_params = params_instance


class _PolarsStubReader:
    def __init__(self, frames: dict[str, pl.LazyFrame]) -> None:
        self._frames = frames

    def read(self, spec: SourceSpec, _params_instance: Any) -> pl.LazyFrame:
        key = spec.table_ref.ref if spec.table_ref is not None else spec.alias
        return self._frames[key]


def _build_lazy_frame(data: list[tuple[Any, ...]], columns: list[str]) -> pl.LazyFrame:
    if not data:
        return pl.DataFrame({col: [] for col in columns}).lazy()
    rows = {col: [row[i] for row in data] for i, col in enumerate(columns)}
    return pl.DataFrame(rows).lazy()


class PolarsStepRunner:
    """In-memory test harness for Polars :class:`~loom.etl.ETLStep` subclasses.

    Seeds are plain Python tuples — no Polars dependency at definition time.
    Internally, each seed is converted to a ``polars.LazyFrame`` so
    ``execute()`` receives the same type as in production.

    No Delta I/O — reads and writes are captured in memory.

    Example::

        def test_double_amount(loom_polars_runner):
            loom_polars_runner.seed("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])
            result = loom_polars_runner.run(DoubleAmountStep, NoParams())
            result.assert_count(2)
            result.assert_schema({"amount": LoomDtype.FLOAT64})
            result.show()
    """

    def __init__(self) -> None:
        self._seeds: dict[str, tuple[list[tuple[Any, ...]], list[str]]] = {}
        self._writer = _PolarsCapturingWriter()

    def seed(
        self,
        ref: str,
        data: list[tuple[Any, ...]],
        columns: list[str],
    ) -> PolarsStepRunner:
        """Register raw data under the logical table reference *ref*.

        Args:
            ref:     Logical table reference, e.g. ``"raw.orders"``.
            data:    Row data as a list of tuples.
            columns: Column names aligned with the tuple positions.

        Returns:
            ``self`` for fluent chaining.
        """
        self._seeds[ref] = (list(data), list(columns))
        return self

    def run(self, step_cls: type[Any], params: Any) -> StepResult:
        """Compile and execute *step_cls* against the seeded tables.

        Args:
            step_cls: :class:`~loom.etl.ETLStep` subclass to execute.
            params:   Concrete params instance for this run.

        Returns:
            :class:`~loom.etl.testing._result.StepResult` for assertions.

        Raises:
            KeyError:     When a source table was not seeded.
            RuntimeError: When the step produced no output.
        """
        frames = {ref: _build_lazy_frame(data, cols) for ref, (data, cols) in self._seeds.items()}
        plan = ETLCompiler().compile_step(step_cls)
        self._writer = _PolarsCapturingWriter()
        ETLExecutor(_PolarsStubReader(frames), self._writer).run_step(plan, params)
        raw = self._writer.frame
        if raw is None:
            raise RuntimeError("Step produced no output — check that target is declared.")
        collected = raw.collect() if isinstance(raw, pl.LazyFrame) else raw
        return StepResult(collected)

    @property
    def target_spec(self) -> TargetSpec:
        """Target spec from the last :meth:`run` call.

        Raises:
            RuntimeError: When :meth:`run` has not been called yet.
        """
        if self._writer.spec is None:
            raise RuntimeError("No spec — call run() first.")
        return self._writer.spec

    @property
    def resolved_predicate(self) -> str | None:
        """SQL predicate resolved from the last run's target spec and params.

        Returns ``None`` when the write mode has no predicate.
        """
        spec = self._writer.spec
        if spec is None or spec.replace_predicate is None:
            return None
        return predicate_to_sql(spec.replace_predicate, self._writer._last_params)
