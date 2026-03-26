"""ETLScenario — backend-agnostic reusable seed dataset, and StepRunnerProto."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class StepRunnerProto(Protocol):
    """Protocol satisfied by both :class:`PolarsStepRunner` and :class:`SparkStepRunner`.

    Any object implementing :meth:`seed` and :meth:`run` is compatible with
    :class:`ETLScenario`.
    """

    def seed(self, ref: str, data: list[tuple[Any, ...]], columns: list[str]) -> Any:
        """Register raw data under the logical table reference *ref*."""
        ...


class ETLScenario:
    """Named, reusable seed dataset for step runner tests.

    Stores input data as plain Python tuples — no backend dependency at
    definition time.  Data is passed to the runner's :meth:`seed` when
    :meth:`apply` is called, so any :class:`StepRunnerProto`-compatible
    runner works.

    Example::

        ORDERS = (
            ETLScenario()
            .with_table("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])
        )

        def test_double_amount(loom_polars_runner):
            ORDERS.apply(loom_polars_runner)
            result = loom_polars_runner.run(DoubleAmountStep, NoParams())
            result.assert_count(2)
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

    def apply(self, runner: StepRunnerProto) -> StepRunnerProto:
        """Seed all tables into *runner* and return it.

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
