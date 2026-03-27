"""StepResult — unified assertion interface for step test output."""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from loom.etl.schema._schema import LoomDtype


@dataclass(frozen=True)
class StepResult:
    """Output of a :class:`PolarsStepRunner` or :class:`SparkStepRunner` run.

    Provides a backend-agnostic assertion surface.  The internal frame is
    always a materialised ``polars.DataFrame`` — Spark results are converted
    before constructing this object.

    Args:
        _frame: Materialised output frame.

    Example::

        result = runner.run(MyStep, params)
        result.assert_count(10)
        result.assert_schema({"id": LoomDtype.INT64, "amount": LoomDtype.FLOAT64})
        result.show()
    """

    _frame: pl.DataFrame

    def to_polars(self) -> pl.DataFrame:
        """Return the materialised output as a ``polars.DataFrame``."""
        return self._frame

    def assert_schema(self, expected: dict[str, LoomDtype]) -> None:
        """Assert that the output columns match the expected Loom schema.

        Args:
            expected: Mapping of column name → :class:`~loom.etl._schema.LoomDtype`.

        Raises:
            AssertionError: When a column is missing or has a different dtype.
        """
        from loom.etl.backends.polars._dtype import loom_type_to_polars

        for col_name, loom_dtype in expected.items():
            actual = self._frame.schema.get(col_name)
            assert actual is not None, f"Column '{col_name}' not found in result schema"
            expected_polars = loom_type_to_polars(loom_dtype)
            assert actual == expected_polars, (
                f"Column '{col_name}': expected {expected_polars}, got {actual}"
            )

    def assert_count(self, n: int) -> None:
        """Assert the result contains exactly *n* rows.

        Args:
            n: Expected row count.

        Raises:
            AssertionError: When the row count differs.
        """
        actual = len(self._frame)
        assert actual == n, f"Expected {n} rows, got {actual}"

    def assert_not_empty(self) -> None:
        """Assert the result contains at least one row.

        Raises:
            AssertionError: When the result is empty.
        """
        assert len(self._frame) > 0, "Expected non-empty result, got 0 rows"

    def show(self, n: int = 10) -> None:
        """Print the first *n* rows to stdout.

        Useful for visual inspection during ``pytest -s`` runs.

        Args:
            n: Number of rows to display. Defaults to 10.
        """
        print(self._frame.head(n))
