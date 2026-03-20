"""Spark testing utilities for the Loom ETL framework.

Provides three public helpers:

* :class:`SparkTestSession` — centralises session setup, JVM discovery, and
  teardown.  Use in ``conftest.py`` to avoid boilerplate.

* :class:`SparkStepRunner` — in-memory test harness for a single ETL step.
  Accepts seeded DataFrames as inputs and captures the output without
  touching Delta storage.

* :class:`SparkScenario` — named, reusable seed dataset.  Define once as a
  module-level constant and ``apply()`` to any runner in any test.

Usage in ``conftest.py``::

    import pytest
    from loom.etl.backends.spark._testing import SparkTestSession, SparkStepRunner

    @pytest.fixture(scope="session")
    def spark():
        with SparkTestSession.start(app="my-tests", parallelism=1) as session:
            yield session

    @pytest.fixture
    def step_runner(spark):
        return SparkStepRunner(spark)

Usage in tests::

    from loom.etl.backends.spark._testing import SparkScenario
    from chispa import assert_df_equality

    ORDERS = (
        SparkScenario("orders")
        .with_table("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])
    )

    def test_double_amount(spark, step_runner):
        ORDERS.apply(step_runner)
        step_runner.run(DoubleAmountStep, NoParams())

        expected = spark.createDataFrame([(1, 20.0), (2, 40.0)], ["id", "amount"])
        assert_df_equality(step_runner.result, expected, ignore_row_order=True)
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from loom.etl._params import ETLParams
    from loom.etl._source import SourceSpec
    from loom.etl._step import ETLStep
    from loom.etl._target import TargetSpec

# ---------------------------------------------------------------------------
# Known JAVA_HOME candidates (macOS + Linux)
# ---------------------------------------------------------------------------

_JAVA_HOME_CANDIDATES: list[Path] = [
    # macOS Homebrew
    Path("/opt/homebrew/opt/openjdk@21"),
    Path("/opt/homebrew/opt/openjdk@17"),
    Path("/opt/homebrew/opt/openjdk"),
    # macOS system
    Path("/Library/Java/JavaVirtualMachines").parent,
    # Linux common
    Path("/usr/lib/jvm/java-21-openjdk-amd64"),
    Path("/usr/lib/jvm/java-17-openjdk-amd64"),
    Path("/usr/lib/jvm/java-11-openjdk-amd64"),
    Path("/usr/local/lib/jvm/openjdk17"),
]


def _discover_java_home() -> Path | None:
    """Return a valid JAVA_HOME path or ``None`` if no JVM is found."""
    # macOS java_home utility
    try:
        result = subprocess.run(
            ["/usr/libexec/java_home"],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0 and result.stdout.strip():
            candidate = Path(result.stdout.strip())
            if candidate.exists():
                return candidate
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass

    for candidate in _JAVA_HOME_CANDIDATES:
        if (candidate / "bin" / "java").exists():
            return candidate

    return None


def _ensure_java_home() -> bool:
    """Set ``JAVA_HOME`` env var if not already set.

    Returns:
        ``True`` if a JVM was found (either pre-set or discovered),
        ``False`` otherwise.
    """
    if os.environ.get("JAVA_HOME"):
        return True

    java_home = _discover_java_home()
    if java_home is None:
        return False

    os.environ["JAVA_HOME"] = str(java_home)
    return True


# ---------------------------------------------------------------------------
# SparkTestSession
# ---------------------------------------------------------------------------


class SparkTestSession:
    """Context manager / factory for a local PySpark + Delta test session.

    Handles JVM discovery, Delta extension configuration, and clean teardown.
    Use as a context manager to guarantee ``spark.stop()`` is called.

    Args:
        app:         Spark application name.
        parallelism: ``spark.sql.shuffle.partitions`` — keep low (1–2) for
                     unit tests to avoid unnecessary overhead.
        memory:      Driver memory string (e.g. ``"1g"``).

    Example::

        with SparkTestSession.start(app="etl-tests") as spark:
            df = spark.createDataFrame([(1, "a")], ["id", "label"])
    """

    def __init__(self, session: SparkSession) -> None:
        self._session = session

    @classmethod
    def is_available(cls) -> bool:
        """Return ``True`` if both PySpark and a JVM are available."""
        try:
            import delta  # noqa: F401
            import pyspark  # noqa: F401
        except ImportError:
            return False
        return _ensure_java_home()

    @classmethod
    def skip_if_unavailable(cls) -> None:  # pragma: no cover
        """Call at the top of a test or fixture to skip when Spark is not available.

        Raises:
            pytest.skip.Exception: When PySpark, delta-spark, or a JVM are absent.
        """
        import pytest

        if not cls.is_available():
            pytest.skip("Spark not available: missing pyspark, delta-spark, or JVM")

    @classmethod
    def start(
        cls,
        *,
        app: str = "loom-etl-test",
        parallelism: int = 1,
        memory: str = "1g",
    ) -> SparkTestSession:
        """Create and return a configured local SparkSession.

        Args:
            app:         Application name shown in Spark UI.
            parallelism: Shuffle partitions — keep low for test speed.
            memory:      Driver heap size.

        Returns:
            A :class:`SparkTestSession` wrapping the active session.

        Raises:
            RuntimeError: If PySpark, delta-spark, or a JVM are not available.
        """
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        if not _ensure_java_home():
            raise RuntimeError(
                "No JVM found. Install Java (e.g. OpenJDK 17) and set JAVA_HOME, "
                "or call SparkTestSession.skip_if_unavailable() to skip gracefully."
            )

        builder = (
            SparkSession.builder.master("local[1]")  # pyright: ignore[reportAttributeAccessIssue]
            .appName(app)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.driver.memory", memory)
            .config("spark.sql.shuffle.partitions", str(parallelism))
        )
        session = configure_spark_with_delta_pip(builder).getOrCreate()
        session.sparkContext.setLogLevel("ERROR")
        return cls(session)

    def __enter__(self) -> SparkSession:
        return self._session

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self._session.stop()

    @property
    def session(self) -> SparkSession:
        """The underlying :class:`pyspark.sql.SparkSession`."""
        return self._session


# ---------------------------------------------------------------------------
# Internal stubs — not part of the public API
# ---------------------------------------------------------------------------


class _StubReader:
    def __init__(self, tables: dict[str, DataFrame]) -> None:
        self._tables = tables

    def read(self, spec: SourceSpec, params_instance: Any) -> DataFrame:
        key = spec.table_ref.ref if spec.table_ref is not None else spec.alias
        return self._tables[key]


class _CapturingWriter:
    def __init__(self) -> None:
        self.frame: DataFrame | None = None
        self.spec: TargetSpec | None = None

    def write(self, frame: DataFrame, spec: TargetSpec, params_instance: Any) -> None:
        self.frame = frame
        self.spec = spec


# ---------------------------------------------------------------------------
# SparkStepRunner
# ---------------------------------------------------------------------------


class SparkStepRunner:
    """In-memory test harness for a single :class:`~loom.etl.ETLStep`.

    Hides :class:`~loom.etl.compiler.ETLCompiler`,
    :class:`~loom.etl.executor.ETLExecutor`, and all I/O stubs so tests focus
    on input data and output assertions — no Delta files, no ``tmp_path``.

    Args:
        spark: Active :class:`pyspark.sql.SparkSession`.

    Example::

        def test_double_amount(spark, step_runner):
            step_runner.seed("raw.orders", spark.createDataFrame([(1, 10.0)], ["id", "amount"]))
            step_runner.run(DoubleAmountStep, NoParams())

            expected = spark.createDataFrame([(1, 20.0)], ["id", "amount"])
            assert_df_equality(step_runner.result, expected, ignore_row_order=True)
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark
        self._tables: dict[str, DataFrame] = {}
        self._writer = _CapturingWriter()

    @property
    def spark(self) -> SparkSession:
        """The underlying :class:`pyspark.sql.SparkSession`."""
        return self._spark

    def seed(self, ref: str, df: DataFrame) -> SparkStepRunner:
        """Register *df* under the logical table reference *ref*.

        Args:
            ref: Logical table name, e.g. ``"raw.orders"``.
            df:  DataFrame returned when the step reads this table.

        Returns:
            ``self`` for fluent chaining.
        """
        self._tables[ref] = df
        return self

    def create_frame(self, data: list[tuple[Any, ...]], columns: list[str]) -> DataFrame:
        """Create a :class:`pyspark.sql.DataFrame` from raw Python data.

        Used by :class:`~loom.etl.testing.ETLScenario` to materialise seed
        tables without coupling the scenario to a SparkSession.

        Args:
            data:    Row data as a list of tuples.
            columns: Column names aligned with the tuple positions.

        Returns:
            A new :class:`pyspark.sql.DataFrame`.
        """
        return self._spark.createDataFrame(data, columns)

    def run(self, step_cls: type[ETLStep[Any]], params: ETLParams) -> SparkStepRunner:
        """Compile and execute *step_cls* against the seeded tables.

        Args:
            step_cls: :class:`~loom.etl.ETLStep` subclass to run.
            params:   Concrete params instance for this execution.

        Returns:
            ``self`` for fluent chaining.

        Raises:
            KeyError: If a source table was not seeded before calling ``run()``.
        """
        from loom.etl.compiler import ETLCompiler
        from loom.etl.executor import ETLExecutor

        plan = ETLCompiler().compile_step(step_cls)
        self._writer = _CapturingWriter()
        ETLExecutor(_StubReader(self._tables), self._writer).run_step(plan, params)
        return self

    @property
    def result(self) -> DataFrame:
        """DataFrame produced by the last :meth:`run` call.

        Raises:
            RuntimeError: If :meth:`run` has not been called yet.
        """
        if self._writer.frame is None:
            raise RuntimeError("No result available — call run() first.")
        return self._writer.frame

    @property
    def target_spec(self) -> TargetSpec:
        """Target spec from the last :meth:`run` call.

        Raises:
            RuntimeError: If :meth:`run` has not been called yet.
        """
        if self._writer.spec is None:
            raise RuntimeError("No spec available — call run() first.")
        return self._writer.spec
