"""Spark testing utilities and pytest fixtures for the Loom ETL framework.

Provides :class:`SparkTestSession` — a context manager that handles Spark
configuration, Delta extension setup, and teardown.

The ``loom_spark_session`` pytest fixture is registered automatically via the
``pytest11`` entry point when ``loom-kernel[etl-spark]`` is installed — no
explicit import or ``pytest_plugins`` declaration is needed.

Usage in ``conftest.py``::

    import pytest
    from loom.etl.testing.spark import SparkTestSession
    from loom.etl.testing import StepRunner

    @pytest.fixture(scope="session")
    def spark():
        with SparkTestSession.start(app="my-tests", parallelism=1) as session:
            yield session

    @pytest.fixture
    def step_runner(spark):
        return StepRunner.spark(spark)
"""

from collections.abc import Generator
from types import TracebackType

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


class SparkTestSession:
    """Context manager for a local PySpark + Delta test session.

    Handles Delta extension configuration and clean teardown.

    Use as a context manager to guarantee ``spark.stop()`` is always called::

        with SparkTestSession.start(app="etl-tests") as spark:
            df = spark.createDataFrame([(1, "a")], ["id", "label"])

    Args:
        session: Active :class:`pyspark.sql.SparkSession`.
    """

    def __init__(self, session: SparkSession) -> None:
        self._session = session

    @classmethod
    def start(
        cls,
        *,
        app: str = "loom-etl-test",
        parallelism: int = 1,
        memory: str = "1g",
    ) -> "SparkTestSession":
        """Create and return a configured local SparkSession.

        Args:
            app:         Spark application name shown in the UI.
            parallelism: ``spark.sql.shuffle.partitions`` — keep low (1–2) for
                         unit tests to reduce overhead.
            memory:      Driver heap size string (e.g. ``"1g"``).

        Returns:
            A :class:`SparkTestSession` wrapping the active session.
        """
        builder = (
            SparkSession.builder.master("local[1]")
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
# pytest fixture — auto-registered via pytest11 entry point
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def loom_spark_session() -> Generator[SparkSession, None, None]:
    """Local SparkSession with Delta Lake extensions.

    Scoped to the test session to amortise the ~5 s JVM startup cost.
    """
    with SparkTestSession.start(app="loom-etl-tests", parallelism=1) as session:
        yield session
