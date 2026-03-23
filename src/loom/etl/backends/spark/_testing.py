"""Spark testing utilities for the Loom ETL framework.

Provides one public helper:

* :class:`SparkTestSession` — centralises session setup, JVM discovery, and
  teardown.  Use in ``conftest.py`` to share a session across all tests and
  amortise the ~5 s JVM startup cost.

Usage in ``conftest.py``::

    import pytest
    from loom.etl.backends.spark._testing import SparkTestSession
    from loom.etl.testing import StepRunner

    @pytest.fixture(scope="session")
    def spark():
        with SparkTestSession.start(app="my-tests", parallelism=1) as session:
            yield session

    @pytest.fixture
    def step_runner(spark):
        return StepRunner.spark(spark)
"""

from __future__ import annotations

import os
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

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
