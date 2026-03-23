"""pytest fixtures for Spark + Delta integration tests.

Registered via the ``pytest11`` entry point so fixtures are available
automatically in any project that installs loom-py — no ``pytest_plugins``
or explicit import required.

Available fixtures
------------------
``loom_spark_session`` (scope=``"session"``)
    A local SparkSession with Delta Lake extensions, shared across the
    entire test run.  Skipped automatically when PySpark or a JVM are
    absent.

Usage in tests::

    def test_my_step(loom_spark_session):
        runner = StepRunner.spark(loom_spark_session)
        ...
"""

from __future__ import annotations

from collections.abc import Generator

import pytest

from loom.etl.backends.spark._testing import SparkTestSession


@pytest.fixture(scope="session")
def loom_spark_session() -> Generator[object, None, None]:
    """Local SparkSession with Delta Lake extensions.

    Scoped to the test session to amortise the ~5 s JVM startup cost.
    Skips automatically when PySpark, delta-spark, or a JVM are absent.
    """
    SparkTestSession.skip_if_unavailable()
    with SparkTestSession.start(app="loom-etl-tests", parallelism=1) as session:
        yield session
