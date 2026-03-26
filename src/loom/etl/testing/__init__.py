"""ETL testing utilities — stubs, runners, scenario, and pytest fixtures.

Public API
----------
* :class:`StubCatalog` — in-memory catalog for compile-time tests.
* :class:`StubSourceReader` — in-memory reader, keyed by table reference.
* :class:`StubTargetWriter` — in-memory writer that captures written frames.
* :class:`StubRunObserver` — in-memory observer that records lifecycle events.
* :class:`PolarsStepRunner` — Polars-backed in-memory step harness.
* :class:`StepResult` — unified assertion surface for step output.
* :class:`ETLScenario` — backend-agnostic reusable seed dataset.
* :class:`StepRunnerProto` — protocol satisfied by both runner types.

Pytest fixtures (auto-registered)
----------------------------------
* ``loom_polars_runner`` — fresh :class:`PolarsStepRunner` per test.
* ``loom_spark_runner``  — fresh :class:`SparkStepRunner` per test (requires
  ``loom_spark_session``, registered in :mod:`loom.etl.testing.spark`).

Spark
-----
Import :mod:`loom.etl.testing.spark` explicitly to access
:class:`~loom.etl.testing.spark.SparkStepRunner` and
:class:`~loom.etl.testing.spark.SparkTestSession`.  That module is the only
place where PySpark is imported at module level — this file has zero PySpark
dependency.
"""

from __future__ import annotations

import pytest

from loom.etl.testing._result import StepResult
from loom.etl.testing._runners import PolarsStepRunner
from loom.etl.testing._scenario import ETLScenario, StepRunnerProto
from loom.etl.testing._stubs import (
    StubCatalog,
    StubRunObserver,
    StubSourceReader,
    StubTargetWriter,
)

__all__ = [
    "StubCatalog",
    "StubSourceReader",
    "StubTargetWriter",
    "StubRunObserver",
    "PolarsStepRunner",
    "StepResult",
    "ETLScenario",
    "StepRunnerProto",
]


@pytest.fixture
def loom_polars_runner() -> PolarsStepRunner:
    """Fresh :class:`PolarsStepRunner` per test — no Delta I/O, in-memory only."""
    return PolarsStepRunner()
