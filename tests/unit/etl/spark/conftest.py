"""Fixtures for ETL Spark integration tests.

Requires ``pyspark>=3.5`` and ``delta-spark>=3.2`` to be installed.
The entire package is skipped automatically when either is absent.

SparkSession is scoped to the test session — Spark startup is expensive
(~5s) so we create it once and share across all tests in this package.
"""

from __future__ import annotations

from collections.abc import Callable, Generator
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

from loom.etl._schema import ColumnSchema  # noqa: E402
from loom.etl._table import TableRef  # noqa: E402
from loom.etl.backends.polars import DeltaCatalog  # noqa: E402
from loom.etl.backends.spark import SparkDeltaReader, SparkDeltaWriter  # noqa: E402
from loom.etl.backends.spark._dtype import spark_to_loom  # noqa: E402
from loom.etl.backends.spark._testing import SparkTestSession  # noqa: E402

# ---------------------------------------------------------------------------
# SparkSession — session-scoped, created once per pytest run
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Local SparkSession with Delta Lake extensions.

    Scoped to the session to avoid the ~5s JVM startup cost per test.
    Log level set to ERROR to suppress verbose Spark output.
    """
    SparkTestSession.skip_if_unavailable()
    with SparkTestSession.start(app="loom-etl-spark-tests", parallelism=1) as session:
        yield session


# ---------------------------------------------------------------------------
# Per-test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def spark_root(tmp_path: Path) -> Generator[Path, None, None]:
    """Temporary Delta catalog root, cleaned up after each test."""
    yield tmp_path


@pytest.fixture
def spark_catalog(spark_root: Path) -> DeltaCatalog:
    """Fresh DeltaCatalog per test (backed by spark_root)."""
    return DeltaCatalog(spark_root)


@pytest.fixture
def spark_reader(spark: SparkSession, spark_root: Path) -> SparkDeltaReader:
    """SparkDeltaReader pointing at spark_root."""
    return SparkDeltaReader(spark, spark_root)


@pytest.fixture
def spark_writer(
    spark: SparkSession,
    spark_root: Path,
    spark_catalog: DeltaCatalog,
) -> SparkDeltaWriter:
    """SparkDeltaWriter pointing at spark_root, backed by spark_catalog."""
    return SparkDeltaWriter(spark, spark_root, spark_catalog)


# ---------------------------------------------------------------------------
# seed_table — write a Spark DataFrame as a Delta table
# ---------------------------------------------------------------------------


def spark_table_path(root: Path, ref: TableRef) -> Path:
    """Resolve a TableRef to a filesystem path under root."""
    return root.joinpath(*ref.ref.split("."))


@pytest.fixture
def seed_spark_table(
    spark: SparkSession,
    spark_root: Path,
    spark_catalog: DeltaCatalog,
) -> Callable[[str | TableRef, DataFrame], Path]:
    """Factory fixture that writes a Spark DataFrame as a real Delta table
    and registers its schema in the catalog.

    Returns:
        ``seed(ref, data) -> Path``
    """

    def _seed(ref: str | TableRef, data: DataFrame) -> Path:
        table_ref = TableRef(ref) if isinstance(ref, str) else ref
        path = spark_table_path(spark_root, table_ref)
        path.mkdir(parents=True, exist_ok=True)
        data.write.format("delta").mode("overwrite").save(str(path))
        schema = tuple(
            ColumnSchema(name=f.name, dtype=spark_to_loom(f.dataType)) for f in data.schema.fields
        )
        spark_catalog.update_schema(table_ref, schema)
        return path

    return _seed
