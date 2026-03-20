"""Fixtures for ETL Polars integration tests.

Requires ``polars>=1.0`` and ``deltalake>=0.25`` to be installed.
The entire package is skipped automatically when either is absent.
"""

from __future__ import annotations

from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

import pytest

# Skip the entire module when polars / deltalake are not installed.
pytest.importorskip("polars")
pytest.importorskip("deltalake")

import polars as pl  # noqa: E402 — import after importorskip guard
from deltalake import DeltaTable, write_deltalake  # noqa: E402

from loom.etl._schema import ColumnSchema, LoomDtype  # noqa: E402
from loom.etl._source import SourceSpec  # noqa: E402
from loom.etl._table import TableRef  # noqa: E402
from loom.etl._target import TargetSpec, WriteMode  # noqa: E402
from loom.etl.testing import StubCatalog  # noqa: E402

# ---------------------------------------------------------------------------
# Polars → LoomDtype mapping
# ---------------------------------------------------------------------------

_POLARS_TO_LOOM: dict[type, LoomDtype] = {
    pl.Int8: LoomDtype.INT8,
    pl.Int16: LoomDtype.INT16,
    pl.Int32: LoomDtype.INT32,
    pl.Int64: LoomDtype.INT64,
    pl.UInt8: LoomDtype.UINT8,
    pl.UInt16: LoomDtype.UINT16,
    pl.UInt32: LoomDtype.UINT32,
    pl.UInt64: LoomDtype.UINT64,
    pl.Float32: LoomDtype.FLOAT32,
    pl.Float64: LoomDtype.FLOAT64,
    pl.String: LoomDtype.UTF8,
    pl.Boolean: LoomDtype.BOOLEAN,
    pl.Date: LoomDtype.DATE,
    pl.Datetime: LoomDtype.DATETIME,
}


def _polars_dtype_to_loom(dtype: pl.DataType) -> LoomDtype:
    return _POLARS_TO_LOOM.get(type(dtype), LoomDtype.NULL)


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def table_path(root: Path, ref: TableRef) -> Path:
    """Resolve a ``TableRef`` to a filesystem path under *root*.

    ``"raw.orders"`` → ``root/raw/orders``.
    """
    return root.joinpath(*ref.ref.split("."))


# ---------------------------------------------------------------------------
# Core fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def delta_root(tmp_path: Path) -> Generator[Path, None, None]:
    """Temporary root directory for Delta tables.

    Yielded so any cleanup (beyond what ``tmp_path`` already does) can be
    injected here in the future.  Currently the directory is removed by
    pytest's own ``tmp_path`` teardown.
    """
    yield tmp_path


@pytest.fixture
def delta_catalog() -> StubCatalog:
    """Fresh in-memory catalog for each test."""
    return StubCatalog()


@pytest.fixture
def seed_table(
    delta_root: Path,
    delta_catalog: StubCatalog,
) -> Callable[[str | TableRef, pl.DataFrame], Path]:
    """Factory fixture that writes a :class:`polars.DataFrame` as a real
    Delta table and registers its schema in the catalog.

    Returns:
        ``seed(ref, data) -> Path`` — call once per source table needed
        by the test.  ``ref`` accepts either a dotted string like
        ``"raw.orders"`` or a :class:`~loom.etl.TableRef`.
    """

    def _seed(ref: str | TableRef, data: pl.DataFrame) -> Path:
        table_ref = TableRef(ref) if isinstance(ref, str) else ref
        path = table_path(delta_root, table_ref)
        path.mkdir(parents=True, exist_ok=True)
        # Use deltalake directly to avoid polars<->deltalake version skew
        write_deltalake(str(path), data.to_arrow(), mode="overwrite")
        schema = tuple(
            ColumnSchema(name=col, dtype=_polars_dtype_to_loom(dt))
            for col, dt in zip(data.columns, data.dtypes, strict=True)
        )
        delta_catalog.update_schema(table_ref, schema)
        return path

    return _seed


# ---------------------------------------------------------------------------
# Minimal Polars + Delta reader / writer (pre-sprint-5 test helpers)
# ---------------------------------------------------------------------------


class MinimalPolarsDeltaReader:
    """Minimal :class:`~loom.etl._io.SourceReader` backed by ``pl.scan_delta()``.

    No predicate pushdown — reads the full table as a lazy frame.
    Intended only for use in tests until the full ``PolarsDeltaReader``
    (sprint 5) is introduced.

    Args:
        root: Catalog root directory.  Table paths are resolved as
              ``root/<schema>/<table>``.
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    def read(self, spec: SourceSpec, params_instance: Any) -> pl.LazyFrame:
        """Return a lazy scan of the Delta table referenced by *spec*.

        Uses ``DeltaTable.to_pyarrow_dataset()`` + ``pl.scan_pyarrow_dataset``
        to avoid polars ↔ deltalake Schema API version skew.
        """
        assert spec.table_ref is not None, f"expected TABLE source, got {spec}"
        path = table_path(self._root, spec.table_ref)
        dataset = DeltaTable(str(path)).to_pyarrow_dataset()
        return pl.scan_pyarrow_dataset(dataset)


class MinimalPolarsDeltaWriter:
    """Minimal :class:`~loom.etl._io.TargetWriter` backed by ``write_delta()``.

    Supports ``REPLACE`` (overwrite) and ``APPEND`` write modes.
    Intended only for use in tests until the full ``PolarsDeltaWriter``
    (sprint 5) is introduced.

    Args:
        root: Catalog root directory.
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    def write(self, frame: pl.LazyFrame, spec: TargetSpec, params_instance: Any) -> None:
        """Collect *frame* and write it to the Delta table referenced by *spec*."""
        assert spec.table_ref is not None, f"expected TABLE target, got {spec}"
        path = table_path(self._root, spec.table_ref)
        path.mkdir(parents=True, exist_ok=True)
        delta_mode = "overwrite" if spec.mode is WriteMode.REPLACE else "append"
        write_deltalake(str(path), frame.collect().to_arrow(), mode=delta_mode)


@pytest.fixture
def polars_reader(delta_root: Path) -> MinimalPolarsDeltaReader:
    """Pre-sprint-5 Polars + Delta source reader for integration tests."""
    return MinimalPolarsDeltaReader(delta_root)


@pytest.fixture
def polars_writer(delta_root: Path) -> MinimalPolarsDeltaWriter:
    """Pre-sprint-5 Polars + Delta target writer for integration tests."""
    return MinimalPolarsDeltaWriter(delta_root)
