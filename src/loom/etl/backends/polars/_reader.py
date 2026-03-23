"""PolarsDeltaReader — SourceReader backed by Polars + delta-rs.

Uses ``DeltaTable.to_pyarrow_dataset()`` for lazy scanning to avoid
Polars ↔ deltalake Schema API version skew.

Predicate pushdown is not yet implemented — all predicates declared via
``.where()`` are ignored at the scan level and must be applied by the step's
``execute()`` method.  Full pushdown is planned for a future sprint.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl
from deltalake import DeltaTable

from loom.etl._source import SourceSpec
from loom.etl._table import TableRef


class PolarsDeltaReader:
    """Read ETL sources as Polars lazy frames from Delta tables.

    Implements :class:`~loom.etl._io.SourceReader`.

    Args:
        root: Filesystem root.  Table paths are resolved as
              ``root/<schema>/<table>/``.

    Example::

        reader = PolarsDeltaReader(Path("/data/delta"))
        frame = reader.read(spec, params)  # returns pl.LazyFrame
    """

    def __init__(self, root: Path) -> None:
        self._root = root

    def read(self, spec: SourceSpec, _params_instance: Any) -> pl.LazyFrame:
        """Return a lazy frame backed by the Delta table referenced by *spec*.

        Uses ``DeltaTable.to_pyarrow_dataset()`` + ``pl.scan_pyarrow_dataset``
        for lazy evaluation — the table is not materialised until the step's
        ``execute()`` result is collected by the writer.

        Args:
            spec:             Compiled source spec.  Must be a TABLE source.
            _params_instance: Concrete params (reserved for predicate pushdown
                              — not yet implemented).

        Returns:
            Lazy Polars frame over the Delta table.

        Raises:
            AssertionError: If *spec* is a FILE source (unsupported here).
        """
        assert spec.table_ref is not None, (
            f"PolarsDeltaReader only supports TABLE sources; got FILE spec: {spec}"
        )
        path = self._table_path(spec.table_ref)
        dataset = DeltaTable(str(path)).to_pyarrow_dataset()
        return pl.scan_pyarrow_dataset(dataset)

    def _table_path(self, ref: TableRef) -> Path:
        return self._root.joinpath(*ref.ref.split("."))
