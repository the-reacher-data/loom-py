"""ETL testing utilities — stub I/O implementations for unit tests.

Provides in-memory implementations of :class:`~loom.etl._io.TableDiscovery`,
:class:`~loom.etl._io.SourceReader`, and :class:`~loom.etl._io.TargetWriter`
that allow step and pipeline tests to run without real storage dependencies.

Example::

    from loom.etl.testing import StubCatalog, StubSourceReader, StubTargetWriter
    from loom.etl.compiler import ETLCompiler

    catalog = StubCatalog({"raw.orders": ("id", "amount"), "staging.out": ()})
    reader  = StubSourceReader({"orders": some_lazy_frame})
    writer  = StubTargetWriter()

    plan = ETLCompiler(catalog=catalog).compile_step(MyStep)
    # pass reader + writer to the executor (sprint 4)
    assert len(writer.written) == 1
"""

from __future__ import annotations

from typing import Any

from loom.etl._source import SourceSpec
from loom.etl._table import TableRef
from loom.etl._target import TargetSpec


class StubCatalog:
    """In-memory :class:`~loom.etl._io.TableDiscovery` for tests.

    Registers tables as a mapping of dotted ref string to column name
    tuples.  Pass an empty tuple to register a table with no known columns.

    Args:
        tables: Mapping of ``"schema.table"`` → ``("col1", "col2", ...)``.

    Example::

        catalog = StubCatalog({
            "raw.orders":   ("id", "amount", "year", "month"),
            "staging.out":  (),
        })
        assert catalog.exists(TableRef("raw.orders"))
        assert catalog.columns(TableRef("raw.orders")) == ("id", "amount", "year", "month")
        assert not catalog.exists(TableRef("raw.missing"))
    """

    def __init__(self, tables: dict[str, tuple[str, ...]] | None = None) -> None:
        self._tables: dict[str, tuple[str, ...]] = dict(tables or {})

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` if the ref was registered in this stub catalog."""
        return ref.ref in self._tables

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return registered column names, or ``()`` for unknown tables."""
        return self._tables.get(ref.ref, ())


class StubSourceReader:
    """In-memory :class:`~loom.etl._io.SourceReader` for tests.

    Returns pre-seeded frames keyed by source alias.  Any alias not found
    returns ``None`` — tests should seed all expected aliases.

    Args:
        frames: Mapping of source alias → frame object.

    Example::

        reader = StubSourceReader({"orders": pl.LazyFrame({"id": [1, 2]})})
        frame = reader.read(orders_spec, params)
        assert frame is not None
    """

    def __init__(self, frames: dict[str, Any] | None = None) -> None:
        self._frames: dict[str, Any] = dict(frames or {})

    def read(self, spec: SourceSpec, params_instance: Any) -> Any:
        """Return the pre-seeded frame for ``spec.alias``, or ``None``."""
        return self._frames.get(spec.alias)


class StubTargetWriter:
    """In-memory :class:`~loom.etl._io.TargetWriter` for tests.

    Captures all write calls in :attr:`written` for post-execution assertion.

    Attributes:
        written: List of ``(frame, spec)`` pairs in write order.

    Example::

        writer = StubTargetWriter()
        writer.write(frame, spec, params)
        assert len(writer.written) == 1
        frame_out, spec_out = writer.written[0]
        assert spec_out.mode == WriteMode.REPLACE
    """

    def __init__(self) -> None:
        self.written: list[tuple[Any, TargetSpec]] = []

    def write(self, frame: Any, spec: TargetSpec, params_instance: Any) -> None:
        """Capture the ``(frame, spec)`` pair for later assertion."""
        self.written.append((frame, spec))
