"""SparkCatalog — TableDiscovery backed by a Unity Catalog SparkSession.

Reads schema live from ``spark.table(ref).schema.fields`` — no delta-rs
dependency required.  Schema always reflects the current on-disk state for
Delta-backed Unity Catalog tables.

``update_schema`` is a no-op: Delta Lake / Unity Catalog maintain the
authoritative schema in the transaction log.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from loom.etl.backends.spark._dtype import spark_to_loom
from loom.etl.schema._schema import ColumnSchema
from loom.etl.schema._table import TableRef


class SparkCatalog:
    """Catalog backed by a Unity Catalog SparkSession.

    Implements :class:`~loom.etl._io.TableDiscovery`.

    Uses ``spark.catalog.tableExists()`` for existence checks and reads
    schema directly from ``spark.table(ref.ref).schema.fields`` — no delta-rs
    dependency required.

    ``update_schema`` is intentionally a no-op: Delta Lake / Unity Catalog
    maintain the authoritative schema in the transaction log.  The writer
    calls ``spark_apply_schema`` before writing, so schema alignment is
    handled at write time.

    Args:
        spark: Active :class:`pyspark.sql.SparkSession` with Unity Catalog
               configured.

    Example::

        from loom.etl.backends.spark import SparkCatalog

        catalog = SparkCatalog(spark)
        if catalog.exists(TableRef("main.raw.orders")):
            print(catalog.columns(TableRef("main.raw.orders")))
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def exists(self, ref: TableRef) -> bool:
        """Return ``True`` if the table exists in the Unity Catalog.

        Args:
            ref: Logical table reference (e.g. ``"main.raw.orders"``).

        Returns:
            ``True`` if the table is registered in the active Spark catalog.
        """
        return bool(self._spark.catalog.tableExists(ref.ref))

    def columns(self, ref: TableRef) -> tuple[str, ...]:
        """Return column names from the live catalog, or ``()`` if absent.

        Args:
            ref: Logical table reference.

        Returns:
            Ordered column names, or an empty tuple when the table does not
            exist yet.
        """
        if not self.exists(ref):
            return ()
        return tuple(self._spark.table(ref.ref).columns)

    def schema(self, ref: TableRef) -> tuple[ColumnSchema, ...] | None:
        """Return the schema from the live catalog, or ``None`` if absent.

        Reads ``spark.table(ref.ref).schema.fields`` — always reflects the
        current on-disk state.

        Args:
            ref: Logical table reference.

        Returns:
            Ordered tuple of :class:`~loom.etl._schema.ColumnSchema`, or
            ``None`` when the table does not yet exist.
        """
        if not self.exists(ref):
            return None
        fields = self._spark.table(ref.ref).schema.fields
        return tuple(
            ColumnSchema(
                name=f.name,
                dtype=spark_to_loom(f.dataType),
                nullable=f.nullable,
            )
            for f in fields
        )

    def update_schema(self, ref: TableRef, schema: tuple[ColumnSchema, ...]) -> None:
        """No-op — Delta / Unity Catalog manages schema in the transaction log."""
