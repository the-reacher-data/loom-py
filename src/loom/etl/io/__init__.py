"""ETL external I/O connectors."""

from loom.etl.declarative.source import (
    ClickHouseSourceSpec,
    FromClickHouse,
    FromMongo,
    SourceRef,
)
from loom.etl.declarative.source._specs import MongoSourceSpec
from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
from loom.etl.io.sources._mongo import MongoSourceReader
from loom.etl.io.targets._clickhouse import (
    ClickHouseTableSpec,
    ClickHouseTargetWriter,
    IntoClickHouse,
)

__all__ = [
    "ClickHouseSourceReader",
    "ClickHouseSourceSpec",
    "ClickHouseTableSpec",
    "ClickHouseTargetWriter",
    "FromClickHouse",
    "FromMongo",
    "IntoClickHouse",
    "MongoSourceReader",
    "MongoSourceSpec",
    "SourceRef",
]
