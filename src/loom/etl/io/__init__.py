"""ETL external I/O connectors."""

from loom.etl.declarative.source import (
    ClickHouseSourceSpec,
    FromClickHouse,
    FromDynamoDb,
    FromMongo,
    SourceRef,
)
from loom.etl.declarative.source._specs import DynamoDbSourceSpec, MongoSourceSpec
from loom.etl.io.sources._clickhouse import ClickHouseSourceReader
from loom.etl.io.sources._dynamodb import DynamoDbSourceReader
from loom.etl.io.sources._mongo import MongoSourceReader
from loom.etl.io.targets._clickhouse import (
    ClickHouseClientExecutor,
    ClickHouseTableSpec,
    ClickHouseTargetWriter,
    IntoClickHouse,
)

__all__ = [
    "ClickHouseClientExecutor",
    "ClickHouseSourceReader",
    "ClickHouseSourceSpec",
    "ClickHouseTableSpec",
    "ClickHouseTargetWriter",
    "DynamoDbSourceReader",
    "DynamoDbSourceSpec",
    "FromClickHouse",
    "FromDynamoDb",
    "FromMongo",
    "IntoClickHouse",
    "MongoSourceReader",
    "MongoSourceSpec",
    "SourceRef",
]
