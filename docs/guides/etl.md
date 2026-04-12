# ETL

`loom.etl` is a declarative ETL subsystem with compile-time validation,
backend-agnostic declarations, and a single runtime entrypoint (`ETLRunner`).

## Install

Choose one backend:

```bash
pip install "loom-kernel[etl-polars]"
# or
pip install "loom-kernel[etl-spark]"
```

## Minimal pipeline

```python
from datetime import date

import polars as pl
from loom.etl import (
    ETLParams,
    ETLStep,
    ETLProcess,
    ETLPipeline,
    ETLRunner,
    FromTable,
    IntoTable,
)


class DailyParams(ETLParams):
    run_date: date


class CleanOrders(ETLStep[DailyParams]):
    orders = FromTable("raw.orders").columns("id", "amount", "run_date")
    target = IntoTable("staging.orders").replace()

    def execute(self, params: DailyParams, *, orders: pl.LazyFrame) -> pl.LazyFrame:
        return orders.filter(pl.col("amount") > 0)


class DailyProcess(ETLProcess[DailyParams]):
    steps = [CleanOrders]


class DailyPipeline(ETLPipeline[DailyParams]):
    processes = [DailyProcess]


runner = ETLRunner.from_dict(
    storage={
        "engine": "polars",
        "defaults": {"table_path": {"uri": "/var/lib/loom/lake"}},
    }
)
runner.run(DailyPipeline, DailyParams(run_date=date(2026, 3, 30)))
```

## File source with JSON payload + final CSV report

```python
import polars as pl
from loom.etl import ETLStep, ETLProcess, ETLPipeline, FromFile, FromTable, IntoFile, IntoTable, Format

class Payload:
    store: str
    amount: float
    items: int

class LoadEvents(ETLStep[DailyParams]):
    events = FromFile("/data/raw/events.csv", format=Format.CSV).parse_json("payload", Payload)
    target = IntoTable("staging.events").replace()

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events.select(
            pl.col("payload").struct.field("store").alias("store"),
            pl.col("payload").struct.field("amount").alias("amount"),
            pl.col("payload").struct.field("items").alias("items"),
        )

class BuildReport(ETLStep[DailyParams]):
    events = FromTable("staging.events")
    target = IntoFile("/data/exports/daily_report.csv", format=Format.CSV)

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events.group_by("store").agg(
            pl.col("amount").sum().alias("gross_amount"),
            pl.col("items").sum().alias("item_count"),
        )

class ReportProcess(ETLProcess[DailyParams]):
    steps = [LoadEvents, BuildReport]

class ReportPipeline(ETLPipeline[DailyParams]):
    processes = [ReportProcess]
```

## YAML config (Polars path)

```yaml
storage:
  engine: polars

  defaults:
    table_path:
      uri: s3://my-lake
      storage_options:
        AWS_REGION: ${oc.env:AWS_REGION}
        AWS_ACCESS_KEY_ID: ${oc.env:AWS_ACCESS_KEY_ID}
        AWS_SECRET_ACCESS_KEY: ${oc.env:AWS_SECRET_ACCESS_KEY}

  tmp_root: /var/lib/loom/lake/_tmp

observability:
  log: true
  slow_step_threshold_ms: 30000
  run_sink:
    # Choose exactly one destination:
    root: /var/lib/loom/lake/_runs
    # database: ops
```

```python
from loom.etl import ETLRunner

runner = ETLRunner.from_yaml("config/etl.yaml")
```

## Write modes

Every `IntoTable` target declares exactly one write mode by chaining a method.

### append

Adds rows to the table. Creates the table on first write.

```python
target = IntoTable("staging.orders").append()
```

### replace

Full overwrite. Replaces all data in the table.

```python
target = IntoTable("staging.orders").replace()
# Overwrite schema too:
target = IntoTable("staging.orders").replace(schema=SchemaMode.OVERWRITE)
```

### replace\_partitions

Replaces only the partitions **present in the batch**. The writer collects
the distinct partition values from the frame at write time — no params required.

```python
target = IntoTable("staging.orders").replace_partitions("year", "month")
```

Use this for incremental loads where the batch carries its own partition identity
(e.g. daily runs writing the day's data).

### replace\_partition

Replaces a **specific partition** whose values come from run params. Resolves
the equality predicate at runtime without collecting from the frame.

```python
from loom.etl import params

target = IntoTable("staging.orders").replace_partition(
    year=params.run_date.year,
    month=params.run_date.month,
)
```

Use this when the partition to replace is known at pipeline design time (e.g.
reprocessing a single day).

**Difference from `replace_partitions`:**

| | `replace_partitions` | `replace_partition` |
|---|---|---|
| Partition values | Collected from frame | Resolved from params |
| Collect step | Yes (distinct) | No |
| Use case | Batch carries its partition | Reprocessing a known partition |

### replace\_where

Replaces rows matching an arbitrary predicate. Accepts the full predicate DSL.

```python
target = IntoTable("staging.orders").replace_where(
    col("date").between(params.start_date, params.end_date)
)
```

### upsert

MERGE on key columns. Inserts new rows and updates existing ones.

```python
target = IntoTable("events.orders").upsert(
    keys=("order_id",),
    partition_cols=("year", "month"),  # strongly recommended for large tables
    exclude=("created_at",),           # columns excluded from UPDATE SET
)
```

`partition_cols` is optional but strongly recommended — without it every MERGE
forces a full table scan.

---

## Running only selected stages

Use `include` with process or step class names:

```python
runner.run(
    DailyPipeline,
    DailyParams(run_date=date(2026, 3, 30)),
    include=["DailyProcess", "CleanOrders"],
)
```

If no name matches, `InvalidStageError` is raised.

## Spark runtime

For Databricks/Unity Catalog runtime:

```python
from loom.etl import ETLRunner

runner = ETLRunner.from_spark(spark)
```

This wires Spark reader/writer/catalog automatically.

## Testing ETL steps

Use the built-in test harnesses:

- `loom.etl.testing.PolarsStepRunner`
- `loom.etl.testing.spark.SparkStepRunner`
- `loom.etl.testing.ETLScenario`

These let you seed source tables, run one step in isolation, and assert output
without wiring full storage infrastructure.

## API reference

The ETL API reference is generated from public docstrings:

- [ETL API reference](../reference/api/etl)
