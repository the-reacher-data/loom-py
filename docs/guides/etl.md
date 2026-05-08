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

## File aliases (FileLocator)

Hard-coding file paths in pipelines couples the logic to the infrastructure.
Use `FromFile.alias()` / `IntoFile.alias()` to declare a **logical name** and
resolve it at runtime through the storage config.

Declare aliases in the `storage.files` block:

```yaml
storage:
  engine: polars
  files:
    - name: events_raw
      path:
        uri: s3://raw-bucket/events/
        storage_options:
          AWS_REGION: eu-west-1
    - name: exports_daily
      path:
        uri: s3://exports-bucket/daily/
```

Reference them in pipelines using the alias API:

```python
from loom.etl import ETLStep, FromFile, IntoFile, Format

class LoadEvents(ETLStep[DailyParams]):
    events = FromFile.alias("events_raw", format=Format.CSV)
    target = IntoFile.alias("exports_daily", format=Format.PARQUET)

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events
```

The runner resolves aliases to physical URIs at job startup —
pipelines never hard-code storage paths or credentials.

You can also implement `FileLocator` directly for custom routing strategies:

```python
from loom.etl.storage import FileLocator, FileLocation

class MyFileLocator:
    def locate(self, name: str) -> FileLocation:
        return FileLocation(uri_template=f"s3://my-bucket/{name}/")
```

---

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
  log:
    enabled: true
  otel:
    enabled: false
  prometheus:
    enabled: true
    pushgateway_url: ${oc.env:PUSHGATEWAY_URL,http://127.0.0.1:9091}
  lineage:
    enabled: true
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

### historify

For SCD Type 2 history tracking use `IntoHistory` instead of `IntoTable`.
See the dedicated [Historify](#historify) section below for full examples,
SNAPSHOT vs LOG mode, and configuration of boundary columns.

| Target | Use case | Details |
|---|---|---|
| `IntoTable(...).append()` | Add rows | Above |
| `IntoTable(...).replace()` | Full overwrite | Above |
| `IntoTable(...).replace_partitions(...)` | Replace batch partitions | Above |
| `IntoTable(...).replace_partition(...)` | Replace known partition | Above |
| `IntoTable(...).replace_where(...)` | Predicate-based overwrite | Above |
| `IntoTable(...).upsert(...)` | Merge by key | Above |
| `IntoHistory(...)` | SCD Type 2 historification | [Below](#historify) |

---

## Historify

SCD Type 2 historification tracks how entity states change over time by creating
a new row for every meaningful change and closing the previous open row.

```python
from loom.etl import IntoHistory, params

target = IntoHistory(
    "wh.dim_players",
    keys=("player_id",),
    track=("team_id",),
    effective_date=params.run_date,
)
```

### Configuration

| Parameter | Default | Description |
|---|---|---|
| `ref` | — | Logical Delta table reference |
| `keys` | — | Entity identity columns (e.g. `("player_id",)`) |
| `track` | `None` | Columns whose change triggers a new history row. `None` means all non-key columns |
| `effective_date` | — | `ParamExpr` for SNAPSHOT mode, or column name for LOG mode |
| `mode` | `"snapshot"` | `"snapshot"` (full state) or `"log"` (event stream) |
| `valid_from` | `"valid_from"` | Period-start boundary column |
| `valid_to` | `"valid_to"` | Period-end boundary column (`NULL` = open) |
| `deleted_at` | `"deleted_at"` | Soft-delete audit column (only written when `delete_policy="soft_delete"`) |
| `delete_policy` | `"close"` | `"ignore"` / `"close"` / `"soft_delete"` |
| `partition_scope` | `None` | Partition columns for read/write pruning |
| `allow_temporal_rerun` | `False` | Allow backfills with past `effective_date` |

### SNAPSHOT mode example

In SNAPSHOT mode the incoming frame represents the full current state of the
dimension. The engine compares it against the existing open vectors and inserts
new rows only for changed states.

**Day 1 incoming snapshot**

| player_id | team_id |
|---|---|
| 1 | RM |

**Result after historify**

| player_id | team_id | valid_from | valid_to |
|---|---|---|---|
| 1 | RM | 2024-01-01 | `NULL` |

**Day 2 incoming snapshot (player changed team)**

| player_id | team_id |
|---|---|
| 1 | BCA |

**Result after historify**

| player_id | team_id | valid_from | valid_to |
|---|---|---|---|
| 1 | RM | 2024-01-01 | 2024-01-01 |
| 1 | BCA | 2024-01-02 | `NULL` |

The old row was closed (`valid_to = effective_date - 1`) and the new row was
opened.

### LOG mode example

In LOG mode the incoming frame is an event stream. Each event carries its own
`effective_date` column.

```python
target = IntoHistory(
    "wh.dim_subscriptions",
    keys=("subscription_id",),
    track=("plan",),
    effective_date="event_date",
    mode="log",
)
```

**Incoming event log**

| subscription_id | plan | event_date |
|---|---|---|
| 1 | basic | 2024-01-01 |
| 1 | pro | 2024-06-01 |

**Result after historify**

| subscription_id | plan | valid_from | valid_to |
|---|---|---|---|
| 1 | basic | 2024-01-01 | 2024-05-31 |
| 1 | pro | 2024-06-01 | `NULL` |

The first event is automatically closed by the second one. The last event
remains open.

### Custom boundary column names

If your data warehouse uses different naming conventions, the three generated
columns are fully configurable:

```python
target = IntoHistory(
    "wh.dim_players",
    keys=("player_id",),
    track=("team_id",),
    effective_date=params.run_date,
    valid_from="vf",
    valid_to="vt",
    deleted_at="removed_at",
)
```

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

## File aliases (FileLocator)

Hard-coding file paths in pipelines couples the logic to the infrastructure.
Use `FromFile.alias()` / `IntoFile.alias()` to declare a **logical name** and
resolve it at runtime through the storage config.

Declare aliases in the `storage.files` block:

```yaml
storage:
  engine: polars
  files:
    - name: events_raw
      path:
        uri: s3://raw-bucket/events/
        storage_options:
          AWS_REGION: eu-west-1
    - name: exports_daily
      path:
        uri: s3://exports-bucket/daily/
```

Reference them in pipelines:

```python
from loom.etl import ETLStep, FromFile, IntoFile, Format

class LoadEvents(ETLStep[DailyParams]):
    events = FromFile.alias("events_raw", format=Format.CSV)
    target = IntoFile.alias("exports_daily", format=Format.PARQUET)

    def execute(self, params: DailyParams, *, events: pl.LazyFrame) -> pl.LazyFrame:
        return events
```

The runner resolves aliases to physical URIs at job startup — pipelines never
hard-code storage paths or credentials.

You can also implement the `FileLocator` protocol directly for custom routing:

```python
from loom.etl.storage import FileLocator, FileLocation

class MyFileLocator:
    def locate(self, name: str) -> FileLocation:
        return FileLocation(uri_template=f"s3://my-bucket/{name}/")
```

---

## Loading config from cloud storage

`ETLRunner.from_yaml()` accepts cloud URIs (`s3://`, `gs://`, `abfss://`, `r2://`, …)
in addition to local paths. Files are fetched via `fsspec` at startup:

```python
runner = ETLRunner.from_yaml("s3://my-bucket/config/etl.yaml")
```

Multi-file composition:

```python
runner = ETLRunner.from_yaml(
    "s3://my-bucket/config/base.yaml",
    "s3://my-bucket/config/prod.yaml",
)
```

> The `includes` directive is not supported for cloud URIs. Use explicit multi-file
> composition instead.

---

## Pluggable config resolvers

Extend the YAML loader with custom `${prefix:key}` placeholders to fetch secrets
from external stores (AWS SSM, Azure Key Vault, …) at parse time:

```python
from loom.core.config import load_config

class SsmResolver:
    name = "ssm"

    def __init__(self, region: str) -> None:
        self._region = region

    def resolve(self, key: str) -> str:
        import boto3
        client = boto3.client("ssm", region_name=self._region)
        return client.get_parameter(Name=key, WithDecryption=True)["Parameter"]["Value"]

cfg = load_config("config/etl.yaml", resolvers=[SsmResolver("eu-west-1")])
```

In YAML, reference secrets via `${ssm:/path/to/secret}`:

```yaml
storage:
  catalogs:
    main:
      token: ${ssm:/prod/databricks/token}
```

Resolvers run at job startup — secret rotation takes effect on the next run
without redeployment.

---

## End-to-end example

A full working example with Polars and Spark pipelines, Delta Lake, and observability is
available in the companion repository:
[`dummy-loom-etl`](https://github.com/MassiveDataScope/dummy-loom-etl).

---

## API reference

The ETL API reference is generated from public docstrings:

- [ETL API reference](../reference/api/etl)
