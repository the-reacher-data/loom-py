# ETL Examples

The companion repository [`dummy-loom-etl`](https://github.com/MassiveDataScope/dummy-loom-etl)
contains runnable end-to-end examples of `loom.etl` with both Polars and Spark
backends.

## What it covers

- Polars and Spark pipeline declarations
- Delta Lake read/write modes (`replace`, `upsert`, `replace_partitions`)
- Cloud storage configuration (S3, GCS, Azure) via `fsspec`
- File aliases with `FromFile.alias` / `IntoFile.alias`
- Observability and run sinks
- Integration tests with `PolarsStepRunner` and `SparkTestSession`

## Quick start

```bash
git clone https://github.com/MassiveDataScope/dummy-loom-etl
cd dummy-loom-etl
make test      # run Polars and Spark test suites
make run       # run the sample pipeline locally
```

## Next steps

- [ETL pipelines guide](pipelines.md)
- [ETL testing guide](testing.md)
- [API reference](../reference/api/etl)
