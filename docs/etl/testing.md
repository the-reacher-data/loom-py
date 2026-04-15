# ETL Testing

`loom.etl.testing` provides in-memory harnesses for testing `ETLStep`
subclasses without spinning up a real catalog or writing to Delta Lake.

## Test a single step with Polars

`PolarsStepRunner` seeds source tables as plain Python tuples, runs one step,
and returns a `StepResult` for assertions.

```python
from loom.etl.testing import PolarsStepRunner

def test_clean_orders():
    runner = PolarsStepRunner()
    runner.seed("raw.orders", [(1, 10.0), (2, -5.0)], ["id", "amount"])
    result = runner.run(CleanOrdersStep, NoParams())

    result.assert_count(1)
    result.assert_schema({"id": "Int64", "amount": "Float64"})
```

`StepResult` methods: `assert_schema`, `assert_count`, `assert_not_empty`,
`show`, `to_polars`.

## Reusable seed datasets with `ETLScenario`

`ETLScenario` stores seed data as plain tuples so the same dataset can be used
with any backend runner.

```python
from loom.etl.testing import ETLScenario

ORDERS = (
    ETLScenario()
    .with_table("raw.orders", [(1, 10.0), (2, 20.0)], ["id", "amount"])
)

def test_double_amount(loom_polars_runner):
    ORDERS.apply(loom_polars_runner)
    result = loom_polars_runner.run(DoubleAmountStep, NoParams())
    result.assert_count(2)
```

Pytest fixtures auto-registered: `loom_polars_runner`, `loom_spark_runner`.

## Spark integration tests

Use `SparkTestSession` for a real PySpark + Delta session:

```python
from loom.etl.testing.spark import SparkTestSession

with SparkTestSession.start() as spark:
    runner = SparkStepRunner(spark)
    runner.seed("raw.orders", [(1, 10.0)], ["id", "amount"])
    result = runner.run(CleanOrdersStep, NoParams())
    result.assert_count(1)
```

## Compile-time stubs

- `StubCatalog` — in-memory catalog.
- `StubSourceReader` / `StubTargetWriter` — in-memory I/O.
- `StubRunObserver` — records pipeline lifecycle events.
