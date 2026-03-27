"""StepSQL — SQL-first ETL step.

Declares the transformation as a SQL string instead of a Python
``execute()`` method.  Sources are registered as temp views keyed by
their ``Sources`` alias.  Params can be interpolated via
``{{ params.x.y }}`` placeholders.

The backend is inferred from the second type parameter:

* ``StepSQL[Params, pl.LazyFrame]``          → Polars (``pl.SQLContext``)
* ``StepSQL[Params, pyspark.sql.DataFrame]`` → Spark  (``newSession()`` isolation)

Internal — not part of the public API surface, exposed via ``loom.etl``.
"""

from __future__ import annotations

import typing
from typing import Any, ClassVar, Generic, TypeVar, cast

from loom.etl.model._step import ETLStep
from loom.etl.sql._sql import resolve_sql

ParamsT = TypeVar("ParamsT")
FrameT = TypeVar("FrameT")


class StepSQL(ETLStep[ParamsT], Generic[ParamsT, FrameT]):
    """SQL-first ETL step — transformation declared as SQL, not Python.

    Declare ``sql`` as a class variable (static SQL) or a ``@staticmethod``
    that receives params and returns a SQL string (dynamic SQL).

    Sources still support ``.where()`` for pre-filtering before the frame
    is registered as a view.  Target write modes work identically to
    :class:`~loom.etl.ETLStep`.

    Backend is inferred from the second type parameter:

    * ``StepSQL[Params, pl.LazyFrame]``          → Polars
    * ``StepSQL[Params, pyspark.sql.DataFrame]`` → Spark

    For Spark, each execution uses an isolated ``spark.newSession()`` so
    parallel steps never share the same view catalog.  The session is
    released when the result DataFrame goes out of scope after the write.

    .. warning::
        Avoid interpolating raw string params directly.  Use
        ``FromTable.where()`` for source-level filtering instead.

    Example::

        class BuildOrdersStep(StepSQL[DailyParams, pl.LazyFrame]):
            sources = Sources(
                orders=FromTable("raw.orders").where(
                    col("year") == params.run_date.year
                ),
                customers=FromTable("raw.customers"),
            )
            target = IntoTable("staging.orders").replace_where(
                col("year") == params.run_date.year
            )
            sql = \"\"\"
                SELECT o.id, o.amount, c.name,
                       {{ params.run_date.year }} AS year
                FROM orders o
                JOIN customers c ON o.customer_id = c.id
            \"\"\"
    """

    sql: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        params_type, frame_type = _extract_stepsql_types(cls)
        # Override _params_type from ETLStep (reads ETLStep[T], not StepSQL[T, F])
        if params_type is not None:
            cls._params_type = params_type
        if frame_type is not None:
            _generate_execute(cls, frame_type)

    def execute(self, params: Any, **frames: Any) -> Any:
        raise NotImplementedError(
            f"{type(self).__qualname__}: declare a 'sql' ClassVar instead of execute(). "
            "Use StepSQL[ParamsT, ReturnType] to declare the backend."
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _extract_stepsql_types(cls: type) -> tuple[type | None, type | None]:
    for base in getattr(cls, "__orig_bases__", ()):
        origin = getattr(base, "__origin__", None)
        if origin is StepSQL:
            args = typing.get_args(base)
            params_t = cast(type, args[0]) if len(args) > 0 else None
            frame_t = cast(type, args[1]) if len(args) > 1 else None
            return params_t, frame_t
    return None, None


def _generate_execute(cls: type, return_type: type) -> None:
    """Inject a backend-dispatching execute() with the correct return annotation."""

    def execute(self: Any, params: Any, **frames: Any) -> Any:
        query = _resolve_query(type(self), params)
        first = next(iter(frames.values()), None)
        if first is not None and _is_spark_frame(first):
            return _run_spark_sql(frames, query)
        return _run_polars_sql(frames, query)

    execute.__annotations__["return"] = return_type
    cls.execute = execute  # type: ignore[attr-defined]


def _resolve_query(cls: type, params: Any) -> str:
    raw = cls.sql  # type: ignore[attr-defined]
    if callable(raw):
        return raw(params)  # type: ignore[no-any-return]
    return resolve_sql(raw, params)


def _is_spark_frame(obj: Any) -> bool:
    return type(obj).__module__.startswith("pyspark")


def _run_polars_sql(frames: dict[str, Any], query: str) -> Any:
    import polars as pl

    return pl.SQLContext(frames).execute(query)


def _run_spark_sql(frames: dict[str, Any], query: str) -> Any:
    first = next(iter(frames.values()))
    spark = first.sparkSession
    # newSession() — isolated view catalog, shared SparkContext, no data movement.
    # Python wrapper is GC'd when execute() returns; JVM session stays alive via
    # the query plan until writer.save() completes.
    # NEVER call isolated.stop() — it would stop the shared SparkContext.
    isolated = spark.newSession()
    for alias, df in frames.items():
        isolated.createDataFrame(df.rdd, df.schema).createOrReplaceTempView(alias)
    return isolated.sql(query)
