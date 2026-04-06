"""Shared Spark source-read transforms (schema, JSON, predicates)."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from loom.etl.backends.spark._dtype import loom_type_to_spark
from loom.etl.io.source import JsonColumnSpec
from loom.etl.schema._schema import ColumnSchema
from loom.etl.sql._predicate_sql import predicate_to_sql


def apply_source_schema_spark(df: DataFrame, schema: tuple[ColumnSchema, ...]) -> DataFrame:
    """Cast declared source columns to Spark-native types."""
    if not schema:
        return df
    for col in schema:
        df = df.withColumn(col.name, F.col(col.name).cast(loom_type_to_spark(col.dtype)))
    return df


def apply_json_decode_spark(df: DataFrame, json_columns: tuple[JsonColumnSpec, ...]) -> DataFrame:
    """Decode JSON string columns using Spark ``from_json``."""
    if not json_columns:
        return df
    for jc in json_columns:
        schema_ddl = loom_type_to_spark(jc.loom_type).simpleString()
        df = df.withColumn(jc.column, F.from_json(F.col(jc.column), schema_ddl))
    return df


def apply_predicates_spark(
    df: DataFrame, predicates: tuple[Any, ...], params_instance: Any
) -> DataFrame:
    """Apply ETL predicate nodes to a Spark DataFrame via SQL translation."""
    if not predicates:
        return df
    for pred in predicates:
        df = df.filter(predicate_to_sql(pred, params_instance))
    return df
