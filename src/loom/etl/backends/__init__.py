"""Compute backends for the Loom ETL framework.

This module provides pluggable compute backends for executing ETL operations.
Each backend implements the ComputeBackend protocol with three operation groups:

- ``read``: Table and file reading operations
- ``schema``: Schema discovery and alignment
- ``write``: Table and file writing operations

The GenericSourceReader and GenericTargetWriter provide a unified I/O layer
that works with any ComputeBackend implementation.

Example:
    >>> from loom.etl.backends import ComputeBackend
    >>> from loom.etl.backends.spark import SparkBackend
    >>> from loom.etl.backends.io import GenericSourceReader
    >>>
    >>> backend = SparkBackend(spark_session)
    >>> reader = GenericSourceReader(
    ...     backend=backend,
    ...     resolver=resolver,
    ...     reader_name="MyReader"
    ... )
"""

from loom.etl.backends._protocols import (
    ComputeBackend,
    ReadOps,
    SchemaOps,
    WriteOps,
)

__all__ = [
    "ComputeBackend",
    "ReadOps",
    "SchemaOps",
    "WriteOps",
]
