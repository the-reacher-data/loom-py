"""Spark target writers split by kind (TABLE/FILE)."""

from .core import SparkTargetWriter
from .file import SparkFileWriter
from .table import SparkDeltaTableWriter

__all__ = ["SparkTargetWriter", "SparkFileWriter", "SparkDeltaTableWriter"]
