"""Polars target writers split by kind (TABLE/FILE)."""

from .core import PolarsTargetWriter
from .file import PolarsFileWriter
from .table import PolarsDeltaTableWriter

__all__ = ["PolarsTargetWriter", "PolarsFileWriter", "PolarsDeltaTableWriter"]
