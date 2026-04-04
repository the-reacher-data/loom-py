"""Polars source readers split by kind (TABLE/FILE)."""

from .core import PolarsSourceReader
from .file import PolarsFileReader
from .table import PolarsDeltaTableReader

__all__ = ["PolarsSourceReader", "PolarsFileReader", "PolarsDeltaTableReader"]
