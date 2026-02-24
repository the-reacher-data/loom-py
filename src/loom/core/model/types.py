from __future__ import annotations

from loom.core.model.field import ColumnType


def String(length: int) -> ColumnType:
    return ColumnType("String", args=(length,))


Integer = ColumnType("Integer")
BigInteger = ColumnType("BigInteger")
Float = ColumnType("Float")
Boolean = ColumnType("Boolean")
Text = ColumnType("Text")


def DateTime(*, tz: bool = True) -> ColumnType:
    return ColumnType("DateTime", kwargs={"timezone": tz})


def Numeric(*, precision: int = 10, scale: int = 2) -> ColumnType:
    return ColumnType("Numeric", args=(precision, scale))
