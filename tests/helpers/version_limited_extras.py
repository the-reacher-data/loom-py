"""Skip the tests of an extra that the running Python version cannot install.

Two extras stop at Python 3.12 today: ``streaming`` leaves bytewax out, because
bytewax publishes no wheel for 3.13 or later, and ``etl-spark``/``pyspark`` leave
PySpark 3.5 out. A test module that imports either calls the matching guard
before that import, so a missing package is a skip with a reason, never a
collection error.

Every bytewax skip goes through ``require_bytewax``: the bound is provisional,
and lifting it (spec 015) means deleting that function and its calls.
"""

from __future__ import annotations

import pytest

BYTEWAX_SKIP_REASON = (
    "bytewax is not installed: the 'streaming' extra installs it only on Python < 3.13"
)
PYSPARK_SKIP_REASON = (
    "pyspark is not installed: the 'etl-spark' and 'pyspark' extras install it only "
    "on Python < 3.13"
)


def require_bytewax() -> None:
    """Skip the calling module, or test, when bytewax is not importable."""
    __tracebackhide__ = True
    pytest.importorskip("bytewax", reason=BYTEWAX_SKIP_REASON)


def require_pyspark() -> None:
    """Skip the calling module, or test, when pyspark is not importable."""
    __tracebackhide__ = True
    pytest.importorskip("pyspark", reason=PYSPARK_SKIP_REASON)
