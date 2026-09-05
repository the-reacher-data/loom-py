"""Pipeline classes referenced by dotted path from the ETL YAML fixtures."""

from __future__ import annotations

from datetime import date
from typing import TypeVar

import msgspec

from loom.etl import ETLPipeline

ParamsT = TypeVar("ParamsT")


class OrdersParams(msgspec.Struct, frozen=True):
    """Parameters of the orders pipelines."""

    run_date: date


class OrdersPipeline(ETLPipeline[OrdersParams]):
    """Pipeline bound to a concrete params struct."""


class OrdersChildPipeline(OrdersPipeline):
    """Pipeline inheriting the binding from its parent."""


class UnboundPipeline(ETLPipeline[ParamsT]):
    """Pipeline leaving the params type unbound."""


class NotAPipeline:
    """Plain class, not an ETLPipeline."""


class NotAStruct:
    """Plain class, not a msgspec.Struct."""


class OtherParams(msgspec.Struct, frozen=True):
    """Alternative params struct used to check that an explicit type wins."""

    run_date: date
