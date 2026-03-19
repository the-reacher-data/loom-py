"""Shared fixtures for ETL unit tests."""

from __future__ import annotations

from datetime import date

import pytest

from loom.etl import ETLParams


class OrdersParams(ETLParams):
    run_date: date
    countries: tuple[str, ...]


class CustomersParams(ETLParams):
    run_date: date


@pytest.fixture()
def orders_params() -> OrdersParams:
    return OrdersParams(run_date=date(2024, 1, 5), countries=("ES", "FR"))
