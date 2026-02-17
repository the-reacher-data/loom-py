from __future__ import annotations

import pytest
from pytest import fixture

from loom.core.repository.abc import PageParams


@fixture
def page_params() -> PageParams:
    return PageParams(page=2, limit=10)
