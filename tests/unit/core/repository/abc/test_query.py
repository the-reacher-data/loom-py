from __future__ import annotations

import msgspec
import pytest

from loom.core.repository.abc import FilterParams, PageParams, build_page_result


class DummyOutput(msgspec.Struct):
    id: int


class TestPageParams:
    def test_offset_calculation(self) -> None:
        params = PageParams(page=3, limit=25)
        assert params.offset == 50

    def test_invalid_page_raises_error(self) -> None:
        with pytest.raises(ValueError, match="page must be >= 1"):
            PageParams(page=0, limit=10)

    def test_invalid_limit_raises_error(self) -> None:
        with pytest.raises(ValueError, match="limit must be in"):
            PageParams(page=1, limit=0)


class TestBuildPageResult:
    def test_build_page_result_calculates_has_next(self, page_params: PageParams) -> None:
        result = build_page_result(items=[DummyOutput(id=1)], total_count=25, page_params=page_params)

        assert result.page == 2
        assert result.limit == 10
        assert result.has_next is True
        assert result.items[0].id == 1


class TestFilterParams:
    def test_filter_params_default_dict(self) -> None:
        params = FilterParams()
        assert params.filters == {}
