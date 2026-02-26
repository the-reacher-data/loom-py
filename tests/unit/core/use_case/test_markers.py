from __future__ import annotations

from loom.core.use_case.markers import Input, Load


class FakeEntity:
    pass


class TestInput:
    def test_has_no_extra_attributes(self) -> None:
        marker = Input()
        assert type(marker).__slots__ == ()


class TestLoad:
    def test_stores_entity_type_and_default_by(self) -> None:
        marker = Load(FakeEntity)

        assert marker.entity_type is FakeEntity
        assert marker.by == "id"

    def test_custom_by_parameter(self) -> None:
        marker = Load(FakeEntity, by="slug")

        assert marker.by == "slug"
