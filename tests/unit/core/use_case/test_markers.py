from __future__ import annotations

import pytest

from loom.core.use_case.markers import Exists, Input, Load, LoadById, OnMissing, SourceKind


class FakeEntity:
    pass


class TestInput:
    def test_has_no_extra_attributes(self) -> None:
        marker = Input()
        assert type(marker).__slots__ == ()


class TestLoad:
    def test_stores_entity_type_and_default_by(self) -> None:
        marker = LoadById(FakeEntity)

        assert marker.entity_type is FakeEntity
        assert marker.by == "id"

    def test_custom_by_parameter(self) -> None:
        marker = LoadById(FakeEntity, by="slug")

        assert marker.by == "slug"


class TestLoadByField:
    def test_from_param_configuration(self) -> None:
        marker = Load(FakeEntity, from_param="email", against="email")

        assert marker.from_kind is SourceKind.PARAM
        assert marker.from_name == "email"
        assert marker.against == "email"
        assert marker.on_missing is OnMissing.RAISE

    def test_from_command_configuration(self) -> None:
        marker = Load(FakeEntity, from_command="email", against="email")

        assert marker.from_kind is SourceKind.COMMAND
        assert marker.from_name == "email"

    def test_invalid_source_configuration_raises(self) -> None:
        with pytest.raises(ValueError, match="exactly one"):
            Load(FakeEntity, against="email")


class TestExists:
    def test_default_on_missing(self) -> None:
        marker = Exists(FakeEntity, from_param="email", against="email")
        assert marker.on_missing is OnMissing.RETURN_FALSE
