"""Tests for MaintenanceStep base class and __init_subclass__ validation."""

from __future__ import annotations

import pytest

from loom.etl.maintenance._builder import MaintainSchema, MaintainTable
from loom.etl.maintenance._step import MaintenanceStep


class TestMaintenanceStepDefinition:
    def test_valid_step(self) -> None:
        class MyStep(MaintenanceStep[None]):
            operations = [MaintainTable("raw.events").vacuum()]

        assert MyStep.operations is not None
        assert len(MyStep.operations) == 1

    def test_empty_operations_is_valid(self) -> None:
        class EmptyStep(MaintenanceStep[None]):
            operations = []

        assert EmptyStep.operations == []

    def test_operations_not_list_raises(self) -> None:
        with pytest.raises(TypeError, match="must be a list"):

            class Bad(MaintenanceStep[None]):  # type: ignore[type-arg]
                operations = MaintainTable("raw.events").vacuum()  # type: ignore[assignment]

    def test_operations_wrong_element_type_raises(self) -> None:
        with pytest.raises(TypeError, match="MaintainTable or MaintainSchema"):

            class Bad(MaintenanceStep[None]):  # type: ignore[type-arg]
                operations = ["not-a-builder"]  # type: ignore[list-item]

    def test_mix_maintain_table_and_schema(self) -> None:
        class Mixed(MaintenanceStep[None]):
            operations = [
                MaintainTable("staging.events").vacuum(),
                MaintainSchema("raw").compact(),
            ]

        assert len(Mixed.operations) == 2

    def test_params_type_extraction(self) -> None:
        class TypedStep(MaintenanceStep[int]):
            operations = []

        assert TypedStep._params_type is int

    def test_params_type_none_for_none_generic(self) -> None:
        class UntypedStep(MaintenanceStep[None]):
            operations = []

        assert UntypedStep._params_type is type(None)
