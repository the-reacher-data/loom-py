from __future__ import annotations

from typing import Any, cast

import pytest
from msgspec import UNSET

from loom.core.command.field import CommandField


class TestCommandFieldDefaults:
    def test_default_values(self) -> None:
        cf = CommandField()
        assert cf.calculated is False
        assert cf.internal is False
        assert cf.default is UNSET

    def test_with_explicit_values(self) -> None:
        cf = CommandField(calculated=True, default=42)
        assert cf.calculated is True
        assert cf.internal is False
        assert cf.default == 42


class TestCommandFieldValidation:
    def test_internal_without_default_is_allowed(self) -> None:
        cf = CommandField(internal=True)
        assert cf.internal is True
        assert cf.default is UNSET

    def test_calculated_without_default_is_allowed(self) -> None:
        cf = CommandField(calculated=True)
        assert cf.calculated is True
        assert cf.default is UNSET

    def test_internal_with_default_succeeds(self) -> None:
        cf = CommandField(internal=True, default=0)
        assert cf.internal is True
        assert cf.default == 0

    def test_calculated_with_default_succeeds(self) -> None:
        cf = CommandField(calculated=True, default=False)
        assert cf.calculated is True
        assert cf.default is False


class TestCommandFieldFrozen:
    def test_cannot_mutate_calculated(self) -> None:
        cf = CommandField()
        with pytest.raises(AttributeError):
            cast(Any, cf).calculated = True

    def test_cannot_mutate_internal(self) -> None:
        cf = CommandField()
        with pytest.raises(AttributeError):
            cast(Any, cf).internal = True

    def test_cannot_mutate_default(self) -> None:
        cf = CommandField()
        with pytest.raises(AttributeError):
            cast(Any, cf).default = 42
