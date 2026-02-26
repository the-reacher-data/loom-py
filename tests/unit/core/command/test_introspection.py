from __future__ import annotations

from loom.core.command import Command, Computed, Internal, Patch
from loom.core.command.introspection import (
    get_calculated_fields,
    get_command_fields,
    get_input_fields,
    get_internal_fields,
    get_patch_fields,
    is_patch_command,
)


class MixedCommand(Command, frozen=True):
    email: str
    tenant_id: Internal[int] = 0
    is_adult: Computed[bool] = False
    nickname: str = "anon"


class PatchCommand(Command, frozen=True):
    email: Patch[str | None]


class PlainCommand(Command, frozen=True):
    email: str


class TestGetCommandFields:
    def test_returns_only_command_fields(self) -> None:
        result = get_command_fields(MixedCommand)
        assert "tenant_id" in result
        assert "is_adult" in result
        assert "nickname" not in result
        assert "email" not in result

    def test_empty_for_no_command_fields(self) -> None:
        result = get_command_fields(PlainCommand)
        assert result == {}


class TestGetInputFields:
    def test_excludes_internal_and_calculated(self) -> None:
        result = get_input_fields(MixedCommand)
        assert "nickname" not in result
        assert "tenant_id" not in result
        assert "is_adult" not in result


class TestGetCalculatedFields:
    def test_only_calculated(self) -> None:
        result = get_calculated_fields(MixedCommand)
        assert list(result.keys()) == ["is_adult"]
        assert result["is_adult"].calculated is True


class TestGetInternalFields:
    def test_only_internal(self) -> None:
        result = get_internal_fields(MixedCommand)
        assert list(result.keys()) == ["tenant_id"]
        assert result["tenant_id"].internal is True


class TestIsPatchCommand:
    def test_patch_true(self) -> None:
        assert is_patch_command(PatchCommand) is True

    def test_patch_false(self) -> None:
        assert is_patch_command(PlainCommand) is False


class TestGetPatchFields:
    def test_only_patch(self) -> None:
        result = get_patch_fields(PatchCommand)
        assert list(result.keys()) == ["email"]
        assert result["email"].patch is True
