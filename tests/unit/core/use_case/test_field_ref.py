from __future__ import annotations

from loom.core.command import Command, Patch
from loom.core.use_case import F, PredicateOp


class UpdateUser(Command, frozen=True):
    email: Patch[str | None]


class TestFieldRef:
    def test_builds_ref_from_command_class(self) -> None:
        ref = F(UpdateUser).email

        assert ref.root is UpdateUser
        assert ref.path == ("email",)
        assert ref.leaf == "email"

    def test_supports_nested_path(self) -> None:
        ref = F("user").profile.name

        assert ref.root == "user"
        assert ref.path == ("profile", "name")
        assert ref.leaf == "name"

    def test_supports_or_expression(self) -> None:
        expr = F(UpdateUser).email | F(UpdateUser).email
        assert expr.op is PredicateOp.OR

    def test_supports_and_expression(self) -> None:
        expr = F(UpdateUser).email & F(UpdateUser).email
        assert expr.op is PredicateOp.AND
