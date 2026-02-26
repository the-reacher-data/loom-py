from __future__ import annotations

from typing import Any

import pytest

from loom.core.errors import (
    Conflict,
    DomainError,
    Forbidden,
    LoomError,
    NotFound,
    RuleViolation,
    RuleViolations,
    SystemError,
)

# ---------------------------------------------------------------------------
# LoomError base
# ---------------------------------------------------------------------------


class TestLoomError:
    def test_stores_message_and_code(self) -> None:
        err = LoomError("something broke", code="broken")
        assert err.message == "something broke"
        assert err.code == "broken"

    def test_is_exception(self) -> None:
        err = LoomError("msg", code="x")
        assert isinstance(err, Exception)

    def test_str_is_message(self) -> None:
        err = LoomError("readable message", code="x")
        assert str(err) == "readable message"


# ---------------------------------------------------------------------------
# Hierarchy
# ---------------------------------------------------------------------------


class TestHierarchy:
    def test_domain_error_is_loom_error(self) -> None:
        assert issubclass(DomainError, LoomError)

    def test_system_error_is_loom_error(self) -> None:
        assert issubclass(SystemError, LoomError)

    def test_not_found_is_domain_error(self) -> None:
        assert issubclass(NotFound, DomainError)

    def test_forbidden_is_domain_error(self) -> None:
        assert issubclass(Forbidden, DomainError)

    def test_conflict_is_domain_error(self) -> None:
        assert issubclass(Conflict, DomainError)

    def test_rule_violation_is_domain_error(self) -> None:
        assert issubclass(RuleViolation, DomainError)

    def test_rule_violations_is_domain_error(self) -> None:
        assert issubclass(RuleViolations, DomainError)

    def test_domain_error_not_system_error(self) -> None:
        assert not issubclass(DomainError, SystemError)


# ---------------------------------------------------------------------------
# NotFound
# ---------------------------------------------------------------------------


class TestNotFound:
    def test_attributes(self) -> None:
        err = NotFound("User", id=42)
        assert err.entity == "User"
        assert err.id == 42

    def test_code(self) -> None:
        assert NotFound("User", id=1).code == "not_found"

    def test_message_format(self) -> None:
        err = NotFound("Product", id="abc-123")
        assert "Product" in err.message
        assert "abc-123" in err.message

    def test_catchable_as_domain_error(self) -> None:
        with pytest.raises(DomainError):
            raise NotFound("User", id=99)

    def test_catchable_as_loom_error(self) -> None:
        with pytest.raises(LoomError):
            raise NotFound("User", id=1)


# ---------------------------------------------------------------------------
# Forbidden
# ---------------------------------------------------------------------------


class TestForbidden:
    def test_default_message(self) -> None:
        err = Forbidden()
        assert "Forbidden" in err.message

    def test_custom_message(self) -> None:
        err = Forbidden("You cannot update other users")
        assert err.message == "You cannot update other users"

    def test_code(self) -> None:
        assert Forbidden().code == "forbidden"


# ---------------------------------------------------------------------------
# Conflict
# ---------------------------------------------------------------------------


class TestConflict:
    def test_message(self) -> None:
        err = Conflict("Email already registered")
        assert err.message == "Email already registered"

    def test_code(self) -> None:
        assert Conflict("x").code == "conflict"


# ---------------------------------------------------------------------------
# SystemError
# ---------------------------------------------------------------------------


class TestSystemError:
    def test_message(self) -> None:
        err = SystemError("Database connection lost")
        assert err.message == "Database connection lost"

    def test_code(self) -> None:
        assert SystemError("x").code == "system_error"

    def test_not_catchable_as_domain_error(self) -> None:
        with pytest.raises(SystemError):
            raise SystemError("boom")
        # Must NOT be caught by DomainError
        caught = False
        try:
            raise SystemError("boom")
        except DomainError:
            caught = True
        except SystemError:
            pass
        assert not caught


# ---------------------------------------------------------------------------
# RuleViolation
# ---------------------------------------------------------------------------


class TestRuleViolation:
    def test_field_and_message(self) -> None:
        err = RuleViolation("email", "Invalid format")
        assert err.field == "email"
        assert err.message == "Invalid format"

    def test_code(self) -> None:
        assert RuleViolation("f", "m").code == "rule_violation"

    def test_str_contains_field_and_message(self) -> None:
        err = RuleViolation("email", "Invalid format")
        assert "email" in str(err)
        assert "Invalid format" in str(err)

    def test_catchable_as_domain_error(self) -> None:
        with pytest.raises(DomainError):
            raise RuleViolation("x", "y")

    def test_backward_compat_import_from_rule_module(self) -> None:
        from loom.core.use_case.rule import RuleViolation as RuleViolationFromRule

        assert RuleViolationFromRule is RuleViolation


# ---------------------------------------------------------------------------
# RuleViolations
# ---------------------------------------------------------------------------


class TestRuleViolations:
    def test_stores_violations_as_tuple(self) -> None:
        v1 = RuleViolation("email", "bad")
        v2 = RuleViolation("name", "empty")
        err = RuleViolations([v1, v2])
        assert err.violations == (v1, v2)

    def test_code(self) -> None:
        assert RuleViolations([]).code == "rule_violations"

    def test_message_joins_violations(self) -> None:
        v1 = RuleViolation("email", "bad")
        v2 = RuleViolation("name", "empty")
        err = RuleViolations([v1, v2])
        assert "email" in err.message
        assert "name" in err.message

    def test_catchable_as_domain_error(self) -> None:
        with pytest.raises(DomainError):
            raise RuleViolations([RuleViolation("f", "m")])

    def test_backward_compat_import_from_rule_module(self) -> None:
        from loom.core.use_case.rule import RuleViolations as RVFromRule

        assert RVFromRule is RuleViolations


# ---------------------------------------------------------------------------
# Executor uses NotFound (integration smoke)
# ---------------------------------------------------------------------------


class TestExecutorUsesNotFound:
    async def test_missing_entity_raises_not_found(self) -> None:
        from unittest.mock import AsyncMock

        from loom.core.engine.compiler import UseCaseCompiler
        from loom.core.engine.executor import RuntimeExecutor
        from loom.core.use_case.markers import Load
        from loom.core.use_case.use_case import UseCase

        class _Entity:
            pass

        class _UC(UseCase[Any, None]):
            async def execute(
                self,
                eid: int,
                entity: _Entity = Load(_Entity, by="eid"),
            ) -> None:
                pass

        repo = AsyncMock()
        repo.get_by_id = AsyncMock(return_value=None)

        executor = RuntimeExecutor(UseCaseCompiler())
        with pytest.raises(NotFound) as exc_info:
            await executor.execute(
                _UC(),
                params={"eid": 7},
                dependencies={_Entity: repo},
            )

        assert exc_info.value.entity == "_Entity"
        assert exc_info.value.id == 7
        assert exc_info.value.code == "not_found"
