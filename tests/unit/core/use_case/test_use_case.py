from __future__ import annotations

from dataclasses import dataclass
from typing import cast
from unittest.mock import AsyncMock

import msgspec
import pytest

from loom.core.command import Command, Computed, Internal, Patch
from loom.core.use_case.rule import RuleViolation, RuleViolations
from loom.core.use_case.use_case import UseCase

# --- Domain models ---


class CreateUser(Command, frozen=True):
    email: str
    name: str
    slug: Computed[str] = ""
    tenant_id: Internal[str] = ""


class UpdateUser(Command, frozen=True):
    email: Patch[str | None]
    name: Patch[str | None]
    tenant_id: Internal[str] = ""


class UserEntity:
    def __init__(
        self, id: int, email: str, name: str, slug: str, tenant_id: str
    ) -> None:
        self.id = id
        self.email = email
        self.name = name
        self.slug = slug
        self.tenant_id = tenant_id


@dataclass
class _CreateExecutor:
    repo: AsyncMock

    async def __call__(
        self, cmd: CreateUser, fields_set: frozenset[str]
    ) -> UserEntity:
        del fields_set
        return cast(UserEntity, await self.repo.create(cmd))


@dataclass
class _UpdateExecutor:
    repo: AsyncMock

    async def __call__(
        self, cmd: UpdateUser, fields_set: frozenset[str]
    ) -> UserEntity:
        return cast(UserEntity, await self.repo.update(cmd, fields_set))


@dataclass
class _CapturedUpdateExecutor:
    repo: AsyncMock
    captured_fields: list[frozenset[str]]

    async def __call__(
        self, cmd: UpdateUser, fields_set: frozenset[str]
    ) -> UserEntity:
        self.captured_fields.append(fields_set)
        return cast(UserEntity, await self.repo.update(cmd, fields_set))


# --- Computes ---


def compute_slug(command: CreateUser, fields_set: frozenset[str]) -> CreateUser:
    slug = command.name.lower().replace(" ", "-")
    return msgspec.structs.replace(command, slug=slug)


def inject_tenant(command: CreateUser, fields_set: frozenset[str]) -> CreateUser:
    return msgspec.structs.replace(command, tenant_id="tenant-abc")


# --- Rules ---


def email_not_disposable(command: CreateUser, fields_set: frozenset[str]) -> None:
    if command.email and command.email.endswith("@throwaway.com"):
        raise RuleViolation("email", "Disposable emails not allowed")


def name_not_empty(command: CreateUser, fields_set: frozenset[str]) -> None:
    if command.name and len(command.name.strip()) == 0:
        raise RuleViolation("name", "Name cannot be blank")


def require_email_on_patch(
    command: UpdateUser, fields_set: frozenset[str]
) -> None:
    if "email" in fields_set and not command.email:
        raise RuleViolation("email", "Email cannot be set to empty")


def bad_email_rule(command: CreateUser, fields_set: frozenset[str]) -> None:
    del command
    del fields_set
    raise RuleViolation("email", "Email is bad")


def bad_name_rule(command: CreateUser, fields_set: frozenset[str]) -> None:
    del command
    del fields_set
    raise RuleViolation("name", "Name is bad")


def pass_rule(command: CreateUser, fields_set: frozenset[str]) -> None:
    del command
    del fields_set
    return None


def uppercase_slug(
    cmd: CreateUser, fields_set: frozenset[str]
) -> CreateUser:
    del fields_set
    return msgspec.structs.replace(cmd, slug=str(cmd.slug).upper())


def require_slug_rule(cmd: CreateUser, fields_set: frozenset[str]) -> None:
    del fields_set
    if not cmd.slug:
        raise RuleViolation("slug", "Slug is required")


class TestUseCaseCreateFlow:
    """Simulates a realistic create-user flow with repo mock."""

    async def test_create_user_with_computed_slug(self) -> None:
        repo = AsyncMock()
        repo.create.return_value = UserEntity(
            id=1,
            email="alice@corp.com",
            name="Alice",
            slug="alice",
            tenant_id="tenant-abc",
        )

        uc = UseCase(
            execute=_CreateExecutor(repo),
            computes=[compute_slug, inject_tenant],
            rules=[email_not_disposable],
        )

        cmd = CreateUser(email="alice@corp.com", name="Alice")
        result = await uc(cmd, frozenset({"email", "name"}))

        created_cmd = repo.create.call_args[0][0]
        assert created_cmd.slug == "alice"
        assert created_cmd.tenant_id == "tenant-abc"
        assert result.id == 1

    async def test_rule_blocks_create_before_repo_called(self) -> None:
        repo = AsyncMock()

        uc = UseCase(
            execute=_CreateExecutor(repo),
            computes=[compute_slug],
            rules=[email_not_disposable],
        )

        cmd = CreateUser(email="spam@throwaway.com", name="Spammer")
        with pytest.raises(RuleViolations) as exc_info:
            await uc(cmd, frozenset({"email", "name"}))

        assert len(exc_info.value.violations) == 1
        assert exc_info.value.violations[0].field == "email"
        repo.create.assert_not_called()


class TestUseCaseAccumulatesViolations:
    """Verifies that UseCase accumulates all rule violations."""

    async def test_accumulates_multiple_violations(self) -> None:
        repo = AsyncMock()

        uc = UseCase(
            execute=_CreateExecutor(repo),
            rules=[bad_email_rule, bad_name_rule],
        )

        cmd = CreateUser(email="a@b.com", name="Alice")
        with pytest.raises(RuleViolations) as exc_info:
            await uc(cmd, frozenset({"email", "name"}))

        assert len(exc_info.value.violations) == 2
        assert exc_info.value.violations[0].field == "email"
        assert exc_info.value.violations[1].field == "name"
        repo.create.assert_not_called()

    async def test_no_violations_does_not_raise(self) -> None:
        repo = AsyncMock()
        repo.create.return_value = UserEntity(
            id=1, email="a@b.com", name="A", slug="", tenant_id=""
        )

        uc = UseCase(execute=_CreateExecutor(repo), rules=[pass_rule])

        cmd = CreateUser(email="a@b.com", name="Alice")
        result = await uc(cmd, frozenset({"email", "name"}))
        assert result.id == 1


class TestUseCasePatchFlow:
    """Simulates a realistic update-user flow with partial fields."""

    async def test_partial_update_passes_fields_set_to_execute(self) -> None:
        repo = AsyncMock()
        repo.update.return_value = UserEntity(
            id=1, email="new@corp.com", name="Alice", slug="alice", tenant_id="t"
        )
        captured_fields: list[frozenset[str]] = []
        uc = UseCase(execute=_CapturedUpdateExecutor(repo, captured_fields))
        cmd, fields_set = UpdateUser.from_payload({"email": "new@corp.com"})
        await uc(cmd, fields_set)

        assert captured_fields == [frozenset({"email"})]

    async def test_patch_rule_validates_only_provided_fields(self) -> None:
        repo = AsyncMock()
        uc = UseCase(execute=_UpdateExecutor(repo), rules=[require_email_on_patch])

        cmd, fields_set = UpdateUser.from_payload({"name": "NewName"})
        await uc(cmd, fields_set)
        repo.update.assert_called_once()


class TestUseCasePipeline:
    """Verifies pipeline ordering and chaining."""

    async def test_computes_chain_in_order(self) -> None:
        repo = AsyncMock()
        repo.create.return_value = UserEntity(
            id=1,
            email="a@b.com",
            name="Alice",
            slug="ALICE",
            tenant_id="t-abc",
        )

        uc = UseCase(
            execute=_CreateExecutor(repo),
            computes=[compute_slug, uppercase_slug],
        )

        cmd = CreateUser(email="a@b.com", name="Alice")
        await uc(cmd, frozenset({"email", "name"}))

        created_cmd = repo.create.call_args[0][0]
        assert created_cmd.slug == "ALICE"

    async def test_rule_sees_enriched_command(self) -> None:
        """Rule validation runs AFTER computes, so it sees computed fields."""
        repo = AsyncMock()
        repo.create.return_value = UserEntity(
            id=1, email="a@b.com", name="A", slug="a", tenant_id="t"
        )

        uc = UseCase(
            execute=_CreateExecutor(repo),
            computes=[compute_slug],
            rules=[require_slug_rule],
        )

        cmd = CreateUser(email="a@b.com", name="Alice")
        result = await uc(cmd, frozenset({"email", "name"}))

        assert result.id == 1
