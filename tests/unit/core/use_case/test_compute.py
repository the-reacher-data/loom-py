from __future__ import annotations

import msgspec

from loom.core.command import Command, Computed, Patch
from loom.core.use_case import Compute, F


class CreateArticle(Command, frozen=True):
    title: str
    slug: Computed[str] = ""


class UpdateArticle(Command, frozen=True):
    title: Patch[str | None]
    slug: Computed[str] = ""


def set_slug(command: CreateArticle, fields_set: frozenset[str]) -> CreateArticle:
    del fields_set
    slug = command.title.lower().replace(" ", "-")
    return msgspec.structs.replace(command, slug=slug)


def maybe_set_slug(
    command: CreateArticle, fields_set: frozenset[str]
) -> CreateArticle:
    if "title" not in fields_set:
        return command
    slug = command.title.lower().replace(" ", "-")
    return msgspec.structs.replace(command, slug=slug)


def normalize_patch_slug(title: str | None) -> str:
    return "" if title is None else str(title).lower()


class TestCompute:
    def test_populates_calculated_field_from_input(self) -> None:
        cmd = CreateArticle(title="Hello World")
        result = set_slug(cmd, frozenset({"title"}))

        assert result.slug == "hello-world"
        assert result.title == "Hello World"

    def test_returns_new_frozen_instance(self) -> None:
        cmd = CreateArticle(title="Test")
        result = set_slug(cmd, frozenset({"title"}))

        assert result is not cmd
        assert type(result) is CreateArticle

    def test_fields_set_available_for_conditional_logic(self) -> None:
        """Compute can skip work when source field is absent (patch scenario)."""

        cmd = CreateArticle(title="Ignored")
        result = maybe_set_slug(cmd, frozenset())

        assert result.slug == ""

    def test_dsl_assigns_single_source(self) -> None:
        compute = Compute.set(F(CreateArticle).slug).from_fields(F(CreateArticle).title).build()

        cmd = CreateArticle(title="Hello")
        result = compute(cmd, frozenset({"title"}))

        assert result.slug == "Hello"

    def test_dsl_assigns_via_callable(self) -> None:
        compute = Compute.set(F(CreateArticle).slug).from_fields(
            F(CreateArticle).title,
            via=lambda title: str(title).lower().replace(" ", "-"),
        ).build()

        cmd = CreateArticle(title="Hello World")
        result = compute(cmd, frozenset({"title"}))

        assert result.slug == "hello-world"

    def test_dsl_when_present_respects_patch_fields_set(self) -> None:
        compute = Compute.set(F(UpdateArticle).slug).from_fields(
            F(UpdateArticle).title,
            via=normalize_patch_slug,
        ).when_present(F(UpdateArticle).title)

        missing, _ = UpdateArticle.from_payload({})
        missing_result = compute(missing, frozenset())
        assert missing_result.slug == ""

        provided, fields_set = UpdateArticle.from_payload({"title": "PATCHED"})
        provided_result = compute(provided, fields_set)
        assert provided_result.slug == "patched"
