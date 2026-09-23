"""Handlebars templating (``ai-templates`` extra): compile one block against its schema."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

import pydantic_handlebars
from pydantic_handlebars import HandlebarsError

__all__ = ["HandlebarsError", "compile_checked"]


def compile_checked(text: str, schema: Mapping[str, Any] | None) -> Callable[[Any], str]:
    """Compile a template, checked against *schema* when one is declared.

    Args:
        text: Handlebars source of the block.
        schema: JSON schema of the state the template renders against, or
            ``None`` to compile unchecked.

    Returns:
        A renderer taking the run's state.

    Raises:
        HandlebarsError: The template fails to parse or the compatibility check.
    """
    if schema is not None:
        pydantic_handlebars.check_template_compatibility(text, dict(schema), raise_on_error=True)
    compiled = pydantic_handlebars.compile(text)
    return compiled.render
