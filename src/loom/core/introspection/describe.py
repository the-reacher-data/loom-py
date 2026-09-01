"""Assembly of the application self-description document."""

from __future__ import annotations

import importlib
from typing import Any, cast

from loom.core.introspection.base import (
    AppIntrospection,
    ContributorRef,
    DescribeContributor,
    IntrospectionError,
)

_IDENTITY_SECTION = "app"


def describe_app(introspection: AppIntrospection) -> dict[str, Any]:
    """Describe an application as one JSON-encodable document.

    The identity always appears under ``"app"``; every contributor adds its
    projection under its own section, in declaration order.  References are
    imported here and nowhere else, which is what keeps ``loom.core`` free of
    pillar imports.

    Args:
        introspection: Application identity plus its contributor references.

    Returns:
        Mapping of section name to contribution, starting with ``"app"``.

    Raises:
        IntrospectionError: When a reference is not ``"module:callable"``, its
            module or attribute cannot be resolved, the attribute is not
            callable, a section is declared twice, or a section is the
            reserved ``"app"``.

    Example::

        describe_app(AppIntrospection(name="billing", version="1.4.0"))
        # {'app': {'name': 'billing', 'version': '1.4.0'}}
    """
    document: dict[str, Any] = {
        _IDENTITY_SECTION: {"name": introspection.name, "version": introspection.version}
    }
    for ref in introspection.contributors:
        _reject_reserved_or_duplicate_section(ref.section, document)
        document[ref.section] = _resolve(ref)(ref.subject)
    return document


def _reject_reserved_or_duplicate_section(section: str, document: dict[str, Any]) -> None:
    if section == _IDENTITY_SECTION:
        raise IntrospectionError(f"section {section!r} is reserved for the application identity.")
    if section in document:
        raise IntrospectionError(f"section {section!r} is declared by more than one contributor.")


def _resolve(ref: ContributorRef) -> DescribeContributor:
    module_name, separator, attribute = ref.contributor.partition(":")
    if not separator or not module_name or not attribute:
        raise IntrospectionError(
            f"contributor {ref.contributor!r} of section {ref.section!r} is not a "
            "'module:callable' reference."
        )
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise IntrospectionError(
            f"contributor module {module_name!r} of section {ref.section!r} cannot be imported."
        ) from exc
    try:
        candidate = getattr(module, attribute)
    except AttributeError as exc:
        raise IntrospectionError(
            f"module {module_name!r} exposes no {attribute!r} for section {ref.section!r}."
        ) from exc
    if not callable(candidate):
        raise IntrospectionError(
            f"contributor {ref.contributor!r} of section {ref.section!r} is not callable."
        )
    return cast("DescribeContributor", candidate)
