"""Engine and provider resolution for the AI pillar.

Resolves the configured engine through :mod:`loom.core.plugins.entrypoints`
(group ``loom.ai.engines``, duplicates rejected — FR-021) and offers the two
helpers engines use to fail fast on missing provider SDKs and settings.

This module never imports a concrete engine: the entry point is loaded by
name and the handshake is a ``getattr`` on the loaded object, never an
``isinstance`` check, so the contract stays structural.
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType
from typing import cast

from loom.ai.abc import AgentEngineProvider
from loom.ai.errors import (
    AgentCompilationError,
    engine_api_mismatch,
    engine_duplicate,
    engine_not_found,
    provider_not_installed,
    provider_setting_missing,
)
from loom.core.plugins.entrypoints import (
    DuplicateEntryPointError,
    list_entry_points,
    select_entry_point,
)

ENGINE_ENTRY_POINT_GROUP = "loom.ai.engines"
"""Entry-point group every engine distribution registers under."""

ENGINE_API_ATTRIBUTE = "LOOM_AI_ENGINE_API"
"""Attribute a provider declares its handshake version on."""

SUPPORTED_ENGINE_APIS: frozenset[int] = frozenset({1})
"""Handshake versions this release of loom accepts."""

_UNKNOWN_DISTRIBUTION = "<unknown distribution>"


def resolve_engine_provider(name: str) -> AgentEngineProvider:
    """Resolve the engine provider registered under ``name``.

    Selects the entry point in group ``loom.ai.engines`` with duplicates
    rejected, loads it, instantiates it when it targets a class, and verifies
    the :data:`ENGINE_API_ATTRIBUTE` handshake via ``getattr``.

    Args:
        name: Entry-point name from ``ai.engine``.

    Returns:
        The provider instance, handshake verified.

    Raises:
        AgentCompilationError: With ``ENGINE_NOT_FOUND`` (listing the
            installed engines), ``ENGINE_DUPLICATE`` (listing every claiming
            distribution) or ``ENGINE_API_MISMATCH`` (missing or unsupported
            handshake version).
    """
    try:
        ep = select_entry_point(ENGINE_ENTRY_POINT_GROUP, name, on_duplicate="error")
    except DuplicateEntryPointError as exc:
        raise AgentCompilationError([engine_duplicate(name, _distributions_for(name))]) from exc
    if ep is None:
        raise AgentCompilationError([engine_not_found(name, _available_engine_names())])
    loaded = ep.load()
    provider = loaded() if isinstance(loaded, type) else loaded
    _verify_engine_api(name, provider)
    # The handshake above is the structural runtime check; the protocol is not
    # runtime-checkable by design, so the cast records the contract.
    return cast(AgentEngineProvider, provider)


def require_provider_sdk(provider: str, module: str, extra: str) -> ModuleType:
    """Import a provider SDK module, failing with the extra to install.

    Args:
        provider: Provider identifier the error should name.
        module: Importable module path of the SDK.
        extra: Loom extra whose installation brings the SDK.

    Returns:
        The imported module.

    Raises:
        AgentCompilationError: With ``PROVIDER_NOT_INSTALLED`` naming the
            extra when the import fails.
    """
    try:
        return import_module(module)
    except ImportError as exc:
        raise AgentCompilationError([provider_not_installed(provider, extra)]) from exc


def require_provider_setting(provider: str, setting: str, value: object | None) -> None:
    """Require a provider setting to be present and non-empty.

    ``None`` and the empty string count as missing; any other value —
    including ``0`` and ``False`` — is accepted, since those can be
    legitimate settings.

    Args:
        provider: Provider identifier the error should name.
        setting: Setting name the error should name.
        value: Resolved setting value.

    Raises:
        AgentCompilationError: With ``PROVIDER_SETTING_MISSING`` naming the
            setting when the value is absent.
    """
    if value is None or value == "":
        raise AgentCompilationError([provider_setting_missing(provider, setting)])


def _verify_engine_api(name: str, provider: object) -> None:
    """Check the handshake attribute on the loaded provider, fail-closed."""
    declared = getattr(provider, ENGINE_API_ATTRIBUTE, None)
    # ``type is int`` rather than isinstance: a bool would be a mistake, not
    # a version.
    if type(declared) is int and declared in SUPPORTED_ENGINE_APIS:
        return
    found = declared if type(declared) is int else 0
    raise AgentCompilationError([engine_api_mismatch(name, found, sorted(SUPPORTED_ENGINE_APIS))])


def _available_engine_names() -> list[str]:
    """Names registered in the engine group, for the not-found message."""
    return sorted({ep.name for ep in list_entry_points(ENGINE_ENTRY_POINT_GROUP)})


def _distributions_for(name: str) -> list[str]:
    """Distributions claiming ``name`` in the engine group, for FR-021."""
    return [
        ep.dist.name if ep.dist is not None else _UNKNOWN_DISTRIBUTION
        for ep in list_entry_points(ENGINE_ENTRY_POINT_GROUP)
        if ep.name == name
    ]
