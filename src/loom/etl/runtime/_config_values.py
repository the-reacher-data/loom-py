"""Resolution of :class:`~loom.etl.FromConfig` values against a config context.

Shared by the compiler, which checks every declared key before a run, and the
executor, which resolves the values a step receives.  Every failure reports
the key and the expected type, never the value: a ``msgspec`` message may
quote the rejected value, so it is dropped together with the exception chain.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any

import msgspec

from loom.core.config import ConfigContext, ConfigError
from loom.etl.declarative.source._from_config import type_label


class ConfigValueFailure(StrEnum):
    """Why a config value could not be supplied."""

    MISSING = "is not set"
    UNRESOLVED = "cannot be resolved (check its interpolations and resolvers)"
    INVALID = "does not validate as"


class ConfigValueError(ConfigError):
    """A declared config value is missing, unresolvable or of the wrong type.

    The message names the key and the expected type only; the value and the
    underlying exception are never attached.

    Attributes:
        key: Dot-separated config path.
        failure: Why the value could not be supplied.
    """

    def __init__(self, key: str, value_type: object, failure: ConfigValueFailure) -> None:
        reason = failure.value
        if failure is ConfigValueFailure.INVALID:
            reason = f"{reason} {type_label(value_type)!r}"
        super().__init__(f"Config key {key!r} {reason}")
        self.key = key
        self.failure = failure


def resolve_config_value(context: ConfigContext, key: str, value_type: object) -> Any:
    """Resolve *key* in *context* and convert it to *value_type*.

    Args:
        context: Config the runner was built from.
        key: Dot-separated config path.
        value_type: Target type for ``msgspec.convert``.

    Returns:
        The converted value.

    Raises:
        ConfigValueError: When the key is absent or null, its interpolation
            cannot be resolved, or the value does not validate.
    """
    failure = _presence_failure(context, key)
    if failure is not None:
        raise ConfigValueError(key, value_type, failure)
    try:
        raw = context.section(key, object)
    except ConfigError:
        raise ConfigValueError(key, value_type, ConfigValueFailure.UNRESOLVED) from None
    try:
        return msgspec.convert(raw, value_type, strict=False)
    except msgspec.ValidationError:
        raise ConfigValueError(key, value_type, ConfigValueFailure.INVALID) from None


def _presence_failure(context: ConfigContext, key: str) -> ConfigValueFailure | None:
    try:
        present = context.has(key)
    except ValueError:
        # OmegaConf reports an interpolation it cannot resolve as a ValueError.
        return ConfigValueFailure.UNRESOLVED
    return None if present else ConfigValueFailure.MISSING
