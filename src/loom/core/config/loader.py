"""Configuration loader backed by omegaConf.

omegaConf is the single source of truth for configuration.  It reads and
merges YAML files, resolves ``${oc.env:VAR}`` interpolations, and returns a
live :class:`omegaconf.DictConfig`.  The framework does **not** impose any
particular shape on the config — the user owns the structure.

For sections that benefit from strict typing, use :func:`section` to
extract and validate a subtree into a user-defined struct or dataclass via
``msgspec.convert``.

Example::

    cfg = load_config("config/base.yaml", "config/production.yaml")

    # Typed extraction of a section
    class DatabaseConfig(msgspec.Struct, kw_only=True):
        url: str
        pool_size: int = 5

    db = section(cfg, "database", DatabaseConfig)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

import msgspec

from loom.core.config.errors import ConfigError

if TYPE_CHECKING:
    from omegaconf import DictConfig

T = TypeVar("T")


def _ensure_omegaconf() -> Any:
    """Import OmegaConf or raise a clear ConfigError."""
    try:
        from omegaconf import OmegaConf

        return OmegaConf
    except ImportError as exc:
        raise ConfigError(
            "omegaconf is required for load_config. Install it with: pip install loom[config]"
        ) from exc


def load_config(*config_files: str) -> DictConfig:
    """Load and merge one or more YAML config files into a DictConfig.

    Files are merged **left-to-right**: values in later files override those in
    earlier ones.  ``${oc.env:VAR}`` interpolations are resolved by OmegaConf.

    The framework does not impose any shape on the resulting config — the user
    owns the structure entirely.  Use :func:`section` to extract typed
    sub-objects where desired.

    Args:
        *config_files: One or more paths to YAML configuration files.

    Returns:
        Merged :class:`omegaconf.DictConfig` with interpolation support.

    Raises:
        ConfigError: If no files are provided, a file is not found, parsing
            fails, or omegaconf is not installed.

    Example::

        cfg = load_config("config/base.yaml", "config/production.yaml")
        db_url = cfg.database.url          # dot access
        debug  = cfg.get("debug", False)   # safe access with default
    """
    if not config_files:
        raise ConfigError("load_config requires at least one config file.")

    omega_conf = _ensure_omegaconf()

    bases = []
    for path in config_files:
        try:
            bases.append(omega_conf.load(path))
        except FileNotFoundError as exc:
            raise ConfigError(f"Configuration file not found: {path!r}") from exc
        except Exception as exc:
            raise ConfigError(f"Failed to parse configuration file {path!r}: {exc}") from exc

    merged = omega_conf.merge(*bases) if len(bases) > 1 else bases[0]
    return merged  # type: ignore[no-any-return]


def section(cfg: DictConfig, key: str, target_type: type[T]) -> T:
    """Extract and validate a config section as a typed object.

    Navigates ``cfg`` by ``key`` (dot-notation supported, e.g.
    ``"database.primary"``), resolves omegaConf interpolations, and converts
    the result to ``target_type`` via ``msgspec.convert``.

    Works with any type supported by ``msgspec.convert``: ``msgspec.Struct``
    subclasses, ``dataclasses``, ``TypedDict``, plain dicts, etc.

    Args:
        cfg: Root :class:`omegaconf.DictConfig` returned by :func:`load_config`.
        key: Dot-separated path to the desired section (e.g. ``"database"``
            or ``"services.cache"``).
        target_type: Type to convert the section into.

    Returns:
        Validated instance of ``target_type``.

    Raises:
        ConfigError: If the key is absent, the section cannot be resolved,
            or ``msgspec`` validation fails.

    Example::

        class DatabaseConfig(msgspec.Struct, kw_only=True):
            url: str
            pool_size: int = 5

        db = section(cfg, "database", DatabaseConfig)
    """
    omega_conf = _ensure_omegaconf()

    # Navigate through dot-separated keys
    node: Any = cfg
    for part in key.split("."):
        try:
            node = node[part]
        except Exception as exc:
            raise ConfigError(
                f"Config section not found: {key!r}  (failed at segment {part!r})"
            ) from exc

    try:
        data = omega_conf.to_container(node, resolve=True)
    except Exception as exc:
        raise ConfigError(
            f"Failed to resolve config section {key!r} "
            f"(check ${{oc.env:VAR}} interpolations): {exc}"
        ) from exc

    try:
        return msgspec.convert(data, target_type, strict=False)
    except msgspec.ValidationError as exc:
        raise ConfigError(
            f"Config section {key!r} failed validation as {target_type.__name__!r}: {exc}"
        ) from exc
