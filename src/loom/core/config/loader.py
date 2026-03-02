"""Configuration loader backed by omegaConf.

omegaConf is the single source of truth for configuration.  It reads and
merges YAML files, resolves ``${oc.env:VAR}`` interpolations, and returns a
live :class:`omegaconf.DictConfig`.  The framework does **not** impose any
particular shape on the config — the user owns the structure.

For sections that benefit from strict typing, use :func:`section` to
extract and validate a subtree into a user-defined struct or dataclass via
``msgspec.convert``.

YAML ``includes`` directive
---------------------------
A config file may declare a top-level ``includes`` list to merge other files
before its own values.  Paths are resolved relative to the declaring file.
The declaring file always takes precedence over its includes.  Includes are
resolved recursively; circular references raise :class:`ConfigError`.

Example::

    # config/app.yaml
    includes:
      - base.yaml       # relative to config/
      - secrets.yaml

    app:
      name: my-service  # overrides anything in base.yaml or secrets.yaml

Function-level composition is still available::

    cfg = load_config("config/base.yaml", "config/production.yaml")

    class DatabaseConfig(msgspec.Struct, kw_only=True):
        url: str
        pool_size: int = 5

    db = section(cfg, "database", DatabaseConfig)
"""

from __future__ import annotations

from pathlib import Path
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


def _load_file(path: str, omega_conf: Any, seen: set[str]) -> Any:
    """Load a single YAML file, resolving its ``includes`` recursively.

    Args:
        path: Absolute or relative path to the YAML file.
        omega_conf: OmegaConf module.
        seen: Set of resolved absolute paths already in the call stack,
            used to detect circular references.

    Returns:
        Merged :class:`omegaconf.DictConfig` for the file (includes merged in,
        ``includes`` key stripped from the result).

    Raises:
        ConfigError: On missing file, parse error, or circular include.
    """
    resolved = str(Path(path).resolve())
    if resolved in seen:
        raise ConfigError(f"Circular include detected: {path!r} is already being loaded.")
    seen = seen | {resolved}  # immutable copy per branch — no shared mutation

    try:
        cfg = omega_conf.load(path)
    except FileNotFoundError as exc:
        raise ConfigError(f"Configuration file not found: {path!r}") from exc
    except Exception as exc:
        raise ConfigError(f"Failed to parse configuration file {path!r}: {exc}") from exc

    raw_includes = omega_conf.select(cfg, "includes", default=None)
    if raw_includes is None:
        return cfg

    include_paths = list(omega_conf.to_container(raw_includes))
    cfg = omega_conf.masked_copy(cfg, [k for k in cfg if k != "includes"])

    base_dir = Path(resolved).parent
    layers: list[Any] = []
    for inc in include_paths:
        inc_path = str((base_dir / inc).resolve())
        layers.append(_load_file(inc_path, omega_conf, seen))

    layers.append(cfg)  # declaring file wins — appended last
    return omega_conf.merge(*layers) if len(layers) > 1 else layers[0]


def load_config(*config_files: str) -> DictConfig:
    """Load and merge one or more YAML config files into a DictConfig.

    Files are merged **left-to-right**: values in later files override those
    in earlier ones.  ``${oc.env:VAR}`` interpolations are resolved by
    OmegaConf.

    Each file may declare a top-level ``includes`` list to pull in additional
    files before its own values.  Included paths are relative to the file that
    declares them.  The declaring file always overrides its includes.
    Circular includes raise :class:`ConfigError`.

    The framework does not impose any shape on the resulting config — the user
    owns the structure entirely.  Use :func:`section` to extract typed
    sub-objects where desired.

    Args:
        *config_files: One or more paths to YAML configuration files.

    Returns:
        Merged :class:`omegaconf.DictConfig` with interpolation support.

    Raises:
        ConfigError: If no files are provided, a file is not found, parsing
            fails, a circular include is detected, or omegaconf is not
            installed.

    Example — single file with inline includes::

        # config.yaml
        # includes:
        #   - base.yaml
        #   - secrets.yaml
        cfg = load_config("config.yaml")

    Example — explicit multi-file composition::

        cfg = load_config("config/base.yaml", "config/production.yaml")
        db_url = cfg.database.url
    """
    if not config_files:
        raise ConfigError("load_config requires at least one config file.")

    omega_conf = _ensure_omegaconf()

    layers = [_load_file(path, omega_conf, set()) for path in config_files]
    merged = omega_conf.merge(*layers) if len(layers) > 1 else layers[0]
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
