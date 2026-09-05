"""Configuration loader backed by omegaConf.

omegaConf is the single source of truth for configuration.  It reads and
merges YAML files, resolves ``${oc.env:VAR}`` interpolations, and returns a
live :class:`omegaconf.DictConfig`.  The framework does **not** impose any
particular shape on the config — the user owns the structure.

For sections that benefit from strict typing, use :func:`section` to
extract and validate a subtree into a user-defined struct or dataclass via
``msgspec.convert``.

Cloud URIs
----------
:func:`load_config` accepts cloud storage URIs (``s3://``, ``gs://``,
``abfss://``, ``r2://`` …) in addition to local filesystem paths.  Cloud
files are fetched via ``fsspec`` at parse time, which means the config is
always resolved against the current state of object storage — no baking
into images or wheels.

Cloud files honour the ``includes`` directive exactly like local files:
relative entries resolve against the including URI, and entries may point
to any scheme.

Custom resolvers
----------------
Pass :class:`~loom.core.config.resolver.ConfigResolver` implementations to
resolve ``${prefix:key}`` placeholders at parse time::

    cfg = load_config("s3://bucket/prod.yaml", resolvers=[SsmResolver("eu-west-1")])

YAML ``includes`` directive
---------------------------
A config file may declare a top-level ``includes`` list to merge other
files before its own values.  Entries are resolved relative to the declaring
file, on any scheme, and may be globs (``*``, ``?``, ``[``); glob matches
are merged in lexicographic order of their resolved path.  The declaring
file always takes precedence over its includes.  Includes are resolved
recursively; circular references and entries matching no file raise
:class:`ConfigError`.

Example::

    # config/app.yaml
    includes:
      - base.yaml            # relative to config/
      - tables/*.yaml        # every YAML under config/tables/
      - s3://bucket/shared.yaml

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

import contextlib
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, TypeVar

import msgspec

from loom.core.config._includes import canonical_key, expand_include, is_cloud_uri, resolve_include
from loom.core.config.errors import ConfigError

if TYPE_CHECKING:
    from omegaconf import DictConfig

    from loom.core.config.resolver import ConfigResolver

T = TypeVar("T")


def _ensure_omegaconf() -> Any:
    """Import OmegaConf or raise a clear ConfigError."""
    try:
        from omegaconf import OmegaConf

        return OmegaConf
    except ImportError as exc:
        raise ConfigError(
            "omegaconf is required for load_config. "
            "Install it with: pip install loom-kernel[config]"
        ) from exc


def _fetch_cloud_content(uri: str) -> str:
    """Fetch raw YAML text from a cloud URI via fsspec.

    Args:
        uri: Cloud storage URI (``s3://``, ``gs://``, ``abfss://``, …).

    Returns:
        Raw YAML string.

    Raises:
        ConfigError: When the fetch fails.
    """
    import fsspec

    try:
        with fsspec.open(uri, mode="r", encoding="utf-8") as fh:
            return str(fh.read())  # pyright: ignore[reportAttributeAccessIssue]
    except Exception as exc:
        raise ConfigError(f"Failed to fetch config from {uri!r}: {exc}") from exc


def _parse_cloud_file(uri: str, omega_conf: Any) -> Any:
    """Fetch and parse a YAML file from a cloud URI.

    Args:
        uri: Cloud storage URI.
        omega_conf: OmegaConf module.

    Returns:
        Parsed :class:`omegaconf.DictConfig`.

    Raises:
        ConfigError: On fetch failure or parse error.
    """
    content = _fetch_cloud_content(uri)
    try:
        return omega_conf.create(content)
    except Exception as exc:
        raise ConfigError(f"Failed to parse config from {uri!r}: {exc}") from exc


def _parse_local_file(path: str, omega_conf: Any) -> Any:
    """Parse a YAML file from the local filesystem.

    Args:
        path: Absolute or relative local path to the YAML file.
        omega_conf: OmegaConf module.

    Returns:
        Parsed :class:`omegaconf.DictConfig`.

    Raises:
        ConfigError: On missing file or parse error.
    """
    try:
        return omega_conf.load(path)
    except FileNotFoundError as exc:
        raise ConfigError(f"Configuration file not found: {path!r}") from exc
    except Exception as exc:
        raise ConfigError(f"Failed to parse configuration file {path!r}: {exc}") from exc


def _load_file(uri: str, omega_conf: Any, seen: set[str]) -> Any:
    """Load a YAML file from any scheme, resolving its ``includes`` recursively.

    Args:
        uri: Local path or cloud URI of the YAML file.
        omega_conf: OmegaConf module.
        seen: Canonical keys of the files already in the call stack, used to
            detect circular references.

    Returns:
        Merged :class:`omegaconf.DictConfig` for the file (includes merged in,
        ``includes`` key stripped from the result).

    Raises:
        ConfigError: On missing file, fetch failure, parse error, unmatched
            include, or circular include.
    """
    key = canonical_key(uri)
    if key in seen:
        raise ConfigError(f"Circular include detected: {uri!r} is already being loaded.")

    cfg = (
        _parse_cloud_file(uri, omega_conf)
        if is_cloud_uri(uri)
        else _parse_local_file(uri, omega_conf)
    )
    raw_includes = omega_conf.select(cfg, "includes", default=None)
    if raw_includes is None:
        return cfg

    entries = list(omega_conf.to_container(raw_includes, resolve=True))
    own = omega_conf.masked_copy(cfg, [k for k in cfg if k != "includes"])
    layers = _load_includes(key, entries, omega_conf, seen | {key})
    layers.append(own)
    return omega_conf.merge(*layers) if len(layers) > 1 else layers[0]


def _load_includes(
    declaring: str, entries: list[Any], omega_conf: Any, seen: set[str]
) -> list[Any]:
    """Load every file designated by the ``includes`` entries of one file.

    Args:
        declaring: Canonical key of the file declaring the entries.
        entries: Include entries with interpolations already resolved.
        omega_conf: OmegaConf module.
        seen: Canonical keys of the files in the call stack, including
            ``declaring``.

    Returns:
        One loaded layer per matched file, in include order and, within a
        glob, in lexicographic order of resolved path.

    Raises:
        ConfigError: Any loading error, suffixed with the declaring file.
    """
    layers: list[Any] = []
    for entry in entries:
        try:
            for match in expand_include(resolve_include(declaring, str(entry))):
                layers.append(_load_file(match, omega_conf, seen))
        except ConfigError as exc:
            raise ConfigError(f"{exc} (included from {declaring!r})") from exc
    return layers


def _register_resolvers(resolvers: Sequence[Any], omega_conf: Any) -> None:
    """Register custom resolvers with OmegaConf before parsing.

    Registration is idempotent: a resolver already registered under the same
    name is silently skipped, so multiple :func:`load_config` calls with the
    same resolvers are safe.

    Args:
        resolvers: Sequence of :class:`~loom.core.config.resolver.ConfigResolver`
            implementations.
        omega_conf: OmegaConf module.
    """
    for resolver in resolvers:
        with contextlib.suppress(Exception):
            omega_conf.register_new_resolver(resolver.name, resolver.resolve, use_cache=True)


def load_config(
    *config_files: str,
    resolvers: Sequence[ConfigResolver] = (),
) -> DictConfig:
    """Load and merge one or more YAML config files into a DictConfig.

    Accepts local filesystem paths and cloud storage URIs
    (``s3://``, ``gs://``, ``abfss://``, ``r2://`` …).  Cloud files are
    fetched via ``fsspec`` at call time.

    Files are merged **left-to-right**: values in later files override those
    in earlier ones.  ``${oc.env:VAR}`` interpolations are resolved by
    OmegaConf.  Custom ``resolvers`` are registered before parsing so their
    ``${name:key}`` placeholders resolve during the same pass.

    Any file may declare a top-level ``includes`` list to pull in
    additional files before its own values.  Entries are relative to the
    declaring file, may live on any scheme, and may be globs.  Circular
    includes and entries matching no file raise :class:`ConfigError`.

    The framework does not impose any shape on the resulting config — the
    user owns the structure entirely.  Use :func:`section` to extract typed
    sub-objects where desired.

    Args:
        *config_files: One or more local paths or cloud URIs.
        resolvers: Optional sequence of
            :class:`~loom.core.config.resolver.ConfigResolver` instances.
            Each resolver registers a ``${name:key}`` placeholder resolved
            at parse time (e.g. from AWS SSM or Azure Key Vault).

    Returns:
        Merged :class:`omegaconf.DictConfig` with interpolation support.

    Raises:
        ConfigError: If no files are provided, a file is not found, parsing
            fails, an include matches no file, a circular include is detected,
            omegaconf is not installed, or a cloud URI fetch fails.

    Example — single local file with inline includes::

        cfg = load_config("config.yaml")

    Example — explicit multi-file composition::

        cfg = load_config("config/base.yaml", "config/production.yaml")
        db_url = cfg.database.url

    Example — cloud URI::

        cfg = load_config("s3://my-bucket/config/prod.yaml")

    Example — with custom resolver::

        cfg = load_config("config/prod.yaml", resolvers=[SsmResolver("eu-west-1")])
    """
    if not config_files:
        raise ConfigError("load_config requires at least one config file.")

    omega_conf = _ensure_omegaconf()
    _register_resolvers(resolvers, omega_conf)

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
