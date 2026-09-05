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

Keyed collections
-----------------
``keyed=`` names dotted paths whose mapping form merges by key across the
files of an ``includes`` composition.  A key declared by two files of the
same composition, or a list form mixed with a mapping form, raises
:class:`ConfigError` naming the files involved.  Explicit layers passed to
:func:`load_config` override keys as usual; only the form mismatch is
reported across them::

    cfg = load_config("config/app.yaml", keyed=("storage.tables",))

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

Provenance = dict[str, dict[str, str]]
"""Keyed path -> key -> file declaring that key."""

_Layer = tuple[str, Any, Provenance]


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


def _load_file(uri: str, omega_conf: Any, seen: set[str], keyed: Sequence[str]) -> _Layer:
    """Load a YAML file from any scheme, resolving its ``includes`` recursively.

    Args:
        uri: Local path or cloud URI of the YAML file.
        omega_conf: OmegaConf module.
        seen: Canonical keys of the files already in the call stack, used to
            detect circular references.
        keyed: Dotted paths of the collections merged by key.

    Returns:
        The file URI, its merged :class:`omegaconf.DictConfig` (includes
        merged in, ``includes`` key stripped) and the provenance of every
        keyed entry reachable from it.

    Raises:
        ConfigError: On missing file, fetch failure, parse error, unmatched
            include, circular include, duplicate keyed entry, or mixed
            list and mapping forms of a keyed collection.
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
        return uri, cfg, _own_provenance(uri, cfg, keyed, omega_conf)

    entries = list(omega_conf.to_container(raw_includes, resolve=True))
    own = omega_conf.masked_copy(cfg, [k for k in cfg if k != "includes"])
    layers = _load_includes(key, entries, omega_conf, seen | {key}, keyed)
    layers.append((uri, own, _own_provenance(uri, own, keyed, omega_conf)))
    provenance = _check_keyed(layers, keyed, omega_conf)
    return uri, _merge_layers(layers, omega_conf), provenance


def _load_includes(
    declaring: str, entries: list[Any], omega_conf: Any, seen: set[str], keyed: Sequence[str]
) -> list[_Layer]:
    """Load every file designated by the ``includes`` entries of one file.

    Args:
        declaring: Canonical key of the file declaring the entries.
        entries: Include entries with interpolations already resolved.
        omega_conf: OmegaConf module.
        seen: Canonical keys of the files in the call stack, including
            ``declaring``.
        keyed: Dotted paths of the collections merged by key.

    Returns:
        One loaded layer per matched file, in include order and, within a
        glob, in lexicographic order of resolved path.

    Raises:
        ConfigError: Any loading error, suffixed with the declaring file.
    """
    layers: list[_Layer] = []
    for entry in entries:
        try:
            for match in expand_include(resolve_include(declaring, str(entry))):
                layers.append(_load_file(match, omega_conf, seen, keyed))
        except ConfigError as exc:
            raise ConfigError(f"{exc} (included from {declaring!r})") from exc
    return layers


def _merge_layers(layers: Sequence[_Layer], omega_conf: Any) -> Any:
    """Merge the configs of ``layers`` left-to-right.

    Args:
        layers: Loaded layers in precedence order, later overriding earlier.
        omega_conf: OmegaConf module.

    Returns:
        The merged :class:`omegaconf.DictConfig`.
    """
    configs = [cfg for _, cfg, _ in layers]
    return omega_conf.merge(*configs) if len(configs) > 1 else configs[0]


def _own_provenance(uri: str, cfg: Any, keyed: Sequence[str], omega_conf: Any) -> Provenance:
    """Record ``uri`` as the declaring file of every keyed entry ``cfg`` holds.

    Args:
        uri: File whose own keys are recorded.
        cfg: Config of that file without its includes.
        keyed: Dotted paths of the collections merged by key.
        omega_conf: OmegaConf module.

    Returns:
        Provenance covering the keyed paths present as mappings in ``cfg``.
    """
    provenance: Provenance = {}
    for path in keyed:
        node = _select_keyed(uri, cfg, path, omega_conf)
        if omega_conf.is_dict(node):
            provenance[path] = dict.fromkeys((str(k) for k in node), uri)
    return provenance


def _check_keyed(layers: Sequence[_Layer], keyed: Sequence[str], omega_conf: Any) -> Provenance:
    """Validate the keyed collections across the layers of one composition.

    Args:
        layers: Included layers in order, followed by the declaring file.
        keyed: Dotted paths of the collections merged by key.
        omega_conf: OmegaConf module.

    Returns:
        The union of the layers' provenance.

    Raises:
        ConfigError: When a key is declared by two files, or the collection
            is a list in one layer and a mapping in another.
    """
    provenance: Provenance = {}
    for path in keyed:
        _check_form(path, layers, omega_conf)
        provenance[path] = _check_duplicates(path, layers)
    return provenance


def _check_form(path: str, layers: Sequence[_Layer], omega_conf: Any) -> None:
    """Reject ``path`` being a list in one layer and a mapping in another.

    Args:
        path: Dotted path of the keyed collection.
        layers: Layers to inspect.
        omega_conf: OmegaConf module.

    Raises:
        ConfigError: Naming the first file using each form.
    """
    list_file: str | None = None
    mapping_file: str | None = None
    for uri, cfg, _ in layers:
        node = _select_keyed(uri, cfg, path, omega_conf)
        if list_file is None and omega_conf.is_list(node):
            list_file = uri
        if mapping_file is None and omega_conf.is_dict(node):
            mapping_file = uri
    if list_file is None or mapping_file is None:
        return
    raise ConfigError(
        f"{path} is a list in {list_file!r} and a mapping in {mapping_file!r}; "
        "use one form within a composition"
    )


def _select_keyed(uri: str, cfg: Any, path: str, omega_conf: Any) -> Any:
    """Select the node at keyed ``path`` in ``cfg``, or ``None`` when absent.

    Args:
        uri: File ``cfg`` was loaded from.
        cfg: Config to inspect.
        path: Dotted path of the keyed collection.
        omega_conf: OmegaConf module.

    Returns:
        The selected node, or ``None`` when the path is absent.

    Raises:
        ConfigError: When the path cannot be selected, e.g. its parent is not
            a container or the node is an unresolvable interpolation.
    """
    # Lazy import: this module keeps omegaconf behind ``_ensure_omegaconf``.
    from omegaconf.errors import OmegaConfBaseException

    try:
        return omega_conf.select(cfg, path, default=None)
    except OmegaConfBaseException as exc:
        raise ConfigError(f"Cannot read keyed collection {path!r} in {uri!r}: {exc}") from exc


def _check_duplicates(path: str, layers: Sequence[_Layer]) -> dict[str, str]:
    """Reject a key of ``path`` declared by two files across ``layers``.

    Args:
        path: Dotted path of the keyed collection.
        layers: Layers whose provenance is merged.

    Returns:
        Key to declaring file across every layer.

    Raises:
        ConfigError: Naming the key and both declaring files.
    """
    declared: dict[str, str] = {}
    for _, _, provenance in layers:
        for key, declaring in provenance.get(path, {}).items():
            if key in declared:
                raise ConfigError(
                    f"{path}[{key!r}] is declared in {declared[key]!r} and in {declaring!r}"
                )
            declared[key] = declaring
    return declared


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
    keyed: Sequence[str] = (),
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

    ``keyed`` names collections whose mapping form merges by key within an
    ``includes`` composition: a key declared by two files of the same
    composition raises :class:`ConfigError`, as does mixing a list form
    with a mapping form.  Explicit ``config_files`` layers override keys
    left-to-right; only the form mismatch is reported across them.

    The framework does not impose any shape on the resulting config — the
    user owns the structure entirely.  Use :func:`section` to extract typed
    sub-objects where desired.

    Args:
        *config_files: One or more local paths or cloud URIs.
        resolvers: Optional sequence of
            :class:`~loom.core.config.resolver.ConfigResolver` instances.
            Each resolver registers a ``${name:key}`` placeholder resolved
            at parse time (e.g. from AWS SSM or Azure Key Vault).
        keyed: Dotted paths (e.g. ``"storage.tables"``) of the collections
            merged by key.

    Returns:
        Merged :class:`omegaconf.DictConfig` with interpolation support.

    Raises:
        ConfigError: If no files are provided, a file is not found, parsing
            fails, an include matches no file, a circular include is detected,
            a keyed collection holds a duplicate key or mixes list and mapping
            forms, omegaconf is not installed, or a cloud URI fetch fails.

    Example — single local file with inline includes::

        cfg = load_config("config.yaml")

    Example — explicit multi-file composition::

        cfg = load_config("config/base.yaml", "config/production.yaml")
        db_url = cfg.database.url

    Example — cloud URI::

        cfg = load_config("s3://my-bucket/config/prod.yaml")

    Example — with custom resolver::

        cfg = load_config("config/prod.yaml", resolvers=[SsmResolver("eu-west-1")])

    Example — keyed collections::

        cfg = load_config("config/app.yaml", keyed=("storage.tables", "storage.files"))
    """
    if not config_files:
        raise ConfigError("load_config requires at least one config file.")

    omega_conf = _ensure_omegaconf()
    _register_resolvers(resolvers, omega_conf)

    layers = [_load_file(path, omega_conf, set(), keyed) for path in config_files]
    for path in keyed:
        _check_form(path, layers, omega_conf)
    return _merge_layers(layers, omega_conf)  # type: ignore[no-any-return]


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
