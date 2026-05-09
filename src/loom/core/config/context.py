"""Runtime config accessor with typed extraction and binding resolution."""

from __future__ import annotations

from typing import TYPE_CHECKING, TypeVar

from loom.core.config.binder import StructBinder
from loom.core.config.configurable import ConfigBinding
from loom.core.config.loader import section as _section

if TYPE_CHECKING:
    from omegaconf import DictConfig

T = TypeVar("T")


class ConfigContext:
    """Runtime config accessor backed by a resolved DictConfig.

    Combines typed section extraction with constructor injection so that
    runner and bootstrap code has a single, consistent entry-point for
    reading config — no scattered ``section()`` calls or bespoke binding
    loops across layers.

    Args:
        config: Resolved OmegaConf config (from :func:`~loom.core.config.load_config`).
        binder: Optional :class:`StructBinder` instance. Defaults to a standard
            non-strict binder. Pass ``StructBinder(strict=True)`` when coercion
            must be disallowed.

    Example::

        ctx = ConfigContext(load_config("config.yaml"))
        db  = ctx.section("database", DatabaseConfig)
        step = ctx.bind(ReadOrdersStep, path="pipeline.steps.orders")
        dep  = ctx.resolve(MyDep.from_config("services.dep", label="prod"))
    """

    __slots__ = ("_binder", "_config")

    def __init__(self, config: DictConfig, *, binder: StructBinder | None = None) -> None:
        self._config = config
        self._binder = binder or StructBinder()

    def section(self, key: str, target: type[T]) -> T:
        """Extract and validate a typed config section by dot-path.

        Args:
            key: Dot-separated path (e.g. ``"database"`` or ``"services.cache"``).
            target: Type to convert the section into.

        Returns:
            Validated instance of *target*.

        Raises:
            ConfigError: If the key is absent or the section fails validation.
        """
        return _section(self._config, key, target)

    def bind(self, target: type[T], *, path: str = "", **overrides: object) -> T:
        """Instantiate *target* from a config section merged with *overrides*.

        *overrides* take precedence over YAML values. Omit *path* to rely
        entirely on *overrides* (useful when no YAML section exists).

        Args:
            target: Class to instantiate.
            path: Dot-separated config path. Omit or pass ``""`` for override-only bindings.
            **overrides: Explicit keyword values applied after YAML values.

        Returns:
            Fully constructed instance of *target*.

        Raises:
            ConfigError: If a required field is missing or type conversion fails.
        """
        raw: dict[str, object] = _section(self._config, path, dict) if path else {}
        return self._binder.bind(target, {**raw, **overrides})

    def resolve(self, binding: ConfigBinding) -> object:
        """Materialize a :class:`~loom.core.config.ConfigBinding` into a live object.

        Delegates to :meth:`bind` using the binding's declared path and overrides.
        Compilers and bootstrap code that operate on pre-declared bindings use
        this method rather than calling :meth:`bind` with unpacked fields.

        Args:
            binding: Deferred binding declaration (created via
                :meth:`~loom.core.config.Configurable.from_config` or
                :meth:`~loom.core.config.Configurable.configure`).

        Returns:
            Instantiated ``binding.target``.

        Raises:
            ConfigError: If resolution or type conversion fails.
        """
        return self.bind(binding.target, path=binding.config_path, **binding.overrides)


__all__ = ["ConfigContext"]
