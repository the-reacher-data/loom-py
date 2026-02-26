"""Minimal dependency injection container.

Provides type-based registration and resolution with application and request
scopes.  The resolution graph is validated at startup (via :meth:`LoomContainer.validate`)
so missing bindings are caught before the first request.

No reflection occurs after :meth:`LoomContainer.validate` — all providers
are resolved eagerly for ``APPLICATION`` scope during validation.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar

from loom.core.di.scope import Scope

T = TypeVar("T")


class ResolutionError(Exception):
    """Raised when a dependency cannot be resolved from the container.

    Args:
        message: Human-readable description of the resolution failure.

    Example::

        raise ResolutionError("No binding registered for: UserRepository")
    """


@dataclass
class _Binding:
    """Internal binding record."""

    provider: Callable[[], Any]
    scope: Scope
    _instance: Any | None = field(default=None, repr=False)


class LoomContainer:
    """Lightweight type-based dependency injection container.

    Dependencies are registered at startup and resolved at runtime without
    dynamic reflection per request.  Application-scope instances are
    singletons; request-scope instances are created fresh per
    :meth:`resolve` call.

    Example::

        container = LoomContainer()
        container.register(UserRepository, SQLAlchemyUserRepository, scope=Scope.REQUEST)
        container.register(AppConfig, lambda: cfg, scope=Scope.APPLICATION)
        container.validate()

        repo = container.resolve(UserRepository)
    """

    def __init__(self) -> None:
        self._bindings: dict[type[Any], _Binding] = {}
        self._repo_bindings: dict[type[Any], type[Any]] = {}

    def register(
        self,
        interface: type[T],
        provider: Callable[[], T] | type[T],
        scope: Scope = Scope.REQUEST,
    ) -> None:
        """Register a provider for ``interface``.

        Args:
            interface: The abstract type (protocol, ABC, or concrete class)
                used as the resolution key.
            provider: A zero-argument callable that returns an instance of
                ``interface``, or a concrete class to instantiate directly.
            scope: Lifetime scope.  Defaults to :attr:`~Scope.REQUEST`.

        Example::

            container.register(UserRepository, SQLAlchemyUserRepository)
            container.register(AppConfig, lambda: load_config(MyCfg), scope=Scope.APPLICATION)
        """
        factory: Callable[[], T] = provider if not isinstance(provider, type) else provider
        self._bindings[interface] = _Binding(provider=factory, scope=scope)

    def resolve(self, interface: type[T]) -> T:
        """Return an instance bound to ``interface``.

        For ``APPLICATION`` scope, the same instance is returned on every call.
        For ``REQUEST`` (and ``TRANSACTION``) scope, a new instance is created
        each time.

        Args:
            interface: The type to resolve.

        Returns:
            An instance satisfying ``interface``.

        Raises:
            ResolutionError: If no binding is registered for ``interface``.
        """
        binding = self._bindings.get(interface)
        if binding is None:
            raise ResolutionError(
                f"No binding registered for: {interface.__qualname__}"
            )
        if binding.scope is Scope.APPLICATION:
            if binding._instance is None:
                binding._instance = binding.provider()
            return binding._instance  # type: ignore[no-any-return]
        return binding.provider()  # type: ignore[no-any-return]

    def validate(self) -> None:
        """Eagerly validate and instantiate all ``APPLICATION`` scope bindings.

        Ensures that application-scope singletons can be created without error
        at startup (fail-fast).  Request-scope bindings are verified to have a
        registered provider but are not instantiated.

        Raises:
            ResolutionError: If an ``APPLICATION`` scope binding fails to
                instantiate.
        """
        for interface, binding in self._bindings.items():
            if binding.scope is Scope.APPLICATION:
                try:
                    self.resolve(interface)
                except ResolutionError:
                    raise
                except Exception as exc:
                    raise ResolutionError(
                        f"Failed to instantiate APPLICATION-scope binding "
                        f"for {interface.__qualname__}: {exc}"
                    ) from exc

    def is_registered(self, interface: type[Any]) -> bool:
        """Return ``True`` if ``interface`` has a registered binding.

        Args:
            interface: The type to check.
        """
        return interface in self._bindings

    def register_repo(self, model: type[Any], interface: type[Any]) -> None:
        """Register a model-to-repository interface mapping.

        Args:
            model: Domain/struct model type.
            interface: DI interface key previously bound via :meth:`register`.
        """
        self._repo_bindings[model] = interface

    def resolve_repo(self, model: type[Any]) -> Any:
        """Resolve a repository instance by model type.

        Args:
            model: Domain/struct model type used as lookup key.

        Returns:
            Repository instance registered for ``model``.

        Raises:
            ResolutionError: If no repository mapping exists for ``model``.
        """
        interface = self._repo_bindings.get(model)
        if interface is None:
            raise ResolutionError(
                f"No repository mapping registered for model: {model.__qualname__}"
            )
        return self.resolve(interface)

    def has_repo_mapping(self, model: type[Any]) -> bool:
        """Return ``True`` when a model-to-repository mapping exists."""
        return model in self._repo_bindings
