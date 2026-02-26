"""UseCase factory for declarative infrastructure injection.

Inspects ``UseCase.__init__`` type hints **once at startup** and caches the
dependency list.  At request time, it resolves each dependency from
:class:`~loom.core.di.container.LoomContainer` and constructs the instance —
zero per-request reflection.
"""

from __future__ import annotations

import inspect
import typing
from typing import Any, TypeVar

import msgspec

from loom.core.di.container import LoomContainer
from loom.core.repository.abc import RepoFor
from loom.core.use_case.use_case import UseCase

ResultT = TypeVar("ResultT")
DepToken = type[Any] | tuple[str, type[Any]]

# Sentinel params that are never treated as injectable dependencies.
_SKIP_PARAMS: frozenset[str] = frozenset({"self", "return"})


class UseCaseFactory:
    """Builds UseCase instances by resolving constructor dependencies from the container.

    The factory inspects ``UseCase.__init__`` type hints once (at startup or
    on first use for each class) and stores the resolved dependency list.
    Every subsequent :meth:`build` call resolves instances from the container
    without further reflection.

    Args:
        container: The :class:`~loom.core.di.container.LoomContainer` to
            resolve dependencies from.

    Example::

        class CreateOrderUseCase(UseCase[Order, OrderResponse]):
            def __init__(self, order_repo: OrderRepository) -> None:
                self._repo = order_repo
            ...

        factory = UseCaseFactory(container)
        use_case = factory.build(CreateOrderUseCase)
    """

    def __init__(self, container: LoomContainer) -> None:
        self._container = container
        # Maps use_case_type -> list of (param_name, resolved_token) pairs.
        # The token is either:
        # - a concrete dependency type (resolved via container.resolve)
        # - ("repo_for", model_type) for model-centric repo resolution
        self._dep_cache: dict[type[Any], list[tuple[str, DepToken]]] = {}

    def build(self, use_case_type: type[UseCase[Any, ResultT]]) -> UseCase[Any, ResultT]:
        """Construct a UseCase instance with dependencies injected from the container.

        Args:
            use_case_type: Concrete :class:`~loom.core.use_case.use_case.UseCase`
                subclass to instantiate.

        Returns:
            A fully constructed instance of ``use_case_type``.

        Raises:
            ~loom.core.di.container.ResolutionError: If a required dependency
                is not registered in the container.
        """
        deps = self._get_deps(use_case_type)
        kwargs: dict[str, Any] = {}
        for name, dep in deps:
            if isinstance(dep, tuple):
                _, model = dep
                kwargs[name] = self._container.resolve_repo(model)
                continue
            kwargs[name] = self._container.resolve(dep)
        return use_case_type(**kwargs)

    def register(self, use_case_type: type[UseCase[Any, Any]]) -> None:
        """Pre-warm the dependency cache for ``use_case_type`` at startup.

        Calling this during bootstrap ensures any missing bindings surface
        as :class:`~loom.core.di.container.ResolutionError` before the first
        request rather than at runtime.

        Args:
            use_case_type: UseCase subclass to inspect and cache.
        """
        self._get_deps(use_case_type)

    def _get_deps(
        self, use_case_type: type[Any]
    ) -> list[tuple[str, DepToken]]:
        """Return the cached (or freshly computed) dependency list for a UseCase.

        Inspection uses ``typing.get_type_hints`` so ``from __future__ import
        annotations`` is handled transparently.

        Args:
            use_case_type: UseCase subclass to inspect.

        Returns:
            List of ``(param_name, resolved_type)`` pairs from ``__init__``.
        """
        if use_case_type in self._dep_cache:
            return self._dep_cache[use_case_type]

        init = use_case_type.__init__
        if init is UseCase.__init__:
            main_model = self._infer_main_model(use_case_type)
            if main_model is None:
                self._dep_cache[use_case_type] = []
                return []
            deps: list[tuple[str, DepToken]] = [("main_repo", ("repo_for", main_model))]
            self._dep_cache[use_case_type] = deps
            return deps

        # UseCase subclasses that don't declare __init__ inherit object.__init__
        if init is object.__init__:
            self._dep_cache[use_case_type] = []
            return []

        try:
            hints = typing.get_type_hints(init)
        except Exception:
            # Fallback: use inspect for non-resolvable annotations
            hints = {
                name: param.annotation
                for name, param in inspect.signature(init).parameters.items()
                if param.annotation is not inspect.Parameter.empty
            }

        deps: list[tuple[str, DepToken]] = []
        for name, typ in hints.items():
            if name in _SKIP_PARAMS:
                continue
            if isinstance(typ, type):
                deps.append((name, typ))
                continue
            repo_model = self._extract_repo_model(typ)
            if repo_model is not None:
                deps.append((name, ("repo_for", repo_model)))
        self._dep_cache[use_case_type] = deps
        return deps

    def _extract_repo_model(self, annotation: object) -> type[Any] | None:
        origin = typing.get_origin(annotation)
        if origin is None:
            return None
        if origin is not RepoFor:
            return None
        args = typing.get_args(annotation)
        if len(args) != 1 or not isinstance(args[0], type):
            return None
        return args[0]

    def _infer_main_model(self, use_case_type: type[Any]) -> type[Any] | None:
        orig_bases = getattr(use_case_type, "__orig_bases__", ())
        for base in orig_bases:
            origin = typing.get_origin(base)
            if origin is not UseCase:
                continue
            args = typing.get_args(base)
            if len(args) != 2:
                raise TypeError(
                    f"{use_case_type.__qualname__} must declare UseCase[TModel, TResult]. "
                    f"Got {len(args)} generic parameter(s)."
                )
            candidate = args[0]
            if candidate is typing.Any:
                return None  # UseCase[Any, Result]
            if not isinstance(candidate, type) or not issubclass(candidate, msgspec.Struct):
                raise TypeError(
                    f"{use_case_type.__qualname__}: TModel must be a msgspec.Struct subclass."
                )
            return candidate
        return None
