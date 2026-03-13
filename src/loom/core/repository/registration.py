from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel, LoomStruct
from loom.core.repository.abc.repo_for import (
    Countable,
    Creatable,
    Deletable,
    Listable,
    Readable,
    Updatable,
)
from loom.core.repository.registry import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
)

_STANDARD_PROTOCOLS: frozenset[Any] = frozenset(
    {Readable, Creatable, Updatable, Deletable, Listable, Countable}
)


def _is_direct_protocol(cls: type) -> bool:
    """True when *cls* explicitly declares ``Protocol`` as a base.

    Uses the Python internal ``_is_protocol`` marker written on the class's
    own ``__dict__`` — not inherited — so concrete implementations of a
    Protocol are not mistaken for protocol declarations.
    """
    return bool(cls.__dict__.get("_is_protocol"))


def _detect_capability_keys(
    repository_type: type[Any],
    model: type[LoomStruct],
) -> tuple[Any, ...]:
    """Return the DI keys this repository should be registered under.

    Detects two categories:

    * **Standard -able generics** — ``Readable[Model]``, ``Creatable[Model]``,
      etc. — for every standard capability protocol found in the repository's
      MRO.
    * **Custom Protocol bases** — any class in ``repository_type.__orig_bases__``
      that is itself a Protocol (i.e. it carries ``_is_protocol = True`` in its
      own ``__dict__``) and is not one of the six standard capabilities.

    Args:
        repository_type: The concrete repository class to inspect.
        model: The Loom struct model the repository is bound to.

    Returns:
        Tuple of DI keys in stable order, with no duplicates.
    """
    keys: list[Any] = []
    mro_set = set(repository_type.__mro__)

    for proto in _STANDARD_PROTOCOLS:
        if proto in mro_set:
            keys.append(proto[model])

    for base in getattr(repository_type, "__orig_bases__", ()):
        origin: Any = getattr(base, "__origin__", base)
        if (
            isinstance(origin, type)
            and _is_direct_protocol(origin)
            and origin not in _STANDARD_PROTOCOLS
        ):
            keys.append(origin)

    return tuple(dict.fromkeys(keys))


def _default_capability_keys(model: type[LoomStruct]) -> tuple[Any, ...]:
    """Return standard capability keys for a default-built (RepositorySQLAlchemy) repo.

    Called only for ``BaseModel`` types that have no explicit registration.
    The default builder always produces a ``RepositorySQLAlchemy`` instance,
    which implements all six standard capabilities.
    """
    return tuple(proto[model] for proto in _STANDARD_PROTOCOLS)


@dataclass(frozen=True)
class _RepositoryBindingSpec:
    registration: RepositoryRegistration | None
    capability_keys: tuple[Any, ...]
    binding_key: object


def _build_repository_specs(
    *,
    models: Sequence[type[BaseModel]],
    explicit_models: Sequence[type[LoomStruct]],
) -> dict[type[LoomStruct], _RepositoryBindingSpec]:
    repository_specs: dict[type[LoomStruct], _RepositoryBindingSpec] = {}

    for model in set(explicit_models) | set(models):
        registration = get_repository_registration(model)
        if registration is not None:
            repository_specs[registration.model] = _registered_repository_spec(registration)

    for model in models:
        if model not in repository_specs:
            repository_specs[model] = _default_repository_spec(model)

    return repository_specs


def _registered_repository_spec(
    registration: RepositoryRegistration,
) -> _RepositoryBindingSpec:
    capability_keys = _detect_capability_keys(registration.repository_type, registration.model)
    binding_key = capability_keys[0] if capability_keys else registration.repository_type
    return _RepositoryBindingSpec(
        registration=registration,
        capability_keys=capability_keys,
        binding_key=binding_key,
    )


def _default_repository_spec(model: type[LoomStruct]) -> _RepositoryBindingSpec:
    return _RepositoryBindingSpec(
        registration=None,
        capability_keys=_default_capability_keys(model),
        binding_key=RepositoryToken(model),
    )


def _build_repository_provider(
    *,
    container: LoomContainer,
    model: type[LoomStruct],
    registration: RepositoryRegistration | None,
    build_registered_repository: Callable[[RepositoryBuildContext, RepositoryRegistration], Any],
) -> Callable[[], Any]:
    context = RepositoryBuildContext(model=model, container=container)

    def _provider() -> Any:
        if registration is not None:
            return build_registered_repository(context, registration)
        if not issubclass(model, BaseModel):
            raise RuntimeError(
                f"No default repository can be built for non-persistible type {model.__qualname__}"
            )
        default_builder = container.resolve(DefaultRepositoryBuilder)
        return default_builder(context)

    return _provider


def _register_repository_binding(
    *,
    container: LoomContainer,
    model: type[LoomStruct],
    capability_keys: tuple[Any, ...],
    binding_key: object,
    provider: Callable[[], Any],
) -> None:
    if not container.is_registered(binding_key):
        container.register(binding_key, provider, scope=Scope.APPLICATION)
    container.register_repo(model, binding_key)
    for key in capability_keys:
        if not container.is_registered(key):
            container.register(key, provider, scope=Scope.APPLICATION)


def build_repository_registration_module(
    *,
    models: Sequence[type[BaseModel]],
    explicit_models: Sequence[type[LoomStruct]] = (),
    build_registered_repository: Callable[[RepositoryBuildContext, RepositoryRegistration], Any],
) -> Callable[[LoomContainer], None]:
    """Build a container module that registers main repositories.

    Explicit registrations declared with ``repository_for(...)`` are preferred.
    Persistible ``BaseModel`` types without an explicit registration use the
    provided default repository builder.

    Capability DI keys (standard ``-able`` generics and custom Protocol bases)
    are auto-detected and registered alongside the primary binding key so that
    use cases can declare fine-grained capability dependencies without casts.
    """
    repository_specs = _build_repository_specs(
        models=models,
        explicit_models=explicit_models,
    )

    def register(container: LoomContainer) -> None:
        for model, spec in repository_specs.items():
            provider = _build_repository_provider(
                container=container,
                model=model,
                registration=spec.registration,
                build_registered_repository=build_registered_repository,
            )
            _register_repository_binding(
                container=container,
                model=model,
                capability_keys=spec.capability_keys,
                binding_key=spec.binding_key,
                provider=provider,
            )

    return register
