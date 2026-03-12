from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, TypeAlias

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel, LoomStruct
from loom.core.repository.registry import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
    list_repository_registrations,
)

_RepositoryBindingSpec: TypeAlias = tuple[
    RepositoryRegistration | None,
    object | None,
    object,
]


def _build_repository_specs(
    *,
    models: Sequence[type[BaseModel]],
    explicit_models: Sequence[type[LoomStruct]],
) -> dict[type[LoomStruct], _RepositoryBindingSpec]:
    explicit_lookup = set(explicit_models) | set(models)
    repository_specs: dict[type[LoomStruct], _RepositoryBindingSpec] = {}

    for registration in list_repository_registrations():
        if registration.model not in explicit_lookup:
            continue
        repository_specs[registration.model] = _registered_repository_spec(registration)

    for model in models:
        if get_repository_registration(model) is not None:
            continue
        repository_specs[model] = _default_repository_spec(model)

    return repository_specs


def _registered_repository_spec(
    registration: RepositoryRegistration,
) -> _RepositoryBindingSpec:
    binding_key = registration.contract or registration.repository_type
    return (
        registration,
        registration.contract,
        binding_key,
    )


def _default_repository_spec(model: type[LoomStruct]) -> _RepositoryBindingSpec:
    return (
        None,
        None,
        RepositoryToken(model),
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
        return default_builder(context, model)

    return _provider


def _register_repository_binding(
    *,
    container: LoomContainer,
    model: type[LoomStruct],
    contract: object | None,
    binding_key: object,
    provider: Callable[[], Any],
) -> None:
    if not container.is_registered(binding_key):
        container.register(binding_key, provider, scope=Scope.APPLICATION)
    container.register_repo(model, binding_key)
    if contract is not None and not container.is_registered(contract):
        container.register(contract, provider, scope=Scope.APPLICATION)


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
    """
    repository_specs = _build_repository_specs(
        models=models,
        explicit_models=explicit_models,
    )

    def register(container: LoomContainer) -> None:
        for model, (registration, contract, binding_key) in repository_specs.items():
            provider = _build_repository_provider(
                container=container,
                model=model,
                registration=registration,
                build_registered_repository=build_registered_repository,
            )
            _register_repository_binding(
                container=container,
                model=model,
                contract=contract,
                binding_key=binding_key,
                provider=provider,
            )

    return register
