from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel, LoomStruct
from loom.core.repository.registry import (
    DefaultRepositoryBuilder,
    RepositoryBuildContext,
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
)


@dataclass(frozen=True)
class _RepositoryBindingSpec:
    registration: RepositoryRegistration | None
    contract: object | None
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
    binding_key = registration.contract or registration.repository_type
    return _RepositoryBindingSpec(
        registration=registration,
        contract=registration.contract,
        binding_key=binding_key,
    )


def _default_repository_spec(model: type[LoomStruct]) -> _RepositoryBindingSpec:
    return _RepositoryBindingSpec(
        registration=None,
        contract=None,
        binding_key=RepositoryToken(model),
    )


def _build_repository_provider(
    *,
    container: LoomContainer,
    model: type[LoomStruct],
    registration: RepositoryRegistration | None,
    build_registered_repository: Callable[[RepositoryBuildContext, RepositoryRegistration], Any],
) -> Callable[[], Any]:
    context = RepositoryBuildContext(model=model)

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
                contract=spec.contract,
                binding_key=spec.binding_key,
                provider=provider,
            )

    return register
