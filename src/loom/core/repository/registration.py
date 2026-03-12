from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel, LoomStruct
from loom.core.repository.registry import (
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
    list_repository_registrations,
)


def build_repository_registration_module(
    *,
    models: Sequence[type[BaseModel]],
    explicit_models: Sequence[type[LoomStruct]] = (),
    build_registered_repository: Callable[[RepositoryRegistration], Any],
    build_default_repository: Callable[[type[BaseModel]], Any],
) -> Callable[[LoomContainer], None]:
    """Build a container module that registers main repositories.

    Explicit registrations declared with ``repository_for(...)`` are preferred.
    Persistible ``BaseModel`` types without an explicit registration use the
    provided default repository builder.
    """

    repositories: dict[type[LoomStruct], tuple[Any, object | None, object]] = {}

    explicit_lookup = set(explicit_models) | set(models)

    for registration in list_repository_registrations():
        if registration.model not in explicit_lookup:
            continue
        binding_key = registration.contract or registration.repository_type
        repositories[registration.model] = (
            build_registered_repository(registration),
            registration.contract,
            binding_key,
        )

    for model in models:
        if get_repository_registration(model) is not None:
            continue
        token = RepositoryToken(model)
        repositories[model] = (
            build_default_repository(model),
            None,
            token,
        )

    def _provider_for(repository: Any) -> Callable[[], Any]:
        def _provider() -> Any:
            return repository

        return _provider

    def register(container: LoomContainer) -> None:
        for model, (repository, contract, binding_key) in repositories.items():
            provider = _provider_for(repository)
            if not container.is_registered(binding_key):
                container.register(binding_key, provider, scope=Scope.APPLICATION)
            container.register_repo(model, binding_key)
            if contract is not None and not container.is_registered(contract):
                container.register(contract, provider, scope=Scope.APPLICATION)

    return register
