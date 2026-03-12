from __future__ import annotations

from collections.abc import Callable, Sequence
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
    list_repository_registrations,
)


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

    repository_specs: dict[
        type[LoomStruct], tuple[RepositoryRegistration | None, object | None, object]
    ] = {}

    explicit_lookup = set(explicit_models) | set(models)

    for registration in list_repository_registrations():
        if registration.model not in explicit_lookup:
            continue
        binding_key = registration.contract or registration.repository_type
        repository_specs[registration.model] = (
            registration,
            registration.contract,
            binding_key,
        )

    for model in models:
        if get_repository_registration(model) is not None:
            continue
        token = RepositoryToken(model)
        repository_specs[model] = (
            None,
            None,
            token,
        )

    def register(container: LoomContainer) -> None:
        for model, (registration, contract, binding_key) in repository_specs.items():
            context = RepositoryBuildContext(model=model, container=container)

            def _provider(
                current_context: RepositoryBuildContext = context,
                current_model: type[LoomStruct] = model,
                current_registration: RepositoryRegistration | None = registration,
            ) -> Any:
                if current_registration is not None:
                    return build_registered_repository(current_context, current_registration)
                if not issubclass(current_model, BaseModel):
                    raise RuntimeError(
                        f"No default repository can be built for non-persistible type "
                        f"{current_model.__qualname__}"
                    )
                default_builder = container.resolve(DefaultRepositoryBuilder)
                return default_builder(current_context, current_model)

            if not container.is_registered(binding_key):
                container.register(binding_key, _provider, scope=Scope.APPLICATION)
            container.register_repo(model, binding_key)
            if contract is not None and not container.is_registered(contract):
                container.register(contract, _provider, scope=Scope.APPLICATION)

    return register
