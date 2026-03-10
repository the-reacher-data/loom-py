from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager


@dataclass(frozen=True)
class RepositoryRegistration:
    """Repository registration metadata for a model-bound SQLAlchemy repository."""

    model: type[BaseModel]
    repository_type: type[RepositorySQLAlchemy[Any, Any]]
    contract: object | None = None


@dataclass(frozen=True)
class RepositoryToken:
    """Internal DI token used for model-centric repository resolution."""

    model: type[BaseModel]


_REGISTRATIONS_BY_MODEL: dict[type[BaseModel], RepositoryRegistration] = {}
_REGISTRATIONS_BY_CONTRACT: dict[object, RepositoryRegistration] = {}


def repository_for(
    model: type[BaseModel],
    *,
    contract: object | None = None,
) -> Callable[[type[RepositorySQLAlchemy[Any, Any]]], type[RepositorySQLAlchemy[Any, Any]]]:
    """Register a SQLAlchemy repository implementation for ``model``.

    Args:
        model: Struct model handled by the repository.
        contract: Optional DI contract bound to the same repository instance.

    Raises:
        RuntimeError: If another repository is already registered for the same
            model or contract.
    """

    def decorator(
        repository_type: type[RepositorySQLAlchemy[Any, Any]],
    ) -> type[RepositorySQLAlchemy[Any, Any]]:
        existing = _REGISTRATIONS_BY_MODEL.get(model)
        if existing is not None and existing.repository_type is not repository_type:
            raise RuntimeError(
                f"Repository already registered for model {model.__qualname__}: "
                f"{existing.repository_type.__qualname__}"
            )
        if contract is not None:
            existing_contract = _REGISTRATIONS_BY_CONTRACT.get(contract)
            if existing_contract is not None and existing_contract.model is not model:
                raise RuntimeError(
                    f"Repository contract {contract!r} is already registered for "
                    f"model {existing_contract.model.__qualname__}"
                )
        registration = RepositoryRegistration(
            model=model,
            repository_type=repository_type,
            contract=contract,
        )
        _REGISTRATIONS_BY_MODEL[model] = registration
        if contract is not None:
            _REGISTRATIONS_BY_CONTRACT[contract] = registration
        return repository_type

    return decorator


def get_repository_registration(model: type[BaseModel]) -> RepositoryRegistration | None:
    """Return the custom repository registration for ``model`` when present."""

    return _REGISTRATIONS_BY_MODEL.get(model)


def build_repository_registration_module(
    session_manager: SessionManager,
    models: Sequence[type[BaseModel]],
) -> Callable[[LoomContainer], None]:
    """Build a DI module that registers model repositories and optional contracts.

    Custom repositories declared with :func:`repository_for` are preferred.
    Models without a custom registration use ``RepositorySQLAlchemy``.
    """

    repositories: dict[type[BaseModel], tuple[RepositorySQLAlchemy[Any, Any], object | None]] = {}
    for model in models:
        registration = get_repository_registration(model)
        repository_type = (
            registration.repository_type if registration is not None else RepositorySQLAlchemy
        )
        contract = registration.contract if registration is not None else None
        repositories[model] = (
            repository_type(session_manager=session_manager, model=model),
            contract,
        )

    def _provider_for(
        repository: RepositorySQLAlchemy[Any, Any],
    ) -> Callable[[], RepositorySQLAlchemy[Any, Any]]:
        def _provider() -> RepositorySQLAlchemy[Any, Any]:
            return repository

        return _provider

    def register(container: LoomContainer) -> None:
        for model, (repository, contract) in repositories.items():
            token = RepositoryToken(model)
            provider = _provider_for(repository)
            container.register(token, provider, scope=Scope.APPLICATION)
            container.register_repo(model, token)
            if contract is not None:
                container.register(contract, provider, scope=Scope.APPLICATION)
        container.register(SessionManager, lambda: session_manager, scope=Scope.APPLICATION)

    return register
