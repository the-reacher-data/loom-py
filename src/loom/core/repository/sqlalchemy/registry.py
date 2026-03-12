from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.model import BaseModel
from loom.core.repository import (
    build_repository_registration_module as build_main_repository_module,
)
from loom.core.repository.registry import (
    RepositoryRegistration,
    RepositoryToken,
    get_repository_registration,
    repository_for,
)
from loom.core.repository.sqlalchemy.repository import RepositorySQLAlchemy
from loom.core.repository.sqlalchemy.session_manager import SessionManager

__all__ = [
    "RepositoryRegistration",
    "RepositoryToken",
    "build_sqlalchemy_repository_registration_module",
    "get_repository_registration",
    "repository_for",
]


def build_sqlalchemy_repository_registration_module(
    session_manager: SessionManager,
    models: Sequence[type[BaseModel]],
    *,
    logical_models: Sequence[type[Any]] = (),
    default_repository_builder: Callable[[type[BaseModel], SessionManager], Any] | None = None,
) -> Callable[[LoomContainer], None]:
    """Build a DI module that registers model repositories and optional contracts."""

    def _default_builder(model: type[BaseModel]) -> Any:
        builder = default_repository_builder or (
            lambda current_model, current_session_manager: RepositorySQLAlchemy(
                session_manager=current_session_manager,
                model=current_model,
            )
        )
        return builder(model, session_manager)

    def _registered_builder(registration: RepositoryRegistration) -> Any:
        repository_type = registration.repository_type
        if not isinstance(repository_type, type):
            raise RuntimeError(
                "Repository registration for "
                f"{registration.model.__qualname__} must use a class type."
            )
        if issubclass(repository_type, RepositorySQLAlchemy):
            if not issubclass(registration.model, BaseModel):
                raise RuntimeError(
                    f"{repository_type.__qualname__} requires BaseModel-compatible registrations."
                )
            return repository_type(session_manager=session_manager, model=registration.model)
        return repository_type()

    register = build_main_repository_module(
        models=models,
        explicit_models=logical_models,
        build_registered_repository=_registered_builder,
        build_default_repository=_default_builder,
    )

    def _register_with_session(container: LoomContainer) -> None:
        register(container)
        if not container.is_registered(SessionManager):
            container.register(SessionManager, lambda: session_manager, scope=Scope.APPLICATION)

    return _register_with_session
