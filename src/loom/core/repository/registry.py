from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from loom.core.model import LoomStruct


@dataclass(frozen=True)
class RepositoryBuildContext:
    """Context available to repository builders.

    Carries only the model being built. Infrastructure dependencies are
    injected into builders at construction time by the DI module — they
    must not be resolved here at call time.
    """

    model: type[LoomStruct]


RepositoryBuilder = Callable[[RepositoryBuildContext], Any]


class DefaultRepositoryBuilder(Protocol):
    """Global fallback strategy for building repositories.

    Implement this protocol to swap the default persistence backend for all
    models that have no explicit ``repository_for`` registration.  The
    implementation receives infrastructure dependencies via its constructor
    (injected by the DI module), not via the context.
    """

    def __call__(self, context: RepositoryBuildContext) -> Any: ...


@dataclass(frozen=True)
class RepositoryRegistration:
    """Repository registration metadata for a logical Loom type."""

    model: type[LoomStruct]
    repository_type: type[Any]
    contract: object | None = None
    builder: RepositoryBuilder | None = None


@dataclass(frozen=True)
class RepositoryToken:
    """Internal DI token used for main-repository resolution."""

    model: type[LoomStruct]


_REGISTRATIONS_BY_MODEL: dict[type[LoomStruct], RepositoryRegistration] = {}
_REGISTRATIONS_BY_CONTRACT: dict[object, RepositoryRegistration] = {}


def repository_for(
    model: type[LoomStruct],
    *,
    contract: object | None = None,
    builder: RepositoryBuilder | None = None,
) -> Callable[[type[Any]], type[Any]]:
    """Register a repository implementation for a Loom logical type."""

    def decorator(repository_type: type[Any]) -> type[Any]:
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
            builder=builder,
        )
        _REGISTRATIONS_BY_MODEL[model] = registration
        if contract is not None:
            _REGISTRATIONS_BY_CONTRACT[contract] = registration
        return repository_type

    return decorator


def get_repository_registration(model: type[LoomStruct]) -> RepositoryRegistration | None:
    """Return the registered repository for ``model`` when present."""

    return _REGISTRATIONS_BY_MODEL.get(model)


def list_repository_registrations() -> tuple[RepositoryRegistration, ...]:
    """Return all registered repositories."""

    return tuple(_REGISTRATIONS_BY_MODEL.values())
