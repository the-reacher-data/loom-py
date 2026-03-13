from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from loom.core.model import LoomStruct

_LOOM_REPOSITORY_ATTR = "__loom_repository__"
_LOOM_CONTRACT_ATTR = "__loom_contract_for__"


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
    """Repository registration metadata for a logical Loom type.

    Stored directly on the model class as ``__loom_repository__`` so that
    no global mutable state is required. The ``contract`` field defines the
    DI binding key that use cases declare as their third generic parameter
    (``UseCase[TModel, TResult, TContract]``) to get typed access to custom
    repository methods without casts.
    """

    model: type[LoomStruct]
    repository_type: type[Any]
    contract: object | None = None
    builder: RepositoryBuilder | None = None


@dataclass(frozen=True)
class RepositoryToken:
    """Internal DI token used for main-repository resolution."""

    model: type[LoomStruct]


def repository_for(
    model: type[LoomStruct],
    *,
    contract: object | None = None,
    builder: RepositoryBuilder | None = None,
) -> Callable[[type[Any]], type[Any]]:
    """Register a repository implementation for a Loom logical type.

    Stores the registration as ``__loom_repository__`` on the model class —
    no global state is written.  The optional ``contract`` is the DI binding
    key that use cases reference as ``UseCase[TModel, TResult, TContract]``
    to get fully-typed access to custom repository methods without casts.

    Args:
        model: The Loom struct model this repository is bound to.
        contract: Optional DI key (Protocol / ABC) exposed to use cases.
        builder: Optional custom factory called with a
            :class:`RepositoryBuildContext` to construct the repository.

    Raises:
        RuntimeError: If another repository is already registered for the
            same model or the same contract is already bound to a different
            model.

    Example::

        class IProductRepository(Protocol):
            async def find_by_sku(self, sku: str) -> Product | None: ...

        @repository_for(Product, contract=IProductRepository)
        class ProductRepository(RepositorySQLAlchemy[Product, int]):
            async def find_by_sku(self, sku: str) -> Product | None:
                ...

        class GetProductUseCase(UseCase[Product, Product | None, IProductRepository]):
            async def execute(self, sku: str) -> Product | None:
                return await self.main_repo.find_by_sku(sku)
    """

    def decorator(repository_type: type[Any]) -> type[Any]:
        existing: RepositoryRegistration | None = getattr(model, _LOOM_REPOSITORY_ATTR, None)
        if existing is not None and existing.repository_type is not repository_type:
            raise RuntimeError(
                f"Repository already registered for model {model.__qualname__}: "
                f"{existing.repository_type.__qualname__}"
            )
        if contract is not None and isinstance(contract, type):
            bound_model = getattr(contract, _LOOM_CONTRACT_ATTR, None)
            if bound_model is not None and bound_model is not model:
                raise RuntimeError(
                    f"Contract {contract.__qualname__!r} is already bound to "
                    f"model {bound_model.__qualname__}"
                )
            contract.__loom_contract_for__ = model  # type: ignore[attr-defined]
        registration = RepositoryRegistration(
            model=model,
            repository_type=repository_type,
            contract=contract,
            builder=builder,
        )
        model.__loom_repository__ = registration  # type: ignore[attr-defined]
        return repository_type

    return decorator


def get_repository_registration(model: type[LoomStruct]) -> RepositoryRegistration | None:
    """Return the repository registration stored on ``model``, if any."""
    return getattr(model, _LOOM_REPOSITORY_ATTR, None)
