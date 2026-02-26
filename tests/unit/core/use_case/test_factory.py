"""Unit tests for UseCaseFactory."""

from __future__ import annotations

from typing import Any, cast

import msgspec
import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope
from loom.core.repository.abc import RepoFor
from loom.core.use_case.factory import UseCaseFactory
from loom.core.use_case.use_case import UseCase

# ---------------------------------------------------------------------------
# Dummy domain objects
# ---------------------------------------------------------------------------


class IOrderRepo:
    pass


class FakeOrderRepo(IOrderRepo):
    pass


class IEmailService:
    pass


class FakeEmailService(IEmailService):
    pass


class Product(msgspec.Struct):
    id: int
    name: str


class FakeProductRepo:
    async def get_by_id(self, obj_id: Any, profile: str = "default") -> Product | None:
        return Product(id=int(obj_id), name="p")

    async def list_paginated(self, *args: Any, **kwargs: Any) -> Any:
        return None

    async def create(self, data: msgspec.Struct) -> Product:
        return Product(id=1, name="p")

    async def update(self, obj_id: Any, data: msgspec.Struct) -> Product | None:
        return Product(id=int(obj_id), name="p")

    async def delete(self, obj_id: Any) -> bool:
        return True


# ---------------------------------------------------------------------------
# UseCase fixtures
# ---------------------------------------------------------------------------


class NoDepsUseCase(UseCase[Any, str]):
    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


class SingleDepUseCase(UseCase[Any, str]):
    def __init__(self, repo: IOrderRepo) -> None:
        self._repo = repo

    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


class MultiDepUseCase(UseCase[Any, str]):
    def __init__(self, repo: IOrderRepo, email: IEmailService) -> None:
        self._repo = repo
        self._email = email

    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


class MainRepoUseCase(UseCase[Product, str]):
    def __init__(self, main_repo: RepoFor[Product]) -> None:
        super().__init__(main_repo)

    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


class MixedRepoUseCase(UseCase[Product, str]):
    def __init__(self, main_repo: RepoFor[Product], email: IEmailService) -> None:
        super().__init__(main_repo)
        self._email = email

    async def execute(self, **kwargs: Any) -> str:  # type: ignore[override]
        return "ok"


class AutoMainRepoUseCase(UseCase[Product, Product]):
    async def execute(self, **kwargs: Any) -> Product:
        return Product(id=1, name="p")


class AutoMainRepoExplicitModelUseCase(UseCase[Product, bool]):

    async def execute(self, **kwargs: Any) -> bool:
        return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _container_with(*pairs: tuple[type, object]) -> LoomContainer:
    c = LoomContainer()
    for iface, impl in pairs:
        obj = impl
        c.register(iface, lambda o=obj: o, scope=Scope.REQUEST)
    return c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_build_no_deps() -> None:
    container = LoomContainer()
    factory = UseCaseFactory(container)
    uc = factory.build(NoDepsUseCase)
    assert isinstance(uc, NoDepsUseCase)


def test_build_single_dep() -> None:
    repo = FakeOrderRepo()
    container = _container_with((IOrderRepo, repo))
    factory = UseCaseFactory(container)

    uc = factory.build(SingleDepUseCase)
    assert isinstance(uc, SingleDepUseCase)
    assert uc._repo is repo  # type: ignore[attr-defined]


def test_build_multi_deps() -> None:
    repo = FakeOrderRepo()
    email = FakeEmailService()
    container = _container_with((IOrderRepo, repo), (IEmailService, email))
    factory = UseCaseFactory(container)

    uc = factory.build(MultiDepUseCase)
    assert uc._repo is repo  # type: ignore[attr-defined]
    assert uc._email is email  # type: ignore[attr-defined]


def test_build_creates_new_instance_each_call() -> None:
    container = _container_with((IOrderRepo, FakeOrderRepo()))
    factory = UseCaseFactory(container)

    a = factory.build(SingleDepUseCase)
    b = factory.build(SingleDepUseCase)
    assert a is not b


def test_build_missing_dep_raises() -> None:
    container = LoomContainer()  # no bindings
    factory = UseCaseFactory(container)

    with pytest.raises(ResolutionError, match="IOrderRepo"):
        factory.build(SingleDepUseCase)


def test_dep_cache_populated_on_register() -> None:
    container = _container_with((IOrderRepo, FakeOrderRepo()))
    factory = UseCaseFactory(container)

    factory.register(SingleDepUseCase)
    assert SingleDepUseCase in factory._dep_cache


def test_dep_cache_populated_after_first_build() -> None:
    container = _container_with((IOrderRepo, FakeOrderRepo()))
    factory = UseCaseFactory(container)

    assert SingleDepUseCase not in factory._dep_cache
    factory.build(SingleDepUseCase)
    assert SingleDepUseCase in factory._dep_cache
    # Same list object returned on second call
    deps_first = factory._dep_cache[SingleDepUseCase]
    factory.build(SingleDepUseCase)
    assert factory._dep_cache[SingleDepUseCase] is deps_first


def test_register_raises_if_dep_not_in_container() -> None:
    """register() itself doesn't raise — only build() does. Validate separately."""
    container = LoomContainer()
    factory = UseCaseFactory(container)
    # register() only caches dep list, doesn't resolve
    factory.register(SingleDepUseCase)
    # Now build should raise
    with pytest.raises(ResolutionError):
        factory.build(SingleDepUseCase)


def test_build_resolves_repo_for_model() -> None:
    repo = FakeProductRepo()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(Product, FakeProductRepo)

    factory = UseCaseFactory(container)
    uc = factory.build(MainRepoUseCase)
    assert uc.main_repo is repo


def test_build_resolves_repo_for_model_and_other_deps() -> None:
    repo = FakeProductRepo()
    email = FakeEmailService()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register(IEmailService, lambda: email, scope=Scope.REQUEST)
    container.register_repo(Product, FakeProductRepo)

    factory = UseCaseFactory(container)
    uc = cast(MixedRepoUseCase, factory.build(MixedRepoUseCase))
    assert uc.main_repo is repo
    assert uc._email is email


def test_build_auto_infers_main_repo_from_use_case_generic() -> None:
    repo = FakeProductRepo()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(Product, FakeProductRepo)

    factory = UseCaseFactory(container)
    uc = factory.build(AutoMainRepoUseCase)
    assert uc.main_repo is repo


def test_build_auto_uses_first_generic_as_main_model_even_when_result_is_not_model() -> None:
    repo = FakeProductRepo()
    container = LoomContainer()
    container.register(FakeProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(Product, FakeProductRepo)

    factory = UseCaseFactory(container)
    uc = factory.build(AutoMainRepoExplicitModelUseCase)
    assert uc.main_repo is repo
