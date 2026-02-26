"""Unit tests for LoomContainer."""

from __future__ import annotations

import pytest

from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.di.scope import Scope

# ---------------------------------------------------------------------------
# Dummy interfaces and implementations
# ---------------------------------------------------------------------------


class IService:
    pass


class ConcreteService(IService):
    def __init__(self) -> None:
        self.created = True


class AnotherService:
    pass


class Product:
    pass


class ProductRepo:
    pass


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def test_register_class_provider() -> None:
    container = LoomContainer()
    container.register(IService, ConcreteService, scope=Scope.REQUEST)
    assert container.is_registered(IService)


def test_register_factory_provider() -> None:
    container = LoomContainer()
    container.register(IService, lambda: ConcreteService(), scope=Scope.REQUEST)
    assert container.is_registered(IService)


def test_register_overrides_previous() -> None:
    class AltService(IService):
        pass

    container = LoomContainer()
    container.register(IService, ConcreteService, scope=Scope.REQUEST)
    container.register(IService, AltService, scope=Scope.REQUEST)

    instance = container.resolve(IService)
    assert isinstance(instance, AltService)


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def test_resolve_request_scope_creates_new_instance() -> None:
    container = LoomContainer()
    container.register(IService, ConcreteService, scope=Scope.REQUEST)

    a = container.resolve(IService)
    b = container.resolve(IService)
    assert a is not b


def test_resolve_application_scope_returns_same_instance() -> None:
    container = LoomContainer()
    container.register(IService, ConcreteService, scope=Scope.APPLICATION)

    a = container.resolve(IService)
    b = container.resolve(IService)
    assert a is b


def test_resolve_transaction_scope_creates_new_instance() -> None:
    # TRANSACTION behaves like REQUEST in v1
    container = LoomContainer()
    container.register(IService, ConcreteService, scope=Scope.TRANSACTION)

    a = container.resolve(IService)
    b = container.resolve(IService)
    assert a is not b


def test_resolve_unregistered_raises() -> None:
    container = LoomContainer()
    with pytest.raises(ResolutionError, match="IService"):
        container.resolve(IService)


def test_resolve_lambda_provider() -> None:
    sentinel = ConcreteService()
    container = LoomContainer()
    container.register(IService, lambda: sentinel, scope=Scope.REQUEST)

    result = container.resolve(IService)
    assert result is sentinel


# ---------------------------------------------------------------------------
# Validate
# ---------------------------------------------------------------------------


def test_validate_instantiates_application_scope() -> None:
    container = LoomContainer()
    container.register(IService, ConcreteService, scope=Scope.APPLICATION)
    container.validate()  # must not raise
    # singleton must be alive after validate
    assert container.resolve(IService).created is True  # type: ignore[attr-defined]


def test_validate_does_not_instantiate_request_scope() -> None:
    call_count = 0

    def provider() -> ConcreteService:
        nonlocal call_count
        call_count += 1
        return ConcreteService()

    container = LoomContainer()
    container.register(IService, provider, scope=Scope.REQUEST)
    container.validate()
    assert call_count == 0


def test_validate_raises_on_failing_application_provider() -> None:
    def bad_provider() -> IService:
        raise RuntimeError("database unreachable")

    container = LoomContainer()
    container.register(IService, bad_provider, scope=Scope.APPLICATION)

    with pytest.raises(ResolutionError, match="IService"):
        container.validate()


def test_validate_multiple_application_bindings() -> None:
    container = LoomContainer()
    container.register(IService, ConcreteService, scope=Scope.APPLICATION)
    container.register(AnotherService, AnotherService, scope=Scope.APPLICATION)
    container.validate()  # both must succeed


# ---------------------------------------------------------------------------
# is_registered
# ---------------------------------------------------------------------------


def test_is_registered_true() -> None:
    container = LoomContainer()
    container.register(IService, ConcreteService)
    assert container.is_registered(IService) is True


def test_is_registered_false() -> None:
    container = LoomContainer()
    assert container.is_registered(IService) is False


def test_register_repo_and_resolve_repo() -> None:
    repo = ProductRepo()
    container = LoomContainer()
    container.register(ProductRepo, lambda: repo, scope=Scope.APPLICATION)
    container.register_repo(Product, ProductRepo)

    resolved = container.resolve_repo(Product)
    assert resolved is repo


def test_resolve_repo_missing_mapping_raises() -> None:
    container = LoomContainer()
    with pytest.raises(ResolutionError, match="Product"):
        container.resolve_repo(Product)
