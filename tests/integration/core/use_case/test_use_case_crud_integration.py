from __future__ import annotations

from collections.abc import Callable

import pytest
from pytest import mark

from loom.core.bootstrap.bootstrap import bootstrap_app
from loom.core.command import Command, Patch
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.executor import RuntimeExecutor
from loom.core.errors import RuleViolations
from loom.core.use_case import Compute, F, Input, Rule
from loom.core.use_case.use_case import UseCase
from loom.testing import RepositoryIntegrationHarness
from tests.integration.fake_repo.product.model import Product


class _Cfg:
    env: str = "test"


class CreateProductCmd(Command, frozen=True):
    name: str
    price: float


class UpdateProductCmd(Command, frozen=True):
    name: Patch[str] = None
    price: Patch[float] = None


class CreateProductUseCase(UseCase[Product, Product]):

    async def execute(self, cmd: CreateProductCmd = Input()) -> Product:
        return await self.main_repo.create(cmd)


class GetProductUseCase(UseCase[Product, Product | None]):

    async def execute(self, product_id: int) -> Product | None:
        return await self.main_repo.get_by_id(product_id)


class UpdateProductUseCase(UseCase[Product, Product | None]):

    async def execute(
        self,
        product_id: int,
        cmd: UpdateProductCmd = Input(),
    ) -> Product | None:
        return await self.main_repo.update(product_id, cmd)


class DeleteProductUseCase(UseCase[Product, bool]):

    async def execute(self, product_id: int) -> bool:
        return await self.main_repo.delete(product_id)


class CreateProductWithRulesCmd(Command, frozen=True):
    name: str
    price: float


def _normalize_product_name(name: str) -> str:
    return name.strip().upper()


def _price_must_be_positive(price: float) -> str | None:
    if price <= 0:
        return "price must be positive"
    return None


NORMALIZE_NAME = Compute.set(F(CreateProductWithRulesCmd).name).from_fields(
    F(CreateProductWithRulesCmd).name,
    via=_normalize_product_name,
).build()

PRICE_RULE = Rule.check(
    F(CreateProductWithRulesCmd).price,
    via=_price_must_be_positive,
).build()


class CreateProductWithComputeAndRulesUseCase(UseCase[Product, Product]):
    computes = [NORMALIZE_NAME]
    rules = [PRICE_RULE]

    async def execute(
        self, cmd: CreateProductWithRulesCmd = Input()
    ) -> Product:
        return await self.main_repo.create(cmd)


def _repo_module(
    integration_context: RepositoryIntegrationHarness,
) -> Callable[[LoomContainer], None]:
    repo = integration_context.product.repository
    repo_interface = type(repo)

    def register(container: LoomContainer) -> None:
        container.register(repo_interface, lambda: repo, scope=Scope.APPLICATION)
        container.register_repo(Product, repo_interface)

    return register


class TestUseCaseCrudIntegration:
    @mark.asyncio
    async def test_use_case_crud_flow_with_main_repo_injected(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=[
                CreateProductUseCase,
                GetProductUseCase,
                UpdateProductUseCase,
                DeleteProductUseCase,
            ],
            modules=[_repo_module(integration_context)],
        )

        executor = RuntimeExecutor(result.compiler)

        create_uc = result.factory.build(CreateProductUseCase)
        created = await executor.execute(
            create_uc,
            payload={"name": "headphones", "price": 55.0},
        )
        assert created.name == "headphones"
        assert created.id == 1

        get_uc = result.factory.build(GetProductUseCase)
        loaded = await executor.execute(get_uc, params={"product_id": 1})
        assert loaded is not None
        assert loaded.name == "headphones"

        update_uc = result.factory.build(UpdateProductUseCase)
        updated = await executor.execute(
            update_uc,
            params={"product_id": 1},
            payload={"price": 49.9},
        )
        assert updated is not None
        assert float(updated.price) == 49.9

        delete_uc = result.factory.build(DeleteProductUseCase)
        deleted = await executor.execute(delete_uc, params={"product_id": 1})
        assert deleted is True

        missing = await executor.execute(get_uc, params={"product_id": 1})
        assert missing is None

    @mark.asyncio
    async def test_use_case_compute_and_rules_success_path(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=[CreateProductWithComputeAndRulesUseCase],
            modules=[_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)
        create_uc = result.factory.build(CreateProductWithComputeAndRulesUseCase)

        created = await executor.execute(
            create_uc,
            payload={"name": "  headset  ", "price": 42.0},
        )

        assert created.name == "HEADSET"
        assert float(created.price) == 42.0

    @mark.asyncio
    async def test_use_case_compute_and_rules_rejects_invalid_input(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=[CreateProductWithComputeAndRulesUseCase],
            modules=[_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)
        create_uc = result.factory.build(CreateProductWithComputeAndRulesUseCase)

        with pytest.raises(RuleViolations):
            await executor.execute(
                create_uc,
                payload={"name": "bad", "price": -1.0},
            )
