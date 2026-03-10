from __future__ import annotations

from collections.abc import Callable

from pytest import mark

from loom.core.bootstrap.bootstrap import bootstrap_app
from loom.core.di.container import LoomContainer
from loom.core.engine.executor import RuntimeExecutor
from loom.core.repository.sqlalchemy import build_repository_registration_module
from loom.testing import RepositoryIntegrationHarness
from tests.integration.fake_repo.product.jobs import GetProductIdByNameJob
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.repository import ProductRepository
from tests.integration.fake_repo.product.use_cases import (
    CreateProductUseCase,
    FindProductByNameUseCase,
)


class _Cfg:
    env: str = "test"


def _repo_module(
    integration_context: RepositoryIntegrationHarness,
) -> Callable[[LoomContainer], None]:
    return build_repository_registration_module(
        integration_context.session_manager,
        (Product,),
    )


class TestCustomRepositoryIntegration:
    @mark.asyncio
    async def test_main_repo_uses_registered_custom_repository(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=[CreateProductUseCase],
            modules=[_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)

        use_case = result.factory.build(CreateProductUseCase)

        assert isinstance(use_case.main_repo, ProductRepository)

        created = await executor.execute(
            use_case,
            payload={"name": "  keyboard  ", "price": 120.0},
        )

        assert created.name == "keyboard"

    @mark.asyncio
    async def test_use_case_constructor_contract_resolves_custom_repository(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=[CreateProductUseCase, FindProductByNameUseCase],
            modules=[_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)

        await executor.execute(
            result.factory.build(CreateProductUseCase),
            payload={"name": "Desk", "price": 55.0},
        )

        use_case = result.factory.build(FindProductByNameUseCase)
        found = await executor.execute(use_case, params={"name": "  desk  "})

        assert isinstance(use_case._product_repo, ProductRepository)
        assert found is not None
        assert found.name == "Desk"

    @mark.asyncio
    async def test_job_constructor_contract_resolves_custom_repository(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=[CreateProductUseCase, GetProductIdByNameJob],
            modules=[_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)

        await executor.execute(
            result.factory.build(CreateProductUseCase),
            payload={"name": "Mouse", "price": 25.0},
        )

        job = result.factory.build(GetProductIdByNameJob)
        product_id = await executor.execute(job, params={"name": "mouse"})

        assert isinstance(job._product_repo, ProductRepository)
        assert product_id == 1
