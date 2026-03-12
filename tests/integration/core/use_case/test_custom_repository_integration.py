from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, cast

from pytest import mark

from loom.core.bootstrap.bootstrap import bootstrap_app
from loom.core.di.container import LoomContainer
from loom.core.di.scope import Scope
from loom.core.engine.compilable import Compilable
from loom.core.engine.executor import RuntimeExecutor
from loom.core.repository import RepositoryBuildContext, repository_for
from loom.core.repository.sqlalchemy import build_sqlalchemy_repository_registration_module
from loom.core.response import Response
from loom.core.use_case.use_case import UseCase
from loom.testing import RepositoryIntegrationHarness
from tests.integration.fake_repo.product.jobs import GetProductIdByNameJob
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.repository import ProductRepository
from tests.integration.fake_repo.product.use_cases import (
    CreateProductUseCase,
    FindProductByNameUseCase,
)
from tests.integration.support.logical_repo_fixtures import (
    GetTaskViewUseCase,
    TaskView,
    TaskViewRepository,
)


class _Cfg:
    env: str = "test"


class BuilderTaskView(Response, frozen=True, kw_only=True):  # type: ignore[misc]
    task_id: str
    state: str


class BuilderTaskViewRepo(Protocol):
    async def get_by_id(
        self,
        obj_id: str,
        profile: str = "default",
    ) -> BuilderTaskView | None: ...


class _BuilderTaskViewRepositoryContract(BuilderTaskViewRepo):
    pass


@dataclass(frozen=True)
class _TaskRepoSettings:
    state: str


def _build_task_view_repository(context: RepositoryBuildContext) -> Any:
    settings = context.container.resolve(_TaskRepoSettings)
    return _BuilderTaskViewRepository(settings)


@repository_for(
    BuilderTaskView,
    contract=_BuilderTaskViewRepositoryContract,
    builder=_build_task_view_repository,
)
class _BuilderTaskViewRepository:
    def __init__(self, settings: _TaskRepoSettings) -> None:
        self._settings = settings

    async def get_by_id(
        self,
        obj_id: str,
        profile: str = "default",
    ) -> BuilderTaskView | None:
        return BuilderTaskView(task_id=obj_id, state=self._settings.state)


class GetBuilderTaskViewUseCase(
    UseCase[BuilderTaskView, BuilderTaskView | None, _BuilderTaskViewRepositoryContract]
):
    async def execute(self, task_id: str) -> BuilderTaskView | None:
        return await self.main_repo.get_by_id(task_id)


def _use_cases(*items: type[Any]) -> tuple[type[Compilable], ...]:
    return cast(tuple[type[Compilable], ...], items)


def _repo_module(
    integration_context: RepositoryIntegrationHarness,
) -> Callable[[LoomContainer], None]:
    return build_sqlalchemy_repository_registration_module(
        integration_context.session_manager,
        (Product,),
    )


def _logical_repo_module(
    integration_context: RepositoryIntegrationHarness,
) -> Callable[[LoomContainer], None]:
    return build_sqlalchemy_repository_registration_module(
        integration_context.session_manager,
        (),
        logical_models=(TaskView,),
    )


def _builder_logical_repo_module(
    integration_context: RepositoryIntegrationHarness,
) -> Callable[[LoomContainer], None]:
    register_repositories = build_sqlalchemy_repository_registration_module(
        integration_context.session_manager,
        (),
        logical_models=(BuilderTaskView,),
    )

    def register(container: LoomContainer) -> None:
        container.register(
            _TaskRepoSettings,
            lambda: _TaskRepoSettings(state="from-builder"),
            scope=Scope.APPLICATION,
        )
        register_repositories(container)

    return register


class TestCustomRepositoryIntegration:
    @mark.asyncio
    async def test_main_repo_uses_registered_custom_repository(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=_use_cases(CreateProductUseCase),
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
            use_cases=_use_cases(CreateProductUseCase, FindProductByNameUseCase),
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
            use_cases=_use_cases(CreateProductUseCase, GetProductIdByNameJob),
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

    @mark.asyncio
    async def test_main_repo_can_resolve_custom_repository_for_non_base_model(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=[GetTaskViewUseCase],
            modules=[_logical_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)

        use_case = result.factory.build(GetTaskViewUseCase)
        found = await executor.execute(use_case, params={"task_id": "t-1"})

        assert isinstance(use_case.main_repo, TaskViewRepository)
        assert found is not None
        assert found.state == "done"

    @mark.asyncio
    async def test_main_repo_builder_can_resolve_custom_repository_dependencies(
        self,
        integration_context: RepositoryIntegrationHarness,
    ) -> None:
        result = bootstrap_app(
            config=_Cfg(),
            use_cases=[GetBuilderTaskViewUseCase],
            modules=[_builder_logical_repo_module(integration_context)],
        )
        executor = RuntimeExecutor(result.compiler)

        use_case = result.factory.build(GetBuilderTaskViewUseCase)
        found = await executor.execute(use_case, params={"task_id": "t-2"})

        assert isinstance(use_case.main_repo, _BuilderTaskViewRepository)
        assert found is not None
        assert found.state == "from-builder"
