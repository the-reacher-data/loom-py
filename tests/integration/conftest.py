from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

from pytest import fixture

from loom.core.backend.sqlalchemy import compile_all, get_metadata, reset_registry
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from tests.helpers.integration_context import IntegrationContext, build_integration_context
from tests.integration.fake_repo.product.category.model import Category
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.relations import ProductCategoryLink
from tests.integration.fake_repo.product.review.model import ProductReview


@fixture
async def integration_context(tmp_path: Path) -> AsyncGenerator[IntegrationContext, None]:
    reset_registry()
    compile_all(Category, ProductReview, ProductCategoryLink, Product)

    db_path = tmp_path / "integration.sqlite"
    database_url = f"sqlite+aiosqlite:///{db_path}"

    manager = SessionManager(
        database_url,
        pool_size=None,
        max_overflow=None,
        pool_timeout=None,
        pool_recycle=None,
        connect_args={},
    )

    integration_context = build_integration_context(
        package_name="tests.integration.fake_repo",
        session_manager=manager,
        load_order=("product", "category", "category_link", "review"),
    )
    async with manager.engine.begin() as conn:
        await conn.run_sync(get_metadata().create_all)
    try:
        yield integration_context
    finally:
        async with manager.engine.begin() as conn:
            await conn.run_sync(get_metadata().drop_all)
        await manager.dispose()
        reset_registry()
        if db_path.exists():
            db_path.unlink()
