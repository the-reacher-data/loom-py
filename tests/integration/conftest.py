from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path

from pytest import fixture

from helpers.integration_context import IntegrationContext, build_integration_context
from loom.core.repository.sqlalchemy.model import BaseModel
from loom.core.repository.sqlalchemy.session_manager import SessionManager


@fixture
async def integration_context(tmp_path: Path) -> AsyncGenerator[IntegrationContext, None]:
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
        await conn.run_sync(BaseModel.metadata.create_all)
    try:
        yield integration_context
    finally:
        async with manager.engine.begin() as conn:
            await conn.run_sync(BaseModel.metadata.drop_all)
        await manager.dispose()
        if db_path.exists():
            db_path.unlink()
