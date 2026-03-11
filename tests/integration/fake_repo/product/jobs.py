from __future__ import annotations

from loom.core.job.job import Job
from tests.integration.fake_repo.product.repository_contract import ProductRepo


class GetProductIdByNameJob(Job[int | None]):
    """Job that resolves the typed Product repository contract from DI."""

    def __init__(self, product_repo: ProductRepo) -> None:
        self._product_repo = product_repo

    async def execute(self, name: str) -> int | None:
        product = await self._product_repo.get_by_name(name)
        if product is None:
            return None
        return int(product.id)
