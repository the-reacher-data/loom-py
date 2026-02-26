from __future__ import annotations

from typing import cast

from loom.core.repository.abc.query import PageParams, PageResult
from loom.core.use_case import Input
from loom.core.use_case.use_case import UseCase
from tests.integration.fake_repo.product.model import Product
from tests.integration.fake_repo.product.schemas import CreateProduct, UpdateProduct


class CreateProductUseCase(UseCase[Product, Product]):
    async def execute(self, cmd: CreateProduct = Input()) -> Product:
        return cast(Product, await self.main_repo.create(cmd))


class GetProductUseCase(UseCase[Product, Product | None]):
    async def execute(self, product_id: str) -> Product | None:
        return await self.main_repo.get_by_id(int(product_id))


class ListProductsUseCase(UseCase[Product, PageResult[Product]]):
    async def execute(self) -> PageResult[Product]:
        return await self.main_repo.list_paginated(PageParams(page=1, limit=100))


class UpdateProductUseCase(UseCase[Product, Product | None]):
    async def execute(
        self,
        product_id: str,
        cmd: UpdateProduct = Input(),
    ) -> Product | None:
        return await self.main_repo.update(int(product_id), cmd)


class DeleteProductUseCase(UseCase[Product, bool]):
    async def execute(self, product_id: str) -> bool:
        return await self.main_repo.delete(int(product_id))
