from __future__ import annotations

from typing import cast

from app.product.model import Product
from app.product.schemas import CreateProduct, UpdateProduct

from loom.core.repository.abc.query import CursorResult, PageResult, QuerySpec
from loom.core.use_case import Input
from loom.core.use_case.use_case import UseCase


class CreateProductUseCase(UseCase[Product, Product]):
    async def execute(self, cmd: CreateProduct = Input()) -> Product:
        return cast(Product, await self.main_repo.create(cmd))


class GetProductUseCase(UseCase[Product, Product | None]):
    async def execute(self, product_id: str, profile: str = "default") -> Product | None:
        return await self.main_repo.get_by_id(int(product_id), profile=profile)


class ListProductsUseCase(UseCase[Product, PageResult[Product] | CursorResult[Product]]):
    async def execute(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[Product] | CursorResult[Product]:
        return await self.main_repo.list_with_query(query, profile=profile)


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
