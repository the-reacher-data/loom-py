from __future__ import annotations

from app.product.model import Product
from app.product.use_cases import (
    CreateProductUseCase,
    DeleteProductUseCase,
    GetProductUseCase,
    ListProductsUseCase,
    UpdateProductUseCase,
)

from loom.rest.model import RestInterface, RestRoute


class ProductRestInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    profile_default = "default"
    allowed_profiles = ("default", "with_details")
    routes = (
        RestRoute(
            use_case=CreateProductUseCase,
            method="POST",
            path="/",
            status_code=201,
        ),
        RestRoute(
            use_case=ListProductsUseCase,
            method="GET",
            path="/",
            expose_profile=True,
        ),
        RestRoute(
            use_case=GetProductUseCase,
            method="GET",
            path="/{product_id}",
            expose_profile=True,
        ),
        RestRoute(
            use_case=UpdateProductUseCase,
            method="PATCH",
            path="/{product_id}",
        ),
        RestRoute(
            use_case=DeleteProductUseCase,
            method="DELETE",
            path="/{product_id}",
        ),
    )
