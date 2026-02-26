# loom-kernel

[![CI](https://img.shields.io/github/actions/workflow/status/the-reacher-data/loom-py/ci-main.yml?branch=master&label=ci)](https://github.com/the-reacher-data/loom-py/actions/workflows/ci-main.yml)
[![Coverage](https://img.shields.io/codecov/c/github/the-reacher-data/loom-py?branch=master)](https://app.codecov.io/gh/the-reacher-data/loom-py)
[![PyPI](https://img.shields.io/pypi/v/loom-kernel)](https://pypi.org/project/loom-kernel/)
[![Python](https://img.shields.io/pypi/pyversions/loom-kernel)](https://pypi.org/project/loom-kernel/)

Framework-agnostic Python toolkit to build backend applications with:

- typed use cases (`msgspec.Struct`)
- repositories decoupled from infrastructure
- REST/FastAPI adapters
- testing utilities for business workflows

## Purpose

`loom-kernel` helps you model domain logic and use cases with clean architecture.
The library separates core contracts from concrete adapters so you can swap
infrastructure (DB, cache, transport) without breaking business logic.

## Main subpaths

| Subpath | What it is for |
| --- | --- |
| `src/loom/core/use_case` | `UseCase` definition, rules (`Rule`), and compute steps (`Compute`). |
| `src/loom/core/engine` | Compilation and runtime execution of a use-case plan. |
| `src/loom/core/repository/abc` | Repository contracts, pagination, and typed query spec. |
| `src/loom/core/repository/sqlalchemy` | Concrete async SQLAlchemy repository implementation. |
| `src/loom/core/model` | Base model, fields, relations, and entity introspection. |
| `src/loom/core/cache` | Decorators and cached repository with dependency invalidation. |
| `src/loom/rest` | Framework-agnostic REST model and route compiler. |
| `src/loom/rest/fastapi` | Direct FastAPI integration (auto wiring and runtime router). |
| `src/loom/prometheus` | Middleware and adapter for runtime metrics. |
| `src/loom/testing` | Harnesses for unit/integration tests and golden tests. |

## Quick example

```python
from loom.core.command import Command, Patch
from loom.core.errors import RuleViolation
from loom.core.model import BaseModel, ColumnField
from loom.core.use_case import Compute, F, Input, UseCase
from loom.rest.fastapi.auto import create_app
from loom.rest.model import RestInterface, RestRoute


class CreateProduct(Command, frozen=True):
    name: str
    price: float


class UpdateProduct(Command, frozen=True):
    name: Patch[str] = None
    price: Patch[float] = None


class Product(BaseModel):
    __tablename__ = "products"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=120)
    price: float = ColumnField()


def normalize_text(value: str) -> str:
    return value.strip()


def validate_create_price(cmd: CreateProduct, _: frozenset[str]) -> None:
    if cmd.price <= 0:
        raise RuleViolation(field="price", message="price must be greater than zero")


def validate_update_price(cmd: UpdateProduct, fields: frozenset[str]) -> None:
    # For Patch fields, validate only when the field was actually sent by caller.
    if "price" in fields and cmd.price is not None and cmd.price <= 0:
        raise RuleViolation(field="price", message="price must be greater than zero")


normalize_create_name = Compute.set(F(CreateProduct).name).from_fields(
    F(CreateProduct).name,
    via=normalize_text,
).when_present(F(CreateProduct).name)

normalize_update_name = Compute.set(F(UpdateProduct).name).from_fields(
    F(UpdateProduct).name,
    via=normalize_text,
).when_present(F(UpdateProduct).name)


class CreateProductUseCase(UseCase[Product, Product]):
    computes = (normalize_create_name,)
    rules = (validate_create_price,)

    async def execute(self, cmd: CreateProduct = Input()) -> Product:
        return await self.main_repo.create(cmd)


class UpdateProductUseCase(UseCase[Product, Product | None]):
    computes = (normalize_update_name,)
    rules = (validate_update_price,)

    async def execute(self, product_id: str, cmd: UpdateProduct = Input()) -> Product | None:
        return await self.main_repo.update(int(product_id), cmd)


class ProductRestInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    profile_default = "default"
    routes = (
        RestRoute(
            use_case=CreateProductUseCase,
            method="POST",
            path="/",
            status_code=201,
        ),
        RestRoute(
            use_case=UpdateProductUseCase,
            method="PATCH",
            path="/{product_id}",
        ),
    )


# Auto app bootstrap from YAML config + discovery
app = create_app(config_path="config/conf.yaml", code_path="src")
```

With this setup, the framework can bootstrap a complete HTTP app
(routes, dependency wiring, and runtime execution) from your interfaces,
use cases, and discovered models.

For deeper references, review the integration examples under
`tests/integration/fake_repo`.
We will publish a full functional demo repository soon with end-to-end
documentation.

## Status

Project under active development.
