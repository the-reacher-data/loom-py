# loom-kernel

[![CI](https://img.shields.io/github/actions/workflow/status/the-reacher-data/loom-py/ci-main.yml?branch=master&label=ci)](https://github.com/the-reacher-data/loom-py/actions/workflows/ci-main.yml)
[![Coverage](https://codecov.io/gh/the-reacher-data/loom-py/branch/master/graph/badge.svg)](https://codecov.io/gh/the-reacher-data/loom-py)
[![PyPI](https://img.shields.io/pypi/v/loom-kernel)](https://pypi.org/project/loom-kernel/)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://github.com/the-reacher-data/loom-py/blob/master/pyproject.toml)

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
from loom.core.use_case import Compute, F, Input, Rule, UseCase
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


def normalize_price(value: float) -> float:
    return round(float(value), 2)


def price_positive_error(value: float) -> str | None:
    if value <= 0:
        return "price must be greater than zero"
    return None


def patch_is_empty(_cmd: UpdateProduct, fields_set: frozenset[str]) -> bool:
    return len(fields_set) == 0


def system_name_change_forbidden(name: str | None, product_id: str) -> bool:
    return product_id == "1" and name is not None


def name_matches_price(name: str | None, price: float | None) -> bool:
    if name is None or price is None:
        return False
    return name.strip() == str(price)


normalize_create_name = Compute.set(F(CreateProduct).name).from_command(
    F(CreateProduct).name,
    via=normalize_text,
)

normalize_create_price = Compute.set(F(CreateProduct).price).from_command(
    F(CreateProduct).price,
    via=normalize_price,
)

normalize_update_name = (
    Compute.set(F(UpdateProduct).name)
    .from_command(F(UpdateProduct).name, via=normalize_text)
    .when_present(F(UpdateProduct).name)
)

normalize_update_price = (
    Compute.set(F(UpdateProduct).price)
    .from_command(F(UpdateProduct).price, via=normalize_price)
    .when_present(F(UpdateProduct).price)
)

create_price_rule = Rule.check(
    F(CreateProduct).price,
    via=price_positive_error,
)

update_not_empty_rule = Rule.forbid(
    patch_is_empty,
    message="at least one field must be provided",
).from_command()

update_system_name_immutable_rule = (
    Rule.forbid(
        system_name_change_forbidden,
        message="system product name cannot be changed",
    )
    .from_command(F(UpdateProduct).name)
    .from_params("product_id")
    .when_present(F(UpdateProduct).name)
)

update_name_price_rule = (
    Rule.forbid(
        name_matches_price,
        message="name cannot be equal to price",
    )
    .from_command(F(UpdateProduct).name, F(UpdateProduct).price)
    .when_present(F(UpdateProduct).name & F(UpdateProduct).price)
)

update_price_rule = Rule.check(
    F(UpdateProduct).price,
    via=price_positive_error,
).when_present(F(UpdateProduct).price)


class CreateProductUseCase(UseCase[Product, Product]):
    computes = (normalize_create_name, normalize_create_price)
    rules = (create_price_rule,)

    async def execute(self, cmd: CreateProduct = Input()) -> Product:
        return await self.main_repo.create(cmd)


class UpdateProductUseCase(UseCase[Product, Product | None]):
    computes = (normalize_update_name, normalize_update_price)
    rules = (
        update_not_empty_rule,
        update_system_name_immutable_rule,
        update_name_price_rule,
        update_price_rule,
    )

    async def execute(self, product_id: str, cmd: UpdateProduct = Input()) -> Product | None:
        # product_id is available to rules via .from_params("product_id")
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

This example shows:
- `Compute.from_command(...)` for deterministic data normalization.
- `Rule.check(...)` for field validation returning a message.
- `Rule.forbid(...).from_command()` for full-command checks.
- `Rule.forbid(...).from_params(...)` to use execute parameters (for example `product_id`).
- `.when_present(...)` to run patch-aware logic only when selected fields are present.

With this setup, the framework can bootstrap a complete HTTP app
(routes, dependency wiring, and runtime execution) from your interfaces,
use cases, and discovered models.

For deeper references, review the integration examples under
`tests/integration/fake_repo`.
We will publish a full functional demo repository soon with end-to-end
documentation.

## Status

Project under active development.
