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
import msgspec

from loom.core.command import Command
from loom.core.use_case import Compute, Input, Rule, UseCase


class CreateProduct(Command):
    name: str
    price: float


class Product(msgspec.Struct):
    id: int
    name: str
    price: float


class CreateProductUseCase(UseCase[CreateProduct, Product]):
    command: Input[CreateProduct]

    positive_price = Rule(lambda p: p.command.price > 0)
    make_output = Compute(
        lambda p: Product(id=1, name=p.command.name, price=p.command.price),
    )
```

## Status

Project under active development.
