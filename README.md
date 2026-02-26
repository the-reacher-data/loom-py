# loom-kernel

[![CI](https://img.shields.io/github/actions/workflow/status/the-reacher-data/loom-py/ci-main.yml?branch=master&label=ci)](https://github.com/the-reacher-data/loom-py/actions/workflows/ci-main.yml)
[![Coverage](https://img.shields.io/codecov/c/github/the-reacher-data/loom-py?branch=master)](https://app.codecov.io/gh/the-reacher-data/loom-py)
[![PyPI](https://img.shields.io/pypi/v/loom-kernel)](https://pypi.org/project/loom-kernel/)
[![Python](https://img.shields.io/pypi/pyversions/loom-kernel)](https://pypi.org/project/loom-kernel/)

Framework Python agnostico para construir aplicaciones de backend con:

- casos de uso tipados (`msgspec.Struct`)
- repositorios desacoplados de infraestructura
- adaptadores REST/FastAPI
- utilidades de testing para flujos de negocio

## Proposito

`loom-kernel` te ayuda a modelar dominio y casos de uso con arquitectura limpia.
La libreria separa contratos del core y adaptadores concretos para que puedas
cambiar infraestructura (DB, cache, transporte) sin romper la logica de negocio.

## Subrutas principales

| Subruta | Para que sirve |
| --- | --- |
| `src/loom/core/use_case` | Definicion de `UseCase`, reglas (`Rule`) y pasos de computo (`Compute`). |
| `src/loom/core/engine` | Compilacion y ejecucion del plan runtime de un caso de uso. |
| `src/loom/core/repository/abc` | Contratos de repositorio, paginacion y query spec tipado. |
| `src/loom/core/repository/sqlalchemy` | Implementacion concreta de repositorios con SQLAlchemy async. |
| `src/loom/core/model` | Modelo base, campos, relaciones e introspeccion de entidades. |
| `src/loom/core/cache` | Decoradores y repositorio cacheado con invalidacion por dependencias. |
| `src/loom/rest` | Modelo REST agnostico y compilador de rutas. |
| `src/loom/rest/fastapi` | Integracion directa con FastAPI (auto wiring y runtime router). |
| `src/loom/prometheus` | Middleware y adaptador para metricas de runtime. |
| `src/loom/testing` | Harnesses para tests unitarios/integracion y golden tests. |

## Ejemplo rapido

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

## Estado

Proyecto en evolucion activa.
