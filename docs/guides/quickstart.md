# Quickstart

`loom-kernel` lets you define business behavior in typed use cases and expose it through interfaces.

## Install

```bash
pip install loom-kernel
```

For REST + SQLAlchemy projects, install optional extras:

```bash
pip install "loom-kernel[rest,sqlalchemy,cache]"
```

## Minimal flow

1. Define your command models.
2. Create a `UseCase` with `rules` and `computes`.
3. Wire a `RestInterface` or run the use case directly from bootstrap/runtime.

## Tiny DSL example

```python
from loom.core.command import Command
from loom.core.use_case import Compute, F, Input, Rule, UseCase


class CreateUser(Command):
    email: str
    name: str
    slug: str | None = None


def normalize_slug(email: str) -> str:
    return email.split("@", 1)[0].lower()


class CreateUserUseCase(UseCase[object, dict[str, str]]):
    computes = (
        Compute.set(F(CreateUser).slug).from_command(F(CreateUser).email, via=normalize_slug),
    )
    rules = (
        Rule.check(F(CreateUser).email, via=lambda email: "@" not in email).from_command(
            F(CreateUser).email
        ),
    )

    async def execute(self, cmd: CreateUser = Input()) -> dict[str, str]:
        return {"email": cmd.email, "slug": cmd.slug or ""}
```

## Next steps

- End-to-end fake repo example: `docs/examples-repo/index.md`
- Full demo app repository: [dummy-loom](https://github.com/the-reacher-data/dummy-loom)
- Use-case DSL details: `docs/guides/use-case-dsl.md`
- AutoCRUD guide: `docs/guides/autocrud.md`
- API reference: `docs/reference/index.rst`
