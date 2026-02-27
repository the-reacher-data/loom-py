# Quickstart

`loom-kernel` lets you define business behavior in typed use cases and expose it through interfaces.

## Install

```bash
pip install loom-kernel
```

## Minimal flow

1. Define your command models.
2. Create a `UseCase` with `rules` and `computes`.
3. Wire a `RestInterface` or run the use case directly from bootstrap/runtime.

See `examples/fake_repo` for runnable end-to-end examples.
