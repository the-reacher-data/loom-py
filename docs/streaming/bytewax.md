# Bytewax Runtime

`loom.streaming.bytewax` is the production adapter that turns a declarative
`StreamFlow` into a real Bytewax dataflow.

For a runnable end-to-end reference implementation, see:
`dummy-loom-streaming <https://github.com/MassiveDataScope/dummy-loom-streaming>`_.

## What it does

- compiles the flow
- resolves config bindings
- wires Kafka sources and sinks
- applies `With` / `WithAsync`
- runs observability hooks

## Public entrypoint

```python
from loom.streaming.bytewax import StreamingRunner
```

## When to use it

Use the Bytewax adapter when you want:

- topic-to-topic streaming
- typed message envelopes
- branch-aware fan-out
- `CollectBatch` before `WithAsync`
- runtime observability for batch and node lifecycle events

## Flow contract

The clean authoring contract is:

```python
from loom.streaming import CollectBatch, IntoTopic, Process, WithAsync

process = Process(
    CollectBatch(max_records=50, timeout_ms=2000),
    WithAsync(
        process=Process(
            # per-message async step
            ...
            IntoTopic(...),
        ),
        max_concurrency=50,
    ),
)
```

`CollectBatch` belongs before `WithAsync` when you want batch aggregation as an
input shape. If you want to wait for a batch outside the async step, keep that
logic in the parent flow, not inside the task itself.

## Runtime notes

- `WithAsync` executes one message per task, concurrently.
- `CollectBatch` emits observable batch events.
- Errors are routed through the explicit error wiring.
- `msg` and `payload` are public expression roots for routing predicates.
