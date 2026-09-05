# Bytewax Runtime

`loom.streaming.bytewax` is the production adapter that turns a declarative
`StreamFlow` into a real Bytewax dataflow.

For a runnable end-to-end reference implementation, see:
[dummy-loom-streaming](https://github.com/the-reacher-data/dummy-loom-streaming).

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

## Writing your own destination

`IntoTopic` and `IntoTable` are the destinations loom ships. A destination it
does not ship is a **frozen dataclass** satisfying `IntoSink`: no base class, no
registration, no loom import beyond the payload type. The compiler recognises it
by structure and resolves its configuration from `streaming.sinks.<name>`.

```python
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, ClassVar

@dataclass(frozen=True)
class IntoJsonl:
    payload: type[IncidentEvent]
    name: str = "incidents"          # the key under streaming.sinks
    router_branch_safe: ClassVar[bool] = True

    def build_partition(
        self,
        config: Any,                 # your own resolved section
        worker_index: int,
        worker_count: int,
        bridge: Any = None,
        session_manager: Any = None,
    ) -> "JsonlPartition":
        return JsonlPartition(config["path"])


class JsonlPartition:
    def __init__(self, path: str) -> None:
        self._path, self._buffer = path, []

    def write_batch(self, items: Sequence[IncidentEvent]) -> None:
        self._buffer.extend(items)   # once per Bytewax epoch

    def close(self) -> None:
        flush(self._path, self._buffer)   # must be idempotent
```

`Process(IntoJsonl(payload=IncidentEvent))` type-checks under `mypy --strict`:
the three attributes are read-only in the protocol, so a frozen dataclass, a
msgspec struct or a class variable all satisfy them, and `config` and
`session_manager` are opaque, so your sink narrows them to whatever its backend
resolves. `write_batch` runs per epoch, per worker; `close` may be called even
when `write_batch` never was.

## Runtime notes

- `WithAsync` executes one message per task, concurrently.
- `CollectBatch` emits observable batch events.
- Errors are routed through the explicit error wiring.
- `msg` and `payload` are public expression roots for routing predicates.
