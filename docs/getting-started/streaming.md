# Streaming Quickstart

`loom.streaming` lets you declare typed streaming flows with explicit sources,
steps, batching, branching, and transport wiring.

The companion demo repository for this subsystem is
`dummy-loom-streaming <https://github.com/MassiveDataScope/dummy-loom-streaming>`_.

## Install

Choose the streaming runtime and Kafka transport:

```bash
pip install "loom-kernel[streaming,kafka]"
```

## Minimal flow

```python
from loom.streaming import (
    CollectBatch,
    FromTopic,
    IntoTopic,
    Process,
    StreamFlow,
    WithAsync,
    payload,
)

from app.streaming.tasks import FetchRequestTask
from app.streaming.types import ScrapeRequest, ScrapeResponse


async_scrape_flow = StreamFlow(
    name="async_smoke_flow",
    source=FromTopic[ScrapeRequest](
        name="scrape.requests",
        payload=ScrapeRequest,
    ),
    process=Process(
        CollectBatch(max_records=50, timeout_ms=2000),
        WithAsync(
            process=Process(
                FetchRequestTask,
                IntoTopic[ScrapeResponse](
                    name="scrape.responses",
                    payload=ScrapeResponse,
                ),
            ),
            max_concurrency=50,
        ),
    ),
)
```

## Run it

Use the Bytewax runner to compile the flow, resolve YAML bindings, and execute
the graph:

```python
from loom.streaming.bytewax import StreamingRunner

runner = StreamingRunner.from_yaml(async_scrape_flow, "config/streaming.yaml")
runner.run()
```

## Public imports

Prefer the public surface:

```python
from loom.streaming import (
    Broadcast,
    CollectBatch,
    ContextFactory,
    Fork,
    FromTopic,
    IntoTopic,
    Message,
    Process,
    RecordStep,
    Router,
    StreamFlow,
    With,
    WithAsync,
    msg,
    payload,
)
```

Use `loom.streaming.nodes`, `loom.streaming.kafka`, and
`loom.streaming.bytewax` only when you need a narrower or runtime-specific
surface.

For a runnable end-to-end example, see the companion repository:
`dummy-loom-streaming <https://github.com/MassiveDataScope/dummy-loom-streaming>`_.
