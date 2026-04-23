# Bytewax Adapter — TDD Plan

## Goal
Drive the implementation of `loom.streaming.bytewax._adapter` through failing tests,
starting from the simplest case and increasing complexity.

## Principles
- One concept per test file.
- Tests run against `TestingSource` / `TestingSink` (no Kafka broker).
- Each test constructs a `CompiledPlan` directly to avoid compiler/Kafka config coupling.

## Phase 1 — Core node execution (no Kafka, no CM)
These verify that each DSL node type translates correctly into Bytewax operators.

### 1. `test_task_node.py`  ✅ DONE
- `Task` transforms a single record.
- `BatchTask` receives a batch and `flat_map` restores record stream.

### 2. `test_shape_adapters.py`
- `CollectBatch` groups N records into one batch.
- `ForEach` expands a batch back into individual records.
- `Drain` swallows the stream (sink produces nothing).

### 3. `test_router_node.py`
- `Router.by` dispatches records to the correct branch.
- `Router.when` evaluates predicates in order.
- `Router` default branch catches unmatched records.

### 4. `test_observability_hooks.py`
- `StreamingFlowObserver` receives `on_node_start` / `on_node_end` for every node.
- `on_node_error` is called when a task raises.

## Phase 2 — Resource lifecycle (context managers)
These verify that `With` / `WithAsync` open/close dependencies correctly.

### 5. `test_with_worker_scope.py`
- Sync CM opened once per worker, reused across batches.
- Async CM opened once per worker through `AsyncBridge`.

### 6. `test_with_batch_scope.py`
- Sync `ContextFactory` creates a fresh CM per batch.
- Async `ContextFactory` creates a fresh CM per batch.
- CM is closed even when the task raises.

### 7. `test_one_emit.py`
- `OneEmit` wraps `With` or `WithAsync` and emits each result individually.

## Phase 3 — Error handling

### 8. `test_error_routing.py`
- Uncaught exception in a task is routed to the error topic configured for that `ErrorKind`.
- Error envelope preserves original message metadata.

## Phase 4 — Wire real Kafka codec (still no broker)

### 9. `test_kafka_codec.py`
- `decode` operator converts raw bytes → `Message[LoomStruct]` using msgspec.
- `encode` operator converts `Message[LoomStruct]` → raw bytes.

## Phase 5 — Integration with Kafka client (unit, not e2e)

### 10. `test_kafka_source.py`
- `KafkaSource` (our wrapper around `loom.streaming.kafka`) integrates with `bw_input`.
- `KafkaSink` integrates with `bw_output`.

## Definition of Done per phase
- All tests in the phase pass.
- `ruff check` and `mypy` clean on modified files.
- No `pickle` concerns (pods are independent).

## Next Action
Review and approve this plan, then implement Phase 1 item 2 (`test_shape_adapters.py`).
