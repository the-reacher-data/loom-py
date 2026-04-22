# Streaming + Bytewax — Implementation Plan

> **Status**: ACTIVE — C1-C4 implemented, C5-C7 pending
> **Date**: 2026-04-22

---

## 1. Key Decisions

**Bytewax owns Kafka I/O and partition management.** Loom does not write custom `FixedPartitionedSource` / `FixedPartitionedSink`. We use `kop.input` / `kop.output` (from `bytewax.connectors.kafka.operators`). They handle partitions, recovery state, offset commits, and worker rescaling.

**Loom owns codecs, typed records, key resolvers, config, errors.** Bytewax `kop.input` emits `KafkaSourceMessage` with raw `bytes`. Loom decodes in explicit operators (`op.map(decode_one)` or `op.map(decode_batch)`) using `KafkaCodec`. Loom encodes in an explicit operator before `kop.output`. No duplication of serialization logic.

**`LoomStruct` / `LoomFrozenStruct` are the payload contract.** The sink receives bytes because we serialize before `kop.output`. The source emits bytes which we decode into `Message[T]` where `T` is our struct.

**Standalone Kafka clients remain.** `KafkaConsumerClient` / `KafkaProducerClient` (C1) are for scripts, CLI, and tests. The Bytewax adapter plugs our codecs and config into Bytewax's native operators. No mirror, no overlap.

**Topic boundaries are logical references.** `FromTopic("orders-input")` and `IntoTopic("validated-orders")` identify config entries, not physical Kafka topics. Physical topics are resolved at runtime from YAML config via `KafkaSettings.consumer_for()` / `KafkaSettings.producer_for()`.

**Shape ≠ decode timing.** Shape (`record`, `batch`, `many`, `none`) defines how data flows between Process nodes. Decode timing is an adapter optimization:
- Record flow → `op.map(decode_one)` per item after source.
- Batch flow → `op.collect` accumulates raw bytes, then `op.map(decode_batch)` decodes the materialized batch.

Tasks never see bytes.

**Batch decode is deferred.** Bytes are carried through `op.collect` without decoding. One `msgspec.msgpack.decode` per batch is faster than N calls, and wire errors are handled at batch boundary before any Task runs.

---

## 2. Module Structure

```
src/loom/streaming/
  __init__.py                  ← public DSL facade (already exists)
  _shape.py                    ✅ StreamShape, ForEach, CollectBatch, Drain
  _message.py                  ✅ Message[T], MessageMeta
  _boundary.py                 ✅ FromTopic, IntoTopic (logical refs)
  _errors.py                   ✅ ErrorKind, ErrorEnvelope
  _resources.py                ✅ ResourceFactory, TaskContext protocols
  _task.py                     ✅ Task[InT, OutT], BatchTask[InT, OutT]
  _process.py                  ✅ StreamFlow, Process
  _partitioning.py             ✅ PartitionStrategy, PartitionPolicy, PartitionGuarantee
  _compiler.py                 ⬜ compile_flow() → CompiledPlan

  routing/                     ✅ complete
    _router.py                 ✅ Router, Route, Router.by, Router.when
    _protocols.py              ✅ Selector, Predicate protocols
    _helpers.py                ✅ msg expression root

  kafka/                       ✅ transport contracts + confluent impl
    _config.py                 ✅ ProducerSettings, ConsumerSettings, KafkaSettings
    _codec.py                  ✅ KafkaCodec, MsgspecCodec
    _message.py                ✅ MessageDescriptor, MessageMetadata, MessageEnvelope
    _record.py                 ✅ KafkaRecord
    _key_resolver.py           ✅ PartitionKeyResolver, FixedKey, FieldKey
    _errors.py                 ✅ Kafka-specific errors
    client/                    ✅ raw bytes transport
      _protocol.py             ✅ KafkaProducer, KafkaConsumer protocols
      _producer.py             ✅ KafkaProducerClient (Confluent)
      _consumer.py             ✅ KafkaConsumerClient (Confluent)
    message/                   ✅ typed envelope layer
      _protocol.py             ✅ MessageProducer, MessageConsumer protocols
      _producer.py             ✅ KafkaMessageProducer (codec + DI)
      _consumer.py             ✅ KafkaMessageConsumer (codec + DI)

  observability/               ✅ complete
    observers/
      protocol.py              ✅ KafkaStreamingObserver
      composite.py             ✅ CompositeKafkaObserver
      noop.py                  ✅ NoopKafkaObserver
      structlog.py             ✅ StructlogKafkaObserver

  bytewax/                     ⬜ runtime adapter
    __init__.py
    _operators.py              ⬜ decode_one, decode_batch, wrap_task, wrap_batch_task, build_sink_message
    _adapter.py                ⬜ compile_plan_to_dataflow(CompiledPlan) → Dataflow
```

Core shared support (already exists):

```
src/loom/core/
  observability.py             ✅ safe_observe, notify_observers
  routing/
    ref.py                     ✅ LogicalRef
    resolver.py                ✅ DefaultingRouteResolver
  expr/
    nodes.py                   ✅ ExprNode, PathRef, BinOp, etc.
    refs.py                    ✅ RootRef
    eval.py                    ✅ evaluate_expr
```

This mirrors the project pattern:
- `etl/pipeline/` ↔ `streaming/` (DSL declarations)
- `etl/compiler/` ↔ `streaming/_compiler.py` (validation)
- `etl/backends/polars/` ↔ `streaming/bytewax/` (runtime)
- `etl/backends/` ↔ `streaming/kafka/` (transport)

---

## 3. Decode Strategy

```
Record flow:
  kop.input → KafkaSourceMessage[bytes, bytes]
    → op.map(extract_raw) → RawKafkaRecord
    → op.map(decode_one) → Message[T] | WireError
    → op.branch(is_ok) → ok / wire_error
    → op.map(wrap_task) → Message[OutT] | ErrorEnvelope
    → op.branch(is_error) → main / task_error
    → op.map(encode) → KafkaSinkMessage
    → kop.output

Batch flow:
  kop.input → KafkaSourceMessage[bytes, bytes]
    → op.map(extract_raw) → RawKafkaRecord
    → op.collect(timeout, max_size) → list[RawKafkaRecord]
    → op.map(decode_batch) → BatchDecodeResult(valid, errors)
    → op.branch(has_errors) → valid / wire_error
    → op.map(wrap_batch_task) → list[Message[OutT]] | ErrorEnvelope
    → op.flat_map(flatten) → Message[OutT]
    → op.map(encode) → KafkaSinkMessage
    → kop.output
```

The adapter infers strategy from flow structure: if the process contains `CollectBatch` before a `BatchTask` → batch path. Otherwise → per-record path.

---

## 4. Commit Sequence

### C1 — Kafka client refactor ✅

- Raw/message client separation with protocol-based DI
- `commit()` on consumer, `key_resolver` on message producer
- Context managers, observability injection, delivery error lifecycle
- 29 unit tests passing

### C2 — Streaming DSL: shapes, messages, boundaries ✅

- `StreamShape`, `ForEach`, `CollectBatch`, `Drain`
- `Message[T]`, `MessageMeta` (transport-neutral)
- `FromTopic`, `IntoTopic` with `LogicalRef` resolution
- `ErrorKind`, `ErrorEnvelope`
- `ResourceFactory`, `TaskContext` protocols

### C3 — Streaming DSL: Task, Process, Flow ✅

- `Task[InT, OutT]`, `BatchTask[InT, OutT]` (abstract, inherited)
- `StreamFlow`, `Process` (declarative composition)
- `PartitionStrategy`, `PartitionPolicy`, `PartitionGuarantee`
- `KafkaSettings` + YAML loading via `core.config`
- Config resolution via `core.routing.DefaultingRouteResolver`

### C4 — Routing DSL ✅

- `Router.by` (key dispatch) and `Router.when` (ordered predicates)
- `loom.core.expr`: reusable expression nodes + in-memory evaluator
- `msg` expression root for declarative predicates (`msg.payload.country == "ES"`)
- `Selector`, `Predicate` custom protocols

### C5 — Compiler

Single file `streaming/_compiler.py` with:

- `compile_flow(flow, kafka_settings) → CompiledPlan`
- `CompiledPlan`: frozen struct with resolved source config, output config, error routes, ordered node list, inferred decode strategy
- Private validators (graph-level only; constructors already validate local invariants):
  - `_validate_shape_chain`: `record→batch` requires `CollectBatch`, `batch→record` requires `ForEach`, `BatchTask` requires upstream shape `batch`, `Router` requires upstream shape `record`, `ForEach` is illegal when upstream is already `record`, `CollectBatch` is redundant when source shape is `BATCH`
  - `_validate_payload_chain`: consecutive nodes have compatible payload types (`Task[InT, OutT]` where `InT` matches previous node output)
  - `_validate_output_resolution`: every terminal branch has a resolvable `IntoTopic` or falls back to `StreamFlow.output`; no ambiguous dual-output (both terminal IntoTopic and Flow.output)
  - `_validate_router_completeness`: `Router.by` without `default` warns if selector key space is not provably exhaustive (best-effort); `Router.when` without `default` warns if predicates are not provably exhaustive
  - `_validate_error_coverage`: if a `Task` or `BatchTask` is present, `ErrorKind.TASK` route is recommended; if `decode_one`/`decode_batch` is present, `ErrorKind.WIRE` route is recommended
  - `_resolve_kafka_config`: source/output/error `LogicalRef`s resolve to `ConsumerSettings`/`ProducerSettings` via `KafkaSettings`
- `CompiledPlan` fields (frozen struct):
  - `name: str`
  - `source: CompiledSource` — `ConsumerSettings`, `payload_type`, `source_shape`, `decode_strategy` (`record` | `batch`)
  - `nodes: tuple[CompiledNode, ...]` — each node tagged with its inferred shape and payload type
  - `output: CompiledOutput | None` — `ProducerSettings`, topic, `PartitionPolicy | None`
  - `error_routes: dict[ErrorKind, CompiledOutput]`
- Tests: valid graphs pass, missing output rejected, illegal shape transition rejected, payload mismatch rejected, unresolvable config rejected

### C6 — Bytewax adapter

Two files under `streaming/bytewax/`:

**`_operators.py`** — Pure functions for Bytewax operator chains:
- `extract_raw(msg: KafkaSourceMessage) → RawKafkaRecord` — unwrap Bytewax message to our record type
- `decode_one(raw: RawKafkaRecord, codec, payload_type) → Message[T] | WireError` — single record decode
- `decode_batch(raws: list[RawKafkaRecord], codec, payload_type) → BatchDecodeResult` — batch decode with error separation
- `wrap_task(task, message, ctx) → Message[OutT] | ErrorEnvelope` — call `Task.execute` with error capture; injects `TaskContext` if task declares a `resource`
- `wrap_batch_task(task, messages, ctx) → list[Message[OutT]] | ErrorEnvelope` — call `BatchTask.execute` with error capture; injects `TaskContext`
- `build_sink_message(message, codec, key_resolver) → KafkaSinkMessage` — encode `Message[OutT]` to bytes; resolves partition key via `PartitionKeyResolver` if configured
- `build_error_sink_message(error, codec) → KafkaSinkMessage` — encode `WireError` or `ErrorEnvelope` for DLQ output

**`_adapter.py`** — Wiring logic:
- `compile_plan_to_dataflow(plan: CompiledPlan) → Dataflow` — translates `CompiledPlan` into Bytewax operator graph
- Uses `kop.input`/`kop.output` for Kafka I/O (no custom source/sink)
- Source shape `BATCH` → `op.collect` immediately after `extract_raw` (before decode)
- `CollectBatch` inside Process → additional `op.collect` at that position
- `ForEach` inside Process → `op.flat_map`
- Task returning `MANY` → `op.flat_map` after `wrap_task` to flatten outputs
- `op.branch` for error routing after every decode and every task
- Router.by → `op.flat_map` emitting `(branch_key, message)` tuples → one `op.filter` per branch → `op.merge` if branches converge to shared output
- Router.when → recursive `op.branch` tree (first match wins)
- Resource lifecycle: `ResourceFactory.create()` called once per worker in a dedicated `op.input` with a `DynamicSource` that yields a single resource item; downstream tasks receive it via closure or `TaskContext` injection

**Note on source shape:** Bytewax `kop.input` always emits individual `KafkaSourceMessage` records. `FromTopic(shape=BATCH)` does not change the source; it only tells the compiler to insert `op.collect` immediately after extraction. This matches the DSL rule: shape is explicit, never implicit.

**Dependency**: Add `bytewax>=0.21,<1.0` as optional dependency (`streaming` extra).

Tests: linear flow, batch flow, router, error branches, wire errors. Use `bytewax.testing.TestingSource` / `TestingSink` for unit tests without real Kafka.

### C7 — Docstrings + integration tests

- Docstrings on all public API (compiler, adapter, operators)
- Integration tests: end-to-end flow with `bytewax.testing` or local Kafka
- Metrics on decode/encode/task operators (reusing `KafkaPrometheusMetrics`)
- Deferred: OTel observer for streaming (mirrors `etl/observability/observers/otel.py`)
- Deferred: migrate ETL `_predicate.py` to `loom.core.expr` after compiler is stable

---

## 5. Dependency Graph

```
C1 (kafka client refactor) ✅
 │
 C2 (shapes, messages, boundaries) ✅
 │
 ├── C3 (Task, Process, Flow) ✅
 │    │
 │    ├── C4 (Router) ✅
 │    │
 │    └── C5 (Compiler) ← needs C2, C3, C4
 │
 C6 (Bytewax adapter) ← needs C5
 │
 C7 (Docs + integration tests)
```

C2–C4 were parallelizable. C5 needs C2–C4. C6 needs C5. C7 needs C6.

---

## 6. Example: User Code vs Runtime Execution

```python
# --- What the user writes ---

StreamFlow(
    name="enrich-orders",
    source=FromTopic("orders.in", payload=OrderType, shape=StreamShape.RECORD),
    process=Process(
        Task(validate_order),
        Task(enrich_order, resource=HttpClientFactory),
    ),
    output=IntoTopic("orders.enriched", payload=EnrichedOrderType),
    errors={
        ErrorKind.WIRE: IntoTopic("orders.dlq"),
        ErrorKind.TASK: IntoTopic("orders.retry"),
    },
)

# --- compile_flow() produces CompiledPlan ---
# source: ConsumerSettings from kafka.consumer_for("orders.in")
# output: ProducerSettings from kafka.producer_for("orders.enriched")
# error_routes: {WIRE: ProducerSettings("orders.dlq"), TASK: ProducerSettings("orders.retry")}
# decode_strategy: RECORD (inferred from shape + no CollectBatch)

# --- compile_plan_to_dataflow() produces Bytewax Dataflow ---
#
#  kop.input("kafka-in", ..., brokers=..., topics=...)
#    → op.map("extract", extract_raw)
#    → op.map("decode", decode_one)
#    → op.branch("wire_err", is_ok) → ok_stream / wire_error_stream
#    → op.map("validate", wrap_task(validate_order))
#    → op.branch("task_err_1", is_ok) → ok_stream / task_error_1
#    → op.map("enrich", wrap_task(enrich_order))
#    → op.branch("task_err_2", is_ok) → ok_stream / task_error_2
#    → op.map("encode", build_sink_message)
#    → kop.output("kafka-out", ..., topic="orders.enriched")
#
#  Error streams:
#    wire_error_stream → op.map("encode_err", build_error_sink_message)
#                      → kop.output("dlq", ..., topic="orders.dlq")
#    task_error_1 + task_error_2 → op.merge("merged_errors", task_error_1, task_error_2)
#                                → op.map("encode_err", build_error_sink_message)
#                                → kop.output("retry", ..., topic="orders.retry")
```

---

## 7. ADR: Why No Custom Source/Sink

**Context:** We have `KafkaConsumerClient` and `KafkaProducerClient` with codecs, observability, and key resolution.

**Decision:** Use Bytewax native `kop.input` / `kop.output`.

**Consequences:**
- ✅ Bytewax handles partitions, recovery, offset commits, rescaling
- ✅ Our `KafkaCodec` reused inside `op.map` operators
- ✅ Standalone clients remain for scripts, tests, CLI
- ❌ No per-poll observability (Bytewax controls polling). Recovered at operator level.
- ❌ No dynamic partition pause/resume. Acceptable for v1.

---

## 8. ADR: Why Deferred Batch Decode

**Context:** Batch flow could decode per-record at source or defer until after `op.collect`.

**Decision:** Decode after `op.collect`, not per-record.

**Consequences:**
- ✅ `op.collect` works on lightweight bytes
- ✅ One `msgspec.msgpack.decode` per batch faster than N calls
- ✅ Wire errors handled at batch boundary before Task
- ❌ Wire errors discovered later in pipeline. Acceptable — still before any Task.
