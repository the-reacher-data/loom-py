# Streaming v1 — Process-Oriented Design Spec

> **Status**: IMPLEMENTATION ALIGNED — C1-C4 implemented, C5 next  
> **Author**: code-architect mode  
> **Date**: 2026-04-21  
> **Related**: `specs/v2/03_kafka_adapter.md`, `specs/20_tasks_celery_kafka_architecture_spec.md`

---

## 1. Summary

This document redesigns streaming v1 around four explicit decisions:

1. **Bytewax is the runtime foundation**.
2. **`Process` is the main public graph concept**.
3. **Data shape is explicit and never converted implicitly**.
4. **Routing and error branches are first-class and open-ended**.

The goal is not to build a custom streaming runtime. The goal is to provide a small, typed, clean public DSL that:
- models topic-based message flows,
- validates shape compatibility,
- supports explicit branching,
- supports resource-aware per-record and per-batch execution,
- maps cleanly to Bytewax.

This design intentionally moves away from ETL mental models where `Step` is the visual center. In streaming, the real authoring unit is the **process graph**: source boundary, explicit shape adapters, routers, tasks, and output or error branches.

---

## 2. Design Principles

### 2.1 Primary principles

1. **Bytewax owns execution orchestration**  
   Loom must not recreate worker scheduling, batching mechanics, or a custom parallel runtime.

2. **`Process` is the public graph unit**  
   Users think in subgraphs, branches, and terminals, not only in isolated handlers.

3. **Shape is part of the contract**  
   `record`, `many`, `batch`, and `none` are logical shapes, and shape transitions must be explicit.

4. **Routing must stay open**  
   Users must be able to route by key, field, header, metadata, predicate, or custom classifier without expanding a closed list of router variants.

5. **Payload typing must reuse Loom core models**  
   Public payload declarations use `loom.core.model.LoomStruct` or
   `LoomFrozenStruct` classes directly. Streaming must not introduce a
   parallel `LoomType` abstraction.

6. **Error branches are graph structure, not hidden configuration**  
   Wire, routing, task, and business errors must be explicit and independently routable.

7. **No unnecessary abstraction**  
   If Bytewax already solves a runtime concern, Loom should wrap it lightly.

---

## 3. What This v1 Is

Streaming v1 is a typed DSL for building event-driven **process graphs**.

A flow should answer these questions:
- What topic-based boundary does data enter through?
- What is the logical payload type?
- What shape arrives at the process boundary?
- Which explicit adapters change that shape?
- Which tasks run on each record or batch?
- How are messages routed to branches?
- Where do success, rejection, and error paths go?

The public model should revolve around:
- **Flow** — top-level declaration.
- **Process** — declarative subgraph.
- **Task** — record-oriented execution unit.
- **BatchTask** — batch-oriented execution unit.
- **Router** — open-ended branching node.
- **Topic boundaries** — `FromTopic`, `IntoTopic`.
- **Shape adapters** — e.g. `ForEach`, `CollectBatch`.
- **Error routes** — explicit branch targets for error classes.

---

## 4. What This v1 Is Not

Streaming v1 is **not**:
- a custom streaming executor,
- a clone of `Pipeline -> Process -> Step` ETL semantics,
- a table-centric ingestion architecture,
- a replacement for Bytewax,
- a hidden runtime for worker-local mutable state,
- a closed routing API with one helper per branch style.

### Explicit exclusions from v1

- exactly-once semantics,
- durable stateful joins/windows with recovery guarantees,
- Schema Registry as a hard requirement,
- table sinks as the core public model,
- multi-layer compiler/runtime stacks unless proven necessary,
- a public API centered on `Step` as the main visual abstraction.

---

## 5. Core Concepts

### 5.1 `StreamFlow`

Top-level declarative unit.

A flow defines:
- one input boundary (`source`),
- one main process graph (`process`),
- an optional fallback output boundary (`output`) — applies when the process does not terminate with its own `IntoTopic`,
- explicit error branches (`errors`),
- codec and transport strategy through infrastructure.

Output resolution rule: if the process (or a router branch sub-process) ends with an `IntoTopic`, that terminal applies. If the process ends with a Task (no explicit terminal), the `output` declared on the StreamFlow applies as fallback. If neither exists, the flow is invalid.

### 5.2 `Process[InT, OutT]`

Main streaming authoring unit.

A process represents a logical subgraph between one input edge and one output edge.
It may contain:
- tasks,
- batch tasks,
- routers,
- shape adapters,
- terminal targets.

`Process` is intentionally more visible than `Step` because streaming logic is graph-oriented.

### 5.3 `Task[InT, OutT]`

Record-oriented execution unit.

A task:
- receives one logical message,
- may emit zero, one, or many outputs,
- may use explicit runtime resources,
- must not hide transport logic or lifecycle ownership.

### 5.4 `BatchTask[InT, OutT]`

Batch-oriented execution unit.

A batch task:
- receives a materialized batch of logical messages,
- processes them as a batch contract,
- may emit a batch or terminate,
- exists because batch is a public business concern, not just optimization.

### 5.5 `Router`

Routing node inside a process.

The router must support two open-ended styles:
- **selector-based routing**: extract one logical key and dispatch by key value,
- **predicate-based routing**: evaluate ordered conditions.

This keeps routing open for:
- message key,
- payload field,
- header,
- topic,
- metadata,
- arbitrary classification logic,
- composite conditions.

### 5.6 Payload Models

Public payloads are ordinary Loom core model classes.

`FromTopic` and `IntoTopic` accept `type[PayloadT]` where `PayloadT` is bound
to `LoomStruct | LoomFrozenStruct`. This keeps the streaming API aligned with
core, REST, and ETL instead of creating a second type system. Schema identity,
versioning, codec hints, and compatibility metadata belong in core model or
codec/message descriptors when they are needed.

### 5.7 Topic Boundaries

The public API should expose **topic semantics**, not a vague generic “stream” abstraction.

Recommended boundaries:
- `FromTopic`
- `IntoTopic`
- `IntoTable` — terminal table sink for batch or record persistence

This makes the model honest for Kafka/Redpanda-style usage without hard-coding a vendor into the public DSL, while allowing table sinks as first-class terminals.

### 5.8 Error Branches

Errors are not one generic bucket.

Streaming v1 should distinguish:
- `wire_error`
- `routing_error`
- `task_error`
- `business_reject`

Each must be routable independently.

---

## 6. Shape Model

### 6.1 Logical shapes

The flow graph operates with explicit logical shapes:

- `record` — one logical message
- `many` — one input produces zero or more output messages
- `batch` — one logical batch of messages
- `none` — terminal / sink-only output

### 6.2 Core rule

> **No implicit shape conversion is allowed.**

If shape changes, the graph must contain an explicit adapter node.

### 6.3 Why this matters

This keeps the model predictable and solves the main ambiguity seen in earlier proposals:
- when a source reads in batches,
- when a task fans out into many outputs,
- when a sink expects records,
- when branching happens after a batch source.

### 6.4 Shape compatibility rules

| From shape | Into shape | Requires explicit adapter? | Valid? |
|------------|------------|----------------------------|--------|
| `record`   | `record`   | No                         | Yes    |
| `record`   | `many`     | No, if task contract emits many | Yes |
| `record`   | `none`     | No                         | Yes    |
| `batch`    | `batch`    | No                         | Yes    |
| `batch`    | `none`     | No                         | Yes    |
| `batch`    | `record`   | Yes, `ForEach`             | Yes    |
| `record`   | `batch`    | Yes, `CollectBatch`        | Yes    |
| `many`     | `record`   | No implicit collapse       | No     |
| `batch`    | `many`     | Only through explicit graph design | Depends |

### 6.5 Shape adapter responsibilities

Recommended explicit adapters:
- `ForEach`: `batch -> record`
- `CollectBatch`: `record -> batch`
- `Drain`: `* -> none` when needed as explicit terminal node

The router itself must not change shape.

---

## 7. Topic Boundaries

### 7.1 Decision

Public boundaries should be **topic-oriented**, not generic `FromStream` / `IntoStream`.

Recommended names:
- `FromTopic`
- `IntoTopic`

### 7.2 Why not `FromStream` / `IntoStream`

`Stream` is too generic for the intended semantics.
It hides the fact that users are working with topic-based event transport.
That creates ambiguity in authoring and makes the public model less honest.

### 7.3 Why not expose only `FromKafka` / `IntoKafka`

Vendor-specific names are too narrow for the public authoring surface.
Kafka and Redpanda can remain infrastructure-specific adapters under one topic-oriented public contract.

### 7.4 Single input boundary rule

Each `StreamFlow` should have one input boundary in v1.
That keeps execution planning and lifecycle semantics simpler.

### 7.5 Partitioning and skew control

Partition assignment must be treated as an explicit design concern.
If Loom only reuses whatever key arrives from upstream, the flow inherits all upstream imbalance problems:
- hot partitions,
- skewed tenants or entities,
- reduced effective parallelism,
- retry concentration,
- poor worker utilization.

The public design must therefore distinguish three related but different concepts:
- **business key** — logical entity identifier such as `order_id` or `tenant_id`,
- **routing decision** — branch-selection input inside a process,
- **partition key** — transport key used to assign output partitions.

These may coincide, but they must not be forced to be the same thing.

### 7.6 Decision

Outgoing topic writes should support an explicit **partitioning policy**.
`IntoTopic` must be able to declare how the outgoing partition key is chosen and what affinity guarantee that strategy provides.

### 7.7 Why this matters

A flow may need one of several conflicting goals:
- preserve ordering by entity,
- preserve upstream key semantics,
- reduce skew from dominant tenants,
- spread retries more evenly,
- rebalance fan-out outputs.

Without an explicit contract, these become hidden transport details instead of validated architectural decisions.

### 7.8 Public partitioning contracts

```python
PartitionedPayloadT = TypeVar("PartitionedPayloadT")


class PartitionStrategy(Protocol, Generic[PartitionedPayloadT]):
    """Compute the outgoing transport partition key for a message.

    This contract separates transport distribution from routing semantics.
    """

    def partition_key(self, message: Message[PartitionedPayloadT]) -> bytes | str | None: ...
```

```python
class PartitionGuarantee(Enum):
    """Declared affinity guarantee of a partitioning strategy."""

    NONE = "none"
    BEST_EFFORT = "best_effort"
    ENTITY_STABLE = "entity_stable"
```

```python
class PartitionPolicy(Generic[PartitionedPayloadT]):
    """Declarative partitioning policy for topic output.

    Attributes:
        strategy: Partition-key strategy used for output records.
        guarantee: Declared affinity guarantee offered by the strategy.
        allow_repartition: Whether the flow may override the incoming key.
    """

    strategy: PartitionStrategy[PartitionedPayloadT]
    guarantee: PartitionGuarantee
    allow_repartition: bool
```

### 7.9 Conceptual strategy families

The public spec should recognize these conceptual strategies even if v1 exposes only a small set of helpers:
- **reuse incoming key** — preserve upstream key behavior,
- **derive key from message data** — e.g. from `payload.order_id`,
- **stable bucketed key** — reduce skew while keeping stable bucket assignment,
- **salted or sharded key** — spread dominant entities more aggressively,
- **affinity-preserving key** — guarantee all events for an entity remain together.

### 7.10 Validation rules

- If a process declares entity ordering requirements, it must not use a partition policy with `PartitionGuarantee.NONE`.
- Repartitioning must be explicit; changing partition strategy is a semantic flow change, not a hidden infrastructure tweak.
- Retry and error outputs may declare different partition policies than success outputs.
- After `ForEach`, partition policy applies per record unless the process explicitly declares batch-level partition semantics.
- Routing and partitioning must remain separate concerns; a router chooses branches, a partition policy chooses transport distribution.

### 7.11 Impact on `IntoTopic`

`IntoTopic` should be able to carry an optional partitioning declaration:

```python
class IntoTopic(Generic[PayloadT]):
    """Declare a topic-based output boundary.

    Attributes:
        name: Logical topic name.
        payload: Optional Loom core payload model class.
        shape: Declared accepted shape.
        partitioning: Optional output partitioning policy.
    """

    name: str
    payload: type[PayloadT] | None
    shape: StreamShape
    partitioning: PartitionPolicy[PayloadT] | None
```

---

## 8. Payload Typing with Loom Core Models

### 8.1 Decision

Public payload declarations use Loom core model classes directly:
`type[PayloadT]` where `PayloadT` is bound to `LoomStruct | LoomFrozenStruct`.

### 8.2 Why this is the current decision

This keeps one model contract across core, REST, ETL, and streaming. It avoids
duplicating type metadata in a streaming-only wrapper and keeps `msgspec` as a
core implementation detail through `LoomStruct` / `LoomFrozenStruct`.

Schema evolution and wire compatibility should be represented by the existing
core model layer and Kafka `MessageDescriptor`, not by a separate streaming
`LoomType`.

### 8.3 Public contract sketch

```python
PayloadT = TypeVar("PayloadT")


class FromTopic(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    name: str
    payload: type[PayloadT]
    shape: StreamShape = StreamShape.RECORD


class IntoTopic(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    name: str
    payload: type[PayloadT] | None = None
    shape: StreamShape = StreamShape.RECORD
    partitioning: PartitionPolicy[PayloadT] | None = None
```

The `name` field is a logical reference. The physical Kafka topic lives in YAML
configuration and is resolved by the Kafka adapter.

---

## 8b. Output Resolution

### 8b.1 The problem

`IntoTopic` can appear in multiple places: as the `output` of a `StreamFlow`, as the last node of a `Process`, or as a branch target in a `Router`. Without a clear rule, the developer does not know where to declare the output.

### 8b.2 Decision: one consistent rule

> **The last node of a Process (or sub-Process) defines where output goes. If the Process does not end with an `IntoTopic`, the `output` field of the `StreamFlow` applies as fallback.**

This gives two clean patterns:

**Pattern 1: Linear pipeline — output on the Flow**

When the process is a straight sequence of Tasks, the output belongs to the Flow:

```python
StreamFlow(
    name="simple",
    source=FromTopic("in", payload=Order),
    process=Process(
        Task(validate),
        Task(enrich),
    ),
    output=IntoTopic("out", payload=EnrichedOrder),   # fallback: applies at end of Process
    errors={...},
)
```

**Pattern 2: Router with divergent branches — each branch terminates explicitly**

When routing sends messages to different topics, each branch owns its terminal:

```python
StreamFlow(
    name="dispatch",
    source=FromTopic("in", payload=Order),
    process=Process(
        Task(validate),
        Router(
            key="status",
            routes={
                "new":       [assign, IntoTopic("orders.new")],
                "returned":  [refund, IntoTopic("orders.returns")],
                "cancelled": IntoTopic("orders.cancelled"),
            },
        ),
    ),
    # no output here — every branch has its own terminal
    errors={...},
)
```

**Pattern 3: Router where branches converge — output on the Flow**

When all branches produce the same output type, the Flow output acts as shared terminal:

```python
StreamFlow(
    name="converge",
    source=FromTopic("in", payload=Order),
    process=Process(
        Router(
            key="priority",
            routes={
                "high":   fast_track,
                "normal": standard_process,
            },
        ),
    ),
    output=IntoTopic("out", payload=ProcessedOrder),   # all branches converge here
    errors={...},
)
```

### 8b.3 Validation rules

1. If a Process ends with `IntoTopic`, the Flow must **not** also declare `output` — that would be ambiguous.
2. If a Router has branches that end with `IntoTopic` and branches that do not, branches without explicit terminals fall through to the Flow `output`.
3. If neither the Process nor the Flow declares an output, and the process does not end with `Drain`, the flow is **invalid**.
4. A `Process` used as a Router branch target follows the same rule: if it ends with `IntoTopic`, that is its terminal; otherwise the parent resolution applies.

### 8b.4 Why this works

The developer reads top-to-bottom:
- **Linear flow?** Look at `output=` on the StreamFlow.
- **Router with different destinations?** Look at the end of each branch.
- **Router where everything converges?** Look at `output=` on the StreamFlow.

One rule, three patterns, zero ambiguity.

---

## 9. Message Model

### 9.1 Core message types

```python
class MessageMeta(LoomFrozenStruct, frozen=True):
    """Transport-neutral metadata for one logical event.

    Attributes:
        message_id: Stable event identifier.
        correlation_id: Optional correlation identifier.
        trace_id: Optional trace identifier.
        topic: Source topic name when available.
        partition: Source partition when available.
        offset: Source offset when available.
        key: Optional transport key when available.
        headers: Opaque transport headers.
    """

    message_id: str
    correlation_id: str | None
    trace_id: str | None
    topic: str | None
    partition: int | None
    offset: int | None
    key: bytes | str | None
    headers: dict[str, bytes]


class Message(LoomFrozenStruct, Generic[PayloadT], frozen=True):
    """Logical typed event.

    Attributes:
        payload: Typed event payload.
        meta: Transport-neutral metadata.
    """

    payload: PayloadT
    meta: MessageMeta
```

### 9.2 Why this stays small

We do not need a deep event hierarchy in v1.
`Message` and `MessageMeta` are enough.

---

## 10. Process and Task Model

### 10.1 Composition ownership: Process owns the graph, Task is a pure unit

This is the central design decision for the streaming DSL:

> **Process owns composition. Task has no knowledge of the graph.**

A `Task` is a typed function unit. It does not know what comes before it, what comes after it, or where its output goes. It does not have a `target`. It does not call other tasks.

A `Process` is an ordered sequence of nodes. It defines the wiring: which tasks run, in what order, with what shape adapters, and where routing branches lead.

**Why this matters:**
- A Task with a `target` couples the function to the graph structure — the same Task cannot be reused in two different flows.
- `ForEach` (batch → record) is not business logic — it is graph structure. It belongs to Process, not Task.
- If a Task could call another Task, we would have an implicit graph inside the explicit graph — guaranteed confusion.
- Keeping Tasks pure makes them testable in isolation without mocking graph infrastructure.

### 10.2 `Process`

The process is the visual and architectural center of the streaming graph.

A process is an **ordered sequence of nodes**:

```python
Process(
    ForEach,                                          # shape adapter
    Task(enrich_order, resource=HttpClientFactory),   # record task
    Task(transform_order),                            # record task
)
```

Valid node types inside a Process:
- `Task` — record-oriented execution unit,
- `BatchTask` — batch-oriented execution unit,
- `Router` — branching node,
- `ForEach` — shape adapter (batch → record),
- `CollectBatch` — shape adapter (record → batch),
- `IntoTopic` — terminal topic output boundary,
- `IntoTable` — terminal table sink boundary,
- `Process` — nested sub-process (for router branches).

A process is also a valid **branch target** inside a Router, enabling recursive composition:

```python
Router(
    key="event_type",
    routes={
        "simple": handle_it,
        "complex": Process(
            Task(validate),
            Task(enrich),
            IntoTopic("complex.out"),
        ),
    },
)
```

### 10.3 `Task`

A task consumes one `record` input and may produce:
- `record`
- `many`
- `none`

A task must not implicitly consume `batch`.

A task is defined by:
- a **callable** with a typed signature,
- an optional **resource factory** for worker-local dependencies,
- an optional **name** for observability.

```python
# Minimal: just a function
Task(enrich_order)

# With resource:
Task(enrich_order, resource=HttpClientFactory)

# With explicit name:
Task(enrich_order, resource=HttpClientFactory, name="enrich")
```

The callable signature must be explicit and typed:

```python
def enrich_order(msg: Message[Order], ctx: TaskContext[HttpClient]) -> EnrichedOrder:
    ...
```

A task does **not** have:
- a `target` field,
- knowledge of what comes next in the graph,
- the ability to invoke other tasks,
- hidden internal branching.

### 10.4 `BatchTask`

A batch task consumes `batch` input and may produce:
- `batch`
- `none`

If users want `batch -> record`, they must declare `ForEach` explicitly in the Process.

```python
# The Process owns the batch-to-record conversion, not the BatchTask:
Process(
    BatchTask(aggregate_metrics),   # batch -> batch
    ForEach,                        # batch -> record (Process-level)
    Task(publish_metric),           # record -> record
)
```

### 10.5 Why Task cannot call Task

Consider a developer who wants: "read a batch, then process each record individually."

**Wrong (Task owns composition):**
```python
class ProcessBatch(BatchTask):
    def execute(self, batch):
        for item in batch:
            self.call(process_one, item)  # hidden graph, untraceable
```

**Right (Process owns composition):**
```python
Process(
    ForEach,              # batch -> record, explicit in the graph
    Task(process_one),    # record -> record, pure function
)
```

The Process makes the shape transition visible. The Task stays pure. The graph stays traceable.

### 10.6 Resource lifecycle

Many real use cases require worker-local or process-local resources, for example:
- HTTP clients,
- external SDK clients,
- producers,
- connection pools.

These resources must be explicit in the public design.

Tasks must not create hidden global or ad hoc runtime state.

### 10.7 Resource contract

```python
ResourceT = TypeVar("ResourceT")


class ResourceFactory(Protocol, Generic[ResourceT]):
    """Create and close task resources under runtime control."""

    def create(self) -> ResourceT: ...

    def close(self, resource: ResourceT) -> None: ...
```

```python
class TaskContext(Protocol, Generic[ResourceT]):
    """Execution context with explicit resource access."""

    @property
    def resource(self) -> ResourceT: ...
```

This ensures no hidden state and keeps lifecycle ownership in the runtime adapter.

### 10.8 Async Task Execution

Tasks may declare `async def` callables. The Bytewax infrastructure adapter provides a per-worker persistent asyncio event loop (same pattern as `loom.celery.event_loop.WorkerEventLoop`). When the adapter detects a coroutine function, it wraps execution so the sync Bytewax operator delegates to the background async loop.

The developer writes normal async code inside the task; the bridge is infrastructure concern.

```python
async def enrich_order(msg: Message[Order], ctx: TaskContext[AsyncHttpClient]) -> EnrichedOrder:
    client = ctx.resource
    return await client.fetch_enriched(msg.payload)
```

### 10.9 Worker-Local Resources and Graph Continuity

A common pattern is: a `BatchTask` prepares a resource (e.g., authenticates an HTTP client, opens a bulk loader), then individual `Task` instances consume that resource per row.

Because Bytewax operators are stateless functions that cannot share mutable Python objects across operator boundaries, the resource must be **worker-local**, not message-local. The correct pattern is:

1. Register the resource via `ResourceFactory` as a **worker-local singleton**.
2. The `BatchTask` may "warm" or "configure" the resource, emitting lightweight context (tokens, IDs) per message.
3. Each downstream `Task` receives the context and accesses the same worker-local resource via `TaskContext`.

```python
Process(
    BatchTask(prepare_scraper_session),   # batch -> batch (warms worker-local client)
    ForEach,                              # batch -> record
    Task(scrape_row, resource=HttpClientFactory),  # record -> record (uses same client)
    IntoTopic("scraped.out"),
)
```

The resource factory is invoked once per worker startup, not per message or per batch.

---

## 11. Routing Model

### 11.1 Problem statement

Earlier routing ideas were too closed because they implied a growing list of special router constructors such as:
- `by_type`
- `by_key`
- `by_field`
- `by_header`

That is not sustainable.
Users need routing to remain open for:
- transport key,
- payload fields,
- metadata,
- topic,
- arbitrary conditions,
- custom classification logic,
- combinations of the above.

Additionally, imperative chaining (`.when().then()`) scales poorly when there are many routes, and lambda-heavy APIs are noisy and hard to read at a glance.

### 11.2 Decision

Routing should be **declarative and mapping-based**, not imperative chaining.

Core principles:
1. **One line per route** — a mapping entry or `Route`, not a method chain.
2. **Selectors and predicates are explicit** — `Router.by(...)` for keyed dispatch,
   `Router.when(...)` for ordered predicate dispatch.
3. **No lambdas required for common cases** — use the `msg` expression root, for
   example `msg.payload.amount > 1000`.
4. **Custom logic stays open** — implement `Selector` or `Predicate` when an
   expression is not enough.
5. **Targets are explicit `Process` values** — the compiler validates branch
   shape and output resolution consistently.

### 11.3 Selector-based routing

A selector extracts one routing key from a message and dispatches by value.

```python
Router.by(
    msg.payload.event_type,
    routes={
        "created": Process(HandleCreated()),
        "cancelled": Process(Notify(), Archive()),
        "shipped": Process(
            ValidateShipment(),
            UpdateTracking(),
            IntoTopic("shipments.out"),
        ),
    },
    default=Process(IntoTopic("events.unhandled")),
)
```

The selector accepts:
- `PathRef` expressions from `msg`, such as `msg.payload.status`.
- `Selector[InT]` objects for custom extraction.

Use selector routing for:
- payload field values (`event_type`, `status`, `kind`),
- message key,
- header value,
- tenant identifier,
- any discrete key-based dispatch.

### 11.4 Predicate-based routing

The router evaluates ordered first-match rules.

```python
Router.when(
    routes=[
        Route(msg.payload.amount > 1000, Process(Escalate())),
        Route(msg.payload.region == "LATAM", latam_flow),
        Route(HighValueLatam(), Process(Escalate(), IntoTopic("payments.high-value"))),
    ],
    default=standard_flow,
)
```

Each route is `Route(predicate, process)`. The predicate can be:
- A `Predicate[InT]` instance — any object with `def matches(self, message: Message[InT]) -> bool`.
- An expression node from `msg`, such as `msg.payload.country == "ES"`.

Use predicate routing for:
- field comparisons,
- composite conditions,
- multi-field business rules,
- metadata-dependent branching.

### 11.5 Branch targets

A branch target is a `Process`. Keeping this explicit avoids hidden graph
conversion and makes branch validation predictable.

| Target | Meaning |
|--------|---------|
| `Process(...)` | A sub-process with its own sequence of nodes |
| `Process(..., IntoTopic(...))` | A sub-process with an explicit terminal |
| `Process(...)` without terminal | Branch falls through to the parent flow output |

The compiler validates branch terminals and fallback output resolution.

### 11.6 Public contracts

```python
InT = TypeVar("InT")
KeyT = TypeVar("KeyT")


class Selector(Protocol, Generic[InT, KeyT]):
    """Extract a routing decision key from one message."""

    def select(self, message: Message[InT]) -> KeyT: ...


class Predicate(Protocol, Generic[InT]):
    """Return whether a route matches the message."""

    def matches(self, message: Message[InT]) -> bool: ...
```

### 11.7 Expression helpers

The public helper is the `msg` expression root, backed by reusable
`loom.core.expr` nodes:

```python
msg.payload.region == "LATAM"
msg.payload.amount > 1000
msg.payload.amount < 100
msg.payload.status.isin(("active", "pending"))
```

Expression predicates are evaluated by the compiler/runtime adapter. Custom
predicates remain ordinary classes:

```python
class HighValueLatam(Predicate[Order]):
    def matches(self, message: Message[Order]) -> bool:
        p = message.payload
        return p.amount > 1000 and p.region == "LATAM"
```

Helpers must not define the architecture — they compile to the same `Predicate` / `Selector` protocols.

### 11.8 Router rules

- The router operates on `record` shape.
- If input is `batch`, a `ForEach` must appear before the router in the Process.
- The router does not transform shape.
- All branches of one router must have compatible output contracts or terminate independently.
- If no `default` branch exists, routing completeness must be validated where possible.
- Predicate routing is ordered first-match in v1.
- Fan-out can be modeled explicitly inside branch `Process` nodes when needed.

---

## 12. Error Model

### 12.1 Error categories

Streaming v1 should distinguish at least these categories:

- `wire_error` — decode failure, malformed transport data, incompatible payload version
- `routing_error` — selector failure, missing routing field, invalid routing decision
- `task_error` — unexpected task failure, IO failure, timeout, runtime exception
- `business_reject` — valid message rejected by business logic

### 12.2 Why this separation matters

These categories often require different handling:
- wire errors usually go to DLQ or audit,
- routing errors usually indicate classification or contract problems,
- task errors may be retryable,
- business rejects are often valid domain outcomes.

### 12.3 Public error envelope contract

```python
ErrorPayloadT = TypeVar("ErrorPayloadT")


class ErrorKind(Enum):
    """Logical categories for flow errors."""

    WIRE = "wire"
    ROUTING = "routing"
    TASK = "task"
    BUSINESS = "business"
```

```python
class ErrorEnvelope(LoomFrozenStruct, Generic[ErrorPayloadT], frozen=True):
    """Structured error payload routed through explicit error branches.

    Attributes:
        kind: Logical error category.
        reason: Human-readable reason.
        original_message: Original message when available.
    """

    kind: ErrorKind
    reason: str
    original_message: Message[ErrorPayloadT] | None
```

### 12.4 Error routes are structural

Error branches must be part of the flow graph, not hidden only in runtime configuration.

---

## 13. Bytewax Integration

### 13.1 Core decision

Bytewax is the execution engine.
Loom owns the public process DSL and compiles it into Bytewax operators.

### 13.2 What Loom owns

- public authoring model,
- shape semantics,
- routing semantics,
- payload typing via Loom core model classes,
- resource lifecycle contracts,
- error branch semantics,
- transport-neutral process graph validation.

### 13.3 What Bytewax owns

- execution scheduling,
- worker orchestration,
- batching/window realization,
- runtime scaling,
- low-level dataflow execution.

### 13.4 Architectural consequence

We do **not** need:
- a custom scheduler,
- a custom parallel runtime,
- a custom worker lifecycle engine beyond adapter glue.

A thin infrastructure adapter layer is enough.

### 13.5 Honest Bytewax Operator Mapping

This section documents how DSL concepts compile to concrete Bytewax primitives. It is provided to set realistic expectations and prevent architectural drift.

| DSL concept | Bytewax primitive | Notes |
|---|---|---|
| `Task` | `op.map` | Sync tasks map directly. Async tasks are wrapped to delegate to the per-worker async loop. |
| `BatchTask` | `op.map` over collected batches | `CollectBatch` precedes the task; the task receives `list[Message[T]]`. |
| `CollectBatch` | `op.collect(timeout, max_size)` | Bytewax owns batch accumulation, timeout, and recovery state. |
| `ForEach` | `op.flat_map` emitting one item per batch element | Flattening is handled by the adapter, not the developer. |
| `Router` | `op.flat_map` + tagged unions + downstream `op.filter` | No native "router" operator exists; the adapter generates the branching subgraph. |
| `ErrorEnvelope` | Tagged union (`Ok` / `Err`) + filters | Exceptions inside tasks are caught by the adapter wrapper and converted to `ErrorEnvelope` on a parallel stream. They are never thrown through Bytewax. |
| `IntoTopic` | `KafkaSink` (Bytewax native) | Configured with Loom codec and partition key resolver. Loom does not create a separate producer. |
| `IntoTable` | Custom sink operator or `op.inspect` + adapter write | Table sinks are adapter-defined; Bytewax does not provide a native table sink. |
| `ResourceFactory` | Worker startup hook | `create()` runs once per worker process. `close()` runs on worker shutdown. |

**Critical consequence**: Bytewax does not have multi-output operators or native error branches. The adapter must generate multiple downstream streams and wire them independently. This is feasible but not "free"; it justifies keeping the adapter layer thin and well-tested.

---

## 14. Proposed Public API

The public API should stay small and stable.

### Recommended public surface

**Flow and graph structure:**
- `StreamFlow`
- `Process`
- `Task`
- `BatchTask`
- `Router`
- `FromTopic`
- `IntoTopic`
- `IntoTable`
- `ForEach`
- `CollectBatch`
- `Drain`

**Routing contracts:**
- `Selector` (protocol)
- `Predicate` (protocol)

**Routing helpers (convenience, not architecture):**
- `msg` expression root (`msg.payload.status == "new"`)
- reusable `loom.core.expr` expression nodes and evaluator

**Typing and payload:**
- `LoomStruct` / `LoomFrozenStruct` payload classes
- `Message`
- `MessageMeta`
- `StreamShape`

**Error model:**
- `ErrorEnvelope`
- `ErrorKind`

**Partitioning:**
- `PartitionStrategy`
- `PartitionPolicy`
- `PartitionGuarantee`

**Resource lifecycle:**
- `ResourceFactory`
- `TaskContext`

### Keep internal

- Bytewax translation details,
- Kafka/Redpanda plumbing,
- offsets and commits,
- worker lifecycle internals,
- `KeyRoute`, `PredicateRoute`, `BranchTarget` (compiler-internal, not part of the developer DSL).

---

## 15. Illustrative Authoring Shapes

These examples are **illustrative Python DSL sketches** for design validation only.
They are not implementation code, but they represent what the developer actually writes.

### 15.1 Simple linear pipeline

```python
StreamFlow(
    name="enrich-orders",
    source=FromTopic("orders.in", payload=OrderEventType, shape="record"),
    process=Process(
        Task(validate_order),
        Task(enrich_order, resource=HttpClientFactory),
        Task(transform_order),
    ),
    output=IntoTopic("orders.enriched", payload=EnrichedOrderType),
    errors={
        ErrorKind.WIRE: IntoTopic("orders.dlq.wire"),
        ErrorKind.TASK: IntoTopic("orders.dlq.task"),
    },
)
```

This validates:
- Process is a flat sequence — one line per node.
- Tasks are pure functions, no targets, no graph knowledge.
- Resource lifecycle is explicit on the Task that needs it.
- Error routes are structural, not hidden config.

### 15.2 Batch input, then per-record processing

```python
StreamFlow(
    name="batch-enrich",
    source=FromTopic("orders.in", payload=OrderEventType, shape="batch"),
    process=Process(
        ForEach,                                          # batch -> record (explicit)
        Task(enrich_order, resource=HttpClientFactory),   # record -> record
    ),
    output=IntoTopic("orders.enriched", payload=EnrichedOrderType),
    errors={
        ErrorKind.WIRE: IntoTopic("orders.dlq.wire"),
        ErrorKind.TASK: IntoTopic("orders.dlq.task"),
    },
)
```

This validates:
- `batch -> record` is explicit via `ForEach` in the Process, not hidden inside a Task.
- The source batch shape does not force a batch sink.

### 15.3 Selector routing — branches converge to same output

```python
StreamFlow(
    name="dispatch-by-type",
    source=FromTopic("events.in", payload=EventEnvelopeType),
    process=Process(
        Router(
            key="event_type",
            routes={
                "created":   handle_created,
                "cancelled": handle_cancelled,
                "merged":    handle_merged,
            },
            default=IntoTopic("events.unhandled"),   # default diverges to its own topic
        ),
    ),
    output=IntoTopic("events.processed", payload=ProcessedEventType),  # branches converge here
    errors={
        ErrorKind.ROUTING: IntoTopic("events.routing-error"),
        ErrorKind.TASK:    IntoTopic("events.retry"),
        ErrorKind.BUSINESS: IntoTopic("events.rejected"),
    },
)
```

This validates:
- Routing by field is one line: `key="event_type"`.
- Branches without explicit `IntoTopic` fall through to the Flow `output` (convergence pattern).
- The `default` branch diverges to its own topic — output resolution per branch.
- Each route is one dict entry — scales to 20+ routes without noise.
- No lambdas needed for the common case.

### 15.4 Predicate routing with fan-out

```python
StreamFlow(
    name="route-payments",
    source=FromTopic("payments.in", payload=PaymentType),
    process=Process(
        Router.when(
            routes=[
                Route(msg.payload.amount > 1000, Process(Escalate())),
                Route(msg.payload.country == "ES", spain_flow),
                Route(HighValueSpain(), Process(Escalate(), spain_flow)),
            ],
            default=standard_flow,
        ),
    ),
    output=IntoTopic("payments.processed", payload=ProcessedPaymentType),  # all branches converge
    errors={
        ErrorKind.ROUTING: IntoTopic("payments.routing-error"),
        ErrorKind.TASK:    IntoTopic("payments.retry"),
    },
)
```

This validates:
- Predicate order matters — routes preserve declaration order.
- Predicate dispatch is ordered first-match behavior.
- Shared branch output is represented by the Flow `output`.
- All branches converge to Flow `output` — no explicit terminals needed.
- Custom predicates (`HighValueSpain`) and `msg` expressions coexist cleanly.

### 15.5 Router with divergent branches — each branch terminates explicitly

```python
StreamFlow(
    name="order-dispatch",
    source=FromTopic("orders.in", payload=OrderType),
    process=Process(
        Task(validate_order),
        Router(
            key="status",
            routes={
                "new": Process(
                    Task(assign_warehouse),
                    Task(reserve_inventory, resource=InventoryClientFactory),
                    IntoTopic("orders.assigned"),               # branch terminal
                ),
                "returned": [notify_customer, process_refund, IntoTopic("orders.returns")],  # fan-out + terminal
                "cancelled": IntoTopic("orders.cancelled"),     # direct terminal, no task needed
            },
            default=IntoTopic("orders.unhandled"),
        ),
    ),
    # no output — every branch has its own terminal
    errors={
        ErrorKind.TASK: IntoTopic("orders.retry"),
    },
)
```

This validates:
- Every branch ends with `IntoTopic` — no Flow `output` needed.
- Branches are explicit sub-processes and may terminate independently.
- Task before Router is valid — Process owns the sequence.
- Recursive composition: Process > Router > Process.

### 15.6 Router with mixed resolution — some branches converge, some diverge

```python
StreamFlow(
    name="batch-route",
    source=FromTopic("mixed-events.in", payload=EventEnvelopeType, shape="batch"),
    process=Process(
        ForEach,                              # batch -> record
        Router(
            key="kind",
            routes={
                "invoice": handle_invoice,                          # no terminal → falls through to Flow output
                "refund":  handle_refund,                           # no terminal → falls through to Flow output
                "unknown": IntoTopic("mixed-events.unhandled"),     # diverges to its own topic
            },
            default=IntoTopic("mixed-events.default"),
        ),
    ),
    output=IntoTopic("mixed-events.processed", payload=ProcessedEventType),  # fallback for branches without terminal
    errors={
        ErrorKind.WIRE:    IntoTopic("mixed-events.dlq"),
        ErrorKind.ROUTING: IntoTopic("mixed-events.routing-error"),
        ErrorKind.TASK:    IntoTopic("mixed-events.retry"),
    },
)
```

This validates:
- `ForEach` is required before Router — Process makes this explicit.
- Mixed output resolution: `invoice`/`refund` converge to Flow `output`, `unknown`/`default` diverge.
- The rule is consistent: branch with `IntoTopic` → uses it; branch without → falls through to Flow `output`.

### 15.7 One record generates many outputs

```python
StreamFlow(
    name="fanout-alerts",
    source=FromTopic("alerts.in", payload=AlertType),
    process=Process(
        Task(derive_notifications),           # record -> many
    ),
    output=IntoTopic("notifications.out", payload=NotificationType, shape="many"),
    errors={
        ErrorKind.TASK: IntoTopic("alerts.retry"),
    },
)
```

This validates:
- `record -> many` is fan-out declared by the Task's output shape.
- Fan-out is not the same as batch.

### 15.8 BatchTask followed by per-record publishing

```python
StreamFlow(
    name="aggregate-and-publish",
    source=FromTopic("metrics.in", payload=RawMetricType, shape="batch"),
    process=Process(
        BatchTask(aggregate_metrics),         # batch -> batch
        ForEach,                              # batch -> record (Process-level)
        Task(publish_metric),                 # record -> record
    ),
    output=IntoTopic("metrics.aggregated", payload=AggregatedMetricType),
    errors={
        ErrorKind.TASK: IntoTopic("metrics.retry"),
    },
)
```

This validates:
- BatchTask and Task coexist in the same Process, connected by ForEach.
- The shape transition is Process-level, not hidden inside BatchTask.
- The developer reads the Process top-to-bottom and sees the full pipeline.

---

## 16. Layer Map

We want clean architecture with minimal but explicit layering.

```text
src/loom/streaming/
├── __init__.py
├── _boundary.py            # FromTopic, IntoTopic
├── _errors.py              # ErrorKind, ErrorEnvelope
├── _message.py             # Message, MessageMeta
├── _partitioning.py        # PartitionStrategy, PartitionPolicy, guarantees
├── _process.py             # StreamFlow, Process
├── _resources.py           # ResourceFactory, TaskContext
├── _shape.py               # StreamShape, ForEach, CollectBatch, Drain
├── _task.py                # Task, BatchTask
├── routing/                # Router, Selector, Predicate, expression helpers
├── compiler/               # C5 validators and validated plan
├── kafka/                  # transport config, codecs, records, clients
├── observability/          # observer protocols and implementations
└── bytewax/                # concrete runtime adapter
```

### Dependency direction

- Root declarations and `routing/` must not depend on Bytewax.
- `compiler/` validates and plans declarations.
- `kafka/` owns transport settings, codecs, records, and standalone clients.
- `bytewax/` adapts the validated graph to Bytewax and concrete transports.
- Shared cross-module concerns live in `loom.core`, for example `core.expr`,
  `core.routing`, `core.observability`, and `core.tracing`.

---

## 17. Public API Recommendation

See section 14 for the canonical public surface list. This section is consolidated there to avoid drift.

---

## 18. V1 Scope

### Include in v1

- Bytewax-based execution adapter
- topic-based boundaries
- Loom core model classes as public payload descriptors
- record tasks
- batch tasks
- explicit shape adapters
- selector-based routing
- predicate-based routing
- explicit error branches
- explicit output partition policies
- anti-skew partitioning strategies
- resource lifecycle contracts
- observability hooks
- in-memory testing support

### Exclude from v1

- exactly-once
- schema registry as a mandatory dependency
- durable stateful joins/windows with recovery guarantees
- table-centric sinks as core public API
- a public API centered on ETL-style `Step`
- a large custom executor stack

---

## 19. Architecture Review of This Direction

### Strengths

- Better fit for real streaming use cases.
- Makes shape rules explicit and enforceable.
- Gives `Process` the right architectural weight.
- Keeps routing open without exploding the public API.
- Models business reject and routing failures separately.
- Supports resource-aware task execution without hidden state.
- Keeps Bytewax as runtime instead of reinventing orchestration.

### Risks

- Future schema/version metadata still needs a clear core-level design when it
  becomes necessary.
- Routing validation may become non-trivial when predicate branches are highly dynamic.
- Topic-oriented naming may need future extension if non-topic transports become first-class.

### Risk decision

These risks are acceptable and smaller than the architectural risk of keeping a step-centric, shape-implicit, closed-router design.

---

## 20. Final Decisions

1. **Use Bytewax as the runtime foundation.**
2. **Make `Process` the public graph center.**
3. **Use explicit shape contracts: `record`, `many`, `batch`, `none`.**
4. **Allow no implicit shape conversion.**
5. **Use `FromTopic` / `IntoTopic` instead of generic `FromStream` / `IntoStream`.**
6. **Use explicit partition policies to control output keys and reduce skew.**
7. **Use Loom core model classes directly in the public API.**
8. **Keep routing open through selector-based and predicate-based contracts.**
9. **Separate routing decision logic from branch destination.**
10. **Treat `wire_error`, `routing_error`, `task_error`, and `business_reject` as explicit routable categories.**
11. **Keep task resources explicit through runtime-managed lifecycle contracts.**

---

## 21. Approval Request

This document proposes a smaller, clearer, and more explicit streaming v1.

Before implementation, approval is needed for these architectural choices:
- topic-oriented public boundaries,
- Loom core model classes as the public payload contract,
- `Process` as the main visual abstraction,
- explicit shape adapters,
- open router design based on selectors and predicates,
- explicit routing and error branches,
- runtime-managed resource lifecycle.
