# Loom Celery Monitoring: ephemeral Flower replacement

## Objective

Design a Celery monitoring product built on top of `loom-py` that can replace the main operational use of Flower, but with these constraints:

- no relational database
- ephemeral state only
- `aiocache` as the storage backend
- retention controlled by environment variables
- frontend built with Svelte + SvelteKit
- packaged from a dedicated repo that can build and ship the whole container

The product goal is not "historical BI for Celery". The goal is:

- live view of worker and task activity
- recent task inspection
- bounded retention in memory/cache
- easy deploy, easy destroy, no migrations


## What the current `loom-py` code already gives us

From the current codebase, the important capabilities are already present:

- `loom.celery` is a formal adapter, not an afterthought.
- Jobs are first-class runtime units and execute through `RuntimeExecutor`.
- `loom-py` already emits lifecycle events such as:
  - `JOB_DISPATCHED`
  - `JOB_STARTED`
  - `JOB_SUCCEEDED`
  - `JOB_RETRYING`
  - `JOB_EXHAUSTED`
- `loom-py` already has `FastAPI` integration.
- `loom-py` already has `msgspec`-centric models and serializers.
- `loom-py` already has an `aiocache` abstraction via `CacheGateway`.

This means the monitoring product can fit the existing architecture well:

- `msgspec` structs for task snapshots
- `loom` use cases for query APIs
- `FastAPI` adapter for HTTP and WebSocket APIs
- `aiocache` for ephemeral retention
- optional Prometheus metrics for infra-level monitoring


## Important product decision

This should not try to be a full clone of Flower.

Flower is centered around:

- broad Celery inspect functionality
- persistent-ish operational dashboard behavior
- lots of broker-level visibility

What you want is narrower and more productized:

- recent task visibility
- bounded memory
- explicit retention rules
- better frontend UX
- easier deploy story

That is a better fit for a Loom-style monitor than for a generic Celery admin tool.


## Product definition

Working concept:

`Loom Monitor` = an ephemeral Celery observability UI for recent activity.

Core product features:

1. Live task stream.
2. Recent task list with filters.
3. Task detail page.
4. Worker health snapshot.
5. Queue-level summary.
6. Bounded retention by count and/or age.
7. Zero SQL persistence.

Non-goals for v1:

- long-term analytics
- permanent audit history
- complex role-based auth
- advanced broker management
- write operations against Celery workers beyond safe inspect actions


## Proposed architecture

### High-level shape

Use one backend service and one frontend app, with the option to package both into one deployable container at the beginning.

Logical components:

1. Celery event ingestor
2. Ephemeral state store
3. Query API
4. Live push API
5. SvelteKit frontend

### Backend components

#### 1. Event ingestor

A dedicated process connects to the Celery broker event stream and normalizes events into internal snapshots.

Input sources:

- Celery event bus
- optionally Loom runtime events when dispatch happens from `loom-py`

Main responsibility:

- transform raw Celery events into a stable task view

Examples of tracked transitions:

- `sent`
- `received`
- `started`
- `retried`
- `succeeded`
- `failed`
- `revoked`

Recommended implementation:

- separate worker-like process inside the monitor backend
- subscribes using Celery event receiver
- writes task snapshots into `aiocache`

#### 2. Ephemeral state store

Use `aiocache` as the only persistence layer.

Recommended storage strategy:

- one key per task snapshot
- one recent ordered index per queue
- one recent ordered index global
- one worker status key per worker
- one queue stats key per queue

Important: no permanent storage, no migrations, no SQL tables.

#### 3. Query API

Serve read-only APIs for the frontend.

Recommended endpoints:

- `GET /api/overview`
- `GET /api/tasks`
- `GET /api/tasks/{task_id}`
- `GET /api/workers`
- `GET /api/queues`

Recommended filters:

- state
- task name
- queue
- worker
- trace id
- last N

#### 4. Live push API

Use WebSocket or SSE for real-time updates.

Recommendation:

- WebSocket if you want richer client state updates
- SSE if you want simpler infra and only server-to-client updates

For this use case, SSE is probably enough for v1.


## Why `aiocache` fits

`aiocache` is a good match here because:

- you explicitly want ephemeral storage
- the data is cache-shaped
- TTL matters more than durability
- `loom-py` already knows this stack

Recommended backend modes:

### Mode A: single-instance memory mode

Use `aiocache.SimpleMemoryCache`.

Good for:

- local development
- demos
- small deployments
- single replica monitor

Pros:

- zero external dependency
- truly ephemeral
- fastest setup

Cons:

- state disappears on restart
- cannot scale horizontally

### Mode B: ephemeral Redis mode

Use `aiocache` with Redis backend, but still treat it as ephemeral.

Good for:

- multi-instance monitor
- safer restart behavior
- shared state across API and ingestor processes

Pros:

- still no SQL
- easy horizontal scale
- TTL support

Cons:

- introduces one infrastructure dependency

Important distinction:

"No database" should mean no persistent relational store. Ephemeral Redis is still acceptable if needed operationally.


## Retention model

This is the key product behavior.

The user asked for retention by:

- maximum number of tasks
- maximum time

That should be supported simultaneously.

Recommended environment variables:

```env
LOOM_MONITOR_MAX_TASKS=5000
LOOM_MONITOR_MAX_AGE_SECONDS=86400
LOOM_MONITOR_CACHE_BACKEND=memory
LOOM_MONITOR_CACHE_TTL_SECONDS=86400
LOOM_MONITOR_BROKER_URL=redis://redis:6379/0
LOOM_MONITOR_CELERY_APP=package.module:celery_app
LOOM_MONITOR_ENABLE_WEBSOCKET=0
LOOM_MONITOR_ENABLE_SSE=1
LOOM_MONITOR_WORKER_SNAPSHOT_TTL_SECONDS=120
```

Retention behavior:

- if a task is older than `MAX_AGE_SECONDS`, drop it
- if the number of tracked tasks exceeds `MAX_TASKS`, evict oldest first
- task detail is best-effort only within the retention window

Recommended policy:

- age-based TTL is the primary cleanup
- count-based eviction is the safety valve


## Internal backend model

Use `msgspec.Struct` for all monitor state.

Suggested structs:

### `TaskSnapshot`

Fields:

- `task_id`
- `task_name`
- `state`
- `queue`
- `worker`
- `args_summary`
- `kwargs_summary`
- `trace_id`
- `retries`
- `received_at`
- `started_at`
- `finished_at`
- `duration_ms`
- `error_type`
- `error_message`

### `WorkerSnapshot`

Fields:

- `worker_name`
- `status`
- `last_seen_at`
- `active_tasks`
- `processed_count`
- `load_avg` if available

### `QueueSnapshot`

Fields:

- `queue_name`
- `active`
- `succeeded_recent`
- `failed_recent`
- `retried_recent`
- `oldest_running_age_seconds`


## Recommended cache layout

Example key design:

- `loom-monitor:task:{task_id}`
- `loom-monitor:index:tasks:recent`
- `loom-monitor:index:queue:{queue_name}`
- `loom-monitor:worker:{worker_name}`
- `loom-monitor:queue:{queue_name}`

Data structure guidance:

- task snapshot as serialized object
- recent indexes as bounded lists
- queue and worker snapshots as individual keys

If using Redis backend:

- list or sorted set for recency indexes

If using `SimpleMemoryCache`:

- keep indexes in process memory and mirror summary entries in cache


## How this fits into `loom-py`

Yes, this can be built with `loom-py`.

Recommended split:

### Use `loom-py` for:

- API backend
- typed config
- `msgspec` models
- use-case execution
- DI bootstrap
- FastAPI adapter
- optional metrics exposure

### Do not force `loom-py` where it adds no value

The Celery event consumer does not need to pretend it is a business use case if it is only an ingestion loop.

Better approach:

- ingestion service module can be plain infrastructure code
- query layer and API can use Loom use cases cleanly

This keeps the architecture honest.


## Suggested backend package structure

Inside the monitoring repo:

```text
apps/
  monitor-backend/
    src/
      monitor/
        config/
        domain/
          models.py
        application/
          use_cases.py
        infrastructure/
          celery_events.py
          cache_store.py
          retention.py
          projections.py
        api/
          interfaces.py
          app.py
```

Recommended responsibility split:

- `domain`: snapshot types and read models
- `application`: query use cases
- `infrastructure`: Celery event subscription + aiocache store
- `api`: Loom/FastAPI interfaces


## Frontend proposal: Svelte + SvelteKit

SvelteKit is a strong fit here.

Why:

- excellent for dashboards
- easy SSR or SPA mode
- good real-time UI ergonomics
- can be built into a compact production artifact

Suggested frontend routes:

- `/`
- `/tasks`
- `/tasks/[taskId]`
- `/workers`
- `/queues`

Suggested UI components:

- overview cards
- live event stream
- task table
- task timeline panel
- worker health panel
- queue status panel
- state filter chips

Frontend data flow:

- initial page data from REST
- real-time updates from SSE/WebSocket
- local stores for client-side filtering


## Single-container vs multi-container

You asked for a repo that can create the whole container.

### Recommended starting point: one repo, one image

Best v1 option:

- build SvelteKit frontend
- serve frontend assets from the backend container
- expose one HTTP port

This keeps deployment simple.

Suggested runtime inside the image:

- monitor backend process
- static frontend assets served by backend
- optional background event ingestor thread/process

This is acceptable because the product is operational tooling, not a high-scale public app.

### When to split later

Split into separate containers only if:

- frontend and backend need independent scaling
- websocket/SSE load grows
- ingestion becomes heavy
- you need stronger isolation


## Recommended repo shape

```text
loom-monitor/
  apps/
    backend/
    web/
  docker/
  docker-compose.yml
  Makefile
  README.md
```

For the "single deployable unit" requirement, the build pipeline can:

1. build the SvelteKit app
2. copy the built assets into the backend image
3. run the backend service as the container entrypoint


## API proposal

Minimal API contract:

### `GET /api/overview`

Returns:

- tasks running
- tasks succeeded recently
- tasks failed recently
- retries recently
- workers online
- queues active

### `GET /api/tasks`

Query params:

- `state`
- `queue`
- `worker`
- `task_name`
- `limit`

### `GET /api/tasks/{task_id}`

Returns full snapshot plus state transition timestamps.

### `GET /api/workers`

Returns current workers and heartbeat freshness.

### `GET /api/queues`

Returns recent queue-level aggregates.

### `GET /api/events/stream`

SSE endpoint for live updates.


## Event ingestion design

Recommended event pipeline:

1. listen to Celery events
2. normalize into internal event DTO
3. update `TaskSnapshot`
4. update queue counters
5. update worker heartbeat
6. publish lightweight live update event to connected clients
7. run retention cleanup

Cleanup should be incremental, not a full scan.

Recommended approach:

- on each write, enforce bounded index size
- periodic cleanup loop for TTL expiration


## Observability of the monitor itself

Do not stop at monitoring Celery. Monitor the monitor.

Recommended metrics:

- events ingested per second
- task snapshots stored
- task evictions by count
- task evictions by age
- websocket or SSE clients connected
- event processing latency
- cache write latency
- cache read latency
- stale worker count

If you already use Prometheus in the ecosystem, expose a `/metrics` endpoint.


## Main technical risk

The main risk is not UI. The main risk is event consistency.

Examples:

- monitor restarts and misses events
- broker events arrive out of order
- no result backend means some fields may be incomplete
- multi-worker timing can create transient state confusion

Mitigation:

- define task state transitions as "best effort recent ops view"
- design idempotent snapshot updates
- accept eventual consistency for the dashboard
- never market it as an audit log


## Why this is better than Flower for your use case

For your stated goals, this design improves on Flower because it is:

- product-shaped instead of generic
- simpler to operate
- bounded in memory
- aligned with Loom architecture
- ready for a custom frontend
- easier to embed into your stack


## What I would implement first

### Phase 1

- Celery event ingestor
- in-memory `aiocache` backend
- task list API
- task detail API
- overview API
- SSE live updates
- simple SvelteKit dashboard

### Phase 2

- Redis-backed ephemeral mode
- worker and queue views
- richer filters
- trace correlation
- per-task timeline

### Phase 3

- optional safe inspect features
- deployment presets
- auth layer if needed


## Strong recommendation

Build this as a separate repo that uses `loom-py` as the backend framework, not as a feature crammed directly into `loom-py`.

Reason:

- the monitor is a product/application
- `loom-py` is the framework/kernel
- you keep the framework clean
- you can still extract reusable monitor primitives back into `loom-py` later if they prove generic

Recommended positioning:

- `loom-py`: framework for APIs and background execution
- `loom-monitor`: ephemeral operational UI for Celery workloads built with Loom


## Final conclusion

Yes, this idea is viable with `loom-py`.

The cleanest v1 is:

- backend built with `loom-py`
- event ingestion from Celery
- ephemeral state in `aiocache`
- retention bounded by `MAX_TASKS` and `MAX_AGE_SECONDS`
- SvelteKit frontend
- one repo that builds one deployable container

That gives you a realistic "Flower replacement for recent activity" without dragging in a database or pretending to solve historical observability.
