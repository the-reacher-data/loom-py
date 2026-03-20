# Loom Monitor delivery plan

## Context

This plan complements:

- `loom_celery_monitoring_ephemeral_analysis.md`
- `flower2_frontend_architecture.md`

The goal is to turn the idea into a real product with a concrete repo structure and implementation sequence.

This document assumes:

- single pod deployment in v1
- no SQL database
- ephemeral retention
- `aiocache` backend
- backend built with `loom-py`
- frontend built with SvelteKit
- future possibility of moving from in-memory to Redis-backed ephemeral state


## Product position

This product is not just "a UI for Celery".

It should be positioned as:

`Loom Monitor` = a modern operational view for Celery workloads, optimized for recent activity, live visibility and better developer UX than Flower.

Key product ideas:

- single pod like Flower
- much better structure and UX than Flower
- bounded state, not historical warehousing
- live operations first
- future-ready for richer control features


## Clarified technical direction

### Deployment model

For v1:

- one repo
- one image
- one pod

Inside that pod:

- backend API
- event ingestion loop
- frontend static assets or SvelteKit output

This matches the operational simplicity of Flower.

### State backend modes

The product should support two runtime modes from the beginning.

#### Mode 1: pure in-memory

- `aiocache.SimpleMemoryCache`
- fully ephemeral
- zero external dependency

Use for:

- local dev
- demo
- single pod simple deployments

#### Mode 2: Redis-backed ephemeral

- `aiocache` Redis backend
- still ephemeral by policy
- state stored in the same Redis ecosystem already used by Celery if desired

Use for:

- larger installations
- restart resilience
- future multi-process or multi-instance backend split

Important:

This is not a contradiction. The product is still "no database" in the sense of no persistent application database. Redis is an optional ephemeral backing store.


## What the product should show

You asked for more than "task list".

The product should aim to support:

### 1. Recent tasks

- running
- succeeded
- failed
- retried
- revoked if detectable

### 2. Queue visibility

At minimum:

- queue name
- active tasks
- recent throughput
- failures
- retries

Important question: "messages in each queue"

This should be split into two concepts:

- queue backlog from broker
- recent processed tasks from monitor state

Recommendation:

- v1 show processed and running task signals from Celery events
- v2 add broker-level queue depth if the broker supports reliable inspection

Reason:

Broker queue depth depends heavily on transport and is not equally trustworthy everywhere.

### 3. Worker visibility

- online/offline
- heartbeat freshness
- tasks processed recently
- tasks currently active
- throughput over recent windows

### 4. Time graphs

Yes, these should be part of the target product.

Recommended charts:

- tasks processed per minute
- failures per minute
- retries per minute
- average runtime by task name
- worker throughput over time
- queue activity over time

This is feasible even with ephemeral storage if you aggregate into time buckets.

### 5. Task detail

- task id
- task name
- queue
- worker
- timestamps
- retries
- args summary
- kwargs summary
- result snippet if available
- traceback snippet if available
- trace id if present

### 6. Future control operations

You mentioned cancellation and that current Celery revoke is not really enough.

That is correct. Real control is harder than display.

Recommendation:

- v1 is read-only
- v2 introduces safe control actions
- cancellation should be treated as best-effort operational control, not guaranteed kill semantics

Possible future actions:

- revoke
- revoke with terminate
- retry task
- resubmit task
- pause or isolate queue if the broker/runtime allows it

The UI and API should be designed so actions can be added later without changing the core data model.


## Backend architecture decision

The backend should be split into these modules:

1. event ingestion
2. ephemeral state store
3. read models and aggregations
4. query API
5. live stream API
6. optional control API

### Recommended backend shape

```text
apps/backend/src/loom_monitor/
  config/
  domain/
    task/
    worker/
    queue/
    metric/
  application/
    tasks/
    workers/
    queues/
    metrics/
    stream/
  infrastructure/
    celery/
    cache/
    retention/
    projections/
  api/
    rest/
    sse/
  bootstrap/
```

This is intentionally more visual than "by technical layer only".

Your preference for "folder by model" is valid here because the product is highly read-model driven.

Recommendation:

- group by domain area first
- then keep application and infrastructure modules inside those areas or in parallel only where cross-cutting reuse matters

If you want maximum visual clarity, this is even better:

```text
apps/backend/src/loom_monitor/
  task/
    domain/
    application/
    infrastructure/
    api/
  worker/
    domain/
    application/
    infrastructure/
    api/
  queue/
    domain/
    application/
    infrastructure/
    api/
  metric/
    domain/
    application/
    infrastructure/
    api/
  shared/
  bootstrap/
```

For this product, I recommend this second option. It is easier to navigate and closer to how the UI will be reasoned about.


## Frontend architecture decision

The file `flower2_frontend_architecture.md` is directionally good:

- SvelteKit
- SSE
- TanStack Table
- virtualization
- feature-based structure

What I would change:

### 1. Align frontend features with product entities

Use:

- `tasks`
- `workers`
- `queues`
- `metrics`
- `overview`
- `controls` later

### 2. Treat SSE as the transport for now

SSE is the right v1 choice.

Why:

- simpler than WebSocket
- perfect for server-to-client live updates
- easier ops in single pod mode

Future:

- if control actions, collaborative sessions or richer bi-directional behavior grows, WebSocket can be introduced later

### 3. Keep frontend state bounded too

The frontend should not become the accidental database.

It should also respect retention:

- max tasks in memory
- max chart points
- incremental updates only


## Recommended full repo structure

This is the repo I would build.

```text
loom-monitor/
  apps/
    backend/
      pyproject.toml
      src/loom_monitor/
      tests/
    web/
      package.json
      src/
      static/
      tests/
  docker/
    backend.Dockerfile
    app.Dockerfile
  infra/
    compose/
      docker-compose.dev.yml
  docs/
    architecture/
    product/
  Makefile
  README.md
```

### Build strategy

Preferred v1:

- `apps/web` builds static assets
- final image copies those assets into backend image
- backend serves API and frontend

That gives you the single-pod behavior you want.


## Data model and storage strategy

### Task snapshots

Store one snapshot per task.

Required state:

- latest known status
- normalized timestamps
- worker and queue assignment
- execution duration
- retries
- lightweight error payload

### Queue projections

Maintain queue projections incrementally:

- current active estimate
- processed counts by recent time bucket
- failure counts by recent time bucket
- retry counts by recent time bucket

### Worker projections

Maintain worker projections incrementally:

- last heartbeat
- active count
- processed totals in recent windows

### Time series model

Do not store every raw event for charts.

Instead:

- aggregate by fixed bucket
- 10s, 30s or 60s buckets

Example:

- `tasks.succeeded[minute_bucket] += 1`
- `worker.{name}.processed[minute_bucket] += 1`
- `queue.{name}.failed[minute_bucket] += 1`

This keeps memory bounded and makes chart rendering cheap.


## Key product decisions by phase

### Phase 0: architectural spike

Goal:

- prove the event ingestion and cache model

Deliverables:

- connect to Celery events
- ingest `task-*` and `worker-*` events
- write snapshots to memory cache
- expose one debug endpoint

Exit criteria:

- can see task lifecycle changes in real time

### Phase 1: backend MVP

Goal:

- usable API for the UI

Deliverables:

- config model from env vars
- event ingestor loop
- retention by age and count
- task list endpoint
- task detail endpoint
- workers endpoint
- queues endpoint
- overview endpoint
- SSE endpoint

Exit criteria:

- backend alone can power a basic UI reliably

### Phase 2: frontend MVP

Goal:

- replace Flower for day-to-day recent monitoring

Deliverables:

- dashboard page
- tasks page with virtualized table
- workers page
- queues page
- task detail drawer
- live updates via SSE
- filters and sorting

Exit criteria:

- users can use it instead of Flower for the common path

### Phase 3: ephemeral analytics

Goal:

- better operational insight than Flower

Deliverables:

- throughput charts
- failure charts
- retry charts
- worker processing charts
- queue activity charts

Exit criteria:

- charts are stable, cheap and useful

### Phase 4: operational controls

Goal:

- start adding action capabilities carefully

Deliverables:

- safe action API shape
- revoke UI
- explicit warnings on best-effort semantics
- audit log in memory or event stream for issued actions

Exit criteria:

- operators can issue safe task control operations knowingly

### Phase 5: production hardening

Goal:

- make it open-source quality

Deliverables:

- tests
- packaging
- docs
- examples
- Redis-backed mode
- load testing on large task volumes


## Ordered implementation steps

This is the concrete order I would execute.

1. Define backend config from env vars.
2. Implement cache-backed retention primitives.
3. Implement normalized event DTOs.
4. Build Celery event receiver and ingestion loop.
5. Build task snapshot store.
6. Build worker snapshot store.
7. Build queue projection store.
8. Build time-bucket metrics aggregation.
9. Expose read-only REST API.
10. Expose SSE stream.
11. Build frontend shell and routing.
12. Build tasks table first.
13. Build task detail drawer.
14. Build workers and queues pages.
15. Add overview dashboard cards.
16. Add charts.
17. Package single image.
18. Add Redis-backed mode.
19. Add action API abstraction.
20. Add revoke as first best-effort control.


## Environment variables

Suggested first contract:

```env
LOOM_MONITOR_HOST=0.0.0.0
LOOM_MONITOR_PORT=8080

LOOM_MONITOR_CELERY_BROKER_URL=redis://redis:6379/0
LOOM_MONITOR_CELERY_APP=

LOOM_MONITOR_CACHE_BACKEND=memory
LOOM_MONITOR_REDIS_URL=redis://redis:6379/9

LOOM_MONITOR_MAX_TASKS=10000
LOOM_MONITOR_MAX_AGE_SECONDS=21600
LOOM_MONITOR_WORKER_TTL_SECONDS=120

LOOM_MONITOR_BUCKET_SECONDS=60
LOOM_MONITOR_MAX_BUCKETS=240

LOOM_MONITOR_ENABLE_SSE=1
LOOM_MONITOR_ENABLE_CONTROLS=0
```


## UX scope for the first polished release

To keep the project focused, the first polished release should include only:

- overview dashboard
- tasks table
- task details
- worker page
- queues page
- live updates
- recent throughput charts

It should not include yet:

- complex inspect controls
- broker management
- role model
- historical persistence


## Main design rules

1. The backend is ephemeral by design.
2. The frontend is also bounded in memory.
3. The single pod is the default deployment shape.
4. Redis is optional, not mandatory.
5. SSE is the default realtime transport.
6. Task control features are future capabilities, not v1 assumptions.
7. Repo structure should be domain-visual, not hidden behind generic folders.
8. Charts must come from bucketed aggregates, not raw event history.


## Recommendation on naming

Avoid naming it internally as "Flower 2.0" in the codebase.

That is useful as a mental shorthand, but weak as a product identity.

Use:

- repo: `loom-monitor`
- package: `loom_monitor`
- UI title: `Loom Monitor`


## Final recommendation

The clean path is:

- keep `loom-py` as the framework
- create a separate `loom-monitor` repo
- ship v1 as a single pod
- use in-memory cache first
- keep Redis mode available early
- start with read-only live monitoring
- add charts before control actions
- treat revoke and cancellation as a future best-effort operator feature

This sequence gives you a realistic, shippable path to a better Flower replacement without over-engineering the first version.
