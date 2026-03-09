# Celery worker

`loom-kernel` bootstraps Celery workers from the same config and discovery system used
by the REST API. Jobs are first-class citizens — they support `Input()`, `LoadById()`,
and `Rule` just like use cases.

## Install

```bash
pip install "loom-kernel[celery]"
```

## Define a job

Jobs declare a `__queue__` and an `execute()` signature. `LoadById` loads the entity
automatically before `execute()` runs:

```python
from loom.core.command import Command
from loom.core.job.job import Job
from loom.core.use_case import Input, LoadById

from app.product.model import Product


class SendRestockEmailJobCommand(Command, frozen=True):
    product_id: int
    recipient_email: str


class SendRestockEmailJob(Job[bool]):
    """Send a restock notification email for a product."""

    __queue__ = "notifications"

    async def execute(
        self,
        product_id: int,
        cmd: SendRestockEmailJobCommand = Input(),
        product: Product = LoadById(Product, by="product_id"),
    ) -> bool:
        if product.stock > 0:
            return False
        # send email to cmd.recipient_email …
        return True
```

## Dispatch from a use case

Use `JobService.dispatch()` to enqueue a job and optionally attach callbacks:

```python
from loom.core.job.service import JobService
from loom.core.use_case.use_case import UseCase

class DispatchRestockEmailUseCase(UseCase[Product, DispatchRestockEmailResponse]):
    def __init__(self, job_service: JobService) -> None:
        self._jobs = job_service

    async def execute(
        self,
        product_id: str,
        cmd: DispatchRestockEmailCommand = Input(),
    ) -> DispatchRestockEmailResponse:
        handle = self._jobs.dispatch(
            SendRestockEmailJob,
            params={"product_id": int(product_id)},
            payload={
                "product_id": int(product_id),
                "recipient_email": cmd.recipient_email,
            },
            on_success=RestockEmailSuccessCallback,
            on_failure=RestockEmailFailureCallback,
        )
        return DispatchRestockEmailResponse(job_id=handle.job_id, queue=handle.queue)
```

`handle.job_id` is a stable string ID you can return to the caller.

## Run a job inline (no queue)

`JobService.run()` executes a job immediately in the calling process — useful when
you need the result before the response is returned, e.g. in a workflow step that
must inspect the output before continuing.

This works in **both** `InlineJobService` (local/test) and `CeleryJobService`
(production with broker). When Celery is configured, `run()` bypasses the broker
entirely: the job executes in the API worker process using the same DI container.
Use `dispatch()` for fire-and-forget work; use `run()` when you need the result now.

```python
class BuildProductSummaryUseCase(UseCase[Product, ProductSummaryResponse]):
    def __init__(self, job_service: JobService) -> None:
        self._jobs = job_service

    async def execute(self, product_id: str) -> ProductSummaryResponse:
        summary = await self._jobs.run(
            BuildProductSummaryJob,
            params={"product_id": int(product_id)},
        )
        return ProductSummaryResponse(product_id=int(product_id), summary=summary)
```

## Callbacks

Callbacks are resolved by the DI container and can call back into the application via
`ApplicationInvoker`. Implement `on_success` and/or `on_failure`:

```python
from loom.core.use_case.invoker import ApplicationInvoker

class RestockEmailSuccessCallback:
    def __init__(self, app: ApplicationInvoker) -> None:
        self._app = app

    async def on_success(self, job_id: str, result: Any, **context: Any) -> None:
        if not bool(result):
            return
        product_id = context.get("product_id")
        entity = self._app.entity(Product)
        product = await entity.get(params={"id": product_id})
        if product:
            await entity.update(
                params={"id": product_id},
                payload={"category": f"{product.category}-restock-notified"},
            )

    def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **context: Any) -> None:
        pass  # log or alert
```

## Worker config

```yaml
# config/celery.yaml
celery:
  broker_url: ${oc.env:CELERY_BROKER_URL,redis://redis:6379/1}
  result_backend: ${oc.env:CELERY_RESULT_BACKEND,redis://redis:6379/2}
  worker_concurrency: 2
  worker_prefetch_multiplier: 1
  task_serializer: json
  queues: [default, notifications, analytics, erp]
  task_default_queue: default
```

`task_default_queue` controls where tasks without an explicit `queue=` are routed —
including the `link` / `link_error` callback signatures produced by `dispatch()`.
It must be one of the declared `queues`; the framework raises `ValueError` at
bootstrap if it is not. Defaulting to `"default"` ensures callbacks land on a queue
the worker actually consumes.

## Worker entry point

```python
# src/app/worker_main.py
from loom.celery import create_app

celery_app = create_app("config/worker.yaml")
```

Start the worker:

```bash
uv run celery -A app.worker_main:celery_app worker --loglevel=INFO
```

## Bootstrap options

### Option 1: Discovery from config

Let the framework discover jobs from the same modules declared in `app.discovery`:

```yaml
# config/worker.yaml
app:
  discovery:
    mode: modules
    modules:
      include:
        - app.product.jobs
        - app.product.callbacks
        - app.product.use_cases

celery:
  broker_url: ${oc.env:CELERY_BROKER_URL,redis://redis:6379/1}
  result_backend: ${oc.env:CELERY_RESULT_BACKEND,redis://redis:6379/2}
```

### Option 2: Manifest (explicit registry)

```python
# app/manifest.py
JOBS = [SendRestockEmailJob, BuildProductSummaryJob, SyncProductToErpJob]
CALLBACKS = [RestockEmailSuccessCallback, RestockEmailFailureCallback]
```

```yaml
# config/worker.yaml
app:
  discovery:
    mode: manifest
    manifest:
      module: app.manifest
```

### Option 3: Explicit job list in code

```python
result = bootstrap_worker(
    "config/worker.yaml",
    jobs=[SendRestockEmailJob, BuildProductSummaryJob],
)
```

If any callback invokes `ApplicationInvoker` (e.g. `app.entity(Product).get(...)`),
the corresponding use-cases must be compiled in the worker process. Pass the interfaces
whose routes the callbacks interact with via `interfaces=`:

```python
result = bootstrap_worker(
    "config/worker.yaml",
    jobs=[SendRestockEmailJob],
    interfaces=[ProductInterface],
    callbacks=[RestockCallback],
)
```

The bootstrap extracts all use-case classes from the interface routes automatically —
including AutoCRUD-generated ones that are not importable by name.

For non-AutoCRUD use-cases you can also pass them directly via `use_cases=`.
Both parameters can be combined.

> **Note**: when using discovery (`app.discovery` in YAML), all use-cases are included
> automatically and neither `interfaces=` nor `use_cases=` is needed.

## Docker-compose stack

The companion [`dummy-loom`](https://github.com/the-reacher-data/dummy-loom) ships a
full compose stack with postgres, redis, API, worker, and Flower:

```bash
make up      # start all services
make logs    # follow logs for all containers
```

Services:

| Service | Port | Description |
| --- | --- | --- |
| `api` | 8000 | FastAPI application |
| `worker` | — | Celery worker (queues: notifications, analytics, erp) |
| `flower` | 5555 | Celery Flower dashboard |
| `postgres` | 5432 | PostgreSQL 16 |
| `redis` | 6379 | Broker and result backend |

Flower is available at `http://localhost:5555` after `make up`.
