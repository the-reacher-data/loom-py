# Use-case DSL

The use-case DSL lets you declare inputs, validations, derived values, and
pre-conditions in a declarative, composable style. The engine resolves everything
before `execute()` runs — keeping the body focused on the business outcome.

---

## Anatomy of a use case

```python
from loom.core.use_case import Compute, Exists, F, Input, LoadById, OnMissing, Rule
from loom.core.use_case.use_case import UseCase

from app.product.model import Product
from app.product.schemas import CreateProduct


class CreateProductUseCase(UseCase[Product, Product]):
    #            ↑ model   ↑ return type

    computes = [CREATE_NORMALIZE_NAME, CREATE_NORMALIZE_PRICE]   # run first
    rules    = [CREATE_NAME_RULE, CREATE_PRICE_RULE]             # run after computes

    async def execute(
        self,
        cmd: CreateProduct = Input(),           # body ← JSON-decoded command
    ) -> Product:
        return await self.main_repo.create(cmd)
```

Execution order for every request:

```
1. Computes   — derive / normalise command fields in declaration order
2. Rules      — validate; any failure raises 422 before execute() is called
3. execute()  — your business logic, guaranteed clean inputs
```

`self.main_repo` is the repository for the model declared in `UseCase[Model, ...]` — no
injection boilerplate needed.

---

## Commands

A `Command` is a typed, immutable input object. Declare it with `msgspec.Struct`
conventions — the framework decodes the HTTP body into it automatically:

```python
from loom.core.command import Command, Patch


class CreateProduct(Command, frozen=True):
    name: str
    price: float
    sku: str


class UpdateProduct(Command, frozen=True):
    # Patch[T] marks a field as optional in partial updates.
    # None means "not provided" — not "set to null".
    name:  Patch[str]   = None
    price: Patch[float] = None
    sku:   Patch[str]   = None
```

> `Patch[T]` is a union type alias for `T | None` with framework-level tracking
> of which fields were actually sent. Rules can gate on whether a `Patch` field
> is present using `.when_present(...)`.

---

## F — typed field reference

`F(CommandClass).field` creates a typed reference to a command field. It is used
everywhere the DSL needs to identify a specific field — in `Compute`, `Rule`, and
predicate conditions:

```python
from loom.core.use_case import F

# reference to the 'name' field of CreateProduct
F(CreateProduct).name

# reference to the 'price' field of UpdateProduct
F(UpdateProduct).price
```

Field references are type-safe. Accessing a field that does not exist on the
command raises an error at bootstrap time, not at runtime.

---

## Input markers

`Input()` in the `execute()` signature tells the engine to inject the decoded
command body as that parameter:

```python
async def execute(self, cmd: CreateProduct = Input()) -> Product: ...
```

`LoadById` fetches an entity by ID from a path or command parameter before
`execute()` runs — the entity is available in rules and the body:

```python
from loom.core.use_case import LoadById

async def execute(
    self,
    product_id: int,                                    # path parameter
    cmd: UpdateProduct = Input(),
    product: Product = LoadById(Product, by="product_id"),  # loaded before execute
) -> Product | None:
    # product is guaranteed to exist here if OnMissing.RAISE (default)
    return await self.main_repo.update(product_id, cmd)
```

`LoadById` parameters:

| Parameter | Description |
|-----------|-------------|
| `model` | Entity class to load |
| `by` | Name of the path/command param that holds the ID |
| `on_missing` | `OnMissing.RAISE` (default) → 404; `OnMissing.NONE` → `None` |

`Exists` checks a DB condition and injects a `bool` — without loading the entity:

```python
from loom.core.use_case import Exists, OnMissing

async def execute(
    self,
    user_id: int,
    cmd: CreateAddress = Input(),
    _user_exists: bool = Exists(
        User,
        from_param="user_id",    # check User.id == user_id
        against="id",
        on_missing=OnMissing.RAISE,  # auto-404 if False
    ),
) -> Address:
    return await self.main_repo.create(...)
```

`Exists` parameters:

| Parameter | Description |
|-----------|-------------|
| `model` | Entity class to check |
| `from_param` | Path parameter whose value is compared |
| `from_command` | Command field whose value is compared |
| `against` | Entity field to compare against |
| `on_missing` | `RAISE` → 404 immediately; `NONE` → inject False |

---

## Compute — derive and normalise fields

`Compute` derives or normalises a command field before rules run. This keeps
normalisation logic out of both the command and the execute body:

```python
from loom.core.use_case import Compute, F


def _normalize_name(value: str) -> str:
    return value.strip()


def _normalize_price(value: float) -> float:
    return round(value, 2)


# Simple normalisation — takes one field, returns the normalised value
CREATE_NORMALIZE_NAME = Compute.set(F(CreateProduct).name).from_command(
    F(CreateProduct).name, via=_normalize_name
)

CREATE_NORMALIZE_PRICE = Compute.set(F(CreateProduct).price).from_command(
    F(CreateProduct).price, via=_normalize_price
)
```

Computes can read multiple fields:

```python
def _compute_subtotal(unit_price: float, quantity: int) -> float:
    return unit_price * quantity

# Derives subtotal from unit_price × quantity
CREATE_SUBTOTAL = Compute.set(F(PricingCommand).subtotal).from_command(
    F(PricingCommand).unit_price,
    F(PricingCommand).quantity,
    via=_compute_subtotal,
)
```

Computes can also read path parameters via `.from_params(...)`:

```python
# Normalise name — but only if product_id != "1" (system product)
UPDATE_NORMALIZE_NAME = (
    Compute.set(F(UpdateProduct).name)
    .from_command(F(UpdateProduct).name, via=_normalize_name_with_context)
    .from_params("product_id")
    .when_present(F(UpdateProduct).name)   # skip if name was not sent
)
```

Computes run in **declaration order** — a later compute can reference a field
already set by an earlier one (e.g. subtotal → tax_amount).

### Apply computes conditionally

`.when_present(field)` skips the compute when the `Patch` field was not provided.
Essential for partial-update commands:

```python
UPDATE_NORMALIZE_PRICE = (
    Compute.set(F(UpdateProduct).price)
    .from_command(F(UpdateProduct).price, via=_normalize_price)
    .when_present(F(UpdateProduct).price)   # skip when price is absent from PATCH body
)
```

---

## Rule — validate before execute

Rules run after all computes. A failing rule raises a structured 422 response
before `execute()` is called.

### Rule.check — field validation

`Rule.check` validates a single field. The `via=` function returns an error
string when the value is invalid, or `None` when it is valid:

```python
from loom.core.use_case import Rule, F


def _name_must_not_be_blank(name: str) -> str | None:
    return None if name.strip() else "name must not be blank"


def _price_must_be_positive(price: float) -> str | None:
    return None if price > 0 else "price must be positive"


CREATE_NAME_RULE  = Rule.check(F(CreateProduct).name,  via=_name_must_not_be_blank)
CREATE_PRICE_RULE = Rule.check(F(CreateProduct).price, via=_price_must_be_positive)
```

### Rule.forbid — invariant enforcement

`Rule.forbid` declares a condition that **must not be true**. The predicate
receives the arguments you attach; it returns `True` when the forbidden condition
holds (triggering the error):

```python
def _patch_payload_is_empty(_cmd: UpdateProduct, fields: frozenset[str]) -> bool:
    """Forbid empty PATCH bodies."""
    return len(fields) == 0


# from_command() with no arguments injects (cmd, fields_set_frozenset)
UPDATE_NOT_EMPTY_RULE = Rule.forbid(
    _patch_payload_is_empty,
    message="at least one field must be provided",
).from_command()
```

### Conditional rules with .when_present

`.when_present(field)` skips the rule entirely when a `Patch` field was not
provided. Use this to avoid running patch validations when the field was not sent:

```python
UPDATE_NAME_RULE = Rule.check(
    F(UpdateProduct).name,
    via=_name_must_not_be_blank,
).when_present(F(UpdateProduct).name)

UPDATE_PRICE_RULE = Rule.check(
    F(UpdateProduct).price,
    via=_price_must_be_positive,
).when_present(F(UpdateProduct).price)
```

### Rules with path parameters

Use `.from_params(...)` when validation needs a value from the request path
(e.g. to check a system-protected entity by ID):

```python
def _is_system_product_name_update_forbidden(
    _cmd: UpdateProduct,
    _fields_set: frozenset[str],
    product_id: str,
) -> bool:
    """Prevent renaming the system product (id=1)."""
    return str(product_id) == "1"


UPDATE_SYSTEM_NAME_IMMUTABLE_RULE = (
    Rule.forbid(
        _is_system_product_name_update_forbidden,
        message="system product name cannot be changed",
    )
    .from_command(F(UpdateProduct).name)
    .from_params("product_id")
    .when_present(F(UpdateProduct).name)
)
```

### Rules with multiple fields

`.from_command(field1, field2, ...)` passes multiple values to the predicate:

```python
def _name_cannot_match_price(name: str | None, price: float | None) -> bool:
    if name is None or price is None:
        return False
    return name.strip() == str(price)


UPDATE_NAME_PRICE_MISMATCH_RULE = (
    Rule.forbid(
        _name_cannot_match_price,
        message="name cannot be equal to price",
    )
    .from_command(F(UpdateProduct).name, F(UpdateProduct).price)
    .when_present(F(UpdateProduct).name & F(UpdateProduct).price)
)
```

---

## Predicate composition

`.when_present(...)` accepts composed predicates using `&` (AND) and `|` (OR):

```python
# Run only when BOTH name AND price are present in the request
.when_present(F(UpdateProduct).name & F(UpdateProduct).price)

# Run when EITHER field is present
.when_present(F(UpdateProduct).name | F(UpdateProduct).price)
```

---

## Full use case example — update with all DSL features

```python
class UpdateProductUseCase(UseCase[Product, Product | None]):
    computes = [
        UPDATE_NORMALIZE_NAME,    # strip whitespace — skipped if name absent
        UPDATE_NORMALIZE_PRICE,   # round to 2 dp  — skipped if price absent
    ]
    rules = [
        UPDATE_NOT_EMPTY_RULE,              # reject empty PATCH body
        UPDATE_NAME_RULE,                   # name not blank (if present)
        UPDATE_PRICE_RULE,                  # price positive (if present)
        UPDATE_SYSTEM_NAME_IMMUTABLE_RULE,  # block rename of id=1 (if name present)
        UPDATE_NAME_PRICE_MISMATCH_RULE,    # name ≠ str(price) (if both present)
    ]

    async def execute(
        self,
        product_id: str,
        cmd: UpdateProduct = Input(),
    ) -> Product | None:
        return await self.main_repo.update(int(product_id), cmd)
```

---

## Declaring use cases on a REST interface

Attach use cases to routes via `RestRoute`:

```python
from loom.rest.model import RestInterface, RestRoute

class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    routes = (
        RestRoute(use_case=CreateProductUseCase, method="POST",   path="/",              status_code=201),
        RestRoute(use_case=GetProductUseCase,    method="GET",    path="/{product_id}"),
        RestRoute(use_case=ListProductsUseCase,  method="GET",    path="/"),
        RestRoute(use_case=UpdateProductUseCase, method="PATCH",  path="/{product_id}"),
        RestRoute(use_case=DeleteProductUseCase, method="DELETE", path="/{product_id}"),
    )
```

Or let the framework generate all five routes automatically:

```python
class ProductInterface(RestInterface[Product]):
    prefix = "/products"
    tags = ("Products",)
    auto = True   # generates GET, POST, PATCH, DELETE, list automatically
```

See [Auto-CRUD guide](autocrud.md) for the full options reference.

---

## Cross-use-case calls with ApplicationInvoker

Use `ApplicationInvoker` to call another use case by type without tight coupling.
The engine resolves it from the DI container:

```python
from loom.core.use_case.invoker import ApplicationInvoker


class RestockWorkflowUseCase(UseCase[Product, RestockWorkflowResponse]):
    def __init__(self, app: ApplicationInvoker, job_service: JobService) -> None:
        self._app = app
        self._jobs = job_service

    async def execute(
        self,
        product_id: str,
        cmd: DispatchRestockEmailCommand = Input(),
    ) -> RestockWorkflowResponse:
        # Call another use case by type — no import of its instance
        summary = await self._app.invoke(
            BuildProductSummaryUseCase,
            params={"product_id": int(product_id)},
        )
        handle = self._jobs.dispatch(
            SendRestockEmailJob,
            params={"product_id": int(product_id)},
            payload={"product_id": int(product_id), "recipient_email": cmd.recipient_email},
        )
        return RestockWorkflowResponse(
            summary=summary.summary,
            restock_job_id=handle.job_id,
            queue=handle.queue,
        )
```

`app.entity(Model)` gives a CRUD-focused facade when you know the entity:

```python
# Equivalent to invoke_name("product:get", params={"id": product_id})
product = await self._app.entity(Product).get(params={"id": product_id})
await self._app.entity(Product).update(params={"id": product_id}, payload={"stock": 0})
```

---

## Execution lifecycle

`RuntimeExecutor.execute` owns one execution end to end — from opening the
unit of work to running the post-commit actions it queued. REST use cases
and Celery jobs run through the same lifecycle; the Celery worker drives it
through the async bridge, not through a lifecycle of its own.

```
EXEC_START
  → unit of work __aenter__      (skipped when read_only or no uow_factory)
  → pipeline                     (binds, loads, computes, rules, execute())
  → unit of work __aexit__       (commit on success, rollback on failure/cancellation, always closes)
→ one terminal event: EXEC_DONE or EXEC_ERROR
→ post-commit actions drained   (job dispatch, on_transaction_committed hooks)
```

The executor drives the unit of work through its context-manager protocol
only (`__aenter__` / `__aexit__`); `begin()` / `commit()` / `rollback()` stay
on the `UnitOfWork` protocol for adapters and hand-driven use — the executor
never calls them directly.

### What runs after commit

Post-commit actions — job dispatches queued through `JobService` and
`on_transaction_committed` hooks enqueued by the `@transactional` decorator
(cache publication joins them in a later release) — are enqueued on a
`PostCommitChannel` bound to the execution and drained only after the unit
of work has closed, outside its transaction, in enqueue order:

- **With a unit of work**: actions run after `commit`, never inside the
  transaction — a broker failure cannot roll back a write that already
  committed.
- **Without a unit of work, or `read_only=True`**: the same actions run at
  the end of the execution — there is nothing to commit, but they still run
  once, not per repository call.
- **On failure or cancellation**: the owner discards its queue instead of
  running it — nothing partially dispatched leaks into a later execution
  sharing the same async context.

A failure during `drain()` does not replace the terminal event already
emitted: it surfaces as `PostCommitError(failures=(...))` raised by
`execute()` after `EXEC_DONE`. Every action still runs — failures are
collected, not short-circuited.

`PostCommitError.committed` says whether the owner that drained had
committed a unit of work of its own:

- `committed=True` — the execution owned a unit of work and it committed.
  A retry would repeat a write that already stands.
- `committed=False` — the execution owned no unit of work (`read_only=True`,
  no `uow_factory`), or the drain came from a hand-driven
  `flush_pending_dispatches()` outside any execution. Nothing was written,
  so the whole operation is safe to retry.

### Nesting

An execution owns the post-commit channel **iff** it owns the unit of work,
or no channel is bound yet:

- A nested call that joins the outer unit of work (or finds none active)
  enqueues on the outer channel, drained once at the outer execution's end.
- A nested call that opens its own unit of work — typically a read-only
  outer execution calling a writing inner one — owns and drains its own
  channel right after its own close. An outer failure afterward cannot
  discard the actions of an inner transaction that already committed.
- An execution started **from** a post-commit action (inside `drain()`)
  finds no channel bound and opens its own lifecycle.

### Cancellation

Cancelling an execution mid-flight is a terminal outcome, not a special
case: the unit of work still closes exactly once. Adapters shield their own
driver I/O (`session.rollback()`, session close, `end_session`) with
`asyncio.shield`, so a cancellation arriving while closing lets the close
finish instead of cutting it off, and reset their `ContextVar` tokens in the
*caller's* context — a shielded coroutine runs in a copied context, where
`ContextVar.reset` raises. The executor itself
never shields; only the adapters do, around their own I/O. `EXEC_ERROR`
carries `error_kind="cancelled"` for a cancelled execution.

### Events

| Event | When | Carries |
|---|---|---|
| `EXEC_START` | Before the unit of work opens (or before the pipeline, when none is configured) | `use_case_name`, `trace_id` |
| `EXEC_DONE` | The only success terminal event — after commit, or after the pipeline when there is no unit of work | `duration_ms`, `pipeline_ms`, `commit_ms` |
| `EXEC_ERROR` | The only failure terminal event | `error_kind` (`begin`, `business`, `commit`, `cancelled`, `post_commit`), `duration_ms`, `pipeline_ms`, `commit_ms` |

`error_kind` names the step that failed: `begin` for `__aenter__`,
`business` for the pipeline, `commit` for the unit-of-work exit, `cancelled`
for a cancellation, and `post_commit` when the propagating exception is a
`PostCommitError` — an inner execution that owned its own unit of work
committed and then failed to drain, so the outer failure is not its own
business logic.

`duration_ms` measures start → terminal event; `pipeline_ms` measures the
pipeline stage alone; `commit_ms` measures the unit-of-work exit (`None`
when no unit of work was opened). A post-commit failure raises no new event
kind — it surfaces only as `PostCommitError`, after `EXEC_DONE`.

### The `transactional` declaration

Every unit-of-work adapter declares `transactional: ClassVar[bool]`: `True`
when `commit` / `rollback` are real (SQLAlchemy, Mongo with
`transactions: true`), `False` for adapters whose writes autocommit
(DynamoDB, Mongo with `transactions: false`). The protocol is
runtime-checkable, so an adapter that omits the declaration no longer
satisfies `isinstance(x, UnitOfWork)`. Nothing reads the flag yet: the
bootstrap check that refuses a use case requiring a transaction on a
non-transactional backend, and the cache rules keyed on it, arrive in a
later release; see [Persistence backends](persistence-backends.md).

### `PostCommitError` in REST and Celery

`PostCommitError` carries `committed`, and both transports read it:

- **REST**: `HttpErrorMapper` maps it to `500` and copies the flag into the
  response body's `detail` as `committed` — `true` means the write stands
  even though the request failed, `false` means nothing was written.
- **Celery**: the worker does **not** retry a `committed=True` failure (a
  retry would repeat the write) and retries a `committed=False` one like any
  other failure — see [Post-commit failures](celery.md#post-commit-failures).

### Hand-driven units of work

Constructing a unit of work directly (e.g. `SQLAlchemyUnitOfWork`) and
calling `begin()` / `commit()` / `rollback()` by hand keeps working — those
methods stay on the protocol. Closing is the context manager's job
(`__aexit__`), not `commit()`'s: a hand-driven caller that never enters the
`async with` block leaves the session open.

### Upgrade notes

- **Metrics timing changed**: `EXEC_DONE` now fires after commit, not
  before — a consumer computing latency from `EXEC_DONE.duration_ms` alone
  now sees the commit included; `pipeline_ms` and `commit_ms` split it back
  out.
- **`PostCommitError` where a rollback used to be reported**: a broker or
  cache-bump failure after a successful commit used to roll back an
  already-committed transaction; it now raises
  `PostCommitError(committed=True)` instead — the write stands, the failure
  is reported separately. The same failure in an execution that owned no
  unit of work raises `PostCommitError(committed=False)`, which REST reports
  as `committed: false` and the Celery worker retries.
- **`flush_pending_dispatches()` now raises `PostCommitError`** instead of
  letting an individual dispatch's exception propagate directly, and
  **raises `RuntimeError` when called inside an execution**: the executor
  owns the post-commit channel and drains it after the unit of work closes.
  Call it only outside an execution, as `InlineJobService` documents.
  `clear_pending_dispatches()` discards the executor's channel when one is
  bound, so a hand-driven caller cancelling dispatches inside an execution
  now really cancels them.
- **`UnitOfWork` is a runtime-checkable protocol with a data member**:
  `isinstance(x, UnitOfWork)` is `False` for an adapter that does not
  declare `transactional`, and `issubclass(X, UnitOfWork)` raises
  `TypeError` — Python cannot check a data member on a class. Third-party
  adapters must declare `transactional: ClassVar[bool]`; code that used
  `issubclass` must switch to `isinstance` on an instance.
- **`error_kind="commit"` means the commit itself failed**: the outcome is
  undetermined — the driver may have committed before the failure surfaced.
  Treat it as "unknown", not as "rolled back"; the adapter still rolled back
  and closed on its side (SQLAlchemy) or ended the session (Mongo). A failure
  while *closing* after a successful commit is logged as `UoWCloseFailed` and
  does not fail the execution on either adapter: the write landed, so the
  terminal event is `EXEC_DONE` and the post-commit actions run.
- **`error_kind="post_commit"`** is new: an outer execution whose inner one
  committed and then failed to drain no longer reports `business`.
- **Hand-driven `begin()`/`commit()` callers must close through the context
  manager** (`async with uow:` or an explicit `__aexit__` call) — calling
  `commit()` alone no longer closes the session.

---

## DSL quick-reference

| Primitive | Purpose |
|-----------|---------|
| `F(Cmd).field` | Typed field reference |
| `Input()` | Inject decoded request body as command |
| `LoadById(Model, by=...)` | Fetch entity by path/command param; 404 on missing |
| `Exists(Model, from_param=..., against=...)` | Check existence; 404 on missing if `RAISE` |
| `Compute.set(F).from_command(...)` | Derive/normalise a command field |
| `Compute.set(F).from_params(...)` | Include path params in derive computation |
| `Rule.check(F, via=fn)` | Validate a field; `fn` returns error string or `None` |
| `Rule.forbid(predicate, message=...)` | Enforce invariant; predicate returns `True` to fail |
| `.from_command(F1, F2, ...)` | Pass command fields to rule predicate |
| `.from_params("name")` | Pass path parameter to rule predicate |
| `.when_present(F)` | Skip rule/compute when `Patch` field absent |
| `F1 & F2` | AND predicate — both must be present |
| `F1 \| F2` | OR predicate — either must be present |
| `ApplicationInvoker.invoke(UseCase, ...)` | Call another use case by type |
| `ApplicationInvoker.entity(Model)` | CRUD facade for a model entity |
