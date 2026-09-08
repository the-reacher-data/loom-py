# Cache

A `cache:` section in the YAML turns on a cache-aside layer in front of every
repository whose class is marked `@cached`. The decorator declares; the
composition root binds. `create_app()` (REST, and the agent-only app) and
`bootstrap_worker()` (Celery) read the section at boot, configure `aiocache`
once, and serve each marked repository wrapped in `CachedRepository`,
whichever backend built it — SQLAlchemy, Mongo, DynamoDB, or an explicit
`repository_for(..., builder=)`. Nothing in your code calls `configure`, and
no global is touched.

## Declaring

`@cached` goes on the repository class. Every read the wrapper knows about —
`get_by_id`, `get_by`, the list and cursor reads — is served through the cache
from then on; writes go straight through and invalidate.

```python
from loom.core.cache import cache_query, cached
from loom.core.repository import repository_for
from loom.core.repository.sqlalchemy import RepositorySQLAlchemy


@cached
@repository_for(User)
class UserRepository(RepositorySQLAlchemy[User, UserCreate, UserUpdate]):
    @cache_query(scope="entity")
    async def profile(self, user_id: int) -> UserProfile: ...

    @cache_query()
    async def active_in(self, region: str) -> list[User]: ...
```

`@cache_query` marks a custom read method. Annotate its return type: the
wrapper derives a codec from it, so a hit and a miss return the same type. A
`msgspec.Struct`, a scalar, or a `list`, `tuple` or optional of those is
supported; anything else emits a `DeprecationWarning` when the repository is
wrapped and the cached call returns the decoded payload instead.

`scope="entity"` requires the model's primary key as the first positional
argument — a keyword argument does not count, and the wrapper raises
`TypeError` before touching the backend. The entry is then keyed on that id and
is evicted when that row changes. Any other read is `scope="list"` (the
default): it is evicted when any row of the entity changes. `ttl_key="order"`
picks another entity's TTL override when the method caches something other
than its own entity.

A method that returns `None` is not cached — the backend cannot tell a stored
`None` from a miss. Treat every cached result as immutable: concurrent callers
that miss together are served the same object.

## Configuring

```yaml
cache:
  default_ttl: 300          # seconds, single-entity reads
  default_list_ttl: 120     # seconds, list and index reads
  ttl_jitter: 0.1           # fraction of the TTL spread on every write, in [0, 1)
  ttl:                      # per-entity overrides; append _list for the list side
    user: 600
    user_list: 300
  max_size: 1000            # injected into every aiocache.SimpleMemoryCache alias
  aiocache_alias: data      # alias that stores entities (default: "default")
  counter_alias: counters   # alias that stores the generation counters (default: same as aiocache_alias)
  aiocache_config:
    data:
      cache: aiocache.SimpleMemoryCache
      serializer:
        class: loom.core.cache.serializer.MsgspecSerializer
    counters:
      cache: aiocache.SimpleMemoryCache
      # no serializer: counters are stored as raw integers
```

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | `false` runs every repository uncached (see below) |
| `default_ttl` | `200` | TTL in seconds for single-entity reads |
| `default_list_ttl` | `120` | TTL in seconds for list and index reads |
| `ttl_jitter` | `0.1` | Random spread applied per write so a burst does not expire at once; `0` disables it |
| `ttl` | `{}` | Per-entity overrides; `user` for the entity read, `user_list` for the list side |
| `max_size` | none | Entry cap for `aiocache.SimpleMemoryCache` aliases; ignored by Redis |
| `aiocache_alias` | `default` | Alias of the data backend |
| `counter_alias` | none | Alias of the counter backend; falls back to `aiocache_alias` |
| `aiocache_config` | `{}` | Alias map handed to `aiocache`, one entry per alias |

The data alias **must** carry
`serializer: {class: loom.core.cache.serializer.MsgspecSerializer}`: it is what
encodes a model struct to bytes and decodes it back to the declared type. The
counter alias carries **no serializer**: the generation counters are plain
integers bumped with the backend's native increment (`SimpleMemoryCache.increment`,
Redis `INCR`), and a serializer in front of them would break that. When
`counter_alias` is omitted the counters share the data alias and the increment
falls back to a non-atomic get-and-set, which is fine for a single process and
not for several.

The YAML reads `aiocache_config:` only. `CacheConfig.from_mapping` also accepts
the short key `aiocache:` for callers building the config by hand; from the
YAML that key — like any other key `CacheConfig` does not declare, `ttls:` for
instance — fails at boot with a configuration error naming it. A typo that
silently disabled every override is the failure mode this guards against.

## What is cached, what invalidates it

Two kinds of entry are stored on the data alias:

- **Entity reads** — `get_by_id` and every `@cache_query(scope="entity")`
  method — keyed on the entity name, the id and a fingerprint of their tags.
- **List indexes** — `get_by`, the paginated and cursor reads and every
  `@cache_query(scope="list")` method — an index of ids plus the entities of
  the page, so a warm index reloads only the rows it is missing, with one
  `<primary key> IN (...)` query.

Every entry lists the tags it depends on; the counters alias holds one
generation counter per tag (`tag:<name>`), and an entry whose fingerprint no
longer matches the counters is a miss. A write bumps only the tags it affects:
`user:list` and `user:id:<k>` for each written id, plus any tag the mutation
event carries. Updating one row leaves every other cached row of the entity
warm.

Inside a use case or a `@transactional` scope the bump is not applied at the
write: it is enqueued on the post-commit channel and published **after the
commit**, ahead of any job dispatch queued in the same pipeline, so a reader
cannot cache a row the transaction later rolled back, and a job cannot read a
stale entry (see [What runs after commit](use-case-dsl.md#what-runs-after-commit)).
Without an open transaction — SQLAlchemy's own session, Mongo without
transactions, DynamoDB — the write commits on its own and the bump runs inline.

The bare `user` tag is in every key's tag list but the framework never bumps
it: it is the manual flush handle. Incrementing `tag:user` on the counter
backend evicts every cached entry of that entity at once.

## Disabled and missing

With `enabled: false`, or with no `cache:` section at all, nothing is wrapped
and every repository runs uncached. Boot logs one `WARNING` `CacheNotConfigured`
per class marked `@cached`, naming it, so a deployment that forgot the section
learns it at start-up rather than from latency.

```yaml
cache:
  enabled: false
```

An empty `cache: {}` is not the same thing: every field has a default and
`enabled` defaults to `true`, so the section is on and the cache runs against a
memory backend. Write `enabled: false` to turn it off.

The wrapping itself is a container binding, `RepositoryDecorator` from
`loom.core.repository`: a single slot that the cache module fills only when it
is empty. A deployment that registers its own decorator before loom's module
replaces the wrapping — a second registration replaces, it does not compose.
Repositories registered in the container by hand, outside the registration
module, are never seen by the decorator.

## Memory to Redis

Moving from memory to Redis is a YAML change: swap the class, add the
connection. The serializer line stays: it is what makes the alias a serialised
one, and only a serialised alias gets the encoding guarantees of
[Failures](#failures).

```yaml
cache:
  aiocache_alias: data
  counter_alias: counters
  aiocache_config:
    data:
      cache: aiocache.RedisCache
      endpoint: ${oc.env:REDIS_HOST,redis}
      port: 6379
      namespace: my_store
      serializer:
        class: loom.core.cache.serializer.MsgspecSerializer
    counters:
      cache: aiocache.RedisCache
      endpoint: ${oc.env:REDIS_HOST,redis}
      port: 6379
      namespace: my_store_counters
      # no serializer: atomic INCR
```

Keep a separate counter alias for any multi-process deployment: with one alias
the counters share the data serializer and lose the atomic increment.
`max_size` has no effect on Redis.

## Failures

On an alias configured with
`serializer: {class: loom.core.cache.serializer.MsgspecSerializer}`, a value the
serializer cannot encode raises `CacheWriteError` (a `ValueError`) at the write,
naming the key and the value's type. Nothing is stored for that call: a batch
write encodes every pair before it writes any, so a bad pair last in the batch
stores none of them. Errors the backend itself raises — a Redis connection
refused, a timeout — propagate unchanged; the wrap covers serialisation only.

The promise is that alias's, not the cache's. An alias carrying any other
serializer — a `JsonSerializer` on Redis, or the counter alias, which carries
none — is a raw alias: loom hands the value to `aiocache` untouched, so a value
that cannot be encoded surfaces as whatever the serializer raises, with no key
and no type in the message. Configure the data alias with `MsgspecSerializer`,
as [Configuring](#configuring) requires, to get the named error.

A class marked `@cached` that does not implement the full `Repository` protocol
aborts start-up. `CachedRepository` overrides `get_by_id`, `get_by`, `exists_by`,
`count`, `list_paginated`, `list_with_query`, `create`, `update` and `delete`, so
wrapping a read-only object would advertise writes it cannot serve and fail with
an opaque `AttributeError` on the first call. The wrapper's constructor refuses
instead, and the container reports it as a `ResolutionError` naming the class and
the methods it lacks. Drop `@cached` from a partial repository, or complete it.

Celery builds the gateways once, pre-fork, when the worker validates its
container. A Redis client shared across forked children is the deployment's
concern until a per-worker init hook exists; the memory backend is unaffected.

## Lifecycle

The REST app closes the cache gateways at shutdown, beside the SQL registry:
one `close` per distinct alias, so two with a separate `counter_alias` and one
when the counters share the data alias.
