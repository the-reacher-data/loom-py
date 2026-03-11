# 🚀 Release 0.2.0 ([#9](https://github.com/the-reacher-data/loom-py/pull/9)) ([`2f669ab`](https://github.com/the-reacher-data/loom-py/commit/2f669ab205c7255eb6494e4cdb8ab8092817af62))

## ✨ Features

### cache
- **cache:** auto-infer ONE_TO_MANY `depends_on` — zero developer declaration needed

### celery
- **celery:** discover jobs from modules or manifest

### core
- **core:** add compiled core model artifact and cache entity keys
- **core:** add kernel app invoker and named use-case registry

### executor
- **executor:** skip UoW for read-only use cases and GET routes

### repository
- **repository:** add `count()` and UPDATE RETURNING optimization
- **repository:** add registry-based custom repository wiring

### worker
- **worker:** WorkerManifest typed contract — eliminate magic strings from manifest discovery
- **worker:** add `interfaces=` to `bootstrap_worker` for AutoCRUD callback support
- **worker:** add `use_cases=` to `bootstrap_worker` for callback ApplicationInvoker support

## 📖 Documentation

### celery
- **celery:** comprehensive guide with full examples and YAML reference

### readme
- **readme:** expand quick start with real patterns from dummy-loom

### (no scope)
- add author attribution — Massive Data Scope
- expand guides, examples-repo, and README with benchmark results

## ♻️ Refactor

### celery
- **celery:** simplify bootstrap — unify merge, single discovery pass, extract pipeline steps
- **celery:** eliminate duplicated deduplication patterns via `dict.fromkeys` and `_merge_unique` chaining

### complexity
- **complexity:** reduce cognitive complexity across celery and engine modules

### constants
- **constants:** centralize all magic strings into typed constants

### discovery
- **discovery:** simplify class collection flow

### projection
- **projection:** remove `ProjectionSource` — compiler picks memory vs SQL per profile

### repository
- **repository:** simplify `update()` to single session scope

### sqlalchemy
- **sqlalchemy:** split core/orm read paths and clarify offset filter helper

## ⚡ Performance

### engine
- **engine:** parallelize load and exists steps with asyncio.gather

### repository
- **repository:** single-query total count for offset pagination

## ✅ Tests

### celery
- **celery:** reset `_SIGNALS_CONNECTED` in `isolated_celery_signals` fixture
- **celery:** simplify dispatch branch in integration use case

### repository
- **repository:** add integration tests for `count()` and UPDATE RETURNING
