# 🚀 Release 0.6.0 ([#22](https://github.com/the-reacher-data/loom-py/pull/22)) ([`5146569`](https://github.com/the-reacher-data/loom-py/commit/51465697115036ed05f620a1099272e4fd216501))


## ✨ Features
### core
- **core:** add unified ObservabilityRuntime<br>
  > Introduces the core observability package: LifecycleEvent, LifecycleObserver
  > protocol, ObservabilityConfig, and ObservabilityRuntime as a single fan-out
  > engine replacing the fragmented per-module observer wiring. Adds
  > StructlogLifecycleObserver, OtelLifecycleObserver, NoopObserver, and
  > PrometheusLifecycleAdapter (with Pushgateway support for ETL batch jobs).
  > Backward-compat re-exports (safe_observe, notify_observers) kept for the
  > streaming composite observer until the legacy cleanup commit.



## 🐛 Fixes
### observability
- **observability:** restore default bootstraps

### rest
- **rest:** correct camel-to-snake conversion for acronyms in filter fields

### streaming
- **streaming:** propagate trace ids through bytewax paths
- **streaming:** keep input trace ids through the micro
- **streaming:** bridge otel trace ids from messages


## 📖 Documentation
- align yaml config and dummy repo links
- restore dummy repo urls
- fix markdown links for streaming dummies

### prometheus
- **prometheus:** update KafkaPrometheusMetrics docstring metric names



## ♻️ Refactor
### observability
- **observability:** simplify otel log correlation
- **observability:** simplify otel exporter imports
- **observability:** expose ObservabilityRuntime in public API and clean architecture debt<br>
  > Export ObservabilityRuntime and LifecycleStatus from loom.core.observability package
  > Compute dict(meta) once in span() instead of three separate allocations
  > Replace _SIGNALS_CONNECTED module-level global with ClassVar on _CeleryAsyncRuntime
  > Add RuntimeError to bootstrap_worker Raises docstring
  > Fix TraceIdMiddleware docstring example (removed spurious ObservabilityRuntime reference)
  > Add missing observability_runtime param to _make_handler docstring

- **observability:** unify runtime across services
- **observability:** clean up dead code and docs

### streaming
- **streaming:** migrate to unified ObservabilityRuntime<br>
  > Replace StreamingObservabilityConfig and the manual observer construction
  > in _load_observability_runtime() with ObservabilityRuntime.from_config().
  > Observability config now lives under streaming.runtime.observability in YAML,
  > eliminating the separate streaming.observability section.
  > Delete src/loom/streaming/_observability.py (StreamingObservabilityConfig)
  > Delete src/loom/streaming/observability/ directory (old observer stack)
  > Add observability: ObservabilityConfig field to BytewaxRuntimeConfig
  > StreamingRunner.from_config() calls ObservabilityRuntime.from_config() directly
  > StreamingTestRunner defaults to ObservabilityRuntime.noop(); callers pass
  > observability_runtime= explicitly when needed
  > Update tests to use new config shape and ObservabilityRuntime([observer])

- **streaming:** replace KafkaStreamingObserver with LifecycleEvent/TRANSPORT<br>
  > Eliminates the parallel KafkaStreamingObserver protocol hierarchy (NoopKafkaObserver,
  > StructlogKafkaObserver) and models all Kafka transport events as LifecycleEvent with
  > Scope.TRANSPORT. KafkaPrometheusMetrics now implements the LifecycleObserver protocol
  > via a single on_event() dispatcher instead of four typed callback methods.

- **streaming:** preserve trace lineage across boundaries

### prometheus
- **prometheus:** add KafkaMetricName enum and drop loom_ prefix<br>
  > Extracts Prometheus metric names into a public KafkaMetricName StrEnum so
  > callers can reference metric names without magic strings. Removes the loom_
  > namespace prefix from all four Kafka instruments (produced_total,
  > consumed_total, encode_duration_seconds, decode_duration_seconds).

- **prometheus:** drop loom prefix from lifecycle metrics

### rest
- **rest:** use core observability runtime
- **rest:** read prometheus from observability config

### celery
- **celery:** adopt async bridge and runtime config

### etl
- **etl:** make spark pytest plugin opt-in



## ✅ Tests
### kafka
- **kafka:** update metric name assertions after loom_ prefix removal

### observability
- **observability:** cover lineage and runtime branches

### integration
- **integration:** add in-memory REST, observability, and bootstrap integration tests

### etl
- **etl:** cover prometheus flush on runner shutdown


## 🛠 Chores
### deps
- **deps:** bump click to 8.3.3




# 🚀 Release 0.5.0 ([#20](https://github.com/the-reacher-data/loom-py/pull/20)) ([`70f7cf8`](https://github.com/the-reacher-data/loom-py/commit/70f7cf85275fcc1f590e06f980dfac91cad50893))


## ✨ Features
### core
- **core:** add shared expression routing primitives

### streaming
- **streaming:** add process DSL and routing
- **streaming:** add kafka transport and observability
- **streaming:** allow task resource kwargs
- **streaming:** add scoped dependency nodes
- **streaming:** make tasks configurable

### config
- **config:** add declarative config bindings











# 🚀 Release 0.4.0 ([#18](https://github.com/the-reacher-data/loom-py/pull/18)) ([`09e5aa3`](https://github.com/the-reacher-data/loom-py/commit/09e5aa340f88e0488daca2bf10320aad7aebbe1d))


## ✨ Features
### etl
- **etl:** add IntoHistory builder and SCD Type 2 domain contracts











# 🚀 Release 0.3.0 ([#14](https://github.com/the-reacher-data/loom-py/pull/14)) ([`ef414c5`](https://github.com/the-reacher-data/loom-py/commit/ef414c5bfd303296af450840318dfbe9d301e5d1))


## ✨ Features
### config
- **config:** add cloud URI support and pluggable resolver extension point<br>
  > Add fsspec as a hard dependency of loom[config]
  > load_config() now accepts s3://, gs://, abfss://, r2:// URIs via fsspec
  > Add ConfigResolver protocol for pluggable ${prefix:key} resolution at
  > parse time (enables SSM, Key Vault, etc. without baking secrets into images)
  > Resolver registration is idempotent; resolvers are evaluated at job startup
  > so secret rotation takes effect on the next run
  > Migrate loom.etl.runner.config_loader to use core load_config, removing
  > the parallel OmegaConf implementation
  > ETL _load_yaml inherits cloud URI and resolver support transparently


### etl
- **etl:** add FileLocator with explicit alias API for file routes<br>
  > Introduces `FileLocator` protocol and `MappingFileLocator` so that
  > `FromFile.alias("name")` / `IntoFile.alias("name")` specs resolve at
  > runtime through `storage.files` config rather than hard-coded URIs.
  > `FileLocation` / `FileLocator` / `MappingFileLocator` in `storage/_file_locator.py`
  > `StorageConfig.to_file_locator()` returns `MappingFileLocator | None`
  > (None when `files` is empty — no conditional needed at call sites)
  > `FromFile.alias()` / `IntoFile.alias()` classmethods set `is_alias=True`
  > on the emitted spec
  > `is_alias: bool` added to `FileSourceSpec` and `FileSpec`
  > Polars and Spark backends resolve aliases via injected `file_locator`
  > Both providers wired: `file_locator = config.to_file_locator()`
  > Full test coverage across io, storage, and backend layers



## 🐛 Fixes
### observability
- **observability:** honor missing table policy for record store writers


## 📖 Documentation
### etl
- **etl:** keep only user guide and drop refactor docs
- **etl:** expand ETL documentation and update directory table<br>
  > Add dummy-loom-etl companion repo link in README and etl guide
  > Expand README subpaths table with loom.etl and loom.core.config entries
  > Add FileLocator/alias API, cloud config URI, and ConfigResolver sections to etl guide
  > Add loom.etl.backends (polars + spark) to etl.rst API reference
  > Add loom.core.config to core.rst API reference




## ♻️ Refactor
### stepsql
- **stepsql:** delegate SQL execution to backend readers





# 🚀 Release 0.2.1 ([#12](https://github.com/the-reacher-data/loom-py/pull/12)) ([`87f7d1f`](https://github.com/the-reacher-data/loom-py/commit/87f7d1f1eb1ccde71d0aca1c5584b83317e30707))

## ✨ Features

### logger
- **logger:** support per-logger levels from config<br>
  > `LoomConfig` now accepts a `loggers` mapping to override the log level per named logger. Resolves `structlog` / stdlib incompatibility when mixing loom-managed and third-party loggers.

### repository
- **repository:** generalize main repo registration for loom structs<br>
  > `repository_for` is now importable from `loom.core.repository` (top-level). The SQLAlchemy-specific import path still works but is no longer the canonical one.

## 🐛 Bug Fixes

### rest
- **rest:** serialize pagination envelopes in camel case<br>
  > `PageResult` and list-envelope responses were serialized in snake_case. All envelope fields now follow the camelCase contract of the HTTP layer.
- **rest:** support loom structs in autocrud tests<br>
  > Auto-CRUD route generation was not exercising the `msgspec.Struct` code path in integration tests.

### prometheus
- **prometheus:** expose metrics at exact path<br>
  > Metrics endpoint was registered with a trailing-slash variant that did not match the documented `/metrics` path.

### docs
- **docs:** fix RTD build failure, logo and docs examples (#10, #11)<br>
  > Mock `starlette`, `celery`, `kombu`, `redis` in `autodoc_mock_imports`. Logo resized to natural proportions with dark-mode safe background. Status badges added to index. Rule/Compute examples updated to named predicates.

---

# 🚀 Release 0.2.0 ([#9](https://github.com/the-reacher-data/loom-py/pull/9)) ([`2f669ab`](https://github.com/the-reacher-data/loom-py/commit/2f669ab205c7255eb6494e4cdb8ab8092817af62))

## ✨ Features

### cache
- **cache:** aiocache gateway with auto-inferred invalidation specs<br>
  > CachedRepository wraps any repository with read-through/write-through caching. ONE_TO_MANY depends_on specs are auto-generated from field annotations — no explicit declaration needed. Explicit depends_on always wins.

### celery
- **celery:** production-ready Celery integration layer<br>
  > CeleryJobService, persistent worker event loop, trace propagation, eager fallback, and task_default_queue routing so callbacks land on the correct consumed queue. bootstrap_worker compiles use cases, repositories, and registers Celery tasks in a single call.

- **celery:** worker job discovery from modules or manifest<br>
  > bootstrap_worker discovers and registers Job classes automatically from module include paths (mode: modules) or from a typed WorkerManifest (mode: manifest). WorkerManifest replaces scattered JOBS/USE_CASES/INTERFACES module attributes with a single typed contract.

- **celery:** interfaces= and use_cases= on bootstrap_worker<br>
  > Callbacks that call ApplicationInvoker need matching use-case keys compiled in the worker. interfaces= extracts use-case types from RestInterface route declarations (including AutoCRUD-generated ones). use_cases= handles non-AutoCRUD scenarios. Both can be combined with discovery mode.

### core
- **core:** typed repository abstractions and SQLAlchemy backend<br>
  > Async repository protocol (RepositoryRead, RepositoryWrite, RepoFor) backed by SQLAlchemy 2.0 async session. Struct-based model system using msgspec.Struct as the single source of truth — models compile to SA mapped classes at startup via compile_all(). count() and UPDATE RETURNING included as first-class operations.

- **core:** use-case DSL with field refs, compute, rules and typed markers<br>
  > Declarative use-case definition via Input, Load, LoadById, Exists, Compute and Rule markers. Signature inspection runs once at compile time; RuntimeExecutor drives execution from an immutable ExecutionPlan. No per-request reflection.

- **core:** ApplicationInvoker and named use-case registry<br>
  > Use cases and job callbacks invoke other use cases by type through ApplicationInvoker without direct coupling. A named registry maps use-case keys to compiled instances at bootstrap, providing a stable cross-invocation contract.

- **core:** compiled model artifact and cache entity keys<br>
  > compile_all() produces a typed CompiledCore artifact exposing stable entity keys used by the cache layer for deterministic repository-level invalidation across reads and writes.

- **core:** executor skips UoW for read-only use cases and GET routes<br>
  > UseCase.read_only=True and all GET routes bypass UoW.begin/commit, removing at minimum one BEGIN+COMMIT round-trip from every read request on PostgreSQL.

### job
- **job:** async job domain model and orchestration primitives<br>
  > Job[ResultT] base class with Celery routing ClassVars. JobHandle / JobGroup with dual-mode waiting (Celery + inline). JobCallback lifecycle with on_success/on_failure. Dispatch is transactionally safe — jobs flush on UoW commit and are cleared on rollback.

### observability
- **observability:** trace_id propagation and Prometheus adapter<br>
  > trace_id injected into every request context and propagated to job callbacks. MetricsAdapter protocol emits execution events; PrometheusAdapter records latency histograms and error counters with low cardinality labels.

### projection
- **projection:** compiler-driven memory/SQL routing<br>
  > Projections are source-agnostic at declaration time. The backend compiler decides at compile_all() whether each projection runs in-memory (relation already loaded in the active profile) or via SQL. Users declare only CountLoader, ExistsLoader, or JoinFieldsLoader — no source= parameter. Internal _Memory* and _Sql* loaders are synthesized at compile time.

### rest
- **rest:** AutoCRUD and FastAPI adapter<br>
  > RestInterface.auto=True generates full CRUD routes at class definition time via build_auto_routes(). OpenAPI contracts expose query params, pagination defaults, and decoupled CreateInput/UpdateInput write DTOs. Discovery engine mounts all declared interfaces at bootstrap.

## 📖 Documentation

- Sphinx documentation platform with full public guides<br>
  > Quickstart, use-case DSL reference, AutoCRUD guide, Celery integration guide (job definition, dispatch, callbacks, YAML reference, bootstrap options, ApplicationInvoker, Docker-compose stack), and dummy-loom examples-repo walkthrough. Deployed to Read the Docs.

## ⚡ Performance

### engine
- **engine:** UPDATE RETURNING replaces SELECT + flush + refresh<br>
  > SQLAlchemyUpdateMixin.update() issues a single UPDATE ... RETURNING round-trip. Server-side onupdate expressions are pre-computed at init time and injected into the SET clause automatically.

### repository
- **repository:** single-query total count for offset pagination<br>
  > list_with_query with PaginationMode.OFFSET issues a single SELECT COUNT(*) instead of a separate full-table scan, eliminating one round-trip per paginated list operation.
