# 🚀 Release 0.11.0 ([#54](https://github.com/the-reacher-data/loom-py/pull/54)) ([`23bff3d`](https://github.com/the-reacher-data/loom-py/commit/23bff3df20607bc12268de0d941ce46df61e16bf))


## ✨ Features
### maintenance
- **maintenance:** add loom.etl.maintenance — agnostic Delta vacuum/optimize<br>
  > New module providing a declarative, backend-agnostic API for running
  > Delta table maintenance operations (vacuum, compact, z-order) via delta-rs.
  > Core abstractions:
  > MaintenanceStep[ParamsT] base class (mirrors ETLStep pattern)
  > MaintainTable fluent builder for explicit per-table ops
  > MaintainSchema for autodiscovery by schema prefix from StorageConfig
  > MaintenanceRunner.from_config() wired to StorageConfig.to_path_locator()
  > MaintenanceReport with per-table error isolation + raise_if_errors()
  > operations_for(params) hook enabling Prefect-parameter-driven operations
  > Storage config:
  > MaintenanceConfig + MaintenanceVacuumConfig added to StorageConfig
  > run_from_config() for config-only maintenance without any Python class
  > Prefect integration:
  > maintenance_flow() factory in loom.prefect.flow (mirrors etl_flow)
  > _common.coerce_tags extracted from _factory — no more private cross-import
  > Deployable via same YAML pattern as ETL pipelines (step_class key)
  > Safety defaults:
  > vacuum dry_run=True everywhere; production must opt-in with dry_run=False
  > continue-on-error per table; MaintenanceReport.raise_if_errors() for gates
  > missing_table_policy="skip" by default
  > 42 unit tests covering builder, step, runner (StubMaintainer), and
  > DeltaRsMaintainer backend against real local Delta tables.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>


### notify
- **notify:** enrich NotifyEvent with duration + env; fix loggers<br>
  > NotifyEvent:
  > duration_seconds: float | None — wall-clock time from Prefect's
  > flow_run.total_run_time. Displayed as '2m 03s', '1h 05m 12s', etc.
  > env: str | None — forwarded from the flow's 'env' parameter so ops
  > teams know which environment triggered the alert.
  > _hooks.py:
  > _duration_from_run() extracts total_run_time.total_seconds()
  > _env_from_params() reads the 'env' param (present in all loom flows)
  > correlation_id stays empty for maintenance flows (no business key) —
  > the run name already carries the timestamp identifier
  > Replace stdlib logging with loom get_logger; convert %s → kwargs
  > SlackNotifier._render:
  > Header: icon + flow_name + state
  > Meta line: run name  env (when set)  correlation (when set)  duration
  > Omits empty fields — maintenance messages stay clean without correlation
  > _fmt_duration() helper: 45s / 2m 03s / 1h 05m 12s
  > dry_run indicator is in the run name for maintenance (Prefect shows
  > params in the UI); a future iteration can surface it in the message
  > field via MaintenanceReport summary.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **notify:** add run summary to Slack — ETL steps + maintenance table count


## 🐛 Fixes
### maintenance
- **maintenance:** use loom StructLogger + fix logging.exception + Sonar smells<br>
  > _runner.py, backends/_delta_rs.py: replace stdlib `import logging` with
  > loom's `from loom.core.logger import get_logger`. StructLogger only accepts
  > (event: str, **kwargs) — convert all %s positional format calls to keyword


### test
- **test:** use type() instead of class statement to avoid Sonar S1118<br>
  > type() triggers __init_subclass__ identically to a class statement but
  > returns a value — static analyzers see a function call, not an unused
  > class declaration. Resolves Sonar 'Remove this unused class declaration'
  > on test_step.py:28 and :34.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **test:** revert to class statement with NOSONAR — type() introduced Bug Major<br>
  > Using type() to avoid Sonar S1118 introduced a new Bug Major ('return
  > value of type() must be used'). The class statement IS the correct pattern
  > for testing __init_subclass__ — the class definition is the act under test.
  > Add # NOSONAR on the class line: SonarQube's official suppression mechanism
  > for confirmed false positives. Tells Sonar the unused class is intentional.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>





## ♻️ Refactor
### maintenance
- **maintenance:** move imports to module level + fix noqa:ARG002<br>
  > _delta_rs.py: move deltalake + protocol types to module level; this module
  > is only loaded when DeltaRsMaintainer is used so the [delta] extra is always
  > present. Removes lazy imports from all three method bodies.
  > _runner.py: move _ops types (CompactSpec, MaintenanceSpec, VacuumSpec,
  > ZOrderSpec) and TableRef to module level. Keep DeltaRsMaintainer import
  > lazy inside from_config() with noqa:PLC0415 — avoids forcing [delta] on
  > users who import MaintenanceRunner for type annotations only.
  > Remove incorrect noqa:ARG002 on run(params): params IS used via
  > operations_for(params) in _resolve_specs.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **maintenance:** iter-1 SOLID — OperationDeclaration protocol (LSP/OCP)<br>
  > Introduce OperationDeclaration as a @runtime_checkable Protocol that both
  > MaintainTable and MaintainSchema implement, replacing the isinstance branch
  > in _resolve_specs with a uniform op.resolve(config) call.
  > _protocol.py: add OperationDeclaration Protocol
  > _builder.py: add resolve() to MaintainTable (returns [_to_spec()]) and
  > MaintainSchema (delegates to _expand)
  > _step.py: operations ClassVar typed as list[OperationDeclaration];
  > __init_subclass__ validates via isinstance(op, OperationDeclaration)
  > _runner.py: _resolve_specs simplified to a single list comprehension
  > __init__.py: export OperationDeclaration
  > Fixes LSP (MaintainTable/MaintainSchema now substitutable) and OCP partial
  > (new operation declaration types no longer require touching the runner).
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **maintenance:** iter-2 SOLID — centralize duplication + reduce CC (SRP/DRY)<br>
  > _builder.py: extract _expand_for_schemas() pure function — single source of
  > truth for schema-prefix filtering. MaintainSchema._expand delegates to it.
  > _runner.py: run_from_config calls _expand_for_schemas instead of reimplementing
  > the loop; extract _resolve_location(table_ref) -> TableLocation | None to
  > encapsulate the try/except + missing_table_policy logic. _run_one CC drops
  > from 7 to 3 (early-return on None + single try block).
  > backends/_delta_rs.py: extract _open(uri, location) to consolidate DeltaTable
  > + WriterProperties + CommitProperties construction repeated in all three methods.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **maintenance:** iter-3 SOLID — polymorphic OpSpec dispatch (OCP)<br>
  > Eliminate the if-chain in _run_one and make the module extensible to new
  > operation types (RESTORE, CHECKPOINT…) without touching runner or builders.
  > Design:
  > _ops.py: VacuumSpec/CompactSpec/ZOrderSpec each gain a ClassVar name and
  > execute(maintainer, uri, location) method — self-dispatching Strategy pattern.
  > MaintenanceSpec replaces three nullable fields with ops: tuple[OpSpec, ...].
  > _protocol.py: add OpSpec @runtime_checkable Protocol (name + execute).
  > TableMaintenanceResult switches to op_results: dict[str, result] with
  > @property vacuum/compact/z_order accessors for backwards compatibility.
  > _builder.py: internal _ops list replaces three separate fields; _to_spec /
  > _expand build the tuple. _expand_for_schemas takes an ops tuple instead of
  > three params — one call site, one contract.
  > _runner.py: _run_one becomes a single loop:
  > for op in spec.ops: op_results[op.name] = op.execute(maintainer, uri, loc)
  > Zero if-chain; adding a new OpSpec type requires zero changes here.
  > To add a new operation (e.g. RESTORE):
  > 1. Create RestoreSpec(name="restore") with execute() → RestoreResult
  > 2. Add .restore() fluent method to MaintainTable / MaintainSchema
  > 3. Done — runner, protocol, and tests need no changes.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **maintenance:** post-iter cleanup — OpSpec ClassVar + deduplicate validation<br>
  > _ops.py: MaintenanceSpec.ops typed as tuple[OpSpec, ...] for consistency
  > with the runner loop; import OpSpec under TYPE_CHECKING.
  > _protocol.py: OpSpec.name declared as ClassVar[str] so mypy correctly
  > validates VacuumSpec/CompactSpec/ZOrderSpec as structural subtypes.
  > _builder.py: extract _assert_no_conflicting_ops() pure function — single
  > source of truth for the compact⊕z_order invariant, called from _to_spec
  > and _expand (was duplicated in 3 places).
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>








# 🚀 Release 0.10.4 ([#52](https://github.com/the-reacher-data/loom-py/pull/52)) ([`3bf88be`](https://github.com/the-reacher-data/loom-py/commit/3bf88bee9210893dbf9522c1cf5c0424c069e43e))


## ✨ Features
### storage
- **storage:** add MissingTablePolicy.ERROR to block auto-creation<br>
  > Tables must be pre-created via the catalog process. With schema_mode,
  > steps using SchemaMode.OVERWRITE could silently create tables bypassing
  > catalog governance. ERROR unconditionally blocks creation regardless of
  > the step's schema_mode.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>



## 🐛 Fixes
### runner
- **runner:** cleanup RUN-scope checkpoints in ETLRunner.run() finally block<br>
  > CheckpointScope.RUN promises cleanup 'in the finally block of every
  > pipeline run' but ETLRunner.run() never called cleanup_run(). RUN-scope
  > checkpoint files accumulated in S3 indefinitely after every run.
  > Wrap flush_runner in its own try/finally so cleanup always executes
  > even if the observability flush raises. Cleanup errors are caught and
  > logged as warnings to avoid masking the original pipeline exception.
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>
  > --------
  > Co-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>











# 🚀 Release 0.10.3 ([#48](https://github.com/the-reacher-data/loom-py/pull/48)) ([`15c54d0`](https://github.com/the-reacher-data/loom-py/commit/15c54d09d74297bac63508a11b23ec5048b8128a))


## ✨ Features
### polars
- **polars:** streaming Arrow writes for replace_partitions<br>
  > Adds a target-level streaming flag on AppendSpec/ReplaceSpec/
  > ReplacePartitionsSpec/ReplaceWhereSpec (UpsertSpec excluded — MERGE has
  > no streaming source path in delta-rs). The Polars backend honours the
  > flag today for replace_partitions against existing tables: the
  > LazyFrame is sunk to a lz4-compressed IPC spool and re-opened as a
  > pyarrow.RecordBatchReader passed straight to write_deltalake, bounding
  > peak RAM to roughly one batch + delta-rs internals.
  > Partition predicate is computed via a projection-pushdown scan over the
  > same spool. Schema alignment and the Null-dtype guard run lazily on the
  > LazyFrame before the sink. Other modes accept the flag for forward
  > compatibility but still materialise the frame.
  > Spool directory is configurable via LOOM_SPOOL_DIR so container
  > workloads with a tmpfs /tmp (Fargate) can spill to a real disk.
  > Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>



## 🐛 Fixes
### clickhouse
- **clickhouse:** compress streaming IPC spool with lz4<br>
  > The read_streaming path spooled Arrow IPC files without compression,
  > producing ~10-15 GB temporary files for large ClickHouse result sets
  > (e.g. 19M-row CDC tables). Combined with a second lz4-compressed spool
  > from the downstream streaming Delta write, total ephemeral disk usage
  > exceeded 20 GB on Fargate tasks.
  > Adding lz4 compression to the CH spool reduces it ~3x (~4-5 GB),
  > bringing the combined footprint to ~8-10 GB — within the Fargate
  > default ephemeral limit without requiring task definition changes.
  > Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>









## 🔖 Other
- chore(version) Update version<br>
  > --------
  > Co-authored-by: Claude Opus 4.7 (1M context) <noreply@anthropic.com>




# 🚀 Release 0.10.2 ([#46](https://github.com/the-reacher-data/loom-py/pull/46)) ([`0fa17f5`](https://github.com/the-reacher-data/loom-py/commit/0fa17f5735be843e68ba446993bb41f2d107c33b))


## ✨ Features
### polars
- **polars:** streaming Arrow writes for replace_partitions











# 🚀 Release 0.10.1 ([#44](https://github.com/the-reacher-data/loom-py/pull/44)) ([`a55cd27`](https://github.com/the-reacher-data/loom-py/commit/a55cd27020dee60aea6849b510500e7e6f5a83b2))



## 🐛 Fixes
### testing
- **testing:** implement read_streaming on _PolarsStubReader (#44)










# 🚀 Release 0.10.0 ([#41](https://github.com/the-reacher-data/loom-py/pull/41)) ([`099de38`](https://github.com/the-reacher-data/loom-py/commit/099de38b47cd7002a9c36fc56c9c85f673431022))


## ✨ Features
### prefect
- **prefect:** add loom.prefect observer module for ETL observability<br>
  > Introduces loom.prefect as an optional integration layer that exposes each
  > ETLStep as an observable Prefect @task, giving Prefect full step-level
  > visibility into ETL pipelines rather than treating the whole run as a
  > single opaque process.
  > Key additions:
  > FlowCtx: typed operational contract (correlation_id, run_id, processes, force, dry_run)
  > RunManifest + ManifestStore: ephemeral S3-backed retry state — zero S3 writes on happy path
  > S3JsonManifestStore: fsspec-backed implementation with correlation_id path validation
  > PrefectObserver: LifecycleObserver forwarding loom events to Prefect UI logs/artifacts
  > FlowConfig + _load_flow_config: per-flow retry policy from YAML
  > build_etl_flow(): factory that compiles a PipelinePlan once at build time,
  > creates the runner once per Fargate container, and submits each step as @task
  > respecting ParallelStepGroup topology and skipping SUCCESS steps from the manifest
  > ETLRunner.run() now accepts optional run_id so Prefect and loom lineage share
  > the same traceability identifier

- **prefect:** add Fargate/Docker launcher + deploy_etl() for ETL flow generation











# 🚀 Release 0.9.2 ([#39](https://github.com/the-reacher-data/loom-py/pull/39)) ([`f1a482d`](https://github.com/the-reacher-data/loom-py/commit/f1a482d03f8b83f8e0c7725e7af6160466ac6126))


## ✨ Features
### config
- **config:** add SecretsManagerResolver + shared resolver utilities











# 🚀 Release 0.9.1 ([#36](https://github.com/the-reacher-data/loom-py/pull/36)) ([`5c420c9`](https://github.com/the-reacher-data/loom-py/commit/5c420c9dd4efe0f697affb8a921054bc6504358f))


## ✨ Features
### streaming
- **streaming:** add register_sink to StreamingRunner<br>
  > Allows applications to declare sinks once via register_sink(cls) and
  > let the runner resolve them from YAML config at startup, eliminating
  > manual sink wiring in entrypoints.
  > New public API:
  > runner = StreamingRunner()
  > runner.register_sink(ClickHouseErrorTableSink)
  > runner.run(flow=build_flow(), config_path=streaming_config_path())
  > Sink classes satisfy the RegisteredSink protocol (sink_type, config_type,
  > build_binding). The runner matches YAML entries by type field, deserializes
  > each section with config_type, and calls build_binding(cfg, ctx).
  > Adds DuplicateErrorSinkError for conflicting ErrorKind assignments.
  > Adds STREAMING_SINKS to ConfigKey.
  > StreamingRunner.__init__ is now no-arg; factory methods unchanged.
  > prepare_run(error_sinks=...) remains supported for tests and overrides.


### config
- **config:** add SSM dot-notation key navigation and resolve logging<br>
  > _split_ssm_key splits /path/param.key into SSM path + JSON key list
  > _navigate_json parses SSM value as JSON and traverses key path
  > resolve() emits INFO log with expanded SSM path (never the value)
  > ConfigResolver.resolve() widened to -> object (str <= object, LSP-safe)
  > Guard against empty key with explicit ConfigError



## 🐛 Fixes
### tests
- **tests:** guard received[0] index access in test_sink_registry







## 🛠 Chores
### release
- **release:** sync pyproject.toml version to 0.9.0<br>
  > The v0.9.0 tag was created by a hotfix but the version bump PR did not
  > merge cleanly, leaving pyproject.toml stuck at 0.8.0 on master. This
  > aligns the declared version with the existing tag so the next release
  > computes 0.10.0 instead of colliding with v0.9.0.





# 🚀 Release 0.8.0 ([#30](https://github.com/the-reacher-data/loom-py/pull/30)) ([`702b99e`](https://github.com/the-reacher-data/loom-py/commit/702b99ec75a4130f273322f8eb488637a2f98a14))


## ✨ Features
### streaming
- **streaming:** add IntoSink/SinkPartition protocols and Decompose node<br>
  > Introduces the two-level contract that makes storage sinks extensible at
  > the node level. Any frozen dataclass satisfying IntoSink is a first-class
  > terminal node — no registration, no inheritance, no framework coupling.
  > core/schema/: new shared module; SchemaMode promoted from ETL (ETL re-exports for backwards compat)
  > nodes/_sink.py: SinkPartition (contravariant per-worker protocol) and IntoSink (runtime-checkable terminal protocol)
  > nodes/_decompose.py: EntityDecomposer protocol and Decompose transformation node
  > validate.py: _is_leaf_terminal() helper unifies all terminal checks; IntoSink recognised as terminal in shape and output validation; _node_output_shape refactored to dispatch map
  > exports: IntoSink, SinkPartition, Decompose, EntityDecomposer in public API
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **streaming:** add IntoTable sink node and rename EntityDecomposer to PayloadExpander<br>
  > IntoTable: frozen dataclass implementing IntoSink for SQLAlchemy and Delta backends
  > Backend enum: SQLALCHEMY and DELTA variants
  > _SQLAlchemyTablePartition: bulk-insert via engine.begin() per epoch batch
  > _DeltaTablePartition: write via deltalake + polars with validated write mode
  > Rename EntityDecomposer -> PayloadExpander, decompose() -> expand(), targets -> outputs
  > Update both streaming.__init__ and streaming.nodes.__init__ public exports
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>


### streaming/compiler
- **streaming/compiler:** teach compiler to resolve IntoSink nodes<br>
  > Add CompiledStorageSink to _plan: holds the IntoSink node and its
  > pre-fetched streaming.sinks.<name> config section
  > Add terminal_storage_sinks field to CompiledPlan (default empty dict
  > keeps all existing tests green)
  > Refactor _build_terminal_sinks and all branch builders to return
  > (kafka_sinks, storage_sinks) tuple in a single walk — no duplicate
  > traversal
  > Add _build_storage_sink: resolves config by node.name or passes {}
  > when name is empty (self-configured sinks)
  > Add validate_storage_sinks phase: reports a clear error for every
  > named IntoSink whose streaming.sinks.<name> section is absent
  > Wire validate_storage_sinks into the compiler pipeline
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **streaming/compiler:** add Decompose shape and structural validation<br>
  > Import Decompose in validate.py
  > Add RECORD to _node_input_shape for Decompose — enforces that it
  > receives individual events, not batches
  > Add Decompose: StreamShape.RECORD to _FIXED_OUTPUT_SHAPES — its
  > output feeds the Router as per-type records
  > Add structural check in _validate_shape_sequence: Decompose must be
  > immediately followed by a Router; clear error message when it is not
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>


### streaming/bytewax
- **streaming/bytewax:** wire IntoSink nodes to Bytewax output operators<br>
  > Add handlers/storage.py: _StorageSinkPartition wraps loom SinkPartition,
  > extracting payload from Message envelopes; _StorageDynamicSink calls
  > node.build_partition(config, worker_index, worker_count) once per
  > Bytewax worker at startup; _apply_into_sink registers the DynamicSink
  > via bw_output for any IntoSink node found in the compiled plan
  > Register IntoSink in _NODE_HANDLERS dispatch map (dispatcher.py) so
  > _wire_process routes any IntoSink node to _apply_into_sink
  > Add IntoSink pass-through in _execute_router_node (routing.py)
  > consistent with the IntoTopic/Drain pattern for inline Router execution
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>






## ♻️ Refactor
### streaming
- **streaming:** improve _SQLAlchemyTablePartition with typed columns and pool config<br>
  > Extract _require_sa_url, _sa_engine_kwargs, _sa_type_for, _sa_table_from_struct,
  > _structs_to_rows helpers for readability and single responsibility
  > Map Python types to SA column types (_sa_type_for) with Optional unwrapping
  > and collection → JSON fallback; mirrors _SCALAR_TYPE_MAP from introspection.py
  > Mirror SessionManager pool defaults: pool_pre_ping, echo, pool_size,
  > max_overflow, pool_timeout, pool_recycle, connect_args from config
  > Replace msgspec.structs.asdict cast hack with direct call via _structs_to_rows
  > Drop connection_string fallback — config contract is url only
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **streaming:** merge sink config from dsl and yaml
- **streaming:** move table sink into package
- **streaming:** resolve table sink config via context
- **streaming:** enforce typed table sink config



## ✅ Tests
### streaming
- **streaming:** add IntoTable SQLAlchemy integration tests against SQLite<br>
  > Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>

- **streaming:** cover resource manager session cache



## 🔖 Other
- Fix Bytewax step flat_map tests
- Refactor streaming validation helpers<br>
  > --------
  > Co-authored-by: Claude Sonnet 4.6 <noreply@anthropic.com>




# 🚀 Release 0.7.0 ([#25](https://github.com/the-reacher-data/loom-py/pull/25)) ([`0d8941a`](https://github.com/the-reacher-data/loom-py/commit/0d8941a724ec768b162a07e756bac3d53157ec85))


## ✨ Features
### config
- **config:** add StructBinder and migrate streaming binding resolution<br>
  > Introduces StructBinder in core/config/binder.py — a Strategy that
  > injects constructor arguments from a config mapping via msgspec.convert,
  > covering primitives, Literal constraints, and LoomFrozenStruct subclasses.
  > Migrates streaming _instantiate_binding to use it, removing ~30 lines of
  > private helpers and adding typed ConfigError on resolution failures.

- **config:** add ConfigContext for typed section extraction and binding resolution<br>
  > Single entry-point for runner and bootstrap code to read config:
  > section() extracts typed sub-trees, bind() injects constructor args
  > from a config path + overrides, resolve() materializes ConfigBinding
  > declarations. Accepts an optional StructBinder for strict-mode control.



## 🐛 Fixes
### core
- **core:** clean async bridge timeout and celery typing

### etl
- **etl:** align missing storage error with config contract




## ♻️ Refactor
### model
- **model:** migrate public config structs to LoomFrozenStruct

### streaming
- **streaming:** clean up _instantiate_binding before extraction<br>
  > Replace mutable dict() + .update() with a single dict unpacking
  > Remove the dead param.annotation fallback (get_type_hints already
  > resolves all forward refs; the fallback silently passed strings to
  > _is_struct_annotation which always returned False)
  > Extract _SKIP_KINDS constant to name the variadic-parameter guard
  > Raise ConfigError instead of TypeError for missing required struct
  > params (semantically a config error, not a type error; both are
  > caught by the enclosing _resolve_binding handler so no behaviour
  > change in streaming)
  > All 7 binding tests pass.


### core
- **core:** homogenize config and runner boundaries
- **core:** add runner and compiler protocols
- **core:** apply runner lifecycle protocols
- **core:** harden runner lifecycle abstraction<br>
  > shutdown_runner/flush_runner now catch and log exceptions instead of
  > propagating, preserving the original exception when called from finally
  > blocks; contract updated in docstrings and covered by new tests
  > CompilerProtocol drops @runtime_checkable — no production isinstance
  > usage existed; Protocol remains valid for static typing
  > StreamingRunner.prepare_run() calls shutdown_runner(self) before
  > overwriting self._shutdown, preventing resource leaks on double calls
  > _build_backend_options extracted from loom.celery.config into
  > loom.core.async_bridge.build_backend_options, eliminating duplication
  > between the Celery and streaming domains
  > _CeleryAsyncRuntime._signals_connected ClassVar removed; the
  > module-level _SIGNALS_CONNECTED flag is the single source of truth
  > for the process-level signal guard
  > ETLRunner.flush() delegates to ETLExecutor.flush(), which checks
  > SupportsFlush via isinstance instead of getattr duck-typing
  > TypeVars in compiler.py renamed to _RequestT/_PlanT to signal they
  > are implementation details of the Protocol, not public API


### etl
- **etl:** make flush an optional runner capability

### celery
- **celery:** adopt runner shutdown helper
- **celery:** replace _SIGNALS_CONNECTED sentinel with dispatch_uid<br>
  > Eliminates the module-level mutable boolean that violated the no-global-
  > mutable-state architecture rule. Idempotency is now handled by Celery's
  > dispatch_uid mechanism: disconnect+connect on each call ensures the most-
  > recently registered closure is always active, so a second bootstrap_worker
  > call wires up fresh dependencies rather than leaving stale closures from
  > the first call.
  > This is strictly better than the sentinel: no global state, no race
  > condition between threads calling bootstrap_worker concurrently, and
  > correct closure replacement when called multiple times in tests.


### rest
- **rest:** use observability config directly







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
