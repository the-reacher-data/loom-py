"""ETLRunner — single entry point that wires YAML config → executor → run.

Responsibilities
----------------
* Load and validate storage config from a YAML file via OmegaConf + msgspec.
* Build reader, writer, and catalog from the resolved config.
* Compile the pipeline plan via :class:`~loom.etl.compiler.ETLCompiler`.
* Optionally filter the plan to a subset of steps or processes.
* Delegate execution to :class:`~loom.etl.executor.ETLExecutor`.

Stage filtering
---------------
Pass *include* with a sequence of step or process class names.  A process name
in *include* runs the entire process; a step name runs only that step (within
whichever process contains it).  Omit *include* (or pass ``None``) to run
every stage.

Parallel groups that collapse to a single node after filtering are flattened
automatically — no unnecessary thread overhead.

Example::

    from loom.etl import ETLRunner
    from myapp.pipelines import DailyOrdersPipeline
    from myapp.params import DailyParams
    from datetime import date

    # All stages
    runner = ETLRunner.from_yaml("loom.yaml")
    runner.run(DailyOrdersPipeline, DailyParams(run_date=date.today()))

    # Only selected stages
    runner.run(
        DailyOrdersPipeline,
        DailyParams(run_date=date.today()),
        include=["NormalizeOrders", "AggregateDaily"],
    )

YAML format (``storage:`` section)::

    storage:
      root: s3://my-lake/
      storage_options:
        AWS_REGION: ${oc.env:AWS_REGION,eu-west-1}
      writer:
        compression: SNAPPY

See :class:`~loom.etl._storage_config.DeltaConfig` for the full reference.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Sequence
from typing import Any, Protocol, cast

import msgspec
from pyspark.sql import SparkSession

from loom.etl._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl._observability_config import ObservabilityConfig
from loom.etl._pipeline import ETLPipeline
from loom.etl._storage_config import (
    DeltaConfig,
    StorageConfig,
    UnityCatalogConfig,
    convert_storage_config,
)
from loom.etl._temp_cleaners import TempCleaner
from loom.etl._temp_store import IntermediateStore
from loom.etl.compiler import ETLCompiler
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    PipelineProcessNode,
    ProcessPlan,
    ProcessStepNode,
    StepPlan,
)
from loom.etl.executor import ETLExecutor, ETLRunObserver, ParallelDispatcher
from loom.etl.executor.observer._composite import CompositeObserver
from loom.etl.executor.observer._events import RunContext

_log = logging.getLogger(__name__)


class InvalidStageError(ValueError):
    """Raised when *include* matches no step or process in the compiled plan.

    Args:
        include: The set of names that produced no match.
    """

    def __init__(self, include: frozenset[str]) -> None:
        super().__init__(
            f"No steps or processes match include={set(include)!r}. "
            "Check that the names match the class names of your ETLStep or ETLProcess subclasses."
        )


class ETLRunner:
    """Wire storage config, backend I/O, and executor into one callable.

    Entry points — choose the one that fits your environment:

    **Programmatic (no YAML, full type safety)**::

        from loom.etl import DeltaConfig, ETLRunner

        config = DeltaConfig(root="s3://my-lake/", writer={"compression": "SNAPPY"})
        runner = ETLRunner.from_config(config)
        runner.run(MyPipeline, MyParams(...))

    **Databricks Unity Catalog (zero-config)**::

        runner = ETLRunner.from_spark(spark)

    **YAML file (OmegaConf interpolation)**::

        runner = ETLRunner.from_yaml("loom.yaml")

    **Pre-resolved dict (testing, external secret sources)**::

        runner = ETLRunner.from_dict({"root": "/tmp/lake"})

    Config evolution — derive a modified config without mutation::

        import msgspec

        base    = DeltaConfig(root="s3://prod-lake/")
        staging = msgspec.structs.replace(base, root="s3://staging-lake/")

    Args:
        reader:     Source reader implementation.
        writer:     Target writer implementation.
        catalog:    Catalog used for schema validation at compile time.
        observers:  Lifecycle observers wrapped in a
                    :class:`~loom.etl.executor.observer.CompositeObserver`.
        dispatcher: Parallel task dispatcher.  Defaults to
                    :class:`~loom.etl.executor.ThreadDispatcher`.
    """

    def __init__(
        self,
        reader: SourceReader,
        writer: TargetWriter,
        catalog: TableDiscovery,
        observers: Sequence[ETLRunObserver] = (),
        dispatcher: ParallelDispatcher | None = None,
        temp_store: IntermediateStore | None = None,
    ) -> None:
        composite = CompositeObserver(observers)
        self._executor = ETLExecutor(reader, writer, (composite,), dispatcher, temp_store)
        self._compiler = ETLCompiler(catalog)
        self._temp_store = temp_store

    @classmethod
    def from_config(
        cls,
        config: StorageConfig,
        obs_config: ObservabilityConfig | None = None,
        *,
        spark: SparkSession | None = None,
        dispatcher: ParallelDispatcher | None = None,
        cleaner: TempCleaner | None = None,
    ) -> ETLRunner:
        """Build an :class:`ETLRunner` from resolved config objects.

        The primary programmatic entry point — construct a typed config struct
        and pass it directly, with no YAML or dict involved::

            from loom.etl import DeltaConfig, ETLRunner

            config = DeltaConfig(
                root="s3://my-lake/",
                storage_options={"AWS_REGION": "eu-west-1"},
                writer={"compression": "SNAPPY"},
            )
            runner = ETLRunner.from_config(config)

        Evolve a base config for different environments without mutation::

            import msgspec

            base    = DeltaConfig(root="s3://prod-lake/")
            staging = msgspec.structs.replace(base, root="s3://staging-lake/")

            runner = ETLRunner.from_config(staging)

        Args:
            config:     Validated storage config (:class:`~loom.etl.DeltaConfig`
                        or :class:`~loom.etl.UnityCatalogConfig`).
            obs_config: Observability config.  Defaults to
                        :class:`~loom.etl._observability_config.ObservabilityConfig`
                        (structlog on, no run sink).
            spark:      Active :class:`pyspark.sql.SparkSession`.  Required when
                        *config* is a :class:`~loom.etl.UnityCatalogConfig`.
            dispatcher: Parallel task dispatcher.
            cleaner:    Cloud-aware temp cleaner.  Defaults to
                        :class:`~loom.etl.AutoTempCleaner`.  Pass
                        :class:`~loom.etl.DbutilsTempCleaner` for ``dbfs:/``
                        paths on Databricks.

        Returns:
            Fully wired :class:`ETLRunner`.

        Raises:
            ValueError: When *config* is :class:`~loom.etl.UnityCatalogConfig`
                        and *spark* is ``None``.
        """
        reader, writer, catalog = _make_backends(config, spark)
        observers = _make_observers(obs_config or ObservabilityConfig())
        temp_store = _make_temp_store(config, spark, cleaner)
        return cls(reader, writer, catalog, observers, dispatcher, temp_store)

    @classmethod
    def from_yaml(
        cls,
        path: str | os.PathLike[str],
        *,
        spark: SparkSession | None = None,
        dispatcher: ParallelDispatcher | None = None,
    ) -> ETLRunner:
        """Load config from a YAML file and build an :class:`ETLRunner`.

        Reads ``storage:`` (required) and ``observability:`` (optional) sections.
        Uses OmegaConf to resolve ``${oc.env:VAR,default}`` interpolations.

        Args:
            path:       Path to the YAML file.
            spark:      Active :class:`pyspark.sql.SparkSession`.  Required when
                        the YAML specifies ``storage.type: unity_catalog``.
            dispatcher: Parallel task dispatcher.

        Returns:
            Fully wired :class:`ETLRunner`.

        Raises:
            KeyError:                When ``storage:`` key is absent from the YAML.
            msgspec.ValidationError: When storage or observability config is malformed.
            TypeError:               When a delta-rs option dict contains an invalid key.
            ValueError:              When ``storage.type: unity_catalog`` and *spark*
                                     is ``None``.
        """
        _log.debug("load yaml path=%s", path)
        storage_config, obs_config = _load_yaml(path)
        storage_config.validate()
        return cls.from_config(storage_config, obs_config, spark=spark, dispatcher=dispatcher)

    @classmethod
    def from_spark(
        cls,
        spark: SparkSession,
        obs_config: ObservabilityConfig | None = None,
        *,
        dispatcher: ParallelDispatcher | None = None,
        cleaner: TempCleaner | None = None,
    ) -> ETLRunner:
        """Build an :class:`ETLRunner` for Databricks Unity Catalog.

        Zero-config entry point for Unity Catalog environments.  Unity Catalog
        manages storage, credentials, and table resolution — the active
        :class:`pyspark.sql.SparkSession` is the only required argument.

        Args:
            spark:      Active :class:`pyspark.sql.SparkSession`, already
                        authenticated by the Databricks cluster runtime.
            obs_config: Observability config.  Defaults to
                        :class:`~loom.etl._observability_config.ObservabilityConfig`
                        (structlog on, no run sink).
            dispatcher: Parallel task dispatcher.
            cleaner:    Cloud-aware temp cleaner.  Defaults to
                        :class:`~loom.etl.AutoTempCleaner`.  Pass
                        :class:`~loom.etl.DbutilsTempCleaner` for ``dbfs:/``
                        paths on Databricks.

        Returns:
            Fully wired :class:`ETLRunner` backed by Unity Catalog.

        Example::

            from loom.etl import ETLRunner

            runner = ETLRunner.from_spark(spark)
            runner.run(MyPipeline, MyParams(...))

            # With DBFS temp storage on Databricks:
            from loom.etl import DbutilsTempCleaner
            runner = ETLRunner.from_spark(spark, cleaner=DbutilsTempCleaner(dbutils))
        """
        config = UnityCatalogConfig(type="unity_catalog")
        return cls.from_config(
            config, obs_config, spark=spark, dispatcher=dispatcher, cleaner=cleaner
        )

    @classmethod
    def from_dict(
        cls,
        storage: dict[str, Any],
        observability: dict[str, Any] | None = None,
        *,
        spark: SparkSession | None = None,
        dispatcher: ParallelDispatcher | None = None,
        cleaner: TempCleaner | None = None,
    ) -> ETLRunner:
        """Build an :class:`ETLRunner` from pre-resolved plain Python dicts.

        Bypasses OmegaConf entirely — no ``${...}`` interpolation is performed.
        All values in *storage* must already be final strings.

        Use this entry point when:

        * Writing tests — build config inline without touching any external
          system (``{"root": "/tmp/lake", "storage_options": {}}``).
        * The config was fetched and resolved by the caller from an external
          source (AWS SSM, GCP Secret Manager, Vault, etc.).

        Args:
            storage:     Plain dict matching the ``storage:`` YAML section.
                         See :class:`~loom.etl.DeltaConfig` and
                         :class:`~loom.etl.UnityCatalogConfig` for the full
                         reference.
            observability: Plain dict matching the ``observability:`` YAML
                           section.  ``None`` activates structlog only.
            spark:       Active :class:`pyspark.sql.SparkSession`.  Required
                         when *storage* declares ``type: unity_catalog``.
            dispatcher:  Parallel task dispatcher.

        Returns:
            Fully wired :class:`ETLRunner`.

        Raises:
            msgspec.ValidationError: When *storage* or *observability* does not
                                     match the expected shape.
            ValueError:              When ``type: unity_catalog`` and *spark*
                                     is ``None``.

        Example::

            runner = ETLRunner.from_dict(
                storage={"root": "/tmp/lake", "storage_options": {}},
            )
            runner.run(MyPipeline, MyParams(...))
        """
        storage_config = convert_storage_config(storage)
        storage_config.validate()
        obs_config = (
            msgspec.convert(observability, ObservabilityConfig)
            if observability is not None
            else ObservabilityConfig()
        )
        return cls.from_config(
            storage_config, obs_config, spark=spark, dispatcher=dispatcher, cleaner=cleaner
        )

    def run(
        self,
        pipeline: type[ETLPipeline[Any]],
        params: Any,
        *,
        include: Sequence[str] | None = None,
        correlation_id: str | None = None,
        attempt: int = 1,
        last_attempt: bool = True,
    ) -> None:
        """Compile, optionally filter, and execute *pipeline*.

        Args:
            pipeline:       The :class:`~loom.etl.ETLPipeline` subclass to run.
            params:         Concrete params instance matching the pipeline's ``ParamsT``.
            include:        Step or process class names to execute.  ``None`` runs all.
            correlation_id: Opaque ID from the orchestrator to group retry attempts.
                            Typically the Databricks or Bytewarx job run ID.
            attempt:        1-based retry counter supplied by the orchestrator.
            last_attempt:   ``True`` when no further retry will be attempted.
                            Defaults to ``True`` — safe for non-orchestrated runs.
                            Set to ``False`` from your orchestrator when more retries
                            remain, so CORRELATION intermediates survive for the next
                            attempt.

        Raises:
            InvalidStageError: When *include* matches no step or process in the plan.
            Exception:         Any unhandled exception from read / execute / write.
        """
        _log.info("compile pipeline=%s", pipeline.__name__)
        plan = self._compiler.compile(pipeline)
        if include is not None:
            _log.debug("filter plan include=%s", sorted(include))
            plan = _filter_plan(plan, frozenset(include))
        ctx = RunContext(
            run_id=_new_run_id(),
            correlation_id=correlation_id,
            attempt=attempt,
            last_attempt=last_attempt,
        )
        self._executor.run_pipeline(plan, params, ctx)

    def cleanup_correlation(self, correlation_id: str) -> None:
        """Remove all CORRELATION-scope intermediates for *correlation_id*.

        Call this after the final failed attempt, or any time you want to
        reclaim storage for a specific logical job.

        Args:
            correlation_id: The same ID passed to :meth:`run`.

        Raises:
            RuntimeError: When the runner has no :class:`~loom.etl._temp_store.IntermediateStore`
                          configured (i.e. ``tmp_root`` was not set in YAML).
        """
        if self._temp_store is None:
            raise RuntimeError(
                "cleanup_correlation() requires a tmp_root to be configured in storage YAML."
            )
        self._temp_store.cleanup_correlation(correlation_id)

    def cleanup_stale_temps(self, *, older_than_seconds: int = 86_400) -> None:
        """Remove run directories not modified within *older_than_seconds*.

        Garbage-collects orphaned RUN-scope directories after a crash that
        prevented normal ``finally``-based cleanup.

        Args:
            older_than_seconds: Age threshold.  Defaults to 86 400 (24 hours).

        Raises:
            RuntimeError: When the runner has no :class:`~loom.etl._temp_store.IntermediateStore`.
        """
        if self._temp_store is None:
            raise RuntimeError(
                "cleanup_stale_temps() requires a tmp_root to be configured in storage YAML."
            )
        self._temp_store.cleanup_stale(older_than_seconds=older_than_seconds)


def _filter_plan(plan: PipelinePlan, include: frozenset[str]) -> PipelinePlan:
    """Return a new :class:`PipelinePlan` keeping only nodes matched by *include*.

    Args:
        plan:    Fully compiled pipeline plan.
        include: Set of step or process class names to keep.

    Returns:
        Filtered :class:`PipelinePlan`.

    Raises:
        InvalidStageError: When no node survives the filter.
    """
    nodes = _filter_pipeline_nodes(plan.nodes, include)
    if not nodes:
        raise InvalidStageError(include)
    return PipelinePlan(
        pipeline_type=plan.pipeline_type,
        params_type=plan.params_type,
        nodes=nodes,
    )


def _filter_pipeline_nodes(
    nodes: tuple[PipelineProcessNode, ...],
    include: frozenset[str],
) -> tuple[PipelineProcessNode, ...]:
    result: list[PipelineProcessNode] = []
    for node in nodes:
        match node:
            case ProcessPlan():
                filtered = _filter_process(node, include)
                if filtered is not None:
                    result.append(filtered)
            case ParallelProcessGroup(plans=plans):
                kept = tuple(
                    p for p in (_filter_process(p, include) for p in plans) if p is not None
                )
                if len(kept) == 1:
                    result.append(kept[0])
                elif kept:
                    result.append(ParallelProcessGroup(plans=kept))
    return tuple(result)


def _filter_process(plan: ProcessPlan, include: frozenset[str]) -> ProcessPlan | None:
    if plan.process_type.__name__ in include:
        return plan
    nodes = _filter_process_nodes(plan.nodes, include)
    if not nodes:
        return None
    return ProcessPlan(
        process_type=plan.process_type,
        params_type=plan.params_type,
        nodes=nodes,
    )


def _filter_process_nodes(
    nodes: tuple[ProcessStepNode, ...],
    include: frozenset[str],
) -> tuple[ProcessStepNode, ...]:
    result: list[ProcessStepNode] = []
    for node in nodes:
        match node:
            case StepPlan() if node.step_type.__name__ in include:
                result.append(node)
            case ParallelStepGroup(plans=plans):
                kept = tuple(p for p in plans if p.step_type.__name__ in include)
                if len(kept) == 1:
                    result.append(kept[0])
                elif kept:
                    result.append(ParallelStepGroup(plans=kept))
    return tuple(result)


class _TempStoreAware(Protocol):
    """Structural protocol satisfied by every StorageConfig variant.

    All current variants (:class:`~loom.etl.DeltaConfig`,
    :class:`~loom.etl.UnityCatalogConfig`) carry these two fields.  Any future
    backend added to :data:`~loom.etl.StorageConfig` must also declare them so
    that :func:`_make_temp_store` works without modification.
    """

    tmp_root: str
    tmp_storage_options: dict[str, str]


def _make_backends(
    config: StorageConfig,
    spark: Any = None,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    # Extension point: add a new `case NewConfig():` branch here when
    # introducing a third storage backend.  Each branch must return
    # (SourceReader, TargetWriter, TableDiscovery) and handle its own
    # spark-requirement check if applicable.
    _log.debug("backend config_type=%s", type(config).__name__)
    match config:
        case DeltaConfig():
            return _make_polars_backends(config)
        case UnityCatalogConfig():
            if spark is None:
                raise ValueError(
                    "A SparkSession is required for UnityCatalogConfig. "
                    "Pass spark=<session> to ETLRunner.from_yaml() or ETLRunner.from_config()."
                )
            return _make_spark_backends(config, spark)


def _make_spark_backends(
    config: UnityCatalogConfig,
    spark: Any,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.spark import SparkCatalog, SparkDeltaReader, SparkDeltaWriter

    catalog = SparkCatalog(spark)
    return SparkDeltaReader(spark), SparkDeltaWriter(spark, None, catalog), catalog


def _make_polars_backends(
    config: DeltaConfig,
) -> tuple[SourceReader, TargetWriter, TableDiscovery]:
    from loom.etl.backends.polars import DeltaCatalog, PolarsDeltaReader, PolarsDeltaWriter

    locator = config.to_locator()
    catalog = DeltaCatalog(locator)
    return PolarsDeltaReader(locator), PolarsDeltaWriter(locator, catalog), catalog


def _make_observers(config: ObservabilityConfig) -> list[ETLRunObserver]:
    from loom.etl.executor.observer._sink_observer import RunSinkObserver
    from loom.etl.executor.observer._structlog import StructlogRunObserver
    from loom.etl.executor.observer.sinks import DeltaRunSink

    observers: list[ETLRunObserver] = []
    if config.log:
        observers.append(StructlogRunObserver(slow_step_threshold_ms=config.slow_step_threshold_ms))
    if config.run_sink is not None and config.run_sink.root:
        from loom.etl._locator import TableLocation

        location = TableLocation(
            uri=config.run_sink.root,
            storage_options=config.run_sink.storage_options,
        )
        observers.append(RunSinkObserver(DeltaRunSink(location)))
    return observers


def _make_temp_store(
    config: StorageConfig,
    spark: Any = None,
    cleaner: TempCleaner | None = None,
) -> IntermediateStore | None:
    """Build an :class:`IntermediateStore` from config, or ``None`` when unconfigured.

    Accesses ``tmp_root`` and ``tmp_storage_options`` through
    :class:`_TempStoreAware` — all :data:`~loom.etl.StorageConfig` variants
    satisfy this protocol, so no dynamic attribute access is needed.
    """
    temp_cfg = cast(_TempStoreAware, config)
    if not temp_cfg.tmp_root:
        _log.debug("temp store disabled (tmp_root not set)")
        return None
    _log.debug("temp store root=%s", temp_cfg.tmp_root)
    return IntermediateStore(
        tmp_root=temp_cfg.tmp_root,
        storage_options=temp_cfg.tmp_storage_options or {},
        spark=spark,
        cleaner=cleaner,
    )


def _new_run_id() -> str:
    import uuid

    return str(uuid.uuid4())


def _read_yaml_file(path: str | os.PathLike[str]) -> str:
    """Read raw YAML text from a local filesystem path.

    Pure I/O — no parsing, no interpolation.  Keeping this separate from
    :func:`_parse_yaml_content` means future entry points (e.g. loading from
    object storage) only need to supply their own fetch step and then call
    :func:`_parse_yaml_content` on the result.
    """
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _parse_yaml_content(
    content: str,
) -> tuple[StorageConfig, ObservabilityConfig]:
    """Parse and resolve a YAML string into typed config objects.

    Uses OmegaConf to resolve ``${oc.env:VAR,default}`` interpolations.
    This is the single point where OmegaConf is used — future resolver
    registrations (e.g. ``loom.ssm``, ``loom.dbsecrets``) should be
    registered before this function is called.

    Args:
        content: Raw YAML text (``storage:`` key required).

    Returns:
        Tuple of validated :data:`~loom.etl.StorageConfig` and
        :class:`~loom.etl.ObservabilityConfig`.

    Raises:
        KeyError:                When ``storage:`` key is absent.
        msgspec.ValidationError: When config shape is invalid.
    """
    from omegaconf import DictConfig, OmegaConf

    created = OmegaConf.create(content)
    assert isinstance(created, DictConfig)
    raw: DictConfig = created
    storage_raw: Any = OmegaConf.to_container(raw["storage"], resolve=True)
    storage_config = convert_storage_config(storage_raw)

    obs_config = ObservabilityConfig()
    if "observability" in raw:
        obs_raw: Any = OmegaConf.to_container(raw["observability"], resolve=True)
        obs_config = msgspec.convert(obs_raw, ObservabilityConfig)

    return storage_config, obs_config


def _load_yaml(
    path: str | os.PathLike[str],
) -> tuple[StorageConfig, ObservabilityConfig]:
    """Read a YAML file from the filesystem and parse it.

    Composes :func:`_read_yaml_file` (I/O) with :func:`_parse_yaml_content`
    (OmegaConf + msgspec).  Keeping these two steps separate means callers
    with non-filesystem sources can call :func:`_parse_yaml_content` directly
    after fetching the content themselves.
    """
    return _parse_yaml_content(_read_yaml_file(path))
