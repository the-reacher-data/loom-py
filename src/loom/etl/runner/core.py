"""ETLRunner — single entry point that wires config, compiler, and executor."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from loom.core.observability.protocol import LifecycleObserver
from loom.core.observability.runtime import ObservabilityRuntime
from loom.core.runner import flush_runner
from loom.etl.checkpoint import CheckpointStore, TempCleaner
from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor, ParallelDispatcher
from loom.etl.lineage._config import ETLObservabilityConfig
from loom.etl.lineage._observer import LineageObserver
from loom.etl.lineage._records import RunContext
from loom.etl.pipeline._pipeline import ETLPipeline
from loom.etl.runner._wiring import (
    make_backends,
    make_checkpoint_store,
    make_lineage_store,
)
from loom.etl.runner.config_loader import _load_yaml
from loom.etl.runner.errors import InvalidStageError
from loom.etl.runner.filtering import _filter_plan
from loom.etl.runtime.contracts import SourceReader, TargetWriter
from loom.etl.storage._config import StorageConfig, convert_storage_config

_log = logging.getLogger(__name__)


class ETLRunner:
    """Wire storage config, backend I/O, and executor into one callable.

    Args:
        reader: Source reader implementation.
        writer: Target writer implementation.
        observers: Lifecycle observers wrapped in a composite observer.
        dispatcher: Parallel task dispatcher.
    """

    def __init__(
        self,
        reader: SourceReader,
        writer: TargetWriter,
        observability: ObservabilityRuntime | None = None,
        dispatcher: ParallelDispatcher | None = None,
        checkpoint_store: CheckpointStore | None = None,
    ) -> None:
        self._executor = ETLExecutor(
            reader,
            writer,
            observability or ObservabilityRuntime.noop(),
            dispatcher,
            checkpoint_store,
        )
        self._compiler = ETLCompiler()
        self._checkpoint_store = checkpoint_store

    @classmethod
    def from_config(
        cls,
        config: StorageConfig,
        obs_config: ETLObservabilityConfig | None = None,
        *,
        spark: SparkSession | None = None,
        dispatcher: ParallelDispatcher | None = None,
        cleaner: TempCleaner | None = None,
        extra_observers: Sequence[LifecycleObserver] | None = None,
    ) -> ETLRunner:
        """Build an :class:`ETLRunner` from resolved config objects.

        Args:
            extra_observers: Optional additional :class:`LifecycleObserver`
                instances appended after the config-derived ones. Use this
                to inject orchestrator-specific observers (e.g. the Prefect
                TaskRun observer) without subclassing the runner.
        """
        resolved_obs_config = obs_config or ETLObservabilityConfig()
        reader, writer = make_backends(config, spark)
        observability = ObservabilityRuntime.from_config(resolved_obs_config)
        if resolved_obs_config.lineage.enabled:
            lineage_store = make_lineage_store(config, resolved_obs_config.lineage, spark)
            if lineage_store is not None:
                observability = ObservabilityRuntime(
                    [*observability.observers, LineageObserver(lineage_store)]
                )
        if extra_observers:
            observability = ObservabilityRuntime([*observability.observers, *extra_observers])
        checkpoint_store = make_checkpoint_store(config, spark, cleaner)
        return cls(reader, writer, observability, dispatcher, checkpoint_store)

    @classmethod
    def from_yaml(
        cls,
        path: str,
        *,
        spark: SparkSession | None = None,
        dispatcher: ParallelDispatcher | None = None,
        extra_observers: Sequence[LifecycleObserver] | None = None,
    ) -> ETLRunner:
        """Load config from a YAML file and build an :class:`ETLRunner`.

        Args:
            extra_observers: Optional additional :class:`LifecycleObserver`
                instances forwarded to :meth:`from_config`.
        """
        _log.debug("load yaml path=%s", path)
        storage_config, obs_config = _load_yaml(path)
        storage_config.validate()
        return cls.from_config(
            storage_config,
            obs_config,
            spark=spark,
            dispatcher=dispatcher,
            extra_observers=extra_observers,
        )

    @classmethod
    def from_spark(
        cls,
        spark: SparkSession,
        obs_config: ETLObservabilityConfig | None = None,
        *,
        dispatcher: ParallelDispatcher | None = None,
        cleaner: TempCleaner | None = None,
    ) -> ETLRunner:
        """Build an :class:`ETLRunner` using Spark as the execution engine."""
        config = StorageConfig(engine="spark")
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
        """Build an :class:`ETLRunner` from pre-resolved plain Python dicts."""
        storage_config = convert_storage_config(storage)
        storage_config.validate()
        obs_config = (
            msgspec.convert(observability, ETLObservabilityConfig)
            if observability is not None
            else ETLObservabilityConfig()
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
        run_id: str | None = None,
        attempt: int = 1,
        last_attempt: bool = True,
    ) -> None:
        """Compile, optionally filter, and execute *pipeline*.

        Args:
            pipeline: ETLPipeline class to compile and execute.
            params: ETLParams instance for this run.
            include: If set, only steps whose names are in this sequence run.
            correlation_id: Logical business unit identifier, stable across retries.
            run_id: Execution-specific identifier for lineage records. If None,
                a random UUID is generated. Callers that share a traceability
                identifier with an orchestrator (e.g. Prefect) should pass it
                explicitly so lineage and orchestrator records align.
            attempt: Current attempt number (1-based).
            last_attempt: Whether this is the final allowed attempt.
        """
        _log.info("compile pipeline=%s", pipeline.__name__)
        plan = self._compiler.compile(pipeline)
        if include is not None:
            _log.debug("filter plan include=%s", sorted(include))
            plan = _filter_plan(plan, frozenset(include))
        ctx = RunContext(
            run_id=run_id if run_id is not None else str(uuid.uuid4()),
            correlation_id=correlation_id,
            attempt=attempt,
            last_attempt=last_attempt,
        )
        try:
            self._executor.run_pipeline(plan, params, ctx)
        finally:
            flush_runner(self)

    def flush(self) -> None:
        """Flush buffered ETL observability sinks after a run."""
        self._executor.flush()

    def cleanup_correlation(self, correlation_id: str) -> None:
        """Remove all CORRELATION-scope intermediates for *correlation_id*."""
        if self._checkpoint_store is None:
            raise RuntimeError(
                "cleanup_correlation() requires checkpoint_root (storage.temp.root) "
                "to be configured in storage YAML."
            )
        self._checkpoint_store.cleanup_correlation(correlation_id)


__all__ = ["ETLRunner", "InvalidStageError"]
