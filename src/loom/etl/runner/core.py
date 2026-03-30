"""ETLRunner — single entry point that wires config, compiler, and executor."""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import msgspec

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

from loom.etl.compiler import ETLCompiler
from loom.etl.executor import ETLExecutor, ETLRunObserver, ParallelDispatcher
from loom.etl.executor.observer._composite import CompositeObserver
from loom.etl.executor.observer._events import RunContext
from loom.etl.pipeline._pipeline import ETLPipeline
from loom.etl.runner.config_loader import _load_yaml
from loom.etl.runner.errors import InvalidStageError
from loom.etl.runner.filtering import _filter_plan
from loom.etl.storage._config import StorageConfig, UnityCatalogConfig, convert_storage_config
from loom.etl.storage._factory import make_backends, make_observers, make_temp_store
from loom.etl.storage._io import SourceReader, TableDiscovery, TargetWriter
from loom.etl.storage._observability import ObservabilityConfig
from loom.etl.temp._cleaners import TempCleaner
from loom.etl.temp._store import IntermediateStore

_log = logging.getLogger(__name__)


class ETLRunner:
    """Wire storage config, backend I/O, and executor into one callable.

    Args:
        reader: Source reader implementation.
        writer: Target writer implementation.
        catalog: Catalog used for schema validation at compile time.
        observers: Lifecycle observers wrapped in a composite observer.
        dispatcher: Parallel task dispatcher.
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
        """Build an :class:`ETLRunner` from resolved config objects."""
        reader, writer, catalog = make_backends(config, spark)
        observers = make_observers(obs_config or ObservabilityConfig())
        temp_store = make_temp_store(config, spark, cleaner)
        return cls(reader, writer, catalog, observers, dispatcher, temp_store)

    @classmethod
    def from_yaml(
        cls,
        path: str | os.PathLike[str],
        *,
        spark: SparkSession | None = None,
        dispatcher: ParallelDispatcher | None = None,
    ) -> ETLRunner:
        """Load config from a YAML file and build an :class:`ETLRunner`."""
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
        """Build an :class:`ETLRunner` for Databricks Unity Catalog."""
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
        """Build an :class:`ETLRunner` from pre-resolved plain Python dicts."""
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
        """Compile, optionally filter, and execute *pipeline*."""
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
        """Remove all CORRELATION-scope intermediates for *correlation_id*."""
        if self._temp_store is None:
            raise RuntimeError(
                "cleanup_correlation() requires a tmp_root to be configured in storage YAML."
            )
        self._temp_store.cleanup_correlation(correlation_id)

    def cleanup_stale_temps(self, *, older_than_seconds: int = 86_400) -> None:
        """Remove run directories not modified within *older_than_seconds*."""
        if self._temp_store is None:
            raise RuntimeError(
                "cleanup_stale_temps() requires a tmp_root to be configured in storage YAML."
            )
        self._temp_store.cleanup_stale(older_than_seconds=older_than_seconds)


def _new_run_id() -> str:
    return str(uuid.uuid4())


__all__ = ["ETLRunner", "InvalidStageError"]
