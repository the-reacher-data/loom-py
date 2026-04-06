"""ObservabilityConfig — YAML-loadable observability configuration.

Defines the optional ``observability:`` section of the Loom YAML config.

YAML reference::

    observability:
      log: true          # StructlogRunObserver — always active by default
      run_sink:
        # Choose one destination mode:
        # root: s3://my-lake/runs/   # path mode
        # database: ops              # metastore mode (Spark)
        root: s3://my-lake/runs/
        storage_options:
          AWS_REGION: ${oc.env:AWS_REGION,eu-west-1}

Omitting the ``observability:`` section entirely activates only
:class:`~loom.etl.executor.StructlogRunObserver` (``log: true`` default).
"""

from __future__ import annotations

from typing import Any

import msgspec


class RunSinkConfig(msgspec.Struct, frozen=True):
    """Delta Lake sink configuration for persisting run records.

    Args:
        root:            Root URI for path-based run tables.
        database:        Database/schema for metastore-backed run tables.
                         Exactly one of ``root`` or ``database`` must be set.
        storage_options: Cloud credentials forwarded to backend writes
                         (path-based mode).
        writer:          Parquet writer settings (path-based mode).
        delta_config:    Delta table properties (path-based mode).
        commit:          Commit metadata (path-based mode).
    """

    root: str = ""
    database: str = ""
    storage_options: dict[str, str] = {}
    writer: dict[str, Any] = {}
    delta_config: dict[str, str | None] = {}
    commit: dict[str, Any] = {}

    def validate(self) -> None:
        """Validate destination selection.

        Raises:
            ValueError: If both or neither of ``root`` and ``database`` are set.
        """
        has_root = bool(self.root.strip())
        has_database = bool(self.database.strip())
        if has_root == has_database:
            raise ValueError(
                "observability.run_sink requires exactly one destination: "
                "'root' (path mode) or 'database' (catalog mode)."
            )


class ObservabilityConfig(msgspec.Struct, frozen=True):
    """Observability configuration loaded from the ``observability:`` YAML key.

    Args:
        log:                    When ``true`` (default), a
                                :class:`~loom.etl.executor.StructlogRunObserver`
                                is added automatically — no configuration needed.
        run_sink:               When set, a
                                :class:`~loom.etl.executor.RunSinkObserver`
                                backed by
                                :class:`~loom.etl.executor.observer.sinks.DeltaRunSink`
                                is added. ``run_sink`` must define exactly one
                                destination: ``root`` (path) or ``database``
                                (catalog).
        slow_step_threshold_ms: When set, the
                                :class:`~loom.etl.executor.StructlogRunObserver`
                                emits a ``WARNING``-level ``slow_step`` event for
                                any step that exceeds this wall-clock duration.
                                ``null`` (default) disables the warning.

    YAML example::

        observability:
          log: true
          slow_step_threshold_ms: 30000   # warn when a step takes > 30 s
          run_sink:
            root: s3://my-lake/runs/
    """

    log: bool = True
    run_sink: RunSinkConfig | None = None
    slow_step_threshold_ms: int | None = None
