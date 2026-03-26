"""ObservabilityConfig — YAML-loadable observability configuration.

Defines the optional ``observability:`` section of the Loom YAML config.

YAML reference::

    observability:
      log: true          # StructlogRunObserver — always active by default
      run_sink:
        root: s3://my-lake/runs/
        storage_options:
          AWS_REGION: ${oc.env:AWS_REGION,eu-west-1}

Omitting the ``observability:`` section entirely activates only
:class:`~loom.etl.executor.StructlogRunObserver` (``log: true`` default).
"""

from __future__ import annotations

import msgspec


class RunSinkConfig(msgspec.Struct, frozen=True):
    """Delta Lake sink configuration for persisting run records.

    Args:
        root:            Root URI for the run tables (``pipeline_runs/``,
                         ``process_runs/``, ``step_runs/`` are appended).
        storage_options: Cloud credentials forwarded verbatim to delta-rs.
    """

    root: str = ""
    storage_options: dict[str, str] = {}


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
                                is added.  Omit to skip Delta persistence.
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
