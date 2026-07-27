"""Prefect flow factory for chunked ETL backfills — mirrors etl_flow()."""

from __future__ import annotations

import inspect
import logging
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

import msgspec
import prefect

from loom.etl.pipeline import ETLPipeline
from loom.etl.runner import ETLRunner
from loom.prefect._meta import LOOM_ETL_META_ATTR, ETLFlowMeta
from loom.prefect._placeholders import resolve_placeholder
from loom.prefect._summary import set_run_summary
from loom.prefect.deploy._schedule import extract_pool_config
from loom.prefect.deploy._yaml import read_yaml
from loom.prefect.flow._body import (
    _build_observers,
    _load_or_init_manifest,
    _maybe_delete_manifest,
)
from loom.prefect.flow._common import coerce_tags as _coerce_tags
from loom.prefect.flow._common import prefect_flow_run_id
from loom.prefect.flow._hooks import make_notification_hooks, pause_schedule_on_failure
from loom.prefect.flow._run_name import make_run_name_callback
from loom.prefect.flow._signature import normalize_datetime_fields, signature_from_params_type
from loom.prefect.manifest import ManifestStore
from loom.prefect.notify import build_notifiers
from loom.prefect.observer._logging_bridge import install_log_bridge, uninstall_log_bridge

_log = logging.getLogger(__name__)

Chunk = Literal["hour", "day", "month"]

_CHUNK_LABEL: dict[Chunk, str] = {"hour": "%Y%m%d%H", "day": "%Y%m%d", "month": "%Y%m"}


def backfill_flow(
    *,
    name: str,
    pipeline: type[ETLPipeline[Any]],
    params_type: type[msgspec.Struct],
    config_path: str,
    source_file: str,
    per_chunk_processes: list[str],
    finalize_processes: list[str],
    chunk: Chunk = "day",
    window_start_field: str = "updated_at_from",
    window_end_field: str = "updated_at_to",
    storage_config_path: str = "/app/config.yaml",
    manifest_store: ManifestStore | None = None,
) -> Any:
    """Build a Prefect flow that backfills a pipeline one chunk at a time.

    Unlike :func:`~loom.prefect.etl_flow`, which runs a pipeline once over the
    whole parameter window, ``backfill_flow`` slices the window
    ``[window_start_field, window_end_field)`` into ``chunk``-aligned partitions
    (``[C, C+1chunk)``) and runs ``per_chunk_processes`` once per chunk, oldest
    first. After every chunk it runs ``finalize_processes`` a single time — the
    finalize run's ``window_end_field`` is pinned to the start of the *current*
    chunk (not ``now``) so downstream batch-sequenced refreshes get a stable
    boundary.

    Args:
        name: Logical flow name shown in the Prefect UI.
        pipeline: ``ETLPipeline`` subclass to execute.
        params_type: ``msgspec.Struct`` whose fields become typed flow kwargs.
            Must carry the two window fields named below.
        config_path: Path to the per-flow YAML (schedule, params, tags, …).
        source_file: ``__file__`` of the calling module (needed by Prefect
            ``from_source`` to locate the flow on disk).
        per_chunk_processes: Process/step names run once for each chunk window.
        finalize_processes: Process/step names run once after all chunks.
        chunk: Partition granularity used to slice the window.
        window_start_field: Name of the datetime field holding the window start.
        window_end_field: Name of the datetime field holding the (exclusive)
            window end; also the field pinned on the finalize run.
        storage_config_path: Path to the loom storage YAML read at runtime.
            Overridable via ``LOOM_STORAGE_CONFIG_PATH``.
        manifest_store: Optional resume/observability manifest backend. Each
            chunk gets its own correlation id so manifests never leak across
            chunks.

    Returns:
        A ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``
        attached for the deployer. It accepts ``start_from`` to resume the
        backfill from a given chunk (earlier chunks are skipped).
    """
    raw_cfg = read_yaml(config_path)
    schedule = raw_cfg.get("schedule")
    raw_params = dict(raw_cfg.get("params") or {})
    pool_config = extract_pool_config(raw_cfg)
    tags = _coerce_tags(raw_cfg.get("tags"))
    notifiers = build_notifiers(raw_cfg.get("notifications"))

    resolved_config_path = str(Path(config_path).resolve())
    resolved_source_file = str(Path(source_file).resolve())

    def _flow_body(**kwargs: Any) -> None:
        kwargs.pop("env", "prod")
        start_from = kwargs.pop("start_from", None)
        resolved = {k: resolve_placeholder(v) for k, v in kwargs.items()}
        resolved = normalize_datetime_fields(resolved, params_type)
        params = msgspec.convert(resolved, type=params_type)

        window_start = _as_utc(getattr(params, window_start_field))
        window_end = _as_utc(getattr(params, window_end_field))
        windows = _chunk_windows(window_start, window_end, chunk, start_from)

        actual_path = os.environ.get("LOOM_STORAGE_CONFIG_PATH") or storage_config_path
        run_id_base = f"{name}-{uuid4().hex[:8]}"
        flow_run_id = prefect_flow_run_id()

        install_log_bridge(flow_run_id)
        try:
            for index, (chunk_start, chunk_end) in enumerate(windows, start=1):
                chunk_params = msgspec.structs.replace(
                    params, **{window_start_field: chunk_start, window_end_field: chunk_end}
                )
                label = format(chunk_start, _CHUNK_LABEL[chunk])
                _log.info(
                    "backfill %s chunk %d/%d window=[%s, %s)",
                    name,
                    index,
                    len(windows),
                    chunk_start.isoformat(),
                    chunk_end.isoformat(),
                )
                _run_slice(
                    actual_path,
                    pipeline,
                    chunk_params,
                    per_chunk_processes,
                    correlation_id=f"{name}-{label}",
                    run_id=f"{run_id_base}-{label}",
                    flow_run_id=flow_run_id,
                    manifest_store=manifest_store,
                )

            finalize_params = msgspec.structs.replace(
                params, **{window_end_field: _floor_chunk(_now_utc(), chunk)}
            )
            _log.info("backfill %s finalize include=%s", name, finalize_processes)
            _run_slice(
                actual_path,
                pipeline,
                finalize_params,
                finalize_processes,
                correlation_id=f"{name}-finalize",
                run_id=f"{run_id_base}-finalize",
                flow_run_id=flow_run_id,
                manifest_store=manifest_store,
            )
        finally:
            uninstall_log_bridge()
        set_run_summary(_backfill_summary(name, len(windows), chunk, finalize_processes))

    safe_name = name.replace("-", "_")
    body: Any = _flow_body
    body.__signature__ = _synthesise_signature(params_type)
    body.__name__ = safe_name
    body.__qualname__ = safe_name

    failure_hooks, completion_hooks = make_notification_hooks(name, notifiers)
    decorated = prefect.flow(
        name=name,
        flow_run_name=make_run_name_callback(name, None),
        validate_parameters=False,
        on_failure=[pause_schedule_on_failure, *failure_hooks],
        on_completion=completion_hooks or None,
    )(body)
    setattr(
        decorated,
        LOOM_ETL_META_ATTR,
        ETLFlowMeta(
            name=name,
            config_path=resolved_config_path,
            source_file=resolved_source_file,
            correlation_field=None,
            schedule=schedule,
            raw_params=raw_params,
            pool_config=pool_config,
            tags=tags,
        ),
    )
    return decorated


def _run_slice(
    config_path: str,
    pipeline: type[ETLPipeline[Any]],
    params: Any,
    include: list[str],
    *,
    correlation_id: str,
    run_id: str,
    flow_run_id: Any,
    manifest_store: ManifestStore | None,
) -> None:
    """Run one backfill slice (a chunk or the finalize pass) as an isolated unit.

    Each slice loads its own manifest under ``correlation_id`` so chunk-level
    resume/observability never leaks across chunks.
    """
    manifest = _load_or_init_manifest(manifest_store, correlation_id)
    observers = _build_observers(flow_run_id, manifest_store, manifest)
    runner = ETLRunner.from_yaml(config_path, extra_observers=observers)
    runner.run(
        pipeline,
        params,
        include=include,
        correlation_id=correlation_id,
        run_id=run_id,
    )
    _maybe_delete_manifest(manifest_store, correlation_id)


def _chunk_windows(
    window_start: datetime,
    window_end: datetime,
    chunk: Chunk,
    start_from: datetime | None,
) -> list[tuple[datetime, datetime]]:
    """Slice ``[window_start, window_end)`` into ``chunk``-aligned windows, oldest first.

    ``start_from`` (if given) resumes at its own chunk, skipping earlier chunks.
    """
    cursor = _floor_chunk(window_start, chunk)
    if start_from is not None:
        resume = _floor_chunk(_as_utc(start_from), chunk)
        if resume > cursor:
            cursor = resume
    windows: list[tuple[datetime, datetime]] = []
    while cursor < window_end:
        nxt = _next_chunk(cursor, chunk)
        windows.append((cursor, nxt))
        cursor = nxt
    return windows


def _floor_chunk(value: datetime, chunk: Chunk) -> datetime:
    value = _as_utc(value).astimezone(UTC)
    if chunk == "hour":
        return value.replace(minute=0, second=0, microsecond=0)
    if chunk == "month":
        return value.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    return value.replace(hour=0, minute=0, second=0, microsecond=0)


def _next_chunk(value: datetime, chunk: Chunk) -> datetime:
    if chunk == "hour":
        return value + timedelta(hours=1)
    if chunk == "month":
        year, month = divmod(value.month, 12)
        return value.replace(year=value.year + year, month=month + 1)
    return value + timedelta(days=1)


def _as_utc(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _backfill_summary(
    name: str, chunk_count: int, chunk: Chunk, finalize_processes: list[str]
) -> str:
    return (
        f"{name} — {chunk_count} {chunk} chunk(s) backfilled  "
        f"finalize: {', '.join(finalize_processes)}"
    )


def _synthesise_signature(params_type: type[msgspec.Struct]) -> inspect.Signature:
    user_params = signature_from_params_type(params_type)
    extra = [
        inspect.Parameter("env", inspect.Parameter.KEYWORD_ONLY, default="prod", annotation=str),
        inspect.Parameter(
            "start_from",
            inspect.Parameter.KEYWORD_ONLY,
            default=None,
            annotation=datetime | None,
        ),
    ]
    return inspect.Signature(parameters=user_params + extra, return_annotation=None)


__all__ = ["Chunk", "backfill_flow"]
