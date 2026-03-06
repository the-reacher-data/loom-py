from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from graphlib import TopologicalSorter
from typing import Any, Protocol, cast

from loom.core.model.projection import (
    PROJECTION_DEFAULT_MISSING,
    Projection,
    ProjectionSource,
)

PROJECTION_DEP_PREFIX = "projection:"


class BackendProjectionLoader(Protocol):
    async def load_many(
        self,
        backend_context: Any,
        parent_ids: Sequence[object],
    ) -> Mapping[object, Any]: ...


class MemoryProjectionLoader(Protocol):
    relation: str

    def load_from_object(
        self,
        obj: Any,
        context: Mapping[str, Any] | None = None,
    ) -> Any: ...


@dataclass(frozen=True, slots=True)
class ProjectionStep:
    name: str
    projection: Projection
    backend_loader: BackendProjectionLoader | None
    memory_loader: MemoryProjectionLoader | None


@dataclass(frozen=True, slots=True)
class ProjectionPlan:
    """Compiled projection execution plan grouped by dependency levels."""

    levels: tuple[tuple[ProjectionStep, ...], ...]


@dataclass(frozen=True, slots=True)
class _ExecutionBuckets:
    backend: tuple[ProjectionStep, ...]
    memory: tuple[ProjectionStep, ...]


def _projection_dependencies(
    name: str,
    projection: Projection,
    known_names: frozenset[str],
) -> frozenset[str]:
    deps: set[str] = set()
    for raw_dep in projection.depends_on:
        dep_name = raw_dep
        if raw_dep.startswith(PROJECTION_DEP_PREFIX):
            dep_name = raw_dep[len(PROJECTION_DEP_PREFIX) :]
        if dep_name in known_names and dep_name != name:
            deps.add(dep_name)
    return frozenset(deps)


def build_projection_plan(projections: Mapping[str, Projection]) -> ProjectionPlan:
    """Compile a deterministic projection plan from projection metadata."""
    if not projections:
        return ProjectionPlan(levels=())

    names = frozenset(projections)
    sorter: TopologicalSorter[str] = TopologicalSorter()
    for name, projection in projections.items():
        sorter.add(name, *_projection_dependencies(name, projection, names))

    sorter.prepare()
    levels: list[tuple[ProjectionStep, ...]] = []
    while sorter.is_active():
        ready = tuple(sorter.get_ready())
        if not ready:
            raise ValueError("Projection dependency cycle detected")
        level = tuple(
            _build_projection_step(name=name, projection=projections[name]) for name in ready
        )
        levels.append(level)
        sorter.done(*ready)

    return ProjectionPlan(levels=tuple(levels))


async def execute_projection_plan(
    plan: ProjectionPlan,
    *,
    objs: Sequence[Any],
    id_attr: str,
    backend_context: Any | None,
) -> dict[int, dict[str, Any]]:
    """Execute projection plan for the given objects.

    ``BACKEND`` loaders are batched through ``load_many`` when
    ``backend_context`` is provided.  ``PRELOADED`` loaders run synchronously
    via ``load_from_object`` against each object.
    """
    if not objs or not plan.levels:
        return {}

    parent_ids = [getattr(obj, id_attr) for obj in objs]
    values_by_index: dict[int, dict[str, Any]] = {index: {} for index in range(len(objs))}

    for level in plan.levels:
        buckets = _partition_execution_steps(level, backend_context=backend_context)
        await _execute_backend_steps(
            steps=buckets.backend,
            backend_context=backend_context,
            parent_ids=parent_ids,
            values_by_index=values_by_index,
        )
        _execute_memory_steps(
            steps=buckets.memory,
            objs=objs,
            values_by_index=values_by_index,
        )

    return values_by_index


def _merge_rows(
    *,
    values_by_index: dict[int, dict[str, Any]],
    parent_ids: Sequence[object],
    field_name: str,
    rows: Mapping[object, Any],
    default: Any,
) -> None:
    for index, parent_id in enumerate(parent_ids):
        loaded = rows.get(parent_id, PROJECTION_DEFAULT_MISSING)
        if loaded is PROJECTION_DEFAULT_MISSING:
            if default is PROJECTION_DEFAULT_MISSING:
                continue
            loaded = default
        values_by_index[index][field_name] = loaded


def _backend_loader(loader: Any) -> BackendProjectionLoader | None:
    if _has_declared_callable(loader, "load_many"):
        return cast(BackendProjectionLoader, loader)
    return None


def _memory_loader(loader: Any) -> MemoryProjectionLoader | None:
    if _has_declared_callable(loader, "load_from_object"):
        return cast(MemoryProjectionLoader, loader)
    return None


def _has_declared_callable(obj: Any, attr_name: str) -> bool:
    try:
        candidate = inspect.getattr_static(obj, attr_name)
    except AttributeError:
        return False
    return callable(candidate)


def _partition_execution_steps(
    level: tuple[ProjectionStep, ...],
    *,
    backend_context: Any | None,
) -> _ExecutionBuckets:
    backend_steps: list[ProjectionStep] = []
    memory_steps: list[ProjectionStep] = []
    for step in level:
        if step.projection.source is ProjectionSource.BACKEND:
            if backend_context is None:
                raise RuntimeError(f"Projection '{step.name}' requires backend context")
            backend_steps.append(step)
        else:
            memory_steps.append(step)
    return _ExecutionBuckets(
        backend=tuple(backend_steps),
        memory=tuple(memory_steps),
    )


async def _execute_backend_steps(
    *,
    steps: tuple[ProjectionStep, ...],
    backend_context: Any | None,
    parent_ids: Sequence[object],
    values_by_index: dict[int, dict[str, Any]],
) -> None:
    if not steps:
        return

    backend_results = await asyncio.gather(
        *(
            cast(BackendProjectionLoader, step.backend_loader).load_many(
                backend_context,
                parent_ids,
            )
            for step in steps
        )
    )
    for step, rows in zip(steps, backend_results, strict=False):
        _merge_rows(
            values_by_index=values_by_index,
            parent_ids=parent_ids,
            field_name=step.name,
            rows=rows,
            default=step.projection.default,
        )


def _execute_memory_steps(
    *,
    steps: tuple[ProjectionStep, ...],
    objs: Sequence[Any],
    values_by_index: dict[int, dict[str, Any]],
) -> None:
    if not steps:
        return
    for index, obj in enumerate(objs):
        row_context = values_by_index[index]
        for step in steps:
            loader = cast(MemoryProjectionLoader, step.memory_loader)
            computed = loader.load_from_object(obj, row_context)
            row_context[step.name] = computed


def _build_projection_step(
    *,
    name: str,
    projection: Projection,
) -> ProjectionStep:
    loader = projection.loader
    backend_loader = _backend_loader(loader)
    memory_loader = _memory_loader(loader)
    _validate_step_configuration(
        name=name,
        source=projection.source,
        backend_loader=backend_loader,
        memory_loader=memory_loader,
    )
    return ProjectionStep(
        name=name,
        projection=projection,
        backend_loader=backend_loader,
        memory_loader=memory_loader,
    )


def _validate_step_configuration(
    *,
    name: str,
    source: ProjectionSource,
    backend_loader: BackendProjectionLoader | None,
    memory_loader: MemoryProjectionLoader | None,
) -> None:
    if source is ProjectionSource.BACKEND and backend_loader is None:
        raise TypeError(f"Projection '{name}' requires a loader with load_many()")
    if source is ProjectionSource.PRELOADED and memory_loader is None:
        raise TypeError(f"Projection '{name}' requires a loader with load_from_object()")
