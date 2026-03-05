from __future__ import annotations

import asyncio
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from graphlib import TopologicalSorter
from typing import Any, Protocol, cast

from loom.core.model.projection import PROJECTION_DEFAULT_MISSING, Projection, ProjectionSource

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


@dataclass(frozen=True, slots=True)
class ProjectionPlan:
    """Compiled projection execution plan grouped by dependency levels."""

    levels: tuple[tuple[ProjectionStep, ...], ...]


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
        level = tuple(ProjectionStep(name=name, projection=projections[name]) for name in ready)
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

    Memory loaders are executed on object data; backend loaders are batched
    through ``load_many`` when ``backend_context`` is provided.
    """
    if not objs or not plan.levels:
        return {}

    parent_ids = [getattr(obj, id_attr) for obj in objs]
    values_by_index: dict[int, dict[str, Any]] = {index: {} for index in range(len(objs))}

    for level in plan.levels:
        backend_steps: list[ProjectionStep] = []
        memory_steps: list[ProjectionStep] = []
        for step in level:
            loader = step.projection.loader
            backend_loader = _backend_loader(loader)
            memory_loader = _memory_loader(loader)
            source = step.projection.source

            if source is ProjectionSource.BACKEND:
                if backend_loader is None:
                    raise TypeError(f"Projection '{step.name}' requires a loader with load_many()")
                if backend_context is None:
                    raise RuntimeError(f"Projection '{step.name}' requires backend context")
                backend_steps.append(step)
                continue

            if source is ProjectionSource.PRELOADED:
                if memory_loader is None:
                    raise TypeError(
                        f"Projection '{step.name}' requires a loader with load_from_object()"
                    )
                memory_steps.append(step)
                continue

            if source is ProjectionSource.AUTO:
                if backend_context is not None and backend_loader is not None:
                    backend_steps.append(step)
                    continue
                if memory_loader is not None:
                    memory_steps.append(step)
                    continue
                if backend_loader is not None:
                    raise RuntimeError(f"Projection '{step.name}' requires backend context")
                raise TypeError(
                    f"Projection '{step.name}' loader must implement "
                    "load_many() or load_from_object()"
                )

        if backend_steps:
            backend_results = await asyncio.gather(
                *(
                    cast(BackendProjectionLoader, step.projection.loader).load_many(
                        backend_context,
                        parent_ids,
                    )
                    for step in backend_steps
                )
            )
            for step, rows in zip(backend_steps, backend_results, strict=False):
                _merge_rows(
                    values_by_index=values_by_index,
                    parent_ids=parent_ids,
                    field_name=step.name,
                    rows=rows,
                    default=step.projection.default,
                )

        if memory_steps:
            for index, obj in enumerate(objs):
                row_context = values_by_index[index]
                for step in memory_steps:
                    loader = cast(MemoryProjectionLoader, step.projection.loader)
                    computed = loader.load_from_object(obj, row_context)
                    row_context[step.name] = computed

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
    if hasattr(loader, "load_many"):
        return cast(BackendProjectionLoader, loader)
    return None


def _memory_loader(loader: Any) -> MemoryProjectionLoader | None:
    if hasattr(loader, "load_from_object"):
        return cast(MemoryProjectionLoader, loader)
    return None
