"""Internal helpers for discovery engine implementations."""

from __future__ import annotations

import importlib
import inspect
import typing
from types import ModuleType
from typing import Any

import msgspec

from loom.core.job.job import Job
from loom.core.model import BaseModel
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface

ItemT = typing.TypeVar("ItemT")


def import_modules(module_paths: list[str]) -> list[ModuleType]:
    return [importlib.import_module(path) for path in module_paths]


def _append_unique(items: list[ItemT], seen: set[ItemT], value: ItemT) -> None:
    if value in seen:
        return
    items.append(value)
    seen.add(value)


def _is_local_class(cls: type[Any], module_name: str) -> bool:
    return cls.__module__ == module_name


def _as_model(cls: type[Any]) -> type[BaseModel] | None:
    if issubclass(cls, BaseModel) and cls is not BaseModel:
        return cls
    return None


def _as_use_case(cls: type[Any]) -> type[UseCase[object, object]] | None:
    if issubclass(cls, UseCase) and cls is not UseCase:
        return typing.cast(type[UseCase[object, object]], cls)
    return None


def _as_interface(cls: type[Any]) -> type[RestInterface[object]] | None:
    if issubclass(cls, RestInterface) and cls is not RestInterface:
        return typing.cast(type[RestInterface[object]], cls)
    return None


def _as_job(cls: type[Any]) -> type[Job[Any]] | None:
    if issubclass(cls, Job) and cls is not Job:
        return cls
    return None


def collect_from_modules(
    modules: list[ModuleType],
) -> tuple[
    list[type[BaseModel]],
    list[type[UseCase[object, object]]],
    list[type[RestInterface[object]]],
    list[type[Job[Any]]],
]:
    models: list[type[BaseModel]] = []
    use_cases: list[type[UseCase[object, object]]] = []
    interfaces: list[type[RestInterface[object]]] = []
    jobs: list[type[Job[Any]]] = []

    seen_models: set[type[BaseModel]] = set()
    seen_use_cases: set[type[UseCase[object, object]]] = set()
    seen_interfaces: set[type[RestInterface[object]]] = set()
    seen_jobs: set[type[Job[Any]]] = set()

    for module in modules:
        module_name = module.__name__
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if not _is_local_class(cls, module_name):
                continue

            model = _as_model(cls)
            if model is not None:
                _append_unique(models, seen_models, model)
                continue

            use_case = _as_use_case(cls)
            if use_case is not None:
                _append_unique(use_cases, seen_use_cases, use_case)
                continue

            interface = _as_interface(cls)
            if interface is not None:
                _append_unique(interfaces, seen_interfaces, interface)
                continue

            job = _as_job(cls)
            if job is not None:
                _append_unique(jobs, seen_jobs, job)

    return models, use_cases, interfaces, jobs


def collect_use_cases_from_interfaces(
    interfaces: list[type[RestInterface[object]]],
) -> list[type[UseCase[object, object]]]:
    """Extract unique UseCases referenced in interface routes.

    Required because auto-generated UseCase classes live in
    ``loom.rest.autocrud``, not in user modules, so
    :func:`collect_from_modules` cannot discover them via module scanning.

    Args:
        interfaces: List of concrete ``RestInterface`` subclasses to inspect.

    Returns:
        Ordered list of unique UseCase classes found across all routes,
        preserving first-seen order.
    """
    result: list[type[UseCase[object, object]]] = []
    seen: set[type[UseCase[object, object]]] = set()
    for iface in interfaces:
        for route in iface.routes:
            uc = typing.cast(type[UseCase[object, object]], route.use_case)
            _append_unique(result, seen, uc)
    return result


def infer_model_from_use_case(
    use_case_type: type[UseCase[object, object]],
) -> type[BaseModel] | None:
    for base in getattr(use_case_type, "__orig_bases__", ()):
        if typing.get_origin(base) is not UseCase:
            continue
        args = typing.get_args(base)
        if len(args) != 2:
            continue
        candidate = args[0]
        if candidate is Any:
            return None
        if isinstance(candidate, type) and issubclass(candidate, msgspec.Struct):
            return typing.cast(type[BaseModel], candidate)
        return None
    return None
