"""Internal helpers for discovery engine implementations."""

from __future__ import annotations

import importlib
import inspect
import typing
from types import ModuleType
from typing import Any

import msgspec

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
        return typing.cast(type[BaseModel], cls)
    return None


def _as_use_case(cls: type[Any]) -> type[UseCase[object, object]] | None:
    if issubclass(cls, UseCase) and cls is not UseCase:
        return typing.cast(type[UseCase[object, object]], cls)
    return None


def _as_interface(cls: type[Any]) -> type[RestInterface[object]] | None:
    if issubclass(cls, RestInterface) and cls is not RestInterface:
        return typing.cast(type[RestInterface[object]], cls)
    return None


def collect_from_modules(
    modules: list[ModuleType],
) -> tuple[
    list[type[BaseModel]],
    list[type[UseCase[object, object]]],
    list[type[RestInterface[object]]],
]:
    models: list[type[BaseModel]] = []
    use_cases: list[type[UseCase[object, object]]] = []
    interfaces: list[type[RestInterface[object]]] = []

    seen_models: set[type[BaseModel]] = set()
    seen_use_cases: set[type[UseCase[object, object]]] = set()
    seen_interfaces: set[type[RestInterface[object]]] = set()

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

    return models, use_cases, interfaces


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
