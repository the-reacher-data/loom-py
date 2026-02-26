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


def import_modules(module_paths: list[str]) -> list[ModuleType]:
    return [importlib.import_module(path) for path in module_paths]


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
            if cls.__module__ != module_name:
                continue
            if issubclass(cls, BaseModel) and cls is not BaseModel:
                if cls not in seen_models:
                    models.append(cls)
                    seen_models.add(cls)
                continue
            if issubclass(cls, UseCase) and cls is not UseCase:
                use_case = typing.cast(type[UseCase[object, object]], cls)
                if use_case not in seen_use_cases:
                    use_cases.append(use_case)
                    seen_use_cases.add(use_case)
                continue
            if issubclass(cls, RestInterface) and cls is not RestInterface:
                interface = typing.cast(type[RestInterface[object]], cls)
                if interface not in seen_interfaces:
                    interfaces.append(interface)
                    seen_interfaces.add(interface)

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
