"""Resolution of declarative config bindings inside streaming flows."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any, TypeGuard, get_type_hints

import msgspec
from omegaconf import DictConfig

from loom.core.config import ConfigBinding, ConfigError, section
from loom.core.model import LoomFrozenStruct, LoomStruct
from loom.streaming.graph._flow import Process, ProcessNode, StreamFlow
from loom.streaming.nodes._boundary import IntoTopic
from loom.streaming.nodes._broadcast import Broadcast, BroadcastRoute
from loom.streaming.nodes._fork import Fork, ForkKind, ForkRoute
from loom.streaming.nodes._router import Route, Router
from loom.streaming.nodes._shape import CollectBatch, Drain, ForEach
from loom.streaming.nodes._step import BatchExpandStep, BatchStep, ExpandStep, RecordStep, Step
from loom.streaming.nodes._with import With, WithAsync


def resolve_flow_bindings(
    flow: StreamFlow[Any, Any],
    runtime_config: DictConfig,
) -> tuple[StreamFlow[Any, Any], list[str]]:
    """Resolve every config binding reachable from a streaming flow.

    The flow declaration remains declarative in user code. This helper
    materializes bindings into real runtime objects before compilation.

    Args:
        flow: User-declared streaming flow.
        runtime_config: Resolved runtime configuration.

    Returns:
        A resolved flow plus a list of binding-resolution errors.
    """
    errors: list[str] = []
    resolved_process = _resolve_process(flow.process, runtime_config, errors)
    resolved_flow = StreamFlow(
        name=flow.name,
        source=flow.source,
        process=resolved_process,
        output=flow.output,
        errors=flow.errors,
    )
    return resolved_flow, errors


def _resolve_process(
    process: Process[Any, Any],
    runtime_config: DictConfig,
    errors: list[str],
) -> Process[Any, Any]:
    nodes = tuple(_resolve_process_node(node, runtime_config, errors) for node in process.nodes)
    return Process(*nodes)


def _resolve_process_node(
    node: ProcessNode,
    runtime_config: DictConfig,
    errors: list[str],
) -> ProcessNode:
    if _is_step_class(node):
        try:
            return node()
        except TypeError as exc:
            errors.append(f"step {node.__qualname__}: {exc}")
            return node
    if isinstance(node, ConfigBinding):
        resolved = _resolve_binding(node, runtime_config, errors)
        if _is_process_node(resolved):
            return resolved
        binding_name = node.config_path or node.target.__name__
        errors.append(f"binding {binding_name}: resolved object is not a process node")
        return node
    if isinstance(node, With):
        return _resolve_with(node, runtime_config, errors)
    if isinstance(node, WithAsync):
        return _resolve_with_async(node, runtime_config, errors)
    if isinstance(node, Router):
        return _resolve_router(node, runtime_config, errors)
    if isinstance(node, Fork):
        return _resolve_fork(node, runtime_config, errors)
    if isinstance(node, Broadcast):
        return _resolve_broadcast(node, runtime_config, errors)
    return node


def _resolve_binding(
    binding: ConfigBinding,
    runtime_config: DictConfig,
    errors: list[str],
) -> object:
    try:
        raw_config = (
            section(runtime_config, binding.config_path, dict) if binding.config_path else {}
        )
        return _instantiate_binding(binding.target, raw_config, binding.overrides)
    except (ConfigError, TypeError, ValueError, msgspec.ValidationError) as exc:
        errors.append(f"binding {binding.config_path or binding.target.__name__}: {exc}")
        return binding


def _instantiate_binding(
    target: type[object],
    raw_config: Mapping[str, object],
    overrides: Mapping[str, object],
) -> object:
    resolved_kwargs = dict(raw_config)
    resolved_kwargs.update(overrides)
    signature = inspect.signature(target.__init__)
    annotations = get_type_hints(target.__init__)

    for name, param in signature.parameters.items():
        if name == "self" or param.kind in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }:
            continue
        annotation = annotations.get(name, param.annotation)
        if not _is_struct_annotation(annotation):
            continue
        if name not in resolved_kwargs:
            if param.default is inspect._empty:
                raise TypeError(f"{target.__name__} requires config field {name!r}")
            continue
        resolved_kwargs[name] = msgspec.convert(resolved_kwargs[name], annotation, strict=False)

    return target(**resolved_kwargs)


def _is_struct_annotation(annotation: object) -> bool:
    return isinstance(annotation, type) and issubclass(annotation, (LoomStruct, LoomFrozenStruct))


def _resolve_with(
    node: With[Any, Any],
    runtime_config: DictConfig,
    errors: list[str],
) -> With[Any, Any]:
    resolved_process = _resolve_process(node.process, runtime_config, errors)
    resolved_deps = _resolve_dependencies(node.sync_contexts, runtime_config, errors)
    resolved_deps.update(_resolve_dependencies(node.context_factories, runtime_config, errors))
    resolved_deps.update(_resolve_dependencies(node.plain_deps, runtime_config, errors))
    return With(scope=node.scope, process=resolved_process, **resolved_deps)


def _resolve_with_async(
    node: WithAsync[Any, Any],
    runtime_config: DictConfig,
    errors: list[str],
) -> WithAsync[Any, Any]:
    resolved_process = _resolve_process(node.process, runtime_config, errors)
    resolved_deps = _resolve_dependencies(node.async_contexts, runtime_config, errors)
    resolved_deps.update(_resolve_dependencies(node.context_factories, runtime_config, errors))
    resolved_deps.update(_resolve_dependencies(node.plain_deps, runtime_config, errors))
    return WithAsync(
        scope=node.scope,
        max_concurrency=node.max_concurrency,
        task_timeout_ms=node.task_timeout_ms,
        process=resolved_process,
        **resolved_deps,
    )


def _resolve_router(
    node: Router[Any, Any],
    runtime_config: DictConfig,
    errors: list[str],
) -> Router[Any, Any]:
    default = _resolve_process(node.default, runtime_config, errors) if node.default else None
    if node.selector is not None:
        routes = {
            key: _resolve_process(process, runtime_config, errors)
            for key, process in node.routes.items()
        }
        return Router.by(node.selector, routes, default=default)
    predicate_routes = tuple(
        Route(
            when=route.when,
            process=_resolve_process(route.process, runtime_config, errors),
        )
        for route in node.predicate_routes
    )
    return Router.when(predicate_routes, default=default)


def _resolve_fork(
    node: Fork[Any],
    runtime_config: DictConfig,
    errors: list[str],
) -> Fork[Any]:
    default = _resolve_process(node.default, runtime_config, errors) if node.default else None
    if node.kind is ForkKind.KEYED:
        selector = node.selector
        if selector is None:
            errors.append("fork selector missing for keyed fork")
            return node
        routes = {
            key: _resolve_process(process, runtime_config, errors)
            for key, process in node.routes.items()
        }
        return Fork.by(selector, routes, default=default)
    predicate_routes = tuple(
        ForkRoute(
            when=route.when,
            process=_resolve_process(route.process, runtime_config, errors),
        )
        for route in node.predicate_routes
    )
    return Fork.when(predicate_routes, default=default)


def _resolve_broadcast(
    node: Broadcast[Any],
    runtime_config: DictConfig,
    errors: list[str],
) -> Broadcast[Any]:
    routes = tuple(
        BroadcastRoute(
            process=_resolve_process(route.process, runtime_config, errors),
            output=route.output,
        )
        for route in node.routes
    )
    return Broadcast(*routes)


def _resolve_dependencies(
    dependencies: Mapping[str, object],
    runtime_config: DictConfig,
    errors: list[str],
) -> dict[str, object]:
    resolved: dict[str, object] = {}
    for name, value in dependencies.items():
        resolved[name] = _resolve_dependency(value, runtime_config, errors)
    return resolved


def _resolve_dependency(value: object, runtime_config: DictConfig, errors: list[str]) -> object:
    if isinstance(value, ConfigBinding):
        return _resolve_binding(value, runtime_config, errors)
    return value


def _is_process_node(value: object) -> TypeGuard[ProcessNode]:
    return isinstance(
        value,
        (
            ConfigBinding,
            RecordStep,
            BatchStep,
            ExpandStep,
            BatchExpandStep,
            With,
            WithAsync,
            CollectBatch,
            ForEach,
            Drain,
            IntoTopic,
            Fork,
            Router,
            Broadcast,
        ),
    )


def _is_step_class(value: object) -> TypeGuard[type[Step[Any, Any]]]:
    return isinstance(value, type) and issubclass(value, Step)
