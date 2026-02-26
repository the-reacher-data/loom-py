from __future__ import annotations

import inspect
import typing
from typing import Any

from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.metrics import MetricsAdapter
from loom.core.engine.plan import (
    ComputeStep,
    ExecutionPlan,
    InputBinding,
    LoadStep,
    ParamBinding,
    RuleStep,
)
from loom.core.logger import LoggerPort, get_logger
from loom.core.use_case.markers import _InputMarker, _LoadMarker
from loom.core.use_case.use_case import UseCase


class CompilationError(Exception):
    """Raised when a UseCase fails structural validation at startup.

    Args:
        message: Human-readable description of the compilation failure.
    """


class UseCaseCompiler:
    """Compiles UseCase subclasses into immutable ExecutionPlans at startup.

    Inspects the ``execute`` signature exactly once per class, validates
    structural constraints, and caches the resulting plan.

    No reflection occurs after compilation — the plan drives all runtime
    execution via ``RuntimeExecutor``.

    Args:
        logger: Optional logger. Defaults to the framework logger.
        metrics: Optional metrics adapter. When provided, receives
            ``COMPILE_START`` and ``COMPILE_DONE`` events.

    Example::

        compiler = UseCaseCompiler(metrics=my_adapter)
        plan = compiler.compile(UpdateUserUseCase)
    """

    def __init__(
        self,
        logger: LoggerPort | None = None,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._cache: dict[type[Any], ExecutionPlan] = {}
        self._logger = logger or get_logger(__name__)
        self._metrics = metrics

    def compile(self, use_case_type: type[UseCase[Any]]) -> ExecutionPlan:
        """Return the ExecutionPlan for ``use_case_type``, compiling if needed.

        Compilation is idempotent: calling this method multiple times with
        the same class returns the cached plan without re-inspection.

        Args:
            use_case_type: Concrete UseCase subclass to compile.

        Returns:
            Immutable ExecutionPlan.

        Raises:
            CompilationError: If the UseCase signature violates constraints.
        """
        if use_case_type in self._cache:
            return self._cache[use_case_type]
        plan = self._build_plan(use_case_type)
        self._cache[use_case_type] = plan
        return plan

    def get_plan(self, use_case_type: type[UseCase[Any]]) -> ExecutionPlan | None:
        """Return the cached plan for ``use_case_type``, or ``None``.

        Args:
            use_case_type: UseCase subclass to look up.

        Returns:
            Cached ExecutionPlan if compiled, otherwise ``None``.
        """
        return self._cache.get(use_case_type)

    def _emit(self, event: RuntimeEvent) -> None:
        if self._metrics is not None:
            self._metrics.on_event(event)

    def _build_plan(self, use_case_type: type[UseCase[Any]]) -> ExecutionPlan:
        uc_name = use_case_type.__qualname__
        self._logger.info(f"[BOOT] Compiling UseCase: {uc_name}", usecase=uc_name)
        self._emit(RuntimeEvent(kind=EventKind.COMPILE_START, use_case_name=uc_name))

        param_bindings, input_binding, load_steps = self._inspect_execute(use_case_type)
        compute_steps = tuple(ComputeStep(fn=fn) for fn in use_case_type.computes)
        rule_steps = tuple(RuleStep(fn=fn) for fn in use_case_type.rules)

        self._logger.info(
            f"[BOOT]  - Validated {len(compute_steps)} compute steps",
            usecase=uc_name,
        )
        self._logger.info(
            f"[BOOT]  - Validated {len(rule_steps)} rules",
            usecase=uc_name,
        )

        total = len(load_steps) + len(compute_steps) + len(rule_steps)
        self._logger.info(
            f"[BOOT]  - ExecutionPlan built ({total} steps)",
            usecase=uc_name,
        )
        self._emit(RuntimeEvent(kind=EventKind.COMPILE_DONE, use_case_name=uc_name))

        plan = ExecutionPlan(
            use_case_type=use_case_type,
            param_bindings=tuple(param_bindings),
            input_binding=input_binding,
            load_steps=tuple(load_steps),
            compute_steps=compute_steps,
            rule_steps=rule_steps,
        )
        use_case_type.__execution_plan__ = plan
        return plan

    def _inspect_execute(
        self,
        use_case_type: type[UseCase[Any]],
    ) -> tuple[list[ParamBinding], InputBinding | None, list[LoadStep]]:
        execute_fn = use_case_type.execute

        if getattr(execute_fn, "__isabstractmethod__", False):
            raise CompilationError(
                f"{use_case_type.__qualname__} must override execute() "
                "before it can be compiled"
            )

        try:
            hints = typing.get_type_hints(execute_fn)
        except Exception:
            hints = {}

        sig = inspect.signature(execute_fn)

        param_bindings: list[ParamBinding] = []
        input_binding: InputBinding | None = None
        load_steps: list[LoadStep] = []
        input_count = 0

        _variadic = (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)

        for name, param in sig.parameters.items():
            if name == "self":
                continue
            if param.kind in _variadic:
                continue

            default = param.default
            annotation = hints.get(name, Any)

            if isinstance(default, _InputMarker):
                input_count += 1
                if input_count > 1:
                    raise CompilationError(
                        f"{use_case_type.__qualname__}.execute: "
                        "only one Input() parameter is allowed"
                    )
                input_binding = InputBinding(name=name, command_type=annotation)
                cmd_name = getattr(annotation, "__name__", repr(annotation))
                self._logger.info(f"[BOOT]  - Detected Input: {cmd_name}")

            elif isinstance(default, _LoadMarker):
                ls = LoadStep(
                    name=name,
                    entity_type=default.entity_type,
                    by=default.by,
                    profile=default.profile,
                )
                load_steps.append(ls)
                self._logger.info(
                    f"[BOOT]  - Detected Load: {default.entity_type.__name__}"
                    f" by {default.by}"
                )

            else:
                param_bindings.append(ParamBinding(name=name, annotation=annotation))

        self._validate_load_refs(use_case_type, param_bindings, load_steps)
        return param_bindings, input_binding, load_steps

    def _validate_load_refs(
        self,
        use_case_type: type[UseCase[Any]],
        param_bindings: list[ParamBinding],
        load_steps: list[LoadStep],
    ) -> None:
        param_names = {pb.name for pb in param_bindings}
        for ls in load_steps:
            if ls.by not in param_names:
                raise CompilationError(
                    f"{use_case_type.__qualname__}.execute: "
                    f"Load({ls.entity_type.__name__}, by='{ls.by}'): "
                    f"parameter '{ls.by}' not found in execute signature"
                )
