from __future__ import annotations

import time
from typing import Any, TypeVar, cast

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.metrics import MetricsAdapter
from loom.core.engine.plan import ExecutionPlan, LoadStep
from loom.core.errors import NotFound
from loom.core.logger import LoggerPort, get_logger
from loom.core.uow.abc import UnitOfWorkFactory
from loom.core.uow.context import _active_uow
from loom.core.use_case.rule import RuleViolation, RuleViolations
from loom.core.use_case.use_case import UseCase

ResultT = TypeVar("ResultT")


class RuntimeExecutor:
    """Executes UseCases from their compiled ExecutionPlan without reflection.

    Receives a fully constructed UseCase instance and drives execution
    through the fixed pipeline: bind params → build command → load entities
    → apply computes → check rules → call execute.

    Optionally manages a :class:`~loom.core.uow.abc.UnitOfWork` lifecycle
    around each execution when a ``uow_factory`` is provided.  Nested calls
    detected via a ``contextvars.ContextVar`` share the outer transaction and
    never open an additional UoW.

    No signature inspection occurs at runtime. All structural information
    comes from the cached ExecutionPlan produced by UseCaseCompiler.

    Emits ``RuntimeEvent`` objects to the optional ``MetricsAdapter`` at
    key lifecycle points (start, done, error). Enriches log calls with
    structured fields (``usecase``, ``duration_ms``, ``status``) for
    structured log consumers.

    Args:
        compiler: Compiler used to retrieve cached plans.
        uow_factory: Optional UoW factory.  When provided, each top-level
            :meth:`execute` call is wrapped in a single atomic transaction
            (begin → commit on success, rollback on exception).  Nested
            executions within the same async context reuse the outer UoW.
        debug_execution: When ``True``, emits ``[STEP]`` logs for every
            pipeline stage. Defaults to ``False`` (summary logs only).
        logger: Optional logger. Defaults to the framework logger.
        metrics: Optional metrics adapter. When provided, receives
            ``EXEC_START``, ``EXEC_DONE``, and ``EXEC_ERROR`` events.

    Example::

        executor = RuntimeExecutor(
            compiler,
            uow_factory=SQLAlchemyUnitOfWorkFactory(session_manager),
            metrics=prometheus_adapter,
        )
        result = await executor.execute(
            use_case,
            params={"user_id": 1},
            payload={"email": "new@corp.com"},
        )
    """

    def __init__(
        self,
        compiler: UseCaseCompiler,
        *,
        uow_factory: UnitOfWorkFactory | None = None,
        debug_execution: bool = False,
        logger: LoggerPort | None = None,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._compiler = compiler
        self._uow_factory = uow_factory
        self._debug = debug_execution
        self._logger = logger or get_logger(__name__)
        self._metrics = metrics

    async def execute(
        self,
        use_case: UseCase[Any, ResultT],
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        dependencies: dict[type[Any], Any] | None = None,
        load_overrides: dict[type[Any], Any] | None = None,
    ) -> ResultT:
        """Execute the UseCase via its compiled plan.

        When a ``uow_factory`` was provided at construction and no UoW is
        already active in the current async context, opens a fresh UoW
        (begin), runs the pipeline, and commits on success or rolls back on
        any exception.  Nested calls reuse the existing UoW transparently.

        Args:
            use_case: Constructed UseCase instance to execute.
            params: Primitive parameter values keyed by name.
            payload: Raw dict for command construction via ``Input()``.
            dependencies: Mapping of entity type to repository, used for
                ``Load()`` steps.
            load_overrides: Pre-loaded entities by type, bypassing repo
                calls. Used by test harnesses.

        Returns:
            The result produced by the UseCase.

        Raises:
            RuleViolations: If one or more rule steps fail.
            NotFound: If a Load step finds no entity in the repository.
        """
        uc_type = type(use_case)
        plan = uc_type.__execution_plan__
        if plan is None:
            plan = self._compiler.compile(uc_type)
        uc_name = uc_type.__qualname__

        self._logger.info(f"[EXEC] {uc_name}", usecase=uc_name)
        self._emit(RuntimeEvent(kind=EventKind.EXEC_START, use_case_name=uc_name))
        start = time.perf_counter()

        _owns_uow = self._uow_factory is not None and _active_uow.get() is None

        if not _owns_uow:
            return await self._run_pipeline(
                plan, use_case, uc_name, start, params, payload, dependencies, load_overrides
            )

        uow = self._uow_factory.create()  # type: ignore[union-attr]
        token = _active_uow.set(uow)
        try:
            async with uow:
                return await self._run_pipeline(
                    plan, use_case, uc_name, start, params, payload, dependencies, load_overrides
                )
        finally:
            _active_uow.reset(token)

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    async def _run_pipeline(
        self,
        plan: ExecutionPlan,
        use_case: UseCase[Any, ResultT],
        uc_name: str,
        start: float,
        params: dict[str, Any] | None,
        payload: dict[str, Any] | None,
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> ResultT:
        try:
            bound: dict[str, Any] = {}

            self._bind_params(plan, params or {}, bound)
            fields_set = self._build_command(plan, payload, bound)
            await self._execute_loads(plan, bound, dependencies, load_overrides)
            self._apply_computes(plan, bound, fields_set)
            self._check_rules(plan, bound, fields_set)

            self._log_step("Execute core logic")
            result: ResultT = await use_case.execute(**bound)

        except RuleViolations as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self._logger.warning(
                f"[FAIL] {uc_name}",
                usecase=uc_name,
                duration_ms=elapsed_ms,
                status="rule_failure",
            )
            self._emit(
                RuntimeEvent(
                    kind=EventKind.EXEC_ERROR,
                    use_case_name=uc_name,
                    duration_ms=elapsed_ms,
                    status="rule_failure",
                    error=exc,
                )
            )
            raise

        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            self._logger.error(
                f"[FAIL] {uc_name}",
                usecase=uc_name,
                duration_ms=elapsed_ms,
                status="failure",
                error=str(exc),
            )
            self._emit(
                RuntimeEvent(
                    kind=EventKind.EXEC_ERROR,
                    use_case_name=uc_name,
                    duration_ms=elapsed_ms,
                    status="failure",
                    error=exc,
                )
            )
            raise

        elapsed_ms = (time.perf_counter() - start) * 1000
        self._logger.info(
            f"[DONE] {elapsed_ms:.1f}ms",
            usecase=uc_name,
            duration_ms=elapsed_ms,
            status="success",
        )
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_DONE,
                use_case_name=uc_name,
                duration_ms=elapsed_ms,
                status="success",
            )
        )
        return result

    # ------------------------------------------------------------------
    # Pipeline stages
    # ------------------------------------------------------------------

    def _bind_params(
        self,
        plan: ExecutionPlan,
        params: dict[str, Any],
        bound: dict[str, Any],
    ) -> None:
        for pb in plan.param_bindings:
            if pb.name not in params:
                raise ValueError(
                    f"{plan.use_case_type.__qualname__}: "
                    f"missing required parameter '{pb.name}'"
                )
            bound[pb.name] = params[pb.name]

    def _build_command(
        self,
        plan: ExecutionPlan,
        payload: dict[str, Any] | None,
        bound: dict[str, Any],
    ) -> frozenset[str]:
        if plan.input_binding is None:
            return frozenset()

        if payload is None:
            raise ValueError(
                f"{plan.use_case_type.__qualname__}: "
                "payload is required for a UseCase with Input()"
            )

        command, fields_set = plan.input_binding.command_type.from_payload(payload)
        bound[plan.input_binding.name] = command
        self._log_step("Bind Input")
        return cast(frozenset[str], fields_set)

    async def _execute_loads(
        self,
        plan: ExecutionPlan,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> None:
        for ls in plan.load_steps:
            entity = await self._resolve_load(ls, bound, dependencies, load_overrides)
            bound[ls.name] = entity
            self._log_step(f"Load {ls.entity_type.__name__}")

    def _apply_computes(
        self,
        plan: ExecutionPlan,
        bound: dict[str, Any],
        fields_set: frozenset[str],
    ) -> None:
        if plan.input_binding is None or not plan.compute_steps:
            return

        command = bound[plan.input_binding.name]
        for cs in plan.compute_steps:
            label = getattr(cs.fn, "__name__", type(cs.fn).__name__)
            command = cs.fn(command, fields_set)
            self._log_step(f"Compute {label}")

        bound[plan.input_binding.name] = command

    def _check_rules(
        self,
        plan: ExecutionPlan,
        bound: dict[str, Any],
        fields_set: frozenset[str],
    ) -> None:
        if plan.input_binding is None or not plan.rule_steps:
            return

        command = bound[plan.input_binding.name]
        violations: list[RuleViolation] = []

        for rs in plan.rule_steps:
            label = getattr(rs.fn, "__name__", type(rs.fn).__name__)
            try:
                rs.fn(command, fields_set)
                self._log_step(f"Rule {label}")
            except RuleViolation as exc:
                violations.append(exc)
                self._logger.warning(
                    f"[RULE] {label} failed: {exc.field}: {exc.message}",
                    usecase=plan.use_case_type.__qualname__,
                    field=exc.field,
                )

        if violations:
            raise RuleViolations(violations)

    # ------------------------------------------------------------------
    # Load resolution
    # ------------------------------------------------------------------

    async def _resolve_load(
        self,
        step: LoadStep,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> Any:
        if load_overrides and step.entity_type in load_overrides:
            return load_overrides[step.entity_type]

        if dependencies is None:
            raise RuntimeError(
                f"No dependencies provided for Load({step.entity_type.__name__})"
            )

        repo = dependencies.get(step.entity_type)
        if repo is None:
            raise RuntimeError(
                f"No repository registered for entity type "
                f"'{step.entity_type.__name__}'"
            )

        value = bound[step.by]
        entity = await repo.get_by_id(value, profile=step.profile)

        if entity is None:
            raise NotFound(step.entity_type.__name__, id=value)

        return entity

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _emit(self, event: RuntimeEvent) -> None:
        if self._metrics is not None:
            self._metrics.on_event(event)

    def _log_step(self, label: str) -> None:
        if self._debug:
            self._logger.info(f"[STEP] {label}")
