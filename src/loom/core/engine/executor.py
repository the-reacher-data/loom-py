from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar, cast

from loom.core.engine.compilable import Compilable
from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.events import EventKind, RuntimeEvent
from loom.core.engine.metrics import MetricsAdapter
from loom.core.engine.plan import ExecutionPlan, ExistsStep, LoadStep
from loom.core.errors import NotFound
from loom.core.job.context import clear_pending_dispatches, flush_pending_dispatches
from loom.core.logger import LoggerPort, get_logger
from loom.core.tracing import get_trace_id
from loom.core.uow.abc import UnitOfWorkFactory
from loom.core.uow.context import _active_uow
from loom.core.use_case.markers import LookupKind, OnMissing, SourceKind
from loom.core.use_case.rule import RuleViolation, RuleViolations

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
        compilable: Compilable,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        dependencies: dict[type[Any], Any] | None = None,
        load_overrides: dict[type[Any], Any] | None = None,
    ) -> ResultT:
        """Execute a compiled instance via its ExecutionPlan.

        Accepts any object satisfying the :class:`~loom.core.engine.compilable.Compilable`
        protocol — both :class:`~loom.core.use_case.use_case.UseCase` and
        :class:`~loom.core.job.job.Job` instances are valid inputs.

        When a ``uow_factory`` was provided at construction and no UoW is
        already active in the current async context, opens a fresh UoW
        (begin), runs the pipeline, and commits on success or rolls back on
        any exception.  Nested calls reuse the existing UoW transparently.

        Args:
            compilable: Constructed instance to execute.
            params: Primitive parameter values keyed by name.
            payload: Raw dict for command construction via ``Input()``.
            dependencies: Mapping of entity type to repository, used for
                ``LoadById()`` / ``Load()`` / ``Exists()`` steps.
            load_overrides: Pre-loaded entities by type, bypassing repo
                calls. Used by test harnesses.

        Returns:
            The result produced by ``execute()``.

        Raises:
            loom.core.errors.RuleViolations: If one or more rule steps fail.
            NotFound: If a Load step finds no entity in the repository.
        """
        uc_type = type(compilable)
        plan = uc_type.__execution_plan__
        if plan is None:
            plan = self._compiler.compile(uc_type)
        uc_name = uc_type.__qualname__

        start = time.perf_counter()

        _owns_uow = self._uow_factory is not None and _active_uow.get() is None

        if not _owns_uow:
            return await self._run_pipeline(
                plan, compilable, uc_name, start, params, payload, dependencies, load_overrides
            )

        uow = self._uow_factory.create()  # type: ignore[union-attr]
        token = _active_uow.set(uow)
        try:
            await uow.begin()
            result: ResultT = await self._run_pipeline(
                plan, compilable, uc_name, start, params, payload, dependencies, load_overrides
            )
            await uow.commit()
            await flush_pending_dispatches()
            return result
        except Exception:
            await uow.rollback()
            clear_pending_dispatches()
            raise
        finally:
            _active_uow.reset(token)

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    async def _run_pipeline(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        uc_name: str,
        start: float,
        params: dict[str, Any] | None,
        payload: dict[str, Any] | None,
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> ResultT:
        trace_id = get_trace_id()
        logger = self._logger.bind(trace_id=trace_id) if trace_id else self._logger

        logger.info(f"[EXEC] {uc_name}", usecase=uc_name)
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_START,
                use_case_name=uc_name,
                trace_id=trace_id,
            )
        )

        try:
            bound: dict[str, Any] = {}

            self._bind_params(plan, params or {}, bound)
            fields_set = self._build_command(plan, payload, bound)
            await self._execute_loads(plan, bound, dependencies, load_overrides)
            await self._execute_exists(plan, bound, dependencies)
            self._apply_computes(plan, bound, fields_set)
            self._check_rules(plan, bound, fields_set)

            self._log_step("Execute core logic")
            execute_fn = compilable.execute
            if inspect.iscoroutinefunction(execute_fn):
                result: ResultT = cast(ResultT, await execute_fn(**bound))
            else:
                result = cast(ResultT, execute_fn(**bound))

        except RuleViolations as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.warning(
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
                    trace_id=trace_id,
                )
            )
            raise

        except Exception as exc:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.error(
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
                    trace_id=trace_id,
                )
            )
            raise

        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
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
                trace_id=trace_id,
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
                    f"{plan.use_case_type.__qualname__}: missing required parameter '{pb.name}'"
                )
            raw = params[pb.name]
            if isinstance(pb.annotation, type):
                try:
                    if not isinstance(raw, pb.annotation):
                        bound[pb.name] = pb.annotation(raw)
                    else:
                        bound[pb.name] = raw
                except (TypeError, ValueError):
                    bound[pb.name] = raw
            else:
                bound[pb.name] = raw

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
                f"{plan.use_case_type.__qualname__}: payload is required for a UseCase with Input()"
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
            entity = await self._resolve_load(ls, plan, bound, dependencies, load_overrides)
            bound[ls.name] = entity
            self._log_step(f"Load {ls.entity_type.__name__}")

    async def _execute_exists(
        self,
        plan: ExecutionPlan,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
    ) -> None:
        for es in plan.exists_steps:
            result = await self._resolve_exists(es, plan, bound, dependencies)
            bound[es.name] = result
            self._log_step(f"Exists {es.entity_type.__name__}")

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
            compute_fn = cast(Any, cs.fn)
            label = getattr(cs.fn, "__name__", type(cs.fn).__name__)
            if self._accepts_context(cs.fn):
                command = compute_fn(command, fields_set, bound)
            else:
                command = compute_fn(command, fields_set)
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
                if self._accepts_context(rs.fn):
                    rs.fn(command, fields_set, bound)
                else:
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
        plan: ExecutionPlan,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> Any:
        if load_overrides and step.entity_type in load_overrides:
            return load_overrides[step.entity_type]

        if dependencies is None:
            raise RuntimeError(
                f"No dependencies provided for LoadById({step.entity_type.__name__})"
            )

        repo = dependencies.get(step.entity_type)
        if repo is None:
            raise RuntimeError(
                f"No repository registered for entity type '{step.entity_type.__name__}'"
            )

        value = self._resolve_lookup_value(step.source_kind, step.source_name, plan, bound)
        entity = await self._get_entity(repo, step, value)

        if entity is None:
            if step.on_missing is OnMissing.RETURN_NONE:
                return None
            if step.on_missing is OnMissing.RETURN_FALSE:
                return False
            raise NotFound(step.entity_type.__name__, id=value)

        return entity

    async def _resolve_exists(
        self,
        step: ExistsStep,
        plan: ExecutionPlan,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
    ) -> bool:
        if dependencies is None:
            raise RuntimeError(f"No dependencies provided for Exists({step.entity_type.__name__})")

        repo = dependencies.get(step.entity_type)
        if repo is None:
            raise RuntimeError(
                f"No repository registered for entity type '{step.entity_type.__name__}'"
            )

        value = self._resolve_lookup_value(step.source_kind, step.source_name, plan, bound)
        exists_by = getattr(repo, "exists_by", None)
        if not callable(exists_by):
            raise RuntimeError(
                "Repository for "
                f"'{step.entity_type.__name__}' must implement exists_by(field, value)"
            )

        exists_by_async = cast(Callable[[str, Any], Awaitable[bool]], exists_by)
        found = bool(await exists_by_async(step.against, value))
        if found:
            return True

        if step.on_missing is OnMissing.RAISE:
            raise NotFound(step.entity_type.__name__, id=value)
        return False

    async def _get_entity(self, repo: Any, step: LoadStep, value: Any) -> Any | None:
        if step.lookup_kind is LookupKind.BY_ID:
            return await repo.get_by_id(value, profile=step.profile)

        get_by = getattr(repo, "get_by", None)
        if not callable(get_by):
            raise RuntimeError(
                f"Repository for '{step.entity_type.__name__}' must implement get_by(field, value)"
            )
        get_by_async = cast(Callable[..., Awaitable[Any | None]], get_by)
        return await get_by_async(step.against, value, profile=step.profile)

    @staticmethod
    def _resolve_lookup_value(
        source_kind: SourceKind,
        source_name: str,
        plan: ExecutionPlan,
        bound: dict[str, Any],
    ) -> Any:
        if source_kind is SourceKind.PARAM:
            return bound[source_name]

        if plan.input_binding is None:
            raise RuntimeError("from_command source requires Input() binding")
        command = bound[plan.input_binding.name]
        if not hasattr(command, source_name):
            raise RuntimeError(f"Command '{type(command).__name__}' has no field '{source_name}'")
        return getattr(command, source_name)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _emit(self, event: RuntimeEvent) -> None:
        if self._metrics is not None:
            self._metrics.on_event(event)

    @staticmethod
    def _accepts_context(fn: Any) -> bool:
        """Return True when callable accepts a third positional context arg."""
        try:
            params = tuple(inspect.signature(fn).parameters.values())
        except (TypeError, ValueError):
            return False

        positional = [
            p
            for p in params
            if p.kind
            in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
        if len(positional) >= 3:
            return True

        return any(p.kind is inspect.Parameter.VAR_POSITIONAL for p in params)

    def _log_step(self, label: str) -> None:
        if self._debug:
            self._logger.info(f"[STEP] {label}")
