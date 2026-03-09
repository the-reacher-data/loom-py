from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any, TypeVar, cast, overload

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

if TYPE_CHECKING:
    from loom.core.job.job import Job
    from loom.core.use_case.use_case import UseCase

ResultT = TypeVar("ResultT")

_LOG_EXEC = "[EXEC]"
_LOG_DONE = "[DONE]"
_LOG_FAIL = "[FAIL]"
_STATUS_SUCCESS = "success"
_STATUS_FAILURE = "failure"
_STATUS_RULE_FAILURE = "rule_failure"


class ParameterBindingError(ValueError):
    """Raised when a primitive execute parameter cannot be coerced to its declared type."""


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
        repo_resolver: Optional callable that resolves a repository instance
            from an entity model type. Used by ``Load``/``Exists`` when
            ``dependencies`` override is not passed to :meth:`execute`.

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
        repo_resolver: Callable[[type[Any]], Any] | None = None,
    ) -> None:
        self._compiler = compiler
        self._uow_factory = uow_factory
        self._debug = debug_execution
        self._logger = logger or get_logger(__name__)
        self._metrics = metrics
        self._repo_resolver = repo_resolver

    @overload
    async def execute(
        self,
        compilable: UseCase[Any, ResultT],
        *,
        params: dict[str, Any] | None = ...,
        payload: dict[str, Any] | None = ...,
        dependencies: dict[type[Any], Any] | None = ...,
        load_overrides: dict[type[Any], Any] | None = ...,
        read_only: bool = ...,
    ) -> ResultT: ...

    @overload
    async def execute(
        self,
        compilable: Job[ResultT],
        *,
        params: dict[str, Any] | None = ...,
        payload: dict[str, Any] | None = ...,
        dependencies: dict[type[Any], Any] | None = ...,
        load_overrides: dict[type[Any], Any] | None = ...,
        read_only: bool = ...,
    ) -> ResultT: ...

    @overload
    async def execute(
        self,
        compilable: Compilable,
        *,
        params: dict[str, Any] | None = ...,
        payload: dict[str, Any] | None = ...,
        dependencies: dict[type[Any], Any] | None = ...,
        load_overrides: dict[type[Any], Any] | None = ...,
        read_only: bool = ...,
    ) -> Any: ...

    async def execute(
        self,
        compilable: Compilable,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        dependencies: dict[type[Any], Any] | None = None,
        load_overrides: dict[type[Any], Any] | None = None,
        read_only: bool = False,
    ) -> Any:
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
            read_only: When ``True``, skips opening a ``UnitOfWork``
                transaction even if a ``uow_factory`` was provided.
                Automatically set to ``True`` by the HTTP layer for GET
                routes.  Also honoured when ``plan.read_only`` is ``True``.

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

        _is_read_only = read_only or plan.read_only
        _owns_uow = (
            self._uow_factory is not None and _active_uow.get() is None and not _is_read_only
        )

        if not _owns_uow:
            return await self._run_pipeline(
                plan, compilable, uc_name, start, params, payload, dependencies, load_overrides
            )

        uow = self._uow_factory.create()  # type: ignore[union-attr]
        token = _active_uow.set(uow)
        try:
            await uow.begin()
            result: Any = await self._run_pipeline(
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
    ) -> Any:
        trace_id, logger = self._begin_execution(uc_name)

        try:
            result = await self._run_core_pipeline(
                plan=plan,
                compilable=compilable,
                params=params,
                payload=payload,
                dependencies=dependencies,
                load_overrides=load_overrides,
            )

        except RuleViolations as exc:
            self._handle_rule_failure(
                logger=logger,
                use_case_name=uc_name,
                start=start,
                error=exc,
                trace_id=trace_id,
            )
            raise

        except Exception as exc:
            self._handle_failure(
                logger=logger,
                use_case_name=uc_name,
                start=start,
                error=exc,
                trace_id=trace_id,
            )
            raise

        self._handle_success(
            logger=logger,
            use_case_name=uc_name,
            start=start,
            trace_id=trace_id,
        )
        return result

    def _begin_execution(self, use_case_name: str) -> tuple[str | None, LoggerPort]:
        trace_id = get_trace_id()
        logger = self._logger.bind(trace_id=trace_id) if trace_id else self._logger
        logger.info(f"{_LOG_EXEC} {use_case_name}", usecase=use_case_name)
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_START,
                use_case_name=use_case_name,
                trace_id=trace_id,
            )
        )
        return trace_id, logger

    async def _run_core_pipeline(
        self,
        *,
        plan: ExecutionPlan,
        compilable: Compilable,
        params: dict[str, Any] | None,
        payload: dict[str, Any] | None,
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> Any:
        bound: dict[str, Any] = {}
        self._bind_params(plan, params or {}, bound)
        fields_set = self._build_command(plan, payload, bound)
        await self._execute_loads(plan, compilable, bound, dependencies, load_overrides)
        await self._execute_exists(plan, compilable, bound, dependencies)
        self._apply_computes(plan, bound, fields_set)
        self._check_rules(plan, bound, fields_set)
        return await self._invoke_execute(compilable, bound)

    async def _invoke_execute(
        self,
        compilable: Compilable,
        bound: dict[str, Any],
    ) -> Any:
        self._log_step("Execute core logic")
        execute_fn = compilable.execute
        if inspect.iscoroutinefunction(execute_fn):
            return await execute_fn(**bound)
        return execute_fn(**bound)

    def _handle_rule_failure(
        self,
        *,
        logger: LoggerPort,
        use_case_name: str,
        start: float,
        error: RuleViolations,
        trace_id: str | None,
    ) -> None:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.warning(
            f"{_LOG_FAIL} {use_case_name}",
            usecase=use_case_name,
            duration_ms=elapsed_ms,
            status=_STATUS_RULE_FAILURE,
        )
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_ERROR,
                use_case_name=use_case_name,
                duration_ms=elapsed_ms,
                status=_STATUS_RULE_FAILURE,
                error=error,
                trace_id=trace_id,
            )
        )

    def _handle_failure(
        self,
        *,
        logger: LoggerPort,
        use_case_name: str,
        start: float,
        error: Exception,
        trace_id: str | None,
    ) -> None:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.error(
            f"{_LOG_FAIL} {use_case_name}",
            usecase=use_case_name,
            duration_ms=elapsed_ms,
            status=_STATUS_FAILURE,
            error=str(error),
        )
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_ERROR,
                use_case_name=use_case_name,
                duration_ms=elapsed_ms,
                status=_STATUS_FAILURE,
                error=error,
                trace_id=trace_id,
            )
        )

    def _handle_success(
        self,
        *,
        logger: LoggerPort,
        use_case_name: str,
        start: float,
        trace_id: str | None,
    ) -> None:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.info(
            f"{_LOG_DONE} {elapsed_ms:.1f}ms",
            usecase=use_case_name,
            duration_ms=elapsed_ms,
            status=_STATUS_SUCCESS,
        )
        self._emit(
            RuntimeEvent(
                kind=EventKind.EXEC_DONE,
                use_case_name=use_case_name,
                duration_ms=elapsed_ms,
                status=_STATUS_SUCCESS,
                trace_id=trace_id,
            )
        )

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
            bound[pb.name] = self._coerce_param(
                param_name=pb.name,
                annotation=pb.annotation,
                raw=raw,
                use_case_name=plan.use_case_type.__qualname__,
            )

    @staticmethod
    def _coerce_param(
        *,
        param_name: str,
        annotation: Any,
        raw: Any,
        use_case_name: str,
    ) -> Any:
        if annotation is Any:
            return raw
        if not isinstance(annotation, type):
            return raw
        if isinstance(raw, annotation):
            return raw
        try:
            return annotation(raw)
        except (TypeError, ValueError) as exc:
            raise ParameterBindingError(
                f"{use_case_name}: invalid value for parameter '{param_name}': "
                f"expected {annotation.__name__}, got {type(raw).__name__} ({raw!r})"
            ) from exc

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

        cmd_type = plan.input_binding.command_type
        if hasattr(cmd_type, "from_payload"):
            command, fields_set = cmd_type.from_payload(payload)
        else:
            raise TypeError(
                f"{plan.use_case_type.__qualname__}: "
                f"command type {cmd_type!r} must implement from_payload()"
            )
        bound[plan.input_binding.name] = command
        self._log_step("Bind Input")
        return cast(frozenset[str], fields_set)

    async def _execute_loads(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> None:
        for ls in plan.load_steps:
            bound[ls.name] = await self._resolve_load(
                ls, plan, compilable, bound, dependencies, load_overrides
            )
            self._log_step(f"Load {ls.entity_type.__name__}")

    async def _execute_exists(
        self,
        plan: ExecutionPlan,
        compilable: Compilable,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
    ) -> None:
        for es in plan.exists_steps:
            bound[es.name] = await self._resolve_exists(es, plan, compilable, bound, dependencies)
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
            if cs.accepts_context:
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
                if rs.accepts_context:
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
        compilable: Compilable,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
        load_overrides: dict[type[Any], Any] | None,
    ) -> Any:
        del compilable
        if load_overrides and step.entity_type in load_overrides:
            return load_overrides[step.entity_type]

        if dependencies is None:
            repo = self._resolve_repo(step.entity_type)
            if repo is None and self._repo_resolver is None:
                raise RuntimeError(
                    f"No dependencies provided for LoadById({step.entity_type.__name__})"
                )
        else:
            repo = dependencies.get(step.entity_type)
            if repo is None:
                repo = self._resolve_repo(step.entity_type)
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
        compilable: Compilable,
        bound: dict[str, Any],
        dependencies: dict[type[Any], Any] | None,
    ) -> bool:
        del compilable
        if dependencies is None:
            repo = self._resolve_repo(step.entity_type)
            if repo is None and self._repo_resolver is None:
                raise RuntimeError(
                    f"No dependencies provided for Exists({step.entity_type.__name__})"
                )
        else:
            repo = dependencies.get(step.entity_type)
            if repo is None:
                repo = self._resolve_repo(step.entity_type)
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

    def _resolve_repo(self, entity_type: type[Any]) -> Any | None:
        if self._repo_resolver is None:
            return None
        return self._repo_resolver(entity_type)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _emit(self, event: RuntimeEvent) -> None:
        if self._metrics is not None:
            self._metrics.on_event(event)

    def _log_step(self, label: str) -> None:
        if self._debug:
            self._logger.info(f"[STEP] {label}")
