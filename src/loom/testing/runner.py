from __future__ import annotations

from typing import Any, Generic, TypeVar

import msgspec

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.executor import RuntimeExecutor
from loom.core.engine.plan import ExecutionPlan
from loom.core.repository.abc import RepoFor
from loom.core.use_case.use_case import UseCase

ResultT = TypeVar("ResultT")


class UseCaseTest(Generic[ResultT]):
    """Fluent test harness for executing UseCases without HTTP or framework overhead.

    Builds and runs the real ExecutionPlan — no shortcuts or mocking of the
    pipeline. Designed for unit and integration tests that must exercise
    computes, rules, and load steps in full.

    Args:
        use_case: Constructed UseCase instance to test.

    Example::

        result = await (
            UseCaseTest(UpdateUserUseCase(repo=fake_repo))
            .with_params(user_id=1)
            .with_input(email="new@example.com")
            .run()
        )
    """

    def __init__(self, use_case: UseCase[Any, ResultT]) -> None:
        self._use_case = use_case
        self._params: dict[str, Any] = {}
        self._payload: dict[str, Any] | None = None
        self._load_overrides: dict[type[Any], Any] = {}
        self._dependencies: dict[type[Any], Any] = {}

    # ------------------------------------------------------------------
    # Builder methods
    # ------------------------------------------------------------------

    def with_params(self, **kwargs: Any) -> UseCaseTest[ResultT]:
        """Set primitive parameter values bound by name.

        Args:
            **kwargs: Parameter names and values matching the UseCase's
                non-Input, non-Load parameters.

        Returns:
            ``self`` for chaining.
        """
        self._params.update(kwargs)
        return self

    def with_input(self, **kwargs: Any) -> UseCaseTest[ResultT]:
        """Set raw payload fields for command construction.

        The payload is passed to the Command's ``from_payload`` method.
        Use ``with_command`` if you have a pre-built Command instance.

        Args:
            **kwargs: Payload fields matching the Command struct.

        Returns:
            ``self`` for chaining.
        """
        if self._payload is None:
            self._payload = {}
        self._payload.update(kwargs)
        return self

    def with_command(self, cmd: Any) -> UseCaseTest[ResultT]:
        """Set a pre-built Command instance as the execution payload.

        Serializes the command via ``msgspec.to_builtins`` so it is
        compatible with the standard ``from_payload`` pipeline.

        Args:
            cmd: A ``Command`` (msgspec.Struct) instance.

        Returns:
            ``self`` for chaining.
        """
        self._payload = msgspec.to_builtins(cmd)
        return self

    def with_loaded(self, entity_type: type[Any], entity: Any) -> UseCaseTest[ResultT]:
        """Pre-load an entity, bypassing repository calls for this type.

        Args:
            entity_type: The entity class used in the ``LoadById()`` marker.
            entity: The pre-loaded entity instance.

        Returns:
            ``self`` for chaining.
        """
        self._load_overrides[entity_type] = entity
        return self

    def with_deps(self, entity_type: type[Any], repo: Any) -> UseCaseTest[ResultT]:
        """Register a repository for a given entity type.

        Used when the UseCase has ``LoadById()`` steps that require a repo.
        ``with_loaded`` takes precedence over ``with_deps`` for the same type.

        Args:
            entity_type: The entity class used in the ``LoadById()`` marker.
            repo: Repository implementing ``get_by_id``.

        Returns:
            ``self`` for chaining.
        """
        self._dependencies[entity_type] = repo
        return self

    def with_main_repo(self, repo: RepoFor[Any]) -> UseCaseTest[ResultT]:
        """Inject the main repository dependency into the UseCase instance.

        This is useful for unit tests of ``UseCase[TModel, TResult]`` where
        the core logic reads from ``self.main_repo``.

        Args:
            repo: Repository instance compatible with the UseCase's main model.

        Returns:
            ``self`` for chaining.
        """
        self._use_case.main_repo = repo
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def run(self) -> ResultT:
        """Compile and execute the UseCase through the full pipeline.

        Returns:
            The result produced by the UseCase.

        Raises:
            RuleViolations: If one or more rule steps fail.
            NotFound: If a Load step finds no entity.
            CompilationError: If the UseCase fails structural validation.
        """
        compiler = UseCaseCompiler()
        executor = RuntimeExecutor(compiler)
        return await executor.execute(
            self._use_case,
            params=self._params,
            payload=self._payload,
            dependencies=self._dependencies if self._dependencies else None,
            load_overrides=self._load_overrides if self._load_overrides else None,
        )

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    @property
    def plan(self) -> ExecutionPlan:
        """Compile and return the ExecutionPlan for the UseCase.

        Useful for asserting plan structure in advanced test scenarios.

        Returns:
            The compiled ``ExecutionPlan``.
        """
        cached = type(self._use_case).__execution_plan__
        if cached is not None:
            return cached
        return UseCaseCompiler().compile(type(self._use_case))
