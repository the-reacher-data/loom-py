from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn

if TYPE_CHECKING:
    from loom.core.engine.plan import ExecutionPlan

ResultT = TypeVar("ResultT")


class UseCase(ABC, Generic[ResultT]):
    """Base class for all use cases.

    Subclass and implement ``execute`` with typed parameters. Parameter
    defaults declare the execution contract:

    - ``Input()``  — command payload, built from the raw request.
    - ``Load(EntityType, by="param")`` — entity prefetched before execution.
    - No default — primitive param bound directly from the caller.

    Class attributes ``computes`` and ``rules`` declare the pre-execution
    pipeline. They are inspected once at startup by ``UseCaseCompiler`` and
    embedded in the immutable ``ExecutionPlan``.

    Attributes:
        computes: Compute transformations applied in order before rule checks.
        rules: Rule validations applied in order after computes.

    Example::

        class UpdateUserUseCase(UseCase[UserResponse]):
            computes = [set_updated_at]
            rules = [email_must_be_valid]

            def __init__(self, user_repo: UserRepository) -> None:
                self._user_repo = user_repo

            async def execute(
                self,
                user_id: int,
                cmd: UpdateUserCommand = Input(),
                user: User = Load(User, by="user_id"),
            ) -> UserResponse:
                ...
    """

    __execution_plan__: ClassVar[ExecutionPlan | None] = None
    computes: ClassVar[Sequence[ComputeFn[Any]]] = ()
    rules: ClassVar[Sequence[RuleFn[Any]]] = ()

    @abstractmethod
    async def execute(self, **kwargs: Any) -> ResultT:
        """Execute core business logic.

        Override with an explicit typed signature. The compiler inspects
        this method once at startup to build the ``ExecutionPlan``.

        Returns:
            The result of the use case operation.
        """
        ...
