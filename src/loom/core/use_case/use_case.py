from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from loom.core.repository.abc import RepoFor
from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.rule import RuleFn

if TYPE_CHECKING:
    from loom.core.engine.plan import ExecutionPlan

ModelT = TypeVar("ModelT")
ResultT = TypeVar("ResultT")


class UseCase(ABC, Generic[ModelT, ResultT]):
    """Base class for all use cases.

    Subclass and implement ``execute`` with typed parameters. Parameter
    defaults declare the execution contract:

    - ``Input()``  — command payload, built from the raw request.
    - ``LoadById(EntityType, by="param")`` — entity prefetched by id.
    - ``Load(EntityType, ...)`` — entity prefetched by arbitrary field.
    - ``Exists(EntityType, ...)`` — boolean existence check by field.
    - No default — primitive param bound directly from the caller.

    Class attributes ``computes``, ``rules``, and ``read_only`` declare the
    pre-execution pipeline and execution policy.  They are inspected once at
    startup by ``UseCaseCompiler`` and embedded in the immutable
    ``ExecutionPlan``.

    Attributes:
        computes: Compute transformations applied in order before rule checks.
        rules: Rule validations applied in order after computes.
        read_only: When ``True``, the executor skips opening a
            ``UnitOfWork`` transaction.  Set this on query-only use cases
            that never mutate state.  GET routes in :class:`RestInterface`
            always bypass the UoW regardless of this flag.

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
                user: User = LoadById(User, by="user_id"),
            ) -> UserResponse:
                ...
    """

    __execution_plan__: ClassVar[ExecutionPlan | None] = None
    computes: ClassVar[Sequence[ComputeFn[Any]]] = ()
    rules: ClassVar[Sequence[RuleFn]] = ()
    read_only: ClassVar[bool] = False

    def __init__(self, main_repo: RepoFor[Any] | None = None) -> None:
        """Initialise the use case base dependencies.

        Args:
            main_repo: Optional main repository dependency. When provided, the
                factory may inject it from ``RepoFor[Model]`` constructor
                annotations.
        """
        self._main_repo = main_repo

    def __class_getitem__(cls, params: object) -> object:
        """Backwards-compatible subscript support.

        Allows legacy ``UseCase[Result]`` at runtime by treating it as
        ``UseCase[Any, Result]`` while the codebase migrates to the explicit
        two-parameter form ``UseCase[Model, Result]``.
        """
        if not isinstance(params, tuple):
            return super().__class_getitem__((Any, params))  # type: ignore[misc]
        return super().__class_getitem__(params)  # type: ignore[misc]

    @property
    def main_repo(self) -> RepoFor[Any]:
        """Main repository injected by the factory."""
        if self._main_repo is None:
            raise RuntimeError(
                f"{type(self).__qualname__} requires a main repository but it was not injected."
            )
        return self._main_repo

    @main_repo.setter
    def main_repo(self, value: RepoFor[Any]) -> None:
        self._main_repo = value

    @abstractmethod
    async def execute(self, *args: Any, **kwargs: Any) -> ResultT:
        """Execute core business logic.

        Override with an explicit typed signature. The compiler inspects
        this method once at startup to build the ``ExecutionPlan``.

        Returns:
            The result of the use case operation.
        """
        ...
