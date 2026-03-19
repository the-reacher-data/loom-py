"""ETL process base type.

An :class:`ETLProcess` is an ordered set of :class:`~loom.etl.ETLStep`
types.  Steps are declared as a list; a nested list marks a parallel group
— all steps in the group run concurrently, and the next item in the outer
list waits for all of them to finish.

Example::

    class BuildStagingProcess(ETLProcess[DailyOrdersParams]):
        steps = [
            ExtractRawOrders,
            [ValidateOrders, ValidateCustomers],  # parallel
            JoinAndStage,
        ]
"""

from __future__ import annotations

import typing
from typing import Any, ClassVar, Generic, TypeVar, cast

ParamsT = TypeVar("ParamsT")

_StepItem = Any  # type[ETLStep[ParamsT]] | list[type[ETLStep[ParamsT]]]


class ETLProcess(Generic[ParamsT]):
    """Base class for ordered sets of ETL steps.

    Declare :attr:`steps` as a list of step types.  A nested list within
    :attr:`steps` declares a parallel group — the executor dispatches all
    steps in the group concurrently and waits for all to complete before
    advancing.

    All steps must share the same ``ParamsT`` as the process — enforced by
    the :class:`~loom.etl.compiler.ETLCompiler`.

    Attributes:
        steps: Ordered list of step types.  Nested lists are parallel groups.

    Example::

        class DailyPipeline(ETLProcess[DailyOrdersParams]):
            steps = [
                IngestStep,
                [EnrichA, EnrichB],   # parallel
                AggregateStep,
            ]
    """

    steps: ClassVar[list[_StepItem]] = []

    _params_type: ClassVar[type[Any] | None] = None

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._params_type = _extract_params_type(cls)
        if not isinstance(cls.__dict__.get("steps", []), list):
            raise TypeError(
                f"{cls.__qualname__}: 'steps' must be a list, "
                f"got {type(cls.__dict__['steps']).__name__}"
            )


def _extract_params_type(cls: type[Any]) -> type[Any] | None:
    for base in getattr(cls, "__orig_bases__", ()):
        origin = getattr(base, "__origin__", None)
        if origin is ETLProcess:
            args = typing.get_args(base)
            if args:
                return cast(type[Any], args[0])
    return None
