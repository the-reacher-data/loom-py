"""ETL step base type.

:class:`ETLStep` is the single unit of ETL work: it declares its sources,
one target, and a pure ``execute()`` transformation method.

The backend (Polars, Spark) is determined by the configured
:class:`~loom.etl.storage.protocols.SourceReader` and
:class:`~loom.etl.storage.protocols.TargetWriter` injected at runner build time —
no declaration is needed on the step itself.

Definition-time validation (via ``__init_subclass__``) catches the most
common structural errors early, before the full :class:`~loom.etl.compiler.ETLCompiler`
runs.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, ClassVar, Generic, TypeVar

from loom.etl.io.source import FromFile, FromTable, FromTemp, Sources, SourceSet
from loom.etl.io.target import IntoFile, IntoTable, IntoTemp
from loom.etl.pipeline._generics import _extract_generic_arg

ParamsT = TypeVar("ParamsT")

_RESERVED_NAMES: frozenset[str] = frozenset(
    {
        "sources",
        "target",
        "streaming",
        "execute",
        "_params_type",
        "_source_form",
        "_inline_sources",
    }
)

_SOURCE_TYPES = (FromTable, FromFile, FromTemp)
_TARGET_TYPES = (IntoTable, IntoFile, IntoTemp)


class _SourceForm(Enum):
    INLINE = "inline"  # Form 1: class-level FromTable / FromFile attributes
    GROUPED = "grouped"  # Form 2: sources = Sources(...) or SourceSet instance
    NONE = "none"  # step declares no sources (generator steps)


class ETLStep(Generic[ParamsT]):
    """Base class for all ETL transformation steps.

    Subclass, declare :attr:`sources` and :attr:`target`, then implement
    :meth:`execute`.  The ``*`` separator in ``execute`` makes all injected
    DataFrame parameters keyword-only — the executor always injects by name.

    Source forms
    ------------
    Three authoring forms are supported (see :mod:`loom.etl._source`).
    Mixing Form 1 and Form 2 in the same class is a definition-time error.

    Example::

        class BuildOrdersStaging(ETLStep[DailyOrdersParams]):
            sources = Sources(
                orders=FromTable("raw.orders").where(
                    (col("year")  == params.run_date.year)
                    & (col("month") == params.run_date.month),
                ),
                customers=FromTable("raw.customers"),
            )
            target = IntoTable("staging.orders").replace_partitions(
                values={"year": params.run_date.year, "month": params.run_date.month}
            )

            def execute(
                self,
                params: DailyOrdersParams,
                *,
                orders: pl.DataFrame,
                customers: pl.DataFrame,
            ) -> pl.DataFrame:
                return orders.join(customers, on="customer_id", how="left")
    """

    sources: ClassVar[Sources | SourceSet[Any] | None] = None
    target: ClassVar[IntoTable | IntoFile | IntoTemp | None] = None
    streaming: ClassVar[bool] = False

    # Set by __init_subclass__
    _params_type: ClassVar[type[Any] | None] = None
    _source_form: ClassVar[_SourceForm] = _SourceForm.NONE
    _inline_sources: ClassVar[dict[str, FromTable | FromFile | FromTemp]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._params_type = _extract_generic_arg(cls, ETLStep)
        cls._inline_sources = {}
        _validate_streaming_flag(cls)
        _validate_and_classify_sources(cls)

    def execute(self, params: ParamsT, **frames: Any) -> Any:
        """Transform source frames into the output frame.

        Must be overridden by every concrete step.  Declare the ``params``
        and frame parameters explicitly — the compiler validates the
        signature against :attr:`sources` at compile time.

        Args:
            params: Typed params instance for this run.
            **frames: Source DataFrames injected by the executor, keyed by
                the source alias declared in :attr:`sources`.

        Returns:
            Transformed DataFrame (type must match the active backend).
        """
        raise NotImplementedError(f"{type(self).__qualname__} must implement execute()")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _validate_and_classify_sources(cls: type[Any]) -> None:
    """Detect source form and enforce mutual exclusion rules."""
    inline = {
        name: val
        for name, val in cls.__dict__.items()
        if isinstance(val, _SOURCE_TYPES) and name not in _RESERVED_NAMES
    }
    has_grouped = isinstance(cls.__dict__.get("sources"), (Sources, SourceSet))

    if inline and has_grouped:
        raise TypeError(
            f"{cls.__qualname__}: cannot mix inline source attributes with "
            f"sources=Sources(...) or sources=SourceSet — use one form only."
        )

    if inline:
        cls._source_form = _SourceForm.INLINE
        cls._inline_sources = inline
    elif has_grouped:
        cls._source_form = _SourceForm.GROUPED
    else:
        cls._source_form = _SourceForm.NONE


def _validate_streaming_flag(cls: type[Any]) -> None:
    """Enforce that ``streaming`` is explicitly boolean at class definition time."""
    streaming = getattr(cls, "streaming", False)
    if not isinstance(streaming, bool):
        raise TypeError(
            f"{cls.__qualname__}: 'streaming' must be bool, got {type(streaming).__name__}"
        )
