from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.core.use_case.compute import ComputeFn
from loom.core.use_case.markers import LookupKind, OnMissing, SourceKind
from loom.core.use_case.rule import RuleFn


@dataclass(frozen=True)
class ParamBinding:
    """A primitive parameter declared in ``execute``.

    Represents a positional/keyword argument that is provided by the
    caller at execution time (e.g. ``user_id: int``).

    Args:
        name: Parameter name as declared in the signature.
        annotation: Resolved type annotation.
    """

    name: str
    annotation: type[Any]


@dataclass(frozen=True)
class InputBinding:
    """A command payload parameter marked with ``Input()``.

    The executor builds the command from the raw payload dict and injects
    it under this parameter name.

    Args:
        name: Parameter name as declared in the signature.
        command_type: The ``Command`` subclass to instantiate.
    """

    name: str
    command_type: type[Any]


@dataclass(frozen=True)
class LoadStep:
    """An entity prefetch step marked with ``LoadById`` or ``Load``.

    The executor resolves the entity from a repository before calling
    ``execute``. Missing behavior is controlled by ``on_missing``.

    Args:
        name: Parameter name as declared in the signature.
        entity_type: Domain entity type to load.
        source_kind: Where the lookup value is extracted from.
        source_name: Name of param/command field used as lookup value.
        lookup_kind: Lookup strategy (id or arbitrary field).
        against: Entity field used in repository lookup.
        profile: Loading profile forwarded to ``repo.get_by_id``.
            Defaults to ``"default"``.
        on_missing: Policy when no entity is found.
    """

    name: str
    entity_type: type[Any]
    source_kind: SourceKind
    source_name: str
    lookup_kind: LookupKind
    against: str
    profile: str = "default"
    on_missing: OnMissing = OnMissing.RAISE


@dataclass(frozen=True)
class ExistsStep:
    """A boolean existence check step marked with ``Exists``."""

    name: str
    entity_type: type[Any]
    source_kind: SourceKind
    source_name: str
    against: str
    on_missing: OnMissing = OnMissing.RETURN_FALSE


@dataclass(frozen=True)
class ComputeStep:
    """A compute transformation step.

    Holds a direct reference to the ``ComputeFn`` so the plan is
    self-contained and requires no back-reference to the UseCase class.

    Args:
        fn: The compute function to apply.
        accepts_context: Pre-computed flag; ``True`` when ``fn`` accepts a
            third positional ``context`` argument.  Resolved at compile time
            to avoid per-request signature inspection.
    """

    fn: ComputeFn[Any]
    accepts_context: bool = False


@dataclass(frozen=True)
class RuleStep:
    """A rule validation step.

    Holds a direct reference to the ``RuleFn`` so the plan is
    self-contained and requires no back-reference to the UseCase class.

    Args:
        fn: The rule function to evaluate.
        accepts_context: Pre-computed flag; ``True`` when ``fn`` accepts a
            third positional ``context`` argument.  Resolved at compile time
            to avoid per-request signature inspection.
    """

    fn: RuleFn
    accepts_context: bool = False


@dataclass(frozen=True)
class ExecutionPlan:
    """Immutable compiled representation of a UseCase's execution flow.

    Built once at startup by ``UseCaseCompiler`` and reused for every
    request. No dynamic reflection occurs after compilation.

    Args:
        use_case_type: The ``UseCase`` subclass this plan was compiled from.
        param_bindings: Primitive parameters bound from the caller.
        input_binding: Command payload binding, or ``None`` if absent.
        load_steps: Entity prefetch steps, in declaration order.
        exists_steps: Boolean existence checks, in declaration order.
        compute_steps: Compute transformations, in declaration order.
        rule_steps: Rule validations, in declaration order.

    Example::

        plan = ExecutionPlan(
            use_case_type=UpdateUserUseCase,
            param_bindings=(ParamBinding("user_id", int),),
            input_binding=InputBinding("cmd", UpdateUserCommand),
            load_steps=(
                LoadStep(
                    "user",
                    User,
                    source_kind=SourceKind.PARAM,
                    source_name="user_id",
                    lookup_kind=LookupKind.BY_ID,
                    against="id",
                ),
            ),
            exists_steps=(),
            compute_steps=(),
            rule_steps=(),
        )
    """

    use_case_type: type[Any]
    param_bindings: tuple[ParamBinding, ...]
    input_binding: InputBinding | None
    load_steps: tuple[LoadStep, ...]
    exists_steps: tuple[ExistsStep, ...]
    compute_steps: tuple[ComputeStep, ...]
    rule_steps: tuple[RuleStep, ...]
