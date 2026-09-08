"""Signature check shared by every declaration built as ``factory(context, **params)``.

Two declarations are built that way — a ``kind: python`` capability and
``dynamic_instructions`` — and both must refuse the same two faults before a
run ever starts: a callable with no slot for the build-time context, and
``params`` the signature does not accept.

The check is here, and the *reporting* is not. It returns a named reason, so
each caller maps it to the issue factory of its own field: a mis-signed
instructions factory is never reported with a ``python`` capability code, and
the two faults do not collapse into one message an operator cannot act on. A
plain string would have forced exactly that collapse, because the two faults
carry two different codes.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Final

_FIRST_POSITIONAL: Final[object] = object()
"""Stand-in for the build-time first positional when binding ``params``."""


class FactoryFault(Enum):
    """Why ``factory(context, **params)`` would not bind.

    Attributes:
        NOT_CALLABLE: The callable has no slot for the build-time context, so
            it is not a factory of this shape at all.
        PARAMS_REJECTED: The context binds, but the declared ``params`` do not.
    """

    NOT_CALLABLE = auto()
    PARAMS_REJECTED = auto()


@dataclass(frozen=True, slots=True)
class FactoryRejection:
    """One refused factory signature, with the detail its message needs.

    Attributes:
        fault: Which of the two faults was found.
        detail: What Python said when the binding failed; empty for
            :attr:`FactoryFault.NOT_CALLABLE`, whose message names no reason
            beyond the shape the caller expected.
    """

    fault: FactoryFault
    detail: str = ""


def reject_factory_signature(
    factory: Callable[..., object], params: Mapping[str, Any]
) -> FactoryRejection | None:
    """Return why ``factory(context, **params)`` would not bind, or ``None``.

    The context positional is bound alone first, so a factory with no slot for
    it is reported as the wrong shape rather than as rejecting the ``params``.
    A callable whose signature cannot be inspected is accepted: Python's own
    call at build reports whatever is wrong.

    Args:
        factory: Imported symbol the artifact declared.
        params: The artifact's ``params``, splatted as keyword arguments.

    Returns:
        The rejection, or ``None`` when the call would bind.
    """
    try:
        signature = inspect.signature(factory)
    except (ValueError, TypeError):
        return None
    try:
        signature.bind_partial(_FIRST_POSITIONAL)
    except TypeError:
        return FactoryRejection(fault=FactoryFault.NOT_CALLABLE)
    try:
        signature.bind(_FIRST_POSITIONAL, **params)
    except TypeError as exc:
        return FactoryRejection(fault=FactoryFault.PARAMS_REJECTED, detail=str(exc))
    return None
