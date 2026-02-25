from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class EventKind(StrEnum):
    """Discriminator for runtime and compile-time events.

    Used by ``MetricsAdapter`` and structured loggers to route events
    without isinstance checks.
    """

    COMPILE_START = "compile_start"
    COMPILE_DONE = "compile_done"
    EXEC_START = "exec_start"
    STEP_DONE = "step_done"
    EXEC_DONE = "exec_done"
    EXEC_ERROR = "exec_error"


@dataclass(frozen=True)
class RuntimeEvent:
    """Immutable event emitted by the compiler and runtime executor.

    Consumed by ``MetricsAdapter`` and structured loggers. Carries no
    transport-specific data — adapters translate to their own formats.

    Args:
        kind: Event discriminator.
        use_case_name: Qualified name of the executing UseCase.
        step_name: Step label (e.g. ``"Load User"``), or ``None``.
        duration_ms: Wall-clock duration in milliseconds, or ``None``.
        status: Outcome label (e.g. ``"success"``, ``"failure"``), or ``None``.
        error: Exception instance if the event represents a failure, or ``None``.

    Example::

        event = RuntimeEvent(
            kind=EventKind.EXEC_DONE,
            use_case_name="UpdateUserUseCase",
            duration_ms=12.4,
            status="success",
        )
    """

    kind: EventKind
    use_case_name: str
    step_name: str | None = None
    duration_ms: float | None = None
    status: str | None = None
    error: BaseException | None = None
