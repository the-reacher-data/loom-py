"""Runner-specific error types."""

from __future__ import annotations


class InvalidStageError(ValueError):
    """Raised when *include* matches no step or process in the compiled plan.

    Args:
        include: The set of names that produced no match.
    """

    def __init__(self, include: frozenset[str]) -> None:
        super().__init__(
            f"No steps or processes match include={set(include)!r}. "
            "Check that the names match the class names of your ETLStep or ETLProcess subclasses."
        )
