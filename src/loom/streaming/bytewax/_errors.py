"""Runtime-assembly errors for the streaming Bytewax adapter."""

from __future__ import annotations

from collections.abc import Sequence

from loom.streaming.compiler._errors import CompilationIssue


class RuntimeConfigurationError(Exception):
    """Raised when a valid compiled plan cannot run under this runtime config.

    Distinct from ``CompilationError`` on purpose. The plan itself is sound —
    it compiled — and nothing is being compiled when this is raised. What fails
    is the *combination* of that plan with the requested runtime, which is only
    knowable while the dataflow is assembled. Reporting it as a compilation
    failure would send the reader looking for a mistake in their flow
    definition instead of in their runtime configuration.

    Attributes:
        issues: Structured issues, each carrying a machine-readable code, the
            component involved, and the configuration field to change.

    Args:
        issues: Issues describing why the runtime rejected the plan.
    """

    def __init__(self, issues: Sequence[CompilationIssue]) -> None:
        self.issues: tuple[CompilationIssue, ...] = tuple(issues)
        messages = [issue.message for issue in self.issues]
        super().__init__(
            f"Runtime configuration rejected the compiled plan "
            f"with {len(messages)} error(s): {'; '.join(messages)}"
        )


__all__ = ["RuntimeConfigurationError"]
