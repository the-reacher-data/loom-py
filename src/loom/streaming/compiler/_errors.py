"""Streaming compilation error codes and structured issues.

``StreamingErrorCode``
    Machine-readable enum of every validation failure the streaming compiler
    can report.

``CompilationIssue``
    Immutable value object carrying ``code``, ``component``, ``field``, and a
    human-readable ``message``.  Each error code has a dedicated factory
    function so call-sites stay intention-revealing and free of string
    formatting, mirroring :class:`loom.etl.compiler.ETLCompilationError`.

Unlike ETL — which raises one exception per failure — the streaming compiler
accumulates every issue found in a run, so the issue is data and
:class:`~loom.streaming.compiler.CompilationError` aggregates the collection.
"""

from __future__ import annotations

from enum import StrEnum

from loom.core.model import LoomFrozenStruct


class StreamingErrorCode(StrEnum):
    """Enumeration of all streaming compiler failure codes."""

    # Binding resolution phase
    STEP_INSTANTIATION_FAILED = "STEP_INSTANTIATION_FAILED"
    BINDING_RESOLUTION_FAILED = "BINDING_RESOLUTION_FAILED"
    BINDING_NOT_PROCESS_NODE = "BINDING_NOT_PROCESS_NODE"
    FORK_SELECTOR_MISSING = "FORK_SELECTOR_MISSING"

    # Validation phase
    SINK_MISSING_NAME = "SINK_MISSING_NAME"
    SINK_CONFIG_INVALID = "SINK_CONFIG_INVALID"
    KAFKA_CONFIG_INVALID = "KAFKA_CONFIG_INVALID"
    MONGO_CONFIG_INVALID = "MONGO_CONFIG_INVALID"
    BATCH_SCOPE_DIRECT_CONTEXT = "BATCH_SCOPE_DIRECT_CONTEXT"
    SHAPE_MISMATCH = "SHAPE_MISMATCH"
    TERMINAL_NOT_LAST = "TERMINAL_NOT_LAST"
    FORK_NOT_LAST = "FORK_NOT_LAST"
    BROADCAST_NOT_LAST = "BROADCAST_NOT_LAST"
    SCOPED_PROCESS_NOT_LAST = "SCOPED_PROCESS_NOT_LAST"
    EXPLODE_WITHOUT_ROUTER = "EXPLODE_WITHOUT_ROUTER"
    FORK_BRANCH_NO_TERMINAL = "FORK_BRANCH_NO_TERMINAL"
    ROUTER_BRANCH_UNSAFE_NODE = "ROUTER_BRANCH_UNSAFE_NODE"
    ROUTER_BRANCH_FANOUT_UNSUPPORTED = "ROUTER_BRANCH_FANOUT_UNSUPPORTED"
    ROUTER_BRANCH_SHAPE_DIVERGENCE = "ROUTER_BRANCH_SHAPE_DIVERGENCE"
    OUTPUT_WITH_FORK = "OUTPUT_WITH_FORK"
    OUTPUT_WITH_BROADCAST = "OUTPUT_WITH_BROADCAST"
    MISSING_TERMINAL_OUTPUT = "MISSING_TERMINAL_OUTPUT"
    WINDOW_STRATEGY_UNSUPPORTED = "WINDOW_STRATEGY_UNSUPPORTED"
    SCOPED_INTO_TOPIC_NOT_LAST = "SCOPED_INTO_TOPIC_NOT_LAST"
    SCOPED_PROCESS_UNSUPPORTED_NODE = "SCOPED_PROCESS_UNSUPPORTED_NODE"

    # Plan-building phase
    ERROR_ENVELOPE_UNPARAMETERIZED = "ERROR_ENVELOPE_UNPARAMETERIZED"
    STORAGE_SINK_UNSUPPORTED = "STORAGE_SINK_UNSUPPORTED"
    PAYLOAD_TYPE_INVALID = "PAYLOAD_TYPE_INVALID"

    # Delivery-semantics phase (partitioned source spec)
    DELIVERY_CONFLICT = "DELIVERY_CONFLICT"
    DELIVERY_KEYED_MULTIPROCESS = "DELIVERY_KEYED_MULTIPROCESS"
    FORK_UNMATCHED_UNROUTED = "FORK_UNMATCHED_UNROUTED"

    # Compatibility bucket for issues built from bare strings
    UNSPECIFIED = "UNSPECIFIED"


class CompilationIssue(LoomFrozenStruct, frozen=True, kw_only=True):
    """One structured compiler failure.

    Args:
        code:      Machine-readable :class:`StreamingErrorCode`.
        message:   Human-readable description; the aggregated exception text
                   is built from these messages.
        component: Node or config section the issue points at
                   (for example ``"router branch 'vip'"``).
        field:     Optional config field involved
                   (for example ``"kafka.consumer.group_id"``).
    """

    code: StreamingErrorCode
    message: str
    component: str = ""
    field: str | None = None

    def prefixed(self, prefix: str) -> CompilationIssue:
        """Return a copy scoped under a branch prefix.

        Used when branch validation nests issues under ``fork branch X`` /
        ``router branch X`` / ``broadcast branch N`` contexts.  The message
        keeps the historical ``"{prefix}: {message}"`` format.
        """
        component = f"{prefix} > {self.component}" if self.component else prefix
        return CompilationIssue(
            code=self.code,
            message=f"{prefix}: {self.message}",
            component=component,
            field=self.field,
        )


def from_message(message: str) -> CompilationIssue:
    """Wrap a bare string as an :data:`StreamingErrorCode.UNSPECIFIED` issue."""
    return CompilationIssue(code=StreamingErrorCode.UNSPECIFIED, message=message)


# ---------------------------------------------------------------------------
# Binding resolution factories
# ---------------------------------------------------------------------------


def step_instantiation_failed(step: type, exc: Exception) -> CompilationIssue:
    """A Step class in the flow could not be instantiated without arguments."""
    return CompilationIssue(
        code=StreamingErrorCode.STEP_INSTANTIATION_FAILED,
        message=f"step {step.__qualname__}: {exc}",
        component=step.__qualname__,
    )


def binding_resolution_failed(binding_name: str, exc: Exception) -> CompilationIssue:
    """A ConfigBinding could not be resolved against the config context."""
    return CompilationIssue(
        code=StreamingErrorCode.BINDING_RESOLUTION_FAILED,
        message=f"binding {binding_name}: {exc}",
        component=binding_name,
    )


def binding_not_process_node(binding_name: str) -> CompilationIssue:
    """A resolved ConfigBinding produced an object that is not a process node."""
    return CompilationIssue(
        code=StreamingErrorCode.BINDING_NOT_PROCESS_NODE,
        message=f"binding {binding_name}: resolved object is not a process node",
        component=binding_name,
    )


def fork_selector_missing() -> CompilationIssue:
    """A keyed fork was declared without a selector expression."""
    return CompilationIssue(
        code=StreamingErrorCode.FORK_SELECTOR_MISSING,
        message="fork selector missing for keyed fork",
        component="fork",
        field="selector",
    )


# ---------------------------------------------------------------------------
# Validation factories
# ---------------------------------------------------------------------------


def sink_missing_name(node: object) -> CompilationIssue:
    """A storage sink node was declared without a config section name."""
    return CompilationIssue(
        code=StreamingErrorCode.SINK_MISSING_NAME,
        message=f"storage sink '{type(node).__name__}': missing name",
        component=type(node).__name__,
        field="name",
    )


def sink_config_invalid(sink_name: str, exc: Exception) -> CompilationIssue:
    """A storage sink config section failed backend resolution."""
    return CompilationIssue(
        code=StreamingErrorCode.SINK_CONFIG_INVALID,
        message=f"storage sink '{sink_name}': {exc}",
        component=sink_name,
        field=f"streaming.sinks.{sink_name}",
    )


def kafka_config_invalid(exc: Exception) -> CompilationIssue:
    """The kafka config section is missing or invalid for a flow that needs it."""
    return CompilationIssue(
        code=StreamingErrorCode.KAFKA_CONFIG_INVALID,
        message=f"kafka: {exc}",
        component="kafka",
        field="kafka",
    )


def mongo_config_invalid(source_name: str, exc: Exception) -> CompilationIssue:
    """The mongo config section is missing or cannot resolve the flow's source."""
    return CompilationIssue(
        code=StreamingErrorCode.MONGO_CONFIG_INVALID,
        message=f"mongo source '{source_name}': {exc}",
        component=f"mongo source '{source_name}'",
        field="mongo",
    )


def batch_scope_direct_context(node: object, direct_cms: list[str]) -> CompilationIssue:
    """With/WithAsync at BATCH scope received direct context manager instances."""
    return CompilationIssue(
        code=StreamingErrorCode.BATCH_SCOPE_DIRECT_CONTEXT,
        message=(
            f"{type(node).__name__} with scope=BATCH cannot use direct context "
            f"manager instances: {', '.join(direct_cms)}. "
            f"Use ContextFactory for batch-scoped resources."
        ),
        component=type(node).__name__,
    )


def shape_mismatch(expected: str, got: str, node: object) -> CompilationIssue:
    """A node received a stream shape different from the one it requires."""
    return CompilationIssue(
        code=StreamingErrorCode.SHAPE_MISMATCH,
        message=f"shape mismatch: expected {expected} but got {got} before {type(node).__name__}",
        component=type(node).__name__,
    )


def terminal_not_last(node: object) -> CompilationIssue:
    """A terminal leaf node is not the last node of its process."""
    return CompilationIssue(
        code=StreamingErrorCode.TERMINAL_NOT_LAST,
        message=f"{type(node).__name__} must be the last node in a process",
        component=type(node).__name__,
    )


def fork_not_last() -> CompilationIssue:
    """A Fork node is not the last node of its process."""
    return CompilationIssue(
        code=StreamingErrorCode.FORK_NOT_LAST,
        message="fork must be the last node in a process",
        component="fork",
    )


def broadcast_not_last() -> CompilationIssue:
    """A Broadcast node is not the last node of its process."""
    return CompilationIssue(
        code=StreamingErrorCode.BROADCAST_NOT_LAST,
        message="broadcast must be the last node in a process",
        component="broadcast",
    )


def scoped_process_not_last(node: object) -> CompilationIssue:
    """A With/WithAsync(process=...) node is not the last node of its process."""
    return CompilationIssue(
        code=StreamingErrorCode.SCOPED_PROCESS_NOT_LAST,
        message=f"{type(node).__name__}(process=...) must be the last node in a process",
        component=type(node).__name__,
    )


def explode_without_router(next_node: object | None) -> CompilationIssue:
    """An Explode node is not immediately followed by a Router."""
    got = type(next_node).__name__ if next_node is not None else "nothing"
    return CompilationIssue(
        code=StreamingErrorCode.EXPLODE_WITHOUT_ROUTER,
        message=f"Explode must be immediately followed by a Router; got {got}",
        component="Explode",
    )


def fork_branch_no_terminal(label: str) -> CompilationIssue:
    """A fork branch has no terminal output."""
    return CompilationIssue(
        code=StreamingErrorCode.FORK_BRANCH_NO_TERMINAL,
        message=f"fork branch {label}: no terminal output found",
        component=f"fork branch {label}",
    )


def router_branch_unsafe_node(label: str, node: object) -> CompilationIssue:
    """A node inside a Router branch does not implement RouterBranchSafe."""
    return CompilationIssue(
        code=StreamingErrorCode.ROUTER_BRANCH_UNSAFE_NODE,
        message=(f"router branch {label}: node {type(node).__name__} is not router-branch safe"),
        component=f"router branch {label}",
    )


def router_branch_fanout_unsupported(label: str, node: object) -> CompilationIssue:
    """A fan-out step was used inside a 1-to-1 Router branch."""
    return CompilationIssue(
        code=StreamingErrorCode.ROUTER_BRANCH_FANOUT_UNSUPPORTED,
        message=(
            f"router branch {label}: {type(node).__name__} is not supported in Router "
            f"branches — Router is 1-to-1; use Fork for fan-out."
        ),
        component=f"router branch {label}",
    )


def router_branch_shape_divergence(ordered_shapes: str) -> CompilationIssue:
    """Router branches produce different output shapes."""
    return CompilationIssue(
        code=StreamingErrorCode.ROUTER_BRANCH_SHAPE_DIVERGENCE,
        message=f"router branches produce different shapes: {ordered_shapes}",
        component="router",
    )


def output_with_fork() -> CompilationIssue:
    """flow.output was combined with a Fork whose branches must be terminal."""
    return CompilationIssue(
        code=StreamingErrorCode.OUTPUT_WITH_FORK,
        message="flow.output cannot be combined with Fork: branches must be terminal",
        component="flow.output",
        field="output",
    )


def output_with_broadcast() -> CompilationIssue:
    """flow.output was combined with a Broadcast whose branches must be terminal."""
    return CompilationIssue(
        code=StreamingErrorCode.OUTPUT_WITH_BROADCAST,
        message="flow.output cannot be combined with Broadcast: branches must be terminal",
        component="flow.output",
        field="output",
    )


def missing_terminal_output() -> CompilationIssue:
    """No terminal output exists anywhere in the flow."""
    return CompilationIssue(
        code=StreamingErrorCode.MISSING_TERMINAL_OUTPUT,
        message="no terminal output found: add IntoTopic, a storage sink node, or flow.output",
        component="flow",
        field="output",
    )


def window_strategy_unsupported(window: object) -> CompilationIssue:
    """CollectBatch declared a window strategy the adapter does not support."""
    return CompilationIssue(
        code=StreamingErrorCode.WINDOW_STRATEGY_UNSUPPORTED,
        message=(
            f"CollectBatch.window={window} is not yet supported by the Bytewax adapter. "
            f"Only WindowStrategy.COLLECT is available in this adapter version."
        ),
        component="CollectBatch",
        field="window",
    )


def scoped_into_topic_not_last(node: object, following: object) -> CompilationIssue:
    """The IntoTopic inside a scoped process is not its last node."""
    return CompilationIssue(
        code=StreamingErrorCode.SCOPED_INTO_TOPIC_NOT_LAST,
        message=(
            f"{type(node).__name__}(process=...) requires IntoTopic to be last; "
            f"found {type(following).__name__} after it."
        ),
        component=type(node).__name__,
    )


def scoped_process_unsupported_node(node: object, inner_node: object) -> CompilationIssue:
    """A scoped process contains a node kind it does not support."""
    return CompilationIssue(
        code=StreamingErrorCode.SCOPED_PROCESS_UNSUPPORTED_NODE,
        message=(
            f"{type(node).__name__}(process=...) only supports RecordStep nodes and an "
            f"optional terminal IntoTopic; found {type(inner_node).__name__}."
        ),
        component=type(node).__name__,
    )


def delivery_conflict(
    consumer_ref: str,
    delivery: str,
    enable_auto_commit: bool,
) -> CompilationIssue:
    """A consumer sets delivery and a contradicting deprecated enable_auto_commit."""
    return CompilationIssue(
        code=StreamingErrorCode.DELIVERY_CONFLICT,
        message=(
            f"kafka consumer '{consumer_ref}': delivery={delivery} conflicts with "
            f"enable_auto_commit={enable_auto_commit}; remove the deprecated enable_auto_commit"
        ),
        component=f"kafka consumer '{consumer_ref}'",
        field="kafka.consumer.enable_auto_commit",
    )


# ---------------------------------------------------------------------------
# Plan-building factories
# ---------------------------------------------------------------------------


def error_envelope_unparameterized(t: object) -> CompilationIssue:
    """ErrorEnvelope appeared in FromMultiTypeTopic without a type parameter."""
    return CompilationIssue(
        code=StreamingErrorCode.ERROR_ENVELOPE_UNPARAMETERIZED,
        message=(
            "ErrorEnvelope in FromMultiTypeTopic must be parameterized, "
            f"e.g. ErrorEnvelope[OrderEvent]. Got: {t!r}"
        ),
        component="FromMultiTypeTopic",
        field="payloads",
    )


def storage_sink_unsupported(node: object) -> CompilationIssue:
    """A storage sink node has no supported backend builder."""
    return CompilationIssue(
        code=StreamingErrorCode.STORAGE_SINK_UNSUPPORTED,
        message=f"Unsupported storage sink: {type(node).__name__}",
        component=type(node).__name__,
    )


def payload_type_invalid(t: object) -> CompilationIssue:
    """A FromMultiTypeTopic payload type does not expose loom_message_type()."""
    return CompilationIssue(
        code=StreamingErrorCode.PAYLOAD_TYPE_INVALID,
        message=(
            f"FromMultiTypeTopic payload {t!r} does not define loom_message_type(); "
            "payloads must be Loom struct types"
        ),
        component="FromMultiTypeTopic",
        field="payloads",
    )
