"""Post-commit channel: actions that run once a transaction has committed.

The executor binds one :class:`PostCommitChannel` per execution that owns a
unit of work (or when no channel is bound) and drains it after the commit.
Actions enqueued while a channel is bound belong to that channel; a
failed transaction discards them.  The owner unbinds the channel before
draining, so an execution started from an action opens its own lifecycle.
"""

from __future__ import annotations

import collections
import inspect
from collections.abc import Awaitable, Callable
from contextvars import ContextVar, Token

from loom.core.errors import LoomError
from loom.core.errors.codes import ErrorCode

PostCommitAction = Callable[[], Awaitable[None] | None]
"""Zero-argument callable: sync (returning ``None`` or an awaitable) or async."""

_channel: ContextVar[PostCommitChannel | None] = ContextVar(
    "_loom_post_commit_channel", default=None
)


class PostCommitError(LoomError):
    """Raised when one or more post-commit actions failed after a commit.

    Every action runs even when an earlier one fails; the error carries all
    failures in enqueue order.

    Args:
        committed: Whether the transaction had committed when the actions ran.
        failures: Exceptions raised by the failed actions, in enqueue order.
    """

    def __init__(self, *, committed: bool, failures: tuple[Exception, ...]) -> None:
        self.committed = committed
        self.failures = failures
        super().__init__(
            f"{len(failures)} post-commit action(s) failed (committed={committed})",
            code=ErrorCode.POST_COMMIT_FAILURE,
        )


class PostCommitChannel:
    """Ordered queue of actions to run after the owning transaction commits.

    Example::

        channel = PostCommitChannel()
        channel.enqueue(lambda: broker.send(message))
        await channel.drain(committed=True)
    """

    __slots__ = ("_actions",)

    def __init__(self) -> None:
        self._actions: list[PostCommitAction] = []

    def enqueue(self, action: PostCommitAction) -> None:
        """Append an action; it runs in enqueue order on :meth:`drain`.

        Args:
            action: Sync callable returning ``None`` or an awaitable, or an
                async callable.
        """
        self._actions.append(action)

    def discard(self) -> None:
        """Drop every queued action without running it."""
        self._actions.clear()

    async def drain(self, *, committed: bool) -> None:
        """Run every queued action in enqueue order.

        The owner unbinds the channel before draining, so an execution
        started from an action opens its own lifecycle.  A failing action
        does not stop the others.  A cancellation stops the drain at once;
        the actions not yet run stay queued so a later drain can run them.

        Args:
            committed: Whether a transaction of this owner's had committed
                when the actions ran.  ``False`` when the owner held no unit
                of work, so a caller may safely retry the whole operation.

        Raises:
            PostCommitError: If any action raised; carries every failure.
        """
        pending = collections.deque(self._actions)
        self._actions = []
        failures: list[Exception] = []
        try:
            await _run_all(pending, failures)
        except BaseException:
            self._actions = [*pending, *self._actions]
            raise
        if failures:
            raise PostCommitError(committed=committed, failures=tuple(failures))


async def _run_all(pending: collections.deque[PostCommitAction], failures: list[Exception]) -> None:
    """Pop and run ``pending`` in order; an interrupted action is not retried."""
    while pending:
        action = pending.popleft()
        try:
            await _run_one(action)
        except Exception as exc:  # collected and re-raised together by drain
            failures.append(exc)


async def _run_one(action: PostCommitAction) -> None:
    result = action()
    if inspect.isawaitable(result):
        await result


def bind_channel(channel: PostCommitChannel) -> Token[PostCommitChannel | None]:
    """Make ``channel`` the active channel for the current context.

    Args:
        channel: Channel that receives subsequent enqueues.

    Returns:
        Token to pass to :func:`reset_channel`.
    """
    return _channel.set(channel)


def reset_channel(token: Token[PostCommitChannel | None]) -> None:
    """Restore the channel that was active before :func:`bind_channel`.

    Args:
        token: Token returned by :func:`bind_channel`.
    """
    _channel.reset(token)


def active_channel() -> PostCommitChannel | None:
    """Return the channel bound to the current context, if any."""
    return _channel.get()
