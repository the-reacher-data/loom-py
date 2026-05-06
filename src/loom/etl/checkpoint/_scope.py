"""Checkpoint lifetime scope declaration."""

from __future__ import annotations

from enum import StrEnum


class CheckpointScope(StrEnum):
    """Lifetime scope of a checkpoint (intermediate) result.

    Declared on :class:`~loom.etl.IntoTemp` to control when the materialised
    intermediate is cleaned up by the executor.

    Values:

    * ``RUN``         — Cleaned in the ``finally`` block of every pipeline run,
                        regardless of success or failure.  Safe default for
                        ephemeral checkpoints within a single attempt.

    * ``CORRELATION`` — Survives a failed run so that the next retry attempt
                        can reuse already-materialised intermediates.  Cleaned
                        automatically on success when ``last_attempt=True`` in
                        :class:`loom.etl.lineage.RunContext`.  On
                        failure the executor emits a structured warning; the
                        caller must invoke
                        :meth:`~loom.etl.ETLRunner.cleanup_correlation`
                        explicitly after the final failed attempt.

    Example::

        target = IntoTemp("normalized_orders", scope=CheckpointScope.CORRELATION)
    """

    RUN = "run"
    CORRELATION = "correlation"
