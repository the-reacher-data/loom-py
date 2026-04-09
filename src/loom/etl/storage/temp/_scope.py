"""ETL intermediate (temp) scope declaration.

See :class:`~loom.etl.IntoTemp` and :class:`~loom.etl.FromTemp` for the full
intermediate system.  This module contains only the lifetime scope enum to
avoid circular imports between :mod:`loom.etl._source` and
:mod:`loom.etl._target`.
"""

from __future__ import annotations

from enum import StrEnum


class TempScope(StrEnum):
    """Lifetime scope of an intermediate (temp) result.

    Declared on :class:`~loom.etl.IntoTemp` to control when the materialised
    intermediate is cleaned up by the executor.

    Values:

    * ``RUN``         — Cleaned in the ``finally`` block of every pipeline run,
                        regardless of success or failure.  Safe default for
                        ephemeral checkpoints within a single attempt.

    * ``CORRELATION`` — Survives a failed run so that the next retry attempt
                        can reuse already-materialised intermediates.  Cleaned
                        automatically on success when ``last_attempt=True`` in
                        :class:`~loom.etl.observability.RunContext`.  On
                        failure the executor emits a structured warning; the
                        caller must invoke
                        :meth:`~loom.etl.ETLRunner.cleanup_correlation`
                        explicitly after the final failed attempt.

    Example::

        target = IntoTemp("normalized_orders", scope=TempScope.CORRELATION)
    """

    RUN = "run"
    CORRELATION = "correlation"
