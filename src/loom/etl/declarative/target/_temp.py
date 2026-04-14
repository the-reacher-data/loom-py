"""TEMP target variant specs.

Two variants replace the single flat ``temp_append: bool`` flag.

Internal module — import from :mod:`loom.etl.declarative.target`.
"""

from __future__ import annotations

from dataclasses import dataclass

from loom.etl.checkpoint import CheckpointScope


@dataclass(frozen=True)
class TempSpec:
    """Write result to a checkpoint store.  Exactly one writer per name.

    Args:
        temp_name:  Logical name identifying this intermediate.
        temp_scope: Lifetime scope (:attr:`~loom.etl.CheckpointScope.RUN` by default).
    """

    temp_name: str
    temp_scope: CheckpointScope


@dataclass(frozen=True)
class TempFanInSpec:
    """Write result to a checkpoint store in fan-in mode.

    Multiple steps may write to the same ``temp_name``; their outputs are
    concatenated and exposed as one logical intermediate.  All writers for
    a given name must use :class:`TempFanInSpec` — mixing with
    :class:`TempSpec` is a compile-time error.

    Args:
        temp_name:  Logical name identifying this intermediate.
        temp_scope: Lifetime scope.
    """

    temp_name: str
    temp_scope: CheckpointScope
