"""Contracts of the data-driven application self-description.

The registry of contributions is *data*: a pillar is named by a
``"module:callable"`` string and resolved at description time, never at import
time.  That is what keeps ``loom.core`` free of any pillar import while still
letting every pillar contribute its own section to the document.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

INTROSPECTION_STATE_ATTR = "loom_introspection"
"""Attribute an application stores its :class:`AppIntrospection` under."""


class IntrospectionError(Exception):
    """Raised when the self-description cannot be produced."""


class DescribeContributor(Protocol):
    """One pillar's contribution to :func:`~loom.core.introspection.describe_app`."""

    def __call__(self, subject: Any, /) -> Any:
        """Project the pillar's compiled state into a JSON-encodable value.

        Args:
            subject: The pillar's compiled state, as declared by its
                :class:`ContributorRef`.

        Returns:
            Builtins only: mappings, sequences, strings, numbers, booleans or
            ``None``.
        """
        ...


@dataclass(frozen=True)
class ContributorRef:
    """Reference to one pillar's contribution, resolved only when described.

    Attributes:
        section: Key the contribution appears under in the document.  Never
            ``"app"``, which is reserved for the application identity.
        contributor: ``"module:callable"`` reference to a
            :class:`DescribeContributor`.
        subject: The pillar's compiled state, handed to that callable.
    """

    section: str
    contributor: str
    subject: object


@dataclass(frozen=True)
class AppIntrospection:
    """Everything needed to describe one application.

    Attributes:
        name: Application name.
        version: Published application version.
        contributors: Pillar contributions, described in declaration order.
    """

    name: str
    version: str
    contributors: tuple[ContributorRef, ...] = ()
