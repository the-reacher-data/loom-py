"""Data-driven application self-description, free of any pillar import.

``loom.core`` knows how to assemble one document out of an application
identity plus a set of *references* to pillar contributors; it never knows
which pillars exist.  References resolve by ``importlib`` inside
:func:`describe_app`, so importing this package pulls in nothing but the
standard library.

Example::

    from loom.core.introspection import AppIntrospection, ContributorRef, describe_app

    introspection = AppIntrospection(
        name="billing",
        version="1.4.0",
        contributors=(
            ContributorRef(
                section="agents",
                contributor="loom.ai.describe:describe_agents",
                subject=plans,
            ),
        ),
    )
    document = describe_app(introspection)
"""

from loom.core.introspection.base import (
    INTROSPECTION_STATE_ATTR,
    AppIntrospection,
    ContributorRef,
    DescribeContributor,
    IntrospectionError,
)
from loom.core.introspection.describe import describe_app

__all__ = [
    "INTROSPECTION_STATE_ATTR",
    "AppIntrospection",
    "ContributorRef",
    "DescribeContributor",
    "IntrospectionError",
    "describe_app",
]
