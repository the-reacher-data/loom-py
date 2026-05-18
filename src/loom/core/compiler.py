"""Compiler contracts shared by Loom domains.

Compilers translate a request object into an executable plan. The concrete
request and plan types vary by domain; this protocol only captures the
shape of the transformation.
"""

from __future__ import annotations

from typing import Protocol, TypeVar

RequestT = TypeVar("RequestT", contravariant=True)
PlanT = TypeVar("PlanT", covariant=True)


class CompilerProtocol(Protocol[RequestT, PlanT]):
    """Structural protocol for a compiler that materializes an execution plan."""

    def compile(self, request: RequestT) -> PlanT:
        """Transform *request* into an executable plan."""
        ...


__all__ = ["CompilerProtocol", "PlanT", "RequestT"]
