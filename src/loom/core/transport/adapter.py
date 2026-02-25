from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from loom.core.use_case.use_case import UseCase


@dataclass(frozen=True)
class AdapterRequest:
    """Normalized, transport-agnostic request for UseCase execution.

    Carries the parsed inputs that a transport layer (REST, CLI, gRPC, etc.)
    extracts from its native request format before handing off to a
    ``LoomAdapter`` implementation.

    Args:
        params: Primitive parameter values keyed by name (e.g. path/query vars).
        payload: Raw dict for command construction via ``Input()``. ``None``
            when the UseCase declares no ``Input()`` marker.
        dependencies: Mapping of entity type to repository instance. Populated
            by the DI container at request scope. Empty dict when no ``Load()``
            steps are declared.

    Example::

        request = AdapterRequest(
            params={"user_id": 1},
            payload={"email": "new@corp.com"},
            dependencies={User: user_repo},
        )
    """

    params: dict[str, Any]
    payload: dict[str, Any] | None = None
    dependencies: dict[type[Any], Any] = field(default_factory=dict)


@runtime_checkable
class LoomAdapter(Protocol):
    """Port for transport adapters.

    Any transport layer (REST, CLI, event bus) that drives UseCase execution
    must implement this protocol. This keeps the engine decoupled from
    transport concerns.

    The adapter is responsible for:
    - Constructing an ``AdapterRequest`` from the native request format.
    - Serializing the result into the native response format.
    - Mapping domain errors to transport-specific error responses.

    Example::

        class MyCliAdapter:
            async def handle(
                self,
                use_case: UseCase[Any],
                request: AdapterRequest,
            ) -> Any:
                ...
    """

    async def handle(
        self,
        use_case: UseCase[Any],
        request: AdapterRequest,
    ) -> Any:
        """Execute the UseCase and return a transport-ready result.

        Args:
            use_case: Constructed UseCase instance.
            request: Normalized request data.

        Returns:
            Transport-ready result (serializable form).
        """
        ...
