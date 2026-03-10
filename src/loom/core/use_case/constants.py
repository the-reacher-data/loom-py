"""Use-case layer constants: CRUD operations and key conventions."""

from __future__ import annotations

from enum import StrEnum


class CrudOp(StrEnum):
    """Standard CRUD operation names used by AutoCRUD route generation.

    Values are plain strings at runtime (``StrEnum``), compatible with any
    API that accepts ``str`` operation identifiers — e.g. the ``include``
    parameter of :func:`~loom.rest.autocrud.build_auto_routes`.

    Example::

        routes = build_auto_routes(User, include=(CrudOp.CREATE, CrudOp.GET))
    """

    CREATE = "create"
    GET = "get"
    LIST = "list"
    UPDATE = "update"
    DELETE = "delete"


KEY_SEPARATOR = ":"
