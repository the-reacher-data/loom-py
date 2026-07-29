"""DynamoDB implementation of the repository contract.

DynamoDB is a key-value / document store, not a relational engine. This
repository implements faithfully only what DynamoDB supports natively through
the item primary key: ``get_by_id``, ``create``, ``update``, ``delete`` and the
key-scoped ``get_by`` / ``exists_by``. Operations that DynamoDB cannot serve
efficiently without a full table scan or a secondary index — arbitrary-field
filters, ``count``, offset pagination with a total count — raise
:class:`DynamoCapabilityError` instead of silently degrading.

Model-to-table mapping is intentionally simple: every model is stored in the
single table configured under ``persistence.dynamodb.table``. The item primary
key attribute is the model's ``primary_key`` field, resolved via
:func:`loom.core.model.introspection.get_id_attribute`.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any, Generic, cast

import msgspec

from loom.core.logger import get_logger
from loom.core.model.introspection import get_column_fields, get_id_attribute
from loom.core.repository.abc import (
    Countable,
    Creatable,
    CursorResult,
    Deletable,
    FilterParams,
    IdT,
    Listable,
    OutputT,
    PageParams,
    PageResult,
    QuerySpec,
    Readable,
    Updatable,
)


class DynamoCapabilityError(NotImplementedError):
    """Raised when an operation is not supported by the DynamoDB key-value backend.

    DynamoDB serves data efficiently only through the item primary key. Any
    operation requiring a full-table scan or a secondary index — arbitrary
    field filters, counting, or offset pagination with a total count — raises
    this error rather than performing an expensive or misleading scan.
    """


class RepositoryDynamoDB(
    Readable[OutputT],
    Creatable[OutputT],
    Updatable[OutputT],
    Deletable[OutputT],
    Listable[OutputT],
    Countable[OutputT],
    Generic[OutputT, IdT],
):
    """Key-value repository backed by a single boto3 DynamoDB ``Table`` resource.

    Args:
        table: A boto3 ``dynamodb`` service-resource ``Table`` (or a compatible
            object exposing ``get_item``/``put_item``/``delete_item``). The
            resource API is used so items are plain Python values rather than
            low-level ``AttributeValue`` dicts.
        model: The Loom struct model this repository is bound to. Its
            ``primary_key`` field becomes the DynamoDB partition key.
    """

    def __init__(self, table: Any, model: type) -> None:
        self._table = table
        self._model = model
        self._id_attr = get_id_attribute(model)
        self._column_fields = frozenset(get_column_fields(model).keys())
        self.log = get_logger(__name__).bind(repository=self.__class__.__name__)

    async def get_by_id(self, obj_id: IdT, profile: str = "default") -> OutputT | None:
        """Fetch one entity by its partition key via ``GetItem``."""
        response = await asyncio.to_thread(self._table.get_item, Key={self._id_attr: obj_id})
        item = response.get("Item")
        if item is None:
            return None
        return self._to_output(_decode_value(item))

    async def get_by(self, field: str, value: Any, profile: str = "default") -> OutputT | None:
        """Fetch one entity by ``field == value``.

        Supported only when ``field`` is the partition key; any other field
        would require a scan or secondary index and raises
        :class:`DynamoCapabilityError`.
        """
        if field == self._id_attr:
            return await self.get_by_id(cast(IdT, value), profile)
        raise DynamoCapabilityError(
            f"get_by('{field}') is not supported by the DynamoDB backend: only the "
            f"primary key '{self._id_attr}' can be queried without a scan or secondary index."
        )

    async def exists_by(self, field: str, value: Any) -> bool:
        """Return whether an item exists for ``field == value`` (primary key only)."""
        if field != self._id_attr:
            raise DynamoCapabilityError(
                f"exists_by('{field}') is not supported by the DynamoDB backend: only the "
                f"primary key '{self._id_attr}' can be queried without a scan or secondary index."
            )
        response = await asyncio.to_thread(self._table.get_item, Key={self._id_attr: value})
        return "Item" in response

    async def create(self, data: msgspec.Struct) -> OutputT:
        """Persist a new item with an autocommit ``PutItem``."""
        internal = self._to_internal(data)
        await asyncio.to_thread(self._table.put_item, Item=_encode_value(internal))
        return self._to_output(internal)

    async def update(self, obj_id: IdT, data: msgspec.Struct) -> OutputT | None:
        """Apply a partial update by read-merge-write, keyed on the partition key.

        Returns ``None`` when no item exists for ``obj_id``.
        """
        response = await asyncio.to_thread(self._table.get_item, Key={self._id_attr: obj_id})
        existing = response.get("Item")
        if existing is None:
            return None
        changes = self._to_internal(data)
        changes.pop(self._id_attr, None)
        merged = {**_decode_value(existing), **changes, self._id_attr: obj_id}
        await asyncio.to_thread(self._table.put_item, Item=_encode_value(merged))
        return self._to_output(merged)

    async def delete(self, obj_id: IdT) -> bool:
        """Delete an item by partition key, returning whether it existed."""
        response = await asyncio.to_thread(
            self._table.delete_item,
            Key={self._id_attr: obj_id},
            ReturnValues="ALL_OLD",
        )
        return "Attributes" in response

    async def count(self) -> int:
        """Not supported: counting requires a full table scan."""
        raise DynamoCapabilityError(
            "count() is not supported by the DynamoDB backend: it would require a full "
            "table scan. Track counts out-of-band (e.g. an atomic counter item) instead."
        )

    async def list_paginated(
        self,
        page_params: PageParams,
        filter_params: FilterParams | None = None,
        profile: str = "default",
    ) -> PageResult[OutputT]:
        """Not supported: offset pagination with a total count needs a scan."""
        raise DynamoCapabilityError(
            "list_paginated() is not supported by the DynamoDB backend: offset pagination "
            "with a total count requires a full table scan."
        )

    async def list_with_query(
        self,
        query: QuerySpec,
        profile: str = "default",
    ) -> PageResult[OutputT] | CursorResult[OutputT]:
        """Not supported: arbitrary structured queries need a scan or index."""
        raise DynamoCapabilityError(
            "list_with_query() is not supported by the DynamoDB backend: arbitrary field "
            "filters and sorting require a full table scan or a secondary index."
        )

    def _to_internal(self, data: msgspec.Struct) -> dict[str, Any]:
        """Serialize a struct to a dict keyed by internal (snake_case) field names."""
        builtins = msgspec.to_builtins(data)
        if not isinstance(builtins, dict):
            raise TypeError("Struct payload must serialize to a dict")
        encoded_to_internal: dict[str, str] = {
            field.encode_name: field.name for field in msgspec.structs.fields(type(data))
        }
        result: dict[str, Any] = {}
        for key, value in builtins.items():
            name = str(key)
            result[encoded_to_internal.get(name, name)] = value
        return result

    def _to_output(self, item: dict[str, Any]) -> OutputT:
        """Build the model output struct from a stored item, ignoring unknown attrs."""
        kwargs = {key: value for key, value in item.items() if key in self._column_fields}
        return cast(OutputT, self._model(**kwargs))


def _encode_value(value: Any) -> Any:
    """Recursively convert ``float`` to ``Decimal`` — DynamoDB rejects floats."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: _encode_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_encode_value(item) for item in value]
    return value


def _decode_value(value: Any) -> Any:
    """Recursively convert DynamoDB ``Decimal`` back to ``int``/``float``."""
    if isinstance(value, Decimal):
        as_int = int(value)
        return as_int if as_int == value else float(value)
    if isinstance(value, dict):
        return {key: _decode_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decode_value(item) for item in value]
    return value
