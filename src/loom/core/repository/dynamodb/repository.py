"""DynamoDB implementation of the repository contract.

DynamoDB is a key-value / document store, not a relational engine. This
repository implements faithfully only what DynamoDB supports natively through
the item primary key: ``get_by_id``, ``create``, ``update``, ``delete`` and the
key-scoped ``get_by`` / ``exists_by``. Operations that DynamoDB cannot serve
efficiently without a full table scan or a secondary index — arbitrary-field
filters, ``count``, offset pagination with a total count — raise
:class:`DynamoCapabilityError` instead of silently degrading.

Because :class:`RepositoryDynamoDB` still declares the ``Listable`` and
``Countable`` capabilities (to keep a single repository contract across
backends), ``count`` / ``list_paginated`` / ``list_with_query`` are wired into
DI but raise :class:`DynamoCapabilityError` at call time. Any REST listing
endpoint served by this backend therefore returns an error rather than a page.
Selectively dropping those capability bindings is a possible follow-up.

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

from loom.core.errors import Conflict
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

# DynamoDB error code returned when a ``ConditionExpression`` is not satisfied.
_CONDITIONAL_CHECK_FAILED = "ConditionalCheckFailedException"


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
    """Key-value repository backed by a single boto3 low-level ``dynamodb`` client.

    The low-level client (not the resource) is used because it is thread-safe:
    every operation runs under :func:`asyncio.to_thread`, and a shared client
    can be called from multiple worker threads concurrently. Items cross the
    wire as low-level ``AttributeValue`` dicts (``{"S": ...}``, ``{"N": ...}``);
    :class:`boto3.dynamodb.types.TypeSerializer` /
    :class:`~boto3.dynamodb.types.TypeDeserializer` translate them to and from
    plain Python values, preserving the ``float``↔``Decimal`` round-trip
    DynamoDB requires.

    ``count`` / ``list_paginated`` / ``list_with_query`` are declared for a
    uniform contract but always raise :class:`DynamoCapabilityError`; REST
    listing endpoints served by this backend therefore return an error.

    Args:
        client: A boto3 low-level ``dynamodb`` client (or a compatible object
            exposing ``get_item`` / ``put_item`` / ``delete_item`` with the
            client-shaped ``TableName`` + ``AttributeValue`` API).
        table_name: Name of the DynamoDB table backing this repository.
        model: The Loom struct model this repository is bound to. Its
            ``primary_key`` field becomes the DynamoDB partition key.
    """

    def __init__(self, client: Any, table_name: str, model: type) -> None:
        # Lazy import: boto3 is an optional dependency (loom[dynamodb]) and must
        # not be required at module import time by SQLAlchemy-backed apps.
        from boto3.dynamodb.types import (  # type: ignore[import-untyped]
            TypeDeserializer,
            TypeSerializer,
        )
        from botocore.exceptions import ClientError  # type: ignore[import-untyped]

        self._client = client
        self._table_name = table_name
        self._model = model
        self._id_attr = get_id_attribute(model)
        self._column_fields = frozenset(get_column_fields(model).keys())
        self._serializer = TypeSerializer()
        self._deserializer = TypeDeserializer()
        self._client_error = ClientError
        self.log = get_logger(__name__).bind(repository=self.__class__.__name__)

    async def get_by_id(self, obj_id: IdT, profile: str = "default") -> OutputT | None:
        """Fetch one entity by its partition key via ``GetItem``."""
        response = await asyncio.to_thread(
            self._client.get_item, TableName=self._table_name, Key=self._encode_key(obj_id)
        )
        item = response.get("Item")
        if item is None:
            return None
        return self._to_output(self._decode_item(item))

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
        response = await asyncio.to_thread(
            self._client.get_item, TableName=self._table_name, Key=self._encode_key(value)
        )
        return "Item" in response

    async def create(self, data: msgspec.Struct) -> OutputT:
        """Persist a new item with a conditional ``PutItem`` (insert, not upsert).

        The ``attribute_not_exists`` condition rejects an overwrite of an
        existing item; a failed condition is surfaced as
        :class:`~loom.core.errors.Conflict`.
        """
        internal = self._to_internal(data)
        try:
            await asyncio.to_thread(
                self._client.put_item,
                TableName=self._table_name,
                Item=self._encode_item(internal),
                ConditionExpression="attribute_not_exists(#pk)",
                ExpressionAttributeNames={"#pk": self._id_attr},
            )
        except self._client_error as exc:
            if _is_conditional_check_failure(exc):
                raise Conflict(
                    f"{self._model.__name__} with {self._id_attr}="
                    f"{internal.get(self._id_attr)!r} already exists."
                ) from exc
            raise
        return self._to_output(internal)

    async def update(self, obj_id: IdT, data: msgspec.Struct) -> OutputT | None:
        """Apply a partial update by read-merge-write, keyed on the partition key.

        The write carries an ``attribute_exists`` condition so a concurrent
        delete between the read and the write cannot resurrect the item. Returns
        ``None`` when no item exists for ``obj_id`` — whether detected by the
        initial read or by the conditional write losing the race.
        """
        response = await asyncio.to_thread(
            self._client.get_item, TableName=self._table_name, Key=self._encode_key(obj_id)
        )
        existing = response.get("Item")
        if existing is None:
            return None
        changes = self._to_internal(data)
        changes.pop(self._id_attr, None)
        merged = {**self._decode_item(existing), **changes, self._id_attr: obj_id}
        try:
            await asyncio.to_thread(
                self._client.put_item,
                TableName=self._table_name,
                Item=self._encode_item(merged),
                ConditionExpression="attribute_exists(#pk)",
                ExpressionAttributeNames={"#pk": self._id_attr},
            )
        except self._client_error as exc:
            if _is_conditional_check_failure(exc):
                return None
            raise
        return self._to_output(merged)

    async def delete(self, obj_id: IdT) -> bool:
        """Delete an item by partition key, returning whether it existed."""
        response = await asyncio.to_thread(
            self._client.delete_item,
            TableName=self._table_name,
            Key=self._encode_key(obj_id),
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

    def _encode_value(self, value: Any) -> dict[str, Any]:
        """Serialize a plain Python value to a low-level ``AttributeValue`` dict.

        Floats are converted to ``Decimal`` first because DynamoDB — and
        ``TypeSerializer`` — reject native floats.
        """
        return cast("dict[str, Any]", self._serializer.serialize(_floats_to_decimal(value)))

    def _decode_value(self, value: dict[str, Any]) -> Any:
        """Deserialize a low-level ``AttributeValue`` dict back to a Python value.

        DynamoDB numbers arrive as ``Decimal``; they are narrowed back to
        ``int``/``float`` to match what was originally stored.
        """
        return _decimals_to_number(self._deserializer.deserialize(value))

    def _encode_item(self, item: dict[str, Any]) -> dict[str, Any]:
        """Serialize a full item dict to low-level ``AttributeValue`` form."""
        return {key: self._encode_value(value) for key, value in item.items()}

    def _decode_item(self, item: dict[str, Any]) -> dict[str, Any]:
        """Deserialize a full low-level item dict back to plain Python values."""
        return {key: self._decode_value(value) for key, value in item.items()}

    def _encode_key(self, obj_id: Any) -> dict[str, Any]:
        """Build the low-level ``Key`` dict for the partition key."""
        return {self._id_attr: self._encode_value(obj_id)}


def _is_conditional_check_failure(exc: Exception) -> bool:
    """Return whether *exc* is a DynamoDB conditional-check failure."""
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    error = response.get("Error", {})
    return isinstance(error, dict) and error.get("Code") == _CONDITIONAL_CHECK_FAILED


def _floats_to_decimal(value: Any) -> Any:
    """Recursively convert ``float`` to ``Decimal`` — DynamoDB rejects floats."""
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {key: _floats_to_decimal(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_floats_to_decimal(item) for item in value]
    return value


def _decimals_to_number(value: Any) -> Any:
    """Recursively convert DynamoDB ``Decimal`` back to ``int``/``float``."""
    if isinstance(value, Decimal):
        as_int = int(value)
        return as_int if as_int == value else float(value)
    if isinstance(value, dict):
        return {key: _decimals_to_number(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_decimals_to_number(item) for item in value]
    return value
