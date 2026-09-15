"""Parity of the two ``LoomType`` implementations and the factory's strictness.

Every boundary carries a ``LoomType`` compiled once from an authored symbol, so
the msgspec and pydantic implementations must agree on what strict JSON means:
no ``int``/``float``/``bool`` coercion from strings, ``int`` accepted for
``float``, ISO datetime, UUID and Decimal accepted as strings, unknown keys and
malformed JSON rejected with one loom exception carrying a one-line message.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import msgspec
import pytest
from pydantic import BaseModel, ConfigDict

from loom.core.model import (
    BoundaryValidationError,
    LoomType,
    UnsupportedBoundaryType,
    loom_type,
    msgspec_type,
)

_LAX_REASON = (
    "the type must reject unknown fields: forbid_unknown_fields=True or "
    "model_config['extra'] == 'forbid' (invariant 5)"
)


class StructInvoice(msgspec.Struct, forbid_unknown_fields=True):
    total: int
    price: float
    paid: bool
    note: str
    when: datetime
    ident: UUID
    amount: Decimal


class ModelInvoice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total: int
    price: float
    paid: bool
    note: str
    when: datetime
    ident: UUID
    amount: Decimal


class LaxStruct(msgspec.Struct):
    total: int


class LaxModel(BaseModel):
    total: int


class KwargsModel(BaseModel, extra="forbid"):
    total: int


class ReopenedModel(ModelInvoice, extra="allow"):
    pass


_PAYLOAD: dict[str, Any] = {
    "total": 3,
    "price": 1.5,
    "paid": True,
    "note": "ok",
    "when": "2026-09-15T10:00:00Z",
    "ident": "12345678-1234-5678-1234-567812345678",
    "amount": "1.50",
}


def _with(**overrides: Any) -> str:
    return json.dumps({**_PAYLOAD, **overrides})


def _root_object(schema: Any) -> dict[str, Any]:
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        root: dict[str, Any] = schema["$defs"][name]
        return root
    return dict(schema)


@pytest.fixture(params=[StructInvoice, ModelInvoice], ids=["msgspec", "pydantic"])
def invoice(request: pytest.FixtureRequest) -> LoomType:
    compiled: LoomType = loom_type(request.param)
    return compiled


class TestDecodeJson:
    """Strict JSON semantics are the same behind both libraries."""

    def test_accepts_the_payload(self, invoice: LoomType) -> None:
        value = invoice.decode_json(_with())

        assert isinstance(value, invoice.type)
        assert value.total == 3
        assert value.price == 1.5
        assert value.paid is True
        assert value.note == "ok"
        assert value.when == datetime(2026, 9, 15, 10, tzinfo=UTC)
        assert value.ident == UUID("12345678-1234-5678-1234-567812345678")
        assert value.amount == Decimal("1.50")

    def test_accepts_bytes(self, invoice: LoomType) -> None:
        assert invoice.decode_json(_with().encode()).total == 3

    @pytest.mark.parametrize(
        "field, wrong",
        [("total", "5"), ("total", 5.5), ("price", "1.5"), ("paid", "true"), ("paid", 1)],
        ids=["int-from-str", "int-from-float", "float-from-str", "bool-from-str", "bool-from-int"],
    )
    def test_rejects_scalar_coercion(self, invoice: LoomType, field: str, wrong: Any) -> None:
        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.decode_json(_with(**{field: wrong}))

        assert field in str(excinfo.value)

    def test_accepts_int_for_float(self, invoice: LoomType) -> None:
        value = invoice.decode_json(_with(price=2))

        assert value.price == 2
        assert isinstance(value.price, float)

    def test_rejects_an_unknown_key(self, invoice: LoomType) -> None:
        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.decode_json(_with(zz=1))

        assert "zz" in str(excinfo.value)

    def test_rejects_a_missing_field(self, invoice: LoomType) -> None:
        payload = dict(_PAYLOAD)
        del payload["note"]

        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.decode_json(json.dumps(payload))

        assert "note" in str(excinfo.value)

    def test_rejects_malformed_json(self, invoice: LoomType) -> None:
        with pytest.raises(BoundaryValidationError):
            invoice.decode_json('{"total": ')

    def test_error_carries_the_library_error_and_a_one_line_message(
        self, invoice: LoomType
    ) -> None:
        with pytest.raises(BoundaryValidationError) as excinfo:
            invoice.decode_json(_with(total="5", zz=1))

        error = excinfo.value
        assert error.code == "boundary_validation"
        assert error.__cause__ is not None
        assert "\n" not in error.message
        assert "input_value" not in error.message
        assert "https://" not in error.message


class TestSchema:
    """The root object forbids additional properties and carries no root title."""

    def test_root_object_forbids_unknown_fields(self, invoice: LoomType) -> None:
        root = _root_object(invoice.schema())

        assert root["type"] == "object"
        assert root["additionalProperties"] is False
        assert set(root["required"]) == set(_PAYLOAD)

    def test_pydantic_root_has_no_title(self) -> None:
        schema = loom_type(ModelInvoice).schema()

        assert "title" not in schema
        assert schema["properties"]["total"] == {"title": "Total", "type": "integer"}

    def test_schema_is_a_fresh_document_each_call(self) -> None:
        compiled = loom_type(ModelInvoice)

        assert compiled.schema() == compiled.schema()


class TestToBuiltins:
    """A decoded value goes back to JSON-mode builtins and decodes again."""

    def test_round_trip(self, invoice: LoomType) -> None:
        value = invoice.decode_json(_with())

        builtins = invoice.to_builtins(value)

        assert builtins["total"] == 3
        assert builtins["price"] == 1.5
        assert builtins["paid"] is True
        assert builtins["ident"] == _PAYLOAD["ident"]
        assert builtins["amount"] == "1.50"
        assert isinstance(builtins["when"], str)
        assert invoice.decode_json(json.dumps(builtins)) == value


class TestIdentity:
    """``type`` and ``library`` tell consumers what they carry."""

    def test_msgspec_identity(self) -> None:
        compiled = loom_type(StructInvoice)

        assert compiled.type is StructInvoice
        assert compiled.library == "msgspec"

    def test_pydantic_identity(self) -> None:
        compiled = loom_type(ModelInvoice)

        assert compiled.type is ModelInvoice
        assert compiled.library == "pydantic"


class TestStrictness:
    """The factory checks strictness once, for both libraries, with one reason."""

    def test_config_dict_is_strict(self) -> None:
        assert loom_type(ModelInvoice).library == "pydantic"

    def test_class_kwargs_are_strict(self) -> None:
        assert loom_type(KwargsModel).library == "pydantic"

    @pytest.mark.parametrize("symbol", [LaxStruct, LaxModel, ReopenedModel])
    def test_lax_types_are_rejected(self, symbol: type) -> None:
        with pytest.raises(UnsupportedBoundaryType) as excinfo:
            loom_type(symbol)

        assert excinfo.value.code == "boundary_type_unsupported"
        assert str(excinfo.value) == _LAX_REASON

    @pytest.mark.parametrize("symbol", [dict, object(), dict[str, Any]])
    def test_other_symbols_are_rejected(self, symbol: Any) -> None:
        with pytest.raises(UnsupportedBoundaryType) as excinfo:
            loom_type(symbol)

        assert excinfo.value.code == "boundary_type_unsupported"
        assert "msgspec.Struct or pydantic.BaseModel" in str(excinfo.value)


class TestMsgspecType:
    """Compiler-generated annotations are wrapped without a strictness check."""

    def test_open_dict(self) -> None:
        compiled = msgspec_type(dict[str, Any])

        assert compiled.library == "msgspec"
        assert compiled.decode_json('{"a": [1, "b"]}') == {"a": [1, "b"]}
        assert compiled.schema()["type"] == "object"

    def test_list_of_int(self) -> None:
        compiled = msgspec_type(list[int])

        assert compiled.decode_json("[1, 2]") == [1, 2]
        with pytest.raises(BoundaryValidationError):
            compiled.decode_json('["1"]')

    def test_defstruct(self) -> None:
        generated = msgspec.defstruct("Answer", [("total", int)], forbid_unknown_fields=True)
        compiled = msgspec_type(generated)

        value = compiled.decode_json('{"total": 1}')

        assert compiled.type is generated
        assert compiled.to_builtins(value) == {"total": 1}
        assert _root_object(compiled.schema())["additionalProperties"] is False
