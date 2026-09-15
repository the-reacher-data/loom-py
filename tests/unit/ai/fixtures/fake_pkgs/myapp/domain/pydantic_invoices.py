"""A pydantic ``type_ref`` target, valid and invalid, for the output phase tests."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class InvoiceSummaryModel(BaseModel):
    """Strict pydantic analogue of ``InvoiceSummary`` (invoices.py): a valid ``type_ref``."""

    model_config = ConfigDict(extra="forbid")

    issuer: str = ""
    total: float = 0.0
    due_date: str | None = None


class LaxModel(BaseModel):
    """A pydantic model that does not forbid extra fields: not a valid ``type_ref``."""

    issuer: str = ""


class LineItemModel(BaseModel):
    """A nested field of ``NestedInvoiceModel``: forces ``$defs``/``$ref`` in its schema."""

    model_config = ConfigDict(extra="forbid")

    label: str
    amount: float = 0.0


class NestedInvoiceModel(BaseModel):
    """A strict ``type_ref`` with a nested model, unlike the flat ``InvoiceSummaryModel``.

    ``model_json_schema()`` emits ``$defs``/``$ref`` for this shape, which is
    exactly where it can diverge from ``msgspec.json.schema()`` -- the point
    this fixture exists to exercise, not ``InvoiceSummaryModel`` alone.
    """

    model_config = ConfigDict(extra="forbid")

    issuer: str = ""
    lines: list[LineItemModel] = []
