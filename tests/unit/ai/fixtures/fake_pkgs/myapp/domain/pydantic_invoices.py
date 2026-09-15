"""A pydantic ``type_ref`` target, valid for the output phase tests."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, model_validator


class InvoiceSummaryModel(BaseModel):
    """Strict pydantic analogue of ``InvoiceSummary`` (invoices.py): a valid ``type_ref``."""

    model_config = ConfigDict(extra="forbid")

    issuer: str = ""
    total: float = 0.0
    due_date: str | None = None


class StrictInvoiceModel(BaseModel):
    """Strict pydantic ``type_ref`` whose ``model_validator`` enforces a business rule (US5).

    Field coercion alone (unknown keys, wrong scalar types) is pydantic-ai's
    own job; this model exercises the case a `model_validator` is for: a rule
    across fields that no schema alone expresses. ``total`` must be
    non-negative, checked once the fields themselves have already validated.
    """

    model_config = ConfigDict(extra="forbid")

    issuer: str
    total: float

    @model_validator(mode="after")
    def _reject_negative_total(self) -> Self:
        if self.total < 0:
            raise ValueError("total must not be negative")
        return self
