"""A pydantic ``type_ref`` target, valid for the output phase tests."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class InvoiceSummaryModel(BaseModel):
    """Strict pydantic analogue of ``InvoiceSummary`` (invoices.py): a valid ``type_ref``."""

    model_config = ConfigDict(extra="forbid")

    issuer: str = ""
    total: float = 0.0
    due_date: str | None = None
