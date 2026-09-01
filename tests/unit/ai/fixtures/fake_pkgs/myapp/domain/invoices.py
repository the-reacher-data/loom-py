"""Invoice domain types referenced by ``output_type_ref.agent.yaml``."""

from __future__ import annotations

import msgspec


class InvoiceSummary(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Structured invoice summary an agent returns."""

    issuer: str = ""
    total: float = 0.0
    due_date: str | None = None
