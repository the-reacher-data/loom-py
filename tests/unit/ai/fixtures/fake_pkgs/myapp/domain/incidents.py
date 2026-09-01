"""Incident domain types referenced by ``full.agent.yaml``."""

from __future__ import annotations

import msgspec


class IncidentReport(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Structured incident report an agent returns."""

    summary: str = ""
    next_step: str = ""
