"""ETL params base type."""

from __future__ import annotations

import msgspec


class ETLParams(msgspec.Struct, frozen=True):
    """Base class for all ETL step parameter types.

    Subclass and declare typed fields.  Instances are immutable and
    serializable via ``msgspec`` — suitable for Celery task payloads and
    ETL run record persistence.

    Date fields (``date``, ``datetime``) are compatible with the
    declarative ``params`` proxy: ``params.run_date.year``,
    ``params.run_date.month``, ``params.run_date.day`` etc. are resolved
    by the compiler against the field type and by the executor at runtime.

    Example::

        from datetime import date

        class DailyOrdersParams(ETLParams):
            run_date: date
            countries: tuple[str, ...]
    """
