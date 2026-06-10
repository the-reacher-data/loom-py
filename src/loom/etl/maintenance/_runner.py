"""MaintenanceRunner — wires StorageConfig → DeltaRsMaintainer → MaintenanceStep."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from loom.etl.maintenance._ops import MaintenanceSpec
    from loom.etl.maintenance._protocol import DeltaTableMaintainer
    from loom.etl.maintenance._step import MaintenanceStep
    from loom.etl.storage._config import StorageConfig
    from loom.etl.storage._locator import TableLocator

from loom.etl.maintenance._builder import MaintainSchema
from loom.etl.maintenance._protocol import (
    OptimizeResult,
    TableMaintenanceResult,
    VacuumResult,
)

_log = logging.getLogger(__name__)


class MaintenanceError(Exception):
    """Raised by :meth:`MaintenanceReport.raise_if_errors` when any table failed."""

    def __init__(self, failed: list[TableMaintenanceResult]) -> None:
        self.failed = failed
        refs = [r.table_ref for r in failed]
        super().__init__(f"Maintenance failed for {len(failed)} table(s): {refs}")


@dataclass
class MaintenanceReport:
    """Aggregate result of a maintenance run.

    Args:
        results: Per-table outcomes (one entry per table processed).
    """

    results: list[TableMaintenanceResult] = field(default_factory=list)

    @property
    def has_errors(self) -> bool:
        """``True`` when at least one table reported an error."""
        return any(not r.ok for r in self.results)

    def raise_if_errors(self) -> None:
        """Raise :class:`MaintenanceError` if any table failed.

        Useful as a Prefect flow gate::

            report = runner.run(NightlyMaintenance)
            report.raise_if_errors()
        """
        failed = [r for r in self.results if not r.ok]
        if failed:
            raise MaintenanceError(failed)


class MaintenanceRunner:
    """Execute a :class:`~loom.etl.maintenance.MaintenanceStep` against Delta tables.

    Construct via :meth:`from_config` for the standard delta-rs path::

        runner = MaintenanceRunner.from_config(config)
        report = runner.run(NightlyMaintenance)
        report.raise_if_errors()

    Args:
        maintainer: Backend implementation (default: :class:`DeltaRsMaintainer`).
        locator: Resolves logical table refs to physical ``TableLocation``.
        config: Full storage config (needed by :class:`MaintainSchema` expansion
            and :meth:`run_from_config`).
        missing_table_policy: ``"skip"`` silently omits tables not present in
            *config* (or not yet initialized as Delta tables); ``"error"``
            raises immediately.
    """

    def __init__(
        self,
        maintainer: DeltaTableMaintainer,
        locator: TableLocator,
        config: StorageConfig,
        *,
        missing_table_policy: Literal["skip", "error"] = "skip",
    ) -> None:
        self._maintainer = maintainer
        self._locator = locator
        self._config = config
        self._missing_table_policy: Literal["skip", "error"] = missing_table_policy

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(
        cls,
        config: StorageConfig,
        *,
        missing_table_policy: Literal["skip", "error"] = "skip",
    ) -> MaintenanceRunner:
        """Build a :class:`MaintenanceRunner` from a :class:`StorageConfig`.

        Uses delta-rs (``deltalake`` package) as the only backend.
        Calls :meth:`~loom.etl.storage._config.StorageConfig.to_path_locator`
        which reuses the same ``PrefixLocator`` / ``MappingLocator`` logic as
        the Polars ETL backend — no private helpers crossed.

        Raises:
            ValueError: If *config* has no path routes configured
                (only catalog/UC refs, which delta-rs cannot resolve directly).
        """
        from loom.etl.maintenance.backends._delta_rs import DeltaRsMaintainer

        locator = config.to_path_locator()
        return cls(
            DeltaRsMaintainer(),
            locator,
            config,
            missing_table_policy=missing_table_policy,
        )

    # ------------------------------------------------------------------
    # Run from a MaintenanceStep subclass
    # ------------------------------------------------------------------

    def run(
        self,
        step_cls: type[MaintenanceStep[Any]],
        params: Any = None,  # noqa: ARG002 — reserved for future parameterisation
    ) -> MaintenanceReport:
        """Execute all operations declared on *step_cls*.

        :class:`MaintainSchema` entries are expanded against the config at
        run time; :class:`MaintainTable` entries are compiled directly.

        Args:
            step_cls: A :class:`~loom.etl.maintenance.MaintenanceStep` subclass.
            params: Reserved for future parameterisation (currently unused).

        Returns:
            :class:`MaintenanceReport` with per-table results.
        """
        specs = self._resolve_specs(step_cls, params)
        results = [self._run_one(spec) for spec in specs]
        return MaintenanceReport(results=results)

    # ------------------------------------------------------------------
    # Run from config (no Python class needed)
    # ------------------------------------------------------------------

    def run_from_config(self) -> MaintenanceReport:
        """Execute maintenance using the ``maintenance`` block in StorageConfig.

        Discovers all tables listed under the configured schema prefixes and
        applies the vacuum / compact / z-order settings from
        ``StorageConfig.maintenance``.

        Returns an empty :class:`MaintenanceReport` when no ``maintenance``
        config is defined or no matching tables are found.
        """
        from loom.etl.maintenance._ops import CompactSpec, MaintenanceSpec, VacuumSpec, ZOrderSpec

        mc = self._config.maintenance
        if not mc.schemas and not mc.vacuum and not mc.compact and not mc.z_order_by:
            _log.debug("run_from_config: no maintenance config — skipping")
            return MaintenanceReport()

        vacuum_spec: VacuumSpec | None = None
        if mc.vacuum is not None:
            vacuum_spec = VacuumSpec(
                retention_hours=mc.vacuum.retention_hours,
                dry_run=mc.vacuum.dry_run,
            )

        compact_spec: CompactSpec | None = CompactSpec() if mc.compact else None
        z_order_spec: ZOrderSpec | None = (
            ZOrderSpec(columns=list(mc.z_order_by)) if mc.z_order_by else None
        )

        if compact_spec is not None and z_order_spec is not None:
            raise ValueError(
                "StorageConfig.maintenance: compact and z_order_by are mutually exclusive"
            )

        specs: list[MaintenanceSpec] = []
        prefixes = [f"{s}." for s in mc.schemas] if mc.schemas else [""]
        for route in self._config.tables:
            if any(route.name.startswith(p) for p in prefixes):
                specs.append(
                    MaintenanceSpec(
                        table_ref=route.name,
                        vacuum=vacuum_spec,
                        compact=compact_spec,
                        z_order=z_order_spec,
                    )
                )

        results = [self._run_one(spec) for spec in specs]
        return MaintenanceReport(results=results)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_specs(
        self, step_cls: type[MaintenanceStep[Any]], params: Any
    ) -> list[MaintenanceSpec]:
        specs: list[MaintenanceSpec] = []
        for op in step_cls().operations_for(params):
            if isinstance(op, MaintainSchema):
                specs.extend(op._expand(self._config))
            else:
                specs.append(op._to_spec())
        return specs

    def _run_one(self, spec: MaintenanceSpec) -> TableMaintenanceResult:
        """Execute all ops for one table, catching errors per table."""
        from loom.etl.declarative.expr._refs import TableRef

        try:
            location = self._locator.locate(TableRef(spec.table_ref))
        except Exception as exc:
            if self._missing_table_policy == "skip":
                _log.warning("maintenance skip table=%s reason=%r", spec.table_ref, exc)
                return TableMaintenanceResult(table_ref=spec.table_ref)
            raise

        t0 = time.monotonic()
        vacuum_result: VacuumResult | None = None
        compact_result: OptimizeResult | None = None
        z_order_result: OptimizeResult | None = None
        error: Exception | None = None

        try:
            if spec.vacuum is not None:
                vacuum_result = self._maintainer.vacuum(location.uri, spec.vacuum, location)
            if spec.compact is not None:
                compact_result = self._maintainer.compact(location.uri, spec.compact, location)
            if spec.z_order is not None:
                z_order_result = self._maintainer.z_order(location.uri, spec.z_order, location)
        except Exception as exc:
            error = exc
            _log.error("maintenance failed table=%s error=%r", spec.table_ref, exc)

        return TableMaintenanceResult(
            table_ref=spec.table_ref,
            vacuum=vacuum_result,
            compact=compact_result,
            z_order=z_order_result,
            error=error,
            duration_seconds=time.monotonic() - t0,
        )
