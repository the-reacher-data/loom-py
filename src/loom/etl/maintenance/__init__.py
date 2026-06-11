"""Delta table maintenance — backend-agnostic vacuum, compact, and Z-Order.

Public API
----------

Authoring::

    from loom.etl.maintenance import MaintenanceStep, MaintainTable, MaintainSchema

Running::

    from loom.etl.maintenance import MaintenanceRunner, MaintenanceReport

Result types::

    from loom.etl.maintenance import (
        VacuumResult,
        OptimizeResult,
        TableMaintenanceResult,
        MaintenanceError,
    )

Backend protocol (for custom implementations)::

    from loom.etl.maintenance import DeltaTableMaintainer

Example::

    class NightlyMaintenance(MaintenanceStep[None]):
        operations = [
            MaintainSchema("raw").vacuum(retention_hours=168, dry_run=False).compact(),
            MaintainTable("staging.orders")
                .vacuum(dry_run=False)
                .z_order_by(["run_date", "_id"]),
        ]

    runner = MaintenanceRunner.from_config(config)
    report = runner.run(NightlyMaintenance)
    report.raise_if_errors()
"""

from loom.etl.maintenance._builder import MaintainSchema, MaintainTable
from loom.etl.maintenance._protocol import (
    DeltaTableMaintainer,
    OperationDeclaration,
    OpSpec,
    OptimizeResult,
    TableMaintenanceResult,
    VacuumResult,
)
from loom.etl.maintenance._runner import MaintenanceError, MaintenanceReport, MaintenanceRunner
from loom.etl.maintenance._step import MaintenanceStep

__all__ = [
    # authoring
    "MaintenanceStep",
    "MaintainTable",
    "MaintainSchema",
    # runner
    "MaintenanceRunner",
    "MaintenanceReport",
    "MaintenanceError",
    # result types
    "VacuumResult",
    "OptimizeResult",
    "TableMaintenanceResult",
    # protocols
    "DeltaTableMaintainer",
    "OperationDeclaration",
    "OpSpec",
]
