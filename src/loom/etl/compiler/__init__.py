"""ETL compiler public API."""

from loom.etl.compiler._compiler import ETLCompiler
from loom.etl.compiler._errors import ETLCompilationError, ETLErrorCode
from loom.etl.compiler._plan import (
    ParallelProcessGroup,
    ParallelStepGroup,
    PipelinePlan,
    ProcessPlan,
    SourceBinding,
    StepPlan,
    TargetBinding,
)
from loom.etl.compiler._validators import (
    StepCompilationContext,
    validate_execute_signature,
    validate_param_exprs,
    validate_params_compat,
    validate_plan_catalog,
    validate_plan_temps,
    validate_process_catalog,
    validate_step,
    validate_step_catalog,
    validate_upsert_spec,
)

__all__ = [
    "ETLCompiler",
    "ETLCompilationError",
    "ETLErrorCode",
    "StepPlan",
    "ProcessPlan",
    "PipelinePlan",
    "SourceBinding",
    "TargetBinding",
    "ParallelStepGroup",
    "ParallelProcessGroup",
    "StepCompilationContext",
    "validate_execute_signature",
    "validate_param_exprs",
    "validate_params_compat",
    "validate_plan_catalog",
    "validate_plan_temps",
    "validate_process_catalog",
    "validate_step",
    "validate_step_catalog",
    "validate_upsert_spec",
]
