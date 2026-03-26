"""ETL compilation error types.

``ETLErrorCode``
    Machine-readable enum of every validation failure the compiler can raise.

``ETLCompilationError``
    Structured exception carrying ``code``, ``component``, ``field``, and a
    human-readable ``message``.  Each error code has a dedicated class-method
    factory so call-sites stay intention-revealing and free of string formatting.
"""

from __future__ import annotations

from enum import StrEnum


class ETLErrorCode(StrEnum):
    """Enumeration of all compiler validation failure codes."""

    INVALID_PARAMS_TYPE = "INVALID_PARAMS_TYPE"
    INVALID_PARAMS_NAME = "INVALID_PARAMS_NAME"
    MISSING_SOURCE_PARAMS = "MISSING_SOURCE_PARAMS"
    EXTRA_SOURCE_PARAMS = "EXTRA_SOURCE_PARAMS"
    MISSING_PARAMS_FIELDS = "MISSING_PARAMS_FIELDS"
    UNKNOWN_SOURCE_TABLE = "UNKNOWN_SOURCE_TABLE"
    UNKNOWN_TARGET_TABLE = "UNKNOWN_TARGET_TABLE"
    TEMP_NOT_PRODUCED = "TEMP_NOT_PRODUCED"
    UPSERT_NO_KEYS = "UPSERT_NO_KEYS"
    UPSERT_EXCLUDE_INCLUDE_CONFLICT = "UPSERT_EXCLUDE_INCLUDE_CONFLICT"
    UPSERT_KEY_IN_EXCLUDE = "UPSERT_KEY_IN_EXCLUDE"
    MISSING_GENERIC_PARAM = "MISSING_GENERIC_PARAM"
    MISSING_TARGET = "MISSING_TARGET"
    INVALID_TARGET_TYPE = "INVALID_TARGET_TYPE"
    INVALID_SOURCES_TYPE = "INVALID_SOURCES_TYPE"
    INVALID_PROCESS_ITEM = "INVALID_PROCESS_ITEM"
    INVALID_STEP_ITEM = "INVALID_STEP_ITEM"


class ETLCompilationError(Exception):
    """Raised when an ETL class fails structural validation.

    Attributes:
        code:      Machine-readable :class:`ETLErrorCode`.
        component: Qualified name of the step or component that failed.
        field:     Optional name of the specific field or parameter involved.

    Use the factory class-methods to construct instances — they own all message
    formatting so call-sites stay readable.

    Example::

        raise ETLCompilationError.upsert_no_keys(MyStep)
    """

    def __init__(
        self,
        code: ETLErrorCode,
        component: str,
        message: str,
        field: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.component = component
        self.field = field

    # ------------------------------------------------------------------
    # Factory methods — one per ETLErrorCode
    # ------------------------------------------------------------------

    @classmethod
    def invalid_params_type(cls, step: type, expected: type) -> ETLCompilationError:
        """First execute() parameter has wrong type annotation."""
        return cls(
            code=ETLErrorCode.INVALID_PARAMS_TYPE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: first parameter must be "
                f"'params: {expected.__name__}'"
            ),
            field="params",
        )

    @classmethod
    def invalid_params_name(cls, step: type, got: str) -> ETLCompilationError:
        """First execute() parameter is not named 'params'."""
        return cls(
            code=ETLErrorCode.INVALID_PARAMS_NAME,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: first parameter must be named 'params', got '{got}'"
            ),
            field=got,
        )

    @classmethod
    def missing_source_params(cls, step: type, missing: frozenset[str]) -> ETLCompilationError:
        """Sources declared in sources but absent as kw-only execute() parameters."""
        return cls(
            code=ETLErrorCode.MISSING_SOURCE_PARAMS,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: source(s) {sorted(missing)} "
                "declared in sources but missing as keyword-only parameter(s) after '*'"
            ),
        )

    @classmethod
    def extra_source_params(cls, step: type, extra: frozenset[str]) -> ETLCompilationError:
        """Kw-only execute() parameters not declared in sources."""
        return cls(
            code=ETLErrorCode.EXTRA_SOURCE_PARAMS,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}.execute: parameter(s) {sorted(extra)} "
                "declared after '*' but not found in sources"
            ),
        )

    @classmethod
    def missing_params_fields(
        cls, component: type, missing: frozenset[str], context: type
    ) -> ETLCompilationError:
        """Component requires params fields that the context params type lacks."""
        return cls(
            code=ETLErrorCode.MISSING_PARAMS_FIELDS,
            component=component.__qualname__,
            message=(
                f"{component.__qualname__} requires params fields "
                f"{sorted(missing)} not present in {context.__name__}"
            ),
        )

    @classmethod
    def unknown_source_table(cls, step: type, alias: str, table_ref: str) -> ETLCompilationError:
        """Source binding references a table not in the catalog."""
        return cls(
            code=ETLErrorCode.UNKNOWN_SOURCE_TABLE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: source '{alias}' references unknown table '{table_ref}'"
            ),
            field=alias,
        )

    @classmethod
    def unknown_target_table(cls, step: type, table_ref: str) -> ETLCompilationError:
        """Target references a table not in the catalog."""
        return cls(
            code=ETLErrorCode.UNKNOWN_TARGET_TABLE,
            component=step.__qualname__,
            message=(f"{step.__qualname__}: target references unknown table '{table_ref}'"),
        )

    @classmethod
    def temp_not_produced(cls, step: type, alias: str, temp_name: str) -> ETLCompilationError:
        """FromTemp references a temp name with no prior IntoTemp."""
        return cls(
            code=ETLErrorCode.TEMP_NOT_PRODUCED,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: source '{alias}' "
                f"references FromTemp({temp_name!r}) but no prior "
                f"IntoTemp({temp_name!r}) found in the plan"
            ),
            field=alias,
        )

    @classmethod
    def upsert_no_keys(cls, step: type) -> ETLCompilationError:
        """upsert() declared with no key columns."""
        return cls(
            code=ETLErrorCode.UPSERT_NO_KEYS,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: upsert() requires at least one key column. "
                'Pass keys=("col",) to identify rows uniquely.'
            ),
        )

    @classmethod
    def upsert_exclude_include_conflict(cls, step: type) -> ETLCompilationError:
        """upsert() has both exclude= and include= set."""
        return cls(
            code=ETLErrorCode.UPSERT_EXCLUDE_INCLUDE_CONFLICT,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: upsert() exclude= and include= are mutually exclusive. "
                "Use one or the other, not both."
            ),
        )

    @classmethod
    def upsert_key_in_exclude(cls, step: type, overlap: frozenset[str]) -> ETLCompilationError:
        """upsert() exclude= contains columns that are also upsert keys."""
        return cls(
            code=ETLErrorCode.UPSERT_KEY_IN_EXCLUDE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: upsert() exclude={sorted(overlap)} overlaps with "
                "upsert keys — key columns are always excluded from UPDATE SET."
            ),
        )

    @classmethod
    def missing_generic_param(cls, cls_type: type, kind: str) -> ETLCompilationError:
        """ETLStep/ETLProcess/ETLPipeline used without a generic parameter."""
        return cls(
            code=ETLErrorCode.MISSING_GENERIC_PARAM,
            component=cls_type.__qualname__,
            message=(
                f"{cls_type.__qualname__}: missing generic parameter — "
                f"use {kind}[YourParams] not bare {kind}"
            ),
        )

    @classmethod
    def missing_target(cls, step: type) -> ETLCompilationError:
        """'target' is required but not declared on the step."""
        return cls(
            code=ETLErrorCode.MISSING_TARGET,
            component=step.__qualname__,
            message=f"{step.__qualname__}: 'target' is required but not declared",
        )

    @classmethod
    def invalid_target_type(cls, step: type) -> ETLCompilationError:
        """'target' is not an IntoTable/IntoFile/IntoTemp instance."""
        return cls(
            code=ETLErrorCode.INVALID_TARGET_TYPE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: 'target' must be "
                "IntoTable(...), IntoFile(...), or IntoTemp(...)"
            ),
        )

    @classmethod
    def invalid_sources_type(cls, step: type) -> ETLCompilationError:
        """'sources' is not a Sources(...) or SourceSet instance."""
        return cls(
            code=ETLErrorCode.INVALID_SOURCES_TYPE,
            component=step.__qualname__,
            message=(
                f"{step.__qualname__}: 'sources' must be Sources(...) or a SourceSet instance"
            ),
        )

    @classmethod
    def invalid_process_item(cls, pipeline: type, item: object) -> ETLCompilationError:
        """An item in pipeline.processes is not an ETLProcess subclass."""
        return cls(
            code=ETLErrorCode.INVALID_PROCESS_ITEM,
            component=pipeline.__qualname__,
            message=(
                f"{pipeline.__qualname__}.processes: "
                f"expected ETLProcess subclass or list thereof, got {item!r}"
            ),
        )

    @classmethod
    def invalid_step_item(cls, process: type, item: object) -> ETLCompilationError:
        """An item in process.steps is not an ETLStep subclass."""
        return cls(
            code=ETLErrorCode.INVALID_STEP_ITEM,
            component=process.__qualname__,
            message=(
                f"{process.__qualname__}.steps: "
                f"expected ETLStep subclass or list thereof, got {item!r}"
            ),
        )
