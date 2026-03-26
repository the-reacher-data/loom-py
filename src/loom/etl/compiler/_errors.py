"""ETL compilation error type."""


class ETLCompilationError(Exception):
    """Raised when an ETL class fails structural validation.

    Args:
        message: Human-readable description of the failure.
    """
