"""Configuration errors."""


class ConfigError(Exception):
    """Raised when configuration loading or validation fails.

    Args:
        message: Human-readable description of the failure.

    Example::

        raise ConfigError("Missing required field: db_url")
    """
