"""AWS SSM Parameter Store resolver for loom configuration.

Resolves ``${ssm:/path/to/parameter}`` placeholders in OmegaConf configs
by fetching values from AWS Systems Manager Parameter Store at parse time.

Example::

    from loom.core.config import load_config
    from loom.core.config.ssm import SsmResolver

    cfg = load_config("config/prod.yaml", resolvers=[SsmResolver("eu-west-1")])
"""

from __future__ import annotations

import os
import re
from typing import Any

try:
    import boto3 as _boto3_module  # type: ignore[import-untyped]
except ImportError:
    _boto3_module = None

from loom.core.config.errors import ConfigError

_ENV_VAR_PATTERN = re.compile(r"\{([A-Z][A-Z0-9_]*)\}")


def _expand_env_vars(key: str) -> str:
    """Expand ``{VAR_NAME}`` placeholders in *key* using environment variables.

    Only uppercase identifiers matching ``[A-Z][A-Z0-9_]*`` are expanded.

    Args:
        key: SSM parameter path, possibly containing ``{VAR_NAME}`` tokens.

    Returns:
        Path with all tokens replaced by their environment variable values.

    Raises:
        ConfigError: When a referenced variable is absent from the environment.
    """
    matches = _ENV_VAR_PATTERN.findall(key)
    if not matches:
        return key

    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            raise ConfigError(
                f"Env var {var_name!r} not found in environment (referenced in SSM path {key!r})"
            )
        return value

    return _ENV_VAR_PATTERN.sub(_replace, key)


def _fetch_parameter(client: Any, name: str, with_decryption: bool) -> str:
    """Fetch a single parameter value from AWS SSM.

    Args:
        client: Boto3 SSM client.
        name: Fully-qualified SSM parameter name.
        with_decryption: Whether to decrypt SecureString parameters.

    Returns:
        The parameter value as a string.

    Raises:
        ConfigError: On any SSM API error.
    """
    try:
        result = client.get_parameter(Name=name, WithDecryption=with_decryption)
    except Exception as exc:
        raise ConfigError(f"Failed to fetch SSM parameter {name!r}: {exc}") from exc
    return str(result["Parameter"]["Value"])


class SsmResolver:
    """Resolves SSM Parameter Store paths for use with :func:`~loom.core.config.load_config`.

    Fetches parameter values from AWS Systems Manager Parameter Store.
    The boto3 client is created lazily on first use and reused across calls.

    Env-var tokens in the form ``{VAR_NAME}`` (uppercase letters, digits,
    and underscores only) are expanded from ``os.environ`` before the
    SSM request is made.

    Args:
        region: AWS region name. Passed directly to ``boto3.client``.
            Defaults to ``None``, which lets boto3 use its own resolution
            chain (env vars, instance metadata, etc.).
        with_decryption: Whether to decrypt SecureString parameters.
            Defaults to ``True``.

    Example::

        resolver = SsmResolver("eu-west-1")
        value = resolver.resolve("/myapp/{ENV}/db_password")
    """

    def __init__(
        self,
        region: str | None = None,
        *,
        with_decryption: bool = True,
    ) -> None:
        self._region = region
        self._with_decryption = with_decryption
        self._client: Any = None

    @property
    def name(self) -> str:
        """Resolver name used as the OmegaConf placeholder prefix.

        Returns:
            The string ``"ssm"``.
        """
        return "ssm"

    def _get_client(self) -> Any:
        """Return the boto3 SSM client, creating it on first call.

        Returns:
            A boto3 SSM client instance.

        Raises:
            ConfigError: When boto3 is not installed.
        """
        if self._client is None:
            if _boto3_module is None:
                raise ConfigError(
                    "boto3 is required for SsmResolver."
                    " Install it with: pip install loom[config-ssm]"
                )
            self._client = _boto3_module.client("ssm", region_name=self._region)
        return self._client

    def resolve(self, key: str) -> str:
        """Resolve an SSM parameter path to its stored value.

        Expands ``{VAR_NAME}`` tokens in *key* from the environment, then
        fetches the parameter from AWS SSM Parameter Store.

        Args:
            key: SSM parameter path, optionally containing ``{VAR_NAME}``
                placeholders that are replaced with environment variable values.

        Returns:
            The parameter value as a string.

        Raises:
            ConfigError: When an env-var placeholder is missing, boto3 is not
                installed, or the SSM API call fails.
        """
        expanded = _expand_env_vars(key)
        client = self._get_client()
        return _fetch_parameter(client, expanded, self._with_decryption)
