"""AWS Secrets Manager resolver for loom configuration.

Resolves ``${secrets:/path/to/secret}`` placeholders in OmegaConf configs
by fetching values from AWS Secrets Manager at parse time.

Example::

    from loom.core.config import load_config
    from loom.core.config.secrets import SecretsManagerResolver

    cfg = load_config("config/prod.yaml", resolvers=[SecretsManagerResolver("eu-west-1")])
"""

from __future__ import annotations

import logging
from typing import Any

try:
    import boto3 as _boto3_module  # type: ignore[import-untyped]
except ImportError:
    _boto3_module = None

from loom.core.config._resolver_utils import (
    _expand_env_vars,
    _navigate_json,
    _split_resolver_key,
)
from loom.core.config.errors import ConfigError

logger = logging.getLogger(__name__)


def _fetch_secret(client: Any, name: str) -> str:
    """Fetch a string secret value from AWS Secrets Manager.

    Args:
        client: Boto3 secretsmanager client.
        name: Secret name or short ARN. ARN-style names are supported for
            plain fetches but not for dot-notation JSON navigation.

    Returns:
        The secret value as a string.

    Raises:
        ConfigError: When the secret is binary, or on any API error.
    """
    try:
        result = client.get_secret_value(SecretId=name)
    except Exception as exc:
        raise ConfigError(f"Failed to fetch Secrets Manager secret {name!r}: {exc}") from exc
    if "SecretString" not in result:
        raise ConfigError(
            f"Secrets Manager secret {name!r} is binary — only string secrets are supported"
        )
    return str(result["SecretString"])


class SecretsManagerResolver:
    """Resolves AWS Secrets Manager paths for use with :func:`~loom.core.config.load_config`.

    Fetches secret values from AWS Secrets Manager. The boto3 client is
    created lazily on first use and reused across calls.

    Env-var tokens in the form ``%VAR_NAME%`` (uppercase letters, digits,
    and underscores only) are expanded from ``os.environ`` before the
    request is made.

    Args:
        region: AWS region name. Passed directly to ``boto3.client``.
            Defaults to ``None``, which lets boto3 use its own resolution
            chain (env vars, instance metadata, etc.).

    Example::

        resolver = SecretsManagerResolver("eu-west-1")
        value = resolver.resolve("/myapp/%ENV%/db_password")
    """

    def __init__(self, region: str | None = None) -> None:
        self._region = region
        self._client: Any = None

    @property
    def name(self) -> str:
        """OmegaConf resolver prefix.

        Returns:
            The string ``"secrets"``.
        """
        return "secrets"

    def _get_client(self) -> Any:
        """Return the boto3 secretsmanager client, creating it on first call.

        Returns:
            A boto3 secretsmanager client instance.

        Raises:
            ConfigError: When boto3 is not installed.
        """
        if self._client is None:
            if _boto3_module is None:
                raise ConfigError(
                    "boto3 is required for SecretsManagerResolver. "
                    "Install it with: pip install loom[config-ssm]"
                )
            self._client = _boto3_module.client("secretsmanager", region_name=self._region)
        return self._client

    def resolve(self, key: str) -> object:
        """Resolve an AWS Secrets Manager path to its stored value.

        Expands ``%VAR_NAME%`` tokens in *key* from the environment, then
        fetches the secret from AWS Secrets Manager.

        Args:
            key: Secret path, optionally containing ``%VAR_NAME%``
                placeholders that are replaced with environment variable values.
                Supports dot-notation for JSON key navigation: ``/path/secret.key``
                fetches ``/path/secret`` and returns ``secret["key"]``.

        Returns:
            Resolved value. A plain string for secrets without dot-notation;
            a structured value (string, int, dict, etc.) when dot-notation
            navigates into a JSON secret.

        Raises:
            ConfigError: When *key* is empty, an env-var placeholder is
                missing, boto3 is not installed, the secret is binary,
                or the API call fails.
        """
        if not key:
            raise ConfigError("Secrets Manager key must not be empty")
        expanded = _expand_env_vars(key)
        path, json_keys = _split_resolver_key(expanded)
        logger.info("secrets_resolver: fetching %s", path)
        raw = _fetch_secret(self._get_client(), path)
        if not json_keys:
            return raw
        return _navigate_json(raw, json_keys, path)


__all__ = ["SecretsManagerResolver"]
