"""AWS SSM Parameter Store resolver for loom configuration.

Resolves ``${ssm:/path/to/parameter}`` placeholders in OmegaConf configs
by fetching values from AWS Systems Manager Parameter Store at parse time.

Example::

    from loom.core.config import load_config
    from loom.core.config.ssm import SsmResolver

    cfg = load_config("config/prod.yaml", resolvers=[SsmResolver("eu-west-1")])
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Any

try:
    import boto3 as _boto3_module  # type: ignore[import-untyped]
except ImportError:
    _boto3_module = None

from loom.core.config.errors import ConfigError

logger = logging.getLogger(__name__)

_ENV_VAR_PATTERN = re.compile(r"%([A-Z][A-Z0-9_]*)%")


def _expand_env_vars(key: str) -> str:
    """Expand ``%VAR_NAME%`` placeholders in *key* using environment variables.

    Only uppercase identifiers matching ``[A-Z][A-Z0-9_]*`` are expanded.
    The ``%VAR%`` syntax is used (not ``{VAR}``) because OmegaConf's grammar
    rejects bare ``{`` inside ``${resolver:...}`` interpolations.

    Args:
        key: SSM parameter path, possibly containing ``%VAR_NAME%`` tokens.

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


def _split_ssm_key(key: str) -> tuple[str, list[str]]:
    """Split an SSM key into the parameter path and JSON navigation keys.

    The first dot after the last '/' separator marks the start of a JSON key
    path. SSM paths use '/' as their separator, so a dot is unambiguously a
    JSON navigation token.

    Args:
        key: Expanded SSM key, e.g. ``"/app/prod/db_config.host"``.

    Returns:
        A 2-tuple ``(ssm_path, json_keys)`` where *ssm_path* is the SSM
        parameter path to fetch and *json_keys* is a list of dict keys to
        navigate (empty list when no dot-notation is present).

    Examples:
        >>> _split_ssm_key("/app/prod/token")
        ('/app/prod/token', [])
        >>> _split_ssm_key("/app/prod/db_config.host")
        ('/app/prod/db_config', ['host'])
        >>> _split_ssm_key("/app/prod/db.connection.host")
        ('/app/prod/db', ['connection', 'host'])
    """
    if not key:
        return key, []
    last_slash = key.rfind("/")
    after_last_slash = key[last_slash + 1 :] if last_slash != -1 else key
    dot_pos = after_last_slash.find(".")
    if dot_pos == -1:
        return key, []
    base_end = (last_slash + 1 + dot_pos) if last_slash != -1 else dot_pos
    json_tail = key[base_end + 1 :]
    return key[:base_end], [k for k in json_tail.split(".") if k]


def _navigate_json(raw: str, keys: list[str], ssm_path: str) -> object:
    """Parse *raw* as JSON and navigate to the value at *keys*.

    Args:
        raw: Raw string value from SSM.
        keys: Ordered list of dict keys to traverse.
        ssm_path: Original SSM path, used only in error messages.

    Returns:
        The value at the navigated position. May be any JSON-compatible type.

    Raises:
        ConfigError: When *raw* is not valid JSON, or a key is absent.
    """
    try:
        data: object = json.loads(
            raw
        )  # json.loads returns Any; annotated as object to avoid implicit Any propagation
    except json.JSONDecodeError as exc:
        raise ConfigError(
            f"SSM parameter {ssm_path!r} is not valid JSON (required for key navigation): {exc}"
        ) from exc
    current: object = data
    for k in keys:
        if not isinstance(current, dict) or k not in current:
            raise ConfigError(f"Key {k!r} not found in SSM parameter {ssm_path!r}")
        current = current[k]
    return current


class SsmResolver:
    """Resolves SSM Parameter Store paths for use with :func:`~loom.core.config.load_config`.

    Fetches parameter values from AWS Systems Manager Parameter Store.
    The boto3 client is created lazily on first use and reused across calls.

    Env-var tokens in the form ``%VAR_NAME%`` (uppercase letters, digits,
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
        value = resolver.resolve("/myapp/%ENV%/db_password")
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

    def resolve(self, key: str) -> object:
        """Resolve an SSM parameter path to its stored value.

        Expands ``%VAR_NAME%`` tokens in *key* from the environment, then
        fetches the parameter from AWS SSM Parameter Store.

        Args:
            key: SSM parameter path, optionally containing ``%VAR_NAME%``
                placeholders that are replaced with environment variable values.
                Supports dot-notation for JSON key navigation: ``/path/param.key``
                fetches ``/path/param`` and returns ``param["key"]``.

        Returns:
            Resolved value. A plain string for parameters without dot-notation;
            a structured value (string, int, dict, etc.) when dot-notation
            navigates into a JSON parameter.

        Raises:
            ConfigError: When *key* is empty, an env-var placeholder is missing,
                boto3 is not installed, or the SSM API call fails.
        """
        if not key:
            raise ConfigError("SSM key must not be empty")
        expanded = _expand_env_vars(key)
        ssm_path, json_keys = _split_ssm_key(expanded)
        logger.info("ssm_resolver: fetching %s", ssm_path)
        client = self._get_client()
        raw = _fetch_parameter(client, ssm_path, self._with_decryption)
        if not json_keys:
            return raw
        return _navigate_json(raw, json_keys, ssm_path)
