"""Unit tests for SsmResolver.

boto3 is mocked at the call level (boto3.client / client.get_parameter).
The library is assumed to be installed in dev — no library-presence tests.
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.resolver import ConfigResolver


def _import_ssm_resolver():
    """Import SsmResolver, raising ImportError if the module is missing."""
    if "loom.core.config.ssm" in sys.modules:
        mod = sys.modules["loom.core.config.ssm"]
    else:
        mod = importlib.import_module("loom.core.config.ssm")
    return mod.SsmResolver


# ===========================================================================
# Group 1: SsmResolver.name
# ===========================================================================


def test_name_returns_ssm() -> None:
    SsmResolver = _import_ssm_resolver()
    assert SsmResolver().name == "ssm"


# ===========================================================================
# Group 2: SsmResolver satisfies the ConfigResolver protocol
# ===========================================================================


def test_implements_config_resolver_protocol() -> None:
    SsmResolver = _import_ssm_resolver()
    assert isinstance(SsmResolver(), ConfigResolver)


# ===========================================================================
# Group 3: resolve — without env-var expansion
# ===========================================================================


def test_resolve_calls_ssm_with_literal_key() -> None:
    SsmResolver = _import_ssm_resolver()

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "ignored"}}

    with patch("boto3.client", return_value=mock_client):
        resolver = SsmResolver()
        resolver.resolve("/prod/token")

    mock_client.get_parameter.assert_called_once_with(Name="/prod/token", WithDecryption=True)


def test_resolve_returns_parameter_value() -> None:
    SsmResolver = _import_ssm_resolver()

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "secret123"}}

    with patch("boto3.client", return_value=mock_client):
        resolver = SsmResolver()
        result = resolver.resolve("/prod/token")

    assert result == "secret123"


def test_resolve_with_decryption_false() -> None:
    SsmResolver = _import_ssm_resolver()

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "plain"}}

    with patch("boto3.client", return_value=mock_client):
        resolver = SsmResolver(with_decryption=False)
        resolver.resolve("/prod/token")

    mock_client.get_parameter.assert_called_once_with(Name="/prod/token", WithDecryption=False)


# ===========================================================================
# Group 4: resolve — env-var expansion in the path
# ===========================================================================


def test_resolve_expands_env_var_in_key(monkeypatch: pytest.MonkeyPatch) -> None:
    SsmResolver = _import_ssm_resolver()
    monkeypatch.setenv("ENVIRONMENT", "prod")

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "tok"}}

    with patch("boto3.client", return_value=mock_client):
        resolver = SsmResolver()
        resolver.resolve("/myapp/{ENVIRONMENT}/token")

    mock_client.get_parameter.assert_called_once_with(Name="/myapp/prod/token", WithDecryption=True)


def test_resolve_expands_multiple_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    SsmResolver = _import_ssm_resolver()
    monkeypatch.setenv("REGION", "eu-west-1")
    monkeypatch.setenv("ENV", "prod")

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "val"}}

    with patch("boto3.client", return_value=mock_client):
        resolver = SsmResolver()
        resolver.resolve("/{REGION}/{ENV}/key")

    mock_client.get_parameter.assert_called_once_with(
        Name="/eu-west-1/prod/key", WithDecryption=True
    )


def test_resolve_raises_config_error_for_missing_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    SsmResolver = _import_ssm_resolver()
    monkeypatch.delenv("MISSING_VAR", raising=False)

    with patch("boto3.client"):
        resolver = SsmResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/path/{MISSING_VAR}/token")


def test_resolve_no_braces_passes_key_unchanged() -> None:
    SsmResolver = _import_ssm_resolver()

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "v"}}

    with patch("boto3.client", return_value=mock_client):
        resolver = SsmResolver()
        resolver.resolve("/prod/plain/key")

    mock_client.get_parameter.assert_called_once_with(Name="/prod/plain/key", WithDecryption=True)


# ===========================================================================
# Group 5: boto3 client created lazily (once per instance)
# ===========================================================================


def test_client_created_once_across_multiple_resolves() -> None:
    SsmResolver = _import_ssm_resolver()

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "v"}}

    with patch("boto3.client", return_value=mock_client) as mock_factory:
        resolver = SsmResolver()
        resolver.resolve("/key/one")
        resolver.resolve("/key/two")

    assert mock_factory.call_count == 1


def test_region_passed_to_boto3_client() -> None:
    SsmResolver = _import_ssm_resolver()

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "v"}}

    with patch("boto3.client", return_value=mock_client) as mock_factory:
        resolver = SsmResolver("eu-west-1")
        resolver.resolve("/some/key")

    mock_factory.assert_called_once_with("ssm", region_name="eu-west-1")


def test_region_none_passes_none_to_boto3_client() -> None:
    SsmResolver = _import_ssm_resolver()

    mock_client = MagicMock()
    mock_client.get_parameter.return_value = {"Parameter": {"Value": "v"}}

    with patch("boto3.client", return_value=mock_client) as mock_factory:
        resolver = SsmResolver()
        resolver.resolve("/some/key")

    mock_factory.assert_called_once_with("ssm", region_name=None)


# ===========================================================================
# Group 6: SSM errors surface as ConfigError
# ===========================================================================


def test_raises_config_error_on_ssm_exception() -> None:
    SsmResolver = _import_ssm_resolver()

    mock_client = MagicMock()
    mock_client.get_parameter.side_effect = Exception("ParameterNotFound")

    with patch("boto3.client", return_value=mock_client):
        resolver = SsmResolver()
        with pytest.raises(ConfigError):
            resolver.resolve("/missing/param")
