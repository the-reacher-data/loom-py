"""Unit tests for SecretsManagerResolver.

boto3 is mocked at the call level (boto3.client / client.get_secret_value).
"""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.resolver import ConfigResolver
from loom.core.config.secrets import SecretsManagerResolver


@pytest.fixture()
def mock_client() -> MagicMock:
    client = MagicMock()
    client.get_secret_value.return_value = {"SecretString": "mysecret"}
    return client


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverIdentity:
    def test_name_returns_secrets(self) -> None:
        assert SecretsManagerResolver().name == "secrets"

    def test_implements_config_resolver_protocol(self) -> None:
        assert isinstance(SecretsManagerResolver(), ConfigResolver)


# ---------------------------------------------------------------------------
# Basic resolve
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverResolve:
    def test_returns_secret_string_value(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client):
            result = SecretsManagerResolver().resolve("/prod/token")
        assert result == "mysecret"

    def test_calls_secretsmanager_with_secret_id(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client):
            SecretsManagerResolver().resolve("/prod/token")
        mock_client.get_secret_value.assert_called_once_with(SecretId="/prod/token")

    def test_raises_config_error_on_binary_secret(self, mock_client: MagicMock) -> None:
        mock_client.get_secret_value.return_value = {"SecretBinary": b"bytes"}
        with patch("boto3.client", return_value=mock_client), pytest.raises(ConfigError):
            SecretsManagerResolver().resolve("/prod/token")


# ---------------------------------------------------------------------------
# Environment variable expansion
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverEnvVarExpansion:
    def test_expands_env_var_in_path(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        with patch("boto3.client", return_value=mock_client):
            SecretsManagerResolver().resolve("/myapp/%ENVIRONMENT%/token")
        mock_client.get_secret_value.assert_called_once_with(SecretId="/myapp/prod/token")

    def test_raises_on_missing_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with patch("boto3.client"), pytest.raises(ConfigError):
            SecretsManagerResolver().resolve("/path/%MISSING_VAR%/token")

    def test_percent_syntax_parses_from_omegaconf_yaml(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: {VAR} syntax broke OmegaConf ANTLR grammar; %VAR% must not."""
        from omegaconf import OmegaConf

        monkeypatch.setenv("ENV", "prod")
        resolver = SecretsManagerResolver()
        OmegaConf.register_new_resolver("secrets", resolver.resolve, replace=True)
        with patch("boto3.client", return_value=mock_client):
            cfg = OmegaConf.create({"val": "${secrets:/app/%ENV%/token}"})
            _ = cfg.val
        mock_client.get_secret_value.assert_called_once_with(SecretId="/app/prod/token")


# ---------------------------------------------------------------------------
# Dot notation (JSON key navigation)
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverDotNotation:
    def test_plain_path_returns_string(self, mock_client: MagicMock) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": "mysecret"}
        with patch("boto3.client", return_value=mock_client):
            result = SecretsManagerResolver().resolve("/prod/token")
        assert result == "mysecret"

    def test_single_key_navigation(self, mock_client: MagicMock) -> None:
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "db.internal"})
        }
        with patch("boto3.client", return_value=mock_client):
            result = SecretsManagerResolver().resolve("/prod/db.host")
        assert result == "db.internal"
        mock_client.get_secret_value.assert_called_once_with(SecretId="/prod/db")

    def test_nested_key_navigation(self, mock_client: MagicMock) -> None:
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"conn": {"host": "internal"}})
        }
        with patch("boto3.client", return_value=mock_client):
            result = SecretsManagerResolver().resolve("/prod/cfg.conn.host")
        assert result == "internal"

    def test_raises_on_invalid_json(self, mock_client: MagicMock) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": "not-json-at-all"}
        with patch("boto3.client", return_value=mock_client), pytest.raises(ConfigError):
            SecretsManagerResolver().resolve("/prod/db.host")

    def test_raises_on_missing_json_key(self, mock_client: MagicMock) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": json.dumps({"host": "db"})}
        with patch("boto3.client", return_value=mock_client), pytest.raises(ConfigError):
            SecretsManagerResolver().resolve("/prod/db.missing_key")

    def test_env_var_expansion_then_dot_navigation(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENV", "prod")
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "prod-db"})
        }
        with patch("boto3.client", return_value=mock_client):
            result = SecretsManagerResolver().resolve("/app/%ENV%/db.host")
        assert result == "prod-db"
        mock_client.get_secret_value.assert_called_once_with(SecretId="/app/prod/db")


# ---------------------------------------------------------------------------
# Client lifecycle
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverClientLifecycle:
    def test_client_created_once_across_multiple_resolves(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client) as mock_factory:
            resolver = SecretsManagerResolver()
            resolver.resolve("/key/one")
            resolver.resolve("/key/two")
        assert mock_factory.call_count == 1

    def test_region_forwarded_to_boto3(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client) as mock_factory:
            SecretsManagerResolver("eu-west-1").resolve("/some/secret")
        mock_factory.assert_called_once_with("secretsmanager", region_name="eu-west-1")

    def test_region_none_by_default(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client) as mock_factory:
            SecretsManagerResolver().resolve("/some/secret")
        mock_factory.assert_called_once_with("secretsmanager", region_name=None)


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverLogging:
    def test_info_log_emitted_with_path(
        self, mock_client: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_client.get_secret_value.return_value = {
            "SecretString": json.dumps({"host": "db.internal"})
        }
        with (
            caplog.at_level(logging.INFO, logger="loom.core.config.secrets"),
            patch("boto3.client", return_value=mock_client),
        ):
            SecretsManagerResolver().resolve("/prod/db.host")
        messages = [r.message for r in caplog.records]
        assert any("/prod/db" in msg for msg in messages), (
            f"Expected a log record containing '/prod/db', got: {messages}"
        )

    def test_log_never_contains_secret_value(
        self, mock_client: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_client.get_secret_value.return_value = {"SecretString": "mysecret"}
        with (
            caplog.at_level(logging.INFO, logger="loom.core.config.secrets"),
            patch("boto3.client", return_value=mock_client),
        ):
            SecretsManagerResolver().resolve("/prod/token")
        messages = [r.message for r in caplog.records]
        assert all("mysecret" not in msg for msg in messages), (
            f"Log must not contain the secret value 'mysecret', got: {messages}"
        )


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestSecretsManagerResolverErrors:
    def test_raises_config_error_on_api_exception(self, mock_client: MagicMock) -> None:
        mock_client.get_secret_value.side_effect = Exception("AccessDenied")
        with patch("boto3.client", return_value=mock_client), pytest.raises(ConfigError):
            SecretsManagerResolver().resolve("/missing/secret")

    def test_raises_config_error_on_empty_key(self) -> None:
        with patch("boto3.client") as mock_factory, pytest.raises(ConfigError):
            SecretsManagerResolver().resolve("")
        mock_factory.assert_not_called()

    def test_raises_config_error_when_boto3_not_installed(self) -> None:
        with (
            patch("loom.core.config.secrets._boto3_module", None),
            pytest.raises(ConfigError, match="boto3 is required"),
        ):
            SecretsManagerResolver().resolve("/some/secret")
