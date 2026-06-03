"""Unit tests for SsmResolver.

boto3 is mocked at the call level (boto3.client / client.get_parameter).
The library is assumed to be installed in dev — no library-presence tests.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.resolver import ConfigResolver
from loom.core.config.ssm import SsmResolver


@pytest.fixture()
def mock_client() -> MagicMock:
    client = MagicMock()
    client.get_parameter.return_value = {"Parameter": {"Value": "secret123"}}
    return client


class TestSsmResolverIdentity:
    def test_name_returns_ssm(self) -> None:
        assert SsmResolver().name == "ssm"

    def test_implements_config_resolver_protocol(self) -> None:
        assert isinstance(SsmResolver(), ConfigResolver)


class TestSsmResolverResolve:
    def test_returns_parameter_value(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client):
            result = SsmResolver().resolve("/prod/token")
        assert result == "secret123"

    def test_calls_ssm_with_literal_key(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client):
            SsmResolver().resolve("/prod/token")
        mock_client.get_parameter.assert_called_once_with(Name="/prod/token", WithDecryption=True)

    def test_with_decryption_false(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client):
            SsmResolver(with_decryption=False).resolve("/prod/token")
        mock_client.get_parameter.assert_called_once_with(Name="/prod/token", WithDecryption=False)

    def test_no_braces_passes_key_unchanged(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client):
            SsmResolver().resolve("/prod/plain/key")
        mock_client.get_parameter.assert_called_once_with(
            Name="/prod/plain/key", WithDecryption=True
        )


class TestSsmResolverEnvVarExpansion:
    def test_expands_single_env_var(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENVIRONMENT", "prod")
        with patch("boto3.client", return_value=mock_client):
            SsmResolver().resolve("/myapp/%ENVIRONMENT%/token")
        mock_client.get_parameter.assert_called_once_with(
            Name="/myapp/prod/token", WithDecryption=True
        )

    def test_expands_multiple_env_vars(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("REGION", "eu-west-1")
        monkeypatch.setenv("ENV", "prod")
        with patch("boto3.client", return_value=mock_client):
            SsmResolver().resolve("/%REGION%/%ENV%/key")
        mock_client.get_parameter.assert_called_once_with(
            Name="/eu-west-1/prod/key", WithDecryption=True
        )

    def test_raises_on_missing_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with patch("boto3.client"), pytest.raises(ConfigError):
            SsmResolver().resolve("/path/%MISSING_VAR%/token")

    def test_percent_syntax_parses_from_omegaconf_yaml(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Regression: {VAR} syntax broke OmegaConf ANTLR grammar; %VAR% must not."""
        from omegaconf import OmegaConf

        monkeypatch.setenv("ENV", "prod")
        resolver = SsmResolver()
        OmegaConf.register_new_resolver("ssm", resolver.resolve, replace=True)
        with patch("boto3.client", return_value=mock_client):
            cfg = OmegaConf.create({"token": "${ssm:/app/%ENV%/token}"})
            _ = cfg.token
        mock_client.get_parameter.assert_called_once_with(
            Name="/app/prod/token", WithDecryption=True
        )


class TestSsmResolverClientLifecycle:
    def test_client_created_once_across_multiple_resolves(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client) as mock_factory:
            resolver = SsmResolver()
            resolver.resolve("/key/one")
            resolver.resolve("/key/two")
        assert mock_factory.call_count == 1

    def test_region_forwarded_to_boto3(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client) as mock_factory:
            SsmResolver("eu-west-1").resolve("/some/key")
        mock_factory.assert_called_once_with("ssm", region_name="eu-west-1")

    def test_region_none_by_default(self, mock_client: MagicMock) -> None:
        with patch("boto3.client", return_value=mock_client) as mock_factory:
            SsmResolver().resolve("/some/key")
        mock_factory.assert_called_once_with("ssm", region_name=None)


class TestSsmResolverErrors:
    def test_raises_config_error_on_ssm_exception(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.side_effect = Exception("ParameterNotFound")
        with patch("boto3.client", return_value=mock_client), pytest.raises(ConfigError):
            SsmResolver().resolve("/missing/param")


class TestSsmResolverDotNotation:
    def test_plain_path_returns_string(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": "secret123"}}
        with patch("boto3.client", return_value=mock_client):
            result = SsmResolver().resolve("/prod/token")
        assert result == "secret123"

    def test_single_key_navigation(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {
            "Parameter": {"Value": '{"host": "mydb.internal", "port": 5432}'}
        }
        with patch("boto3.client", return_value=mock_client):
            result = SsmResolver().resolve("/prod/db_config.host")
        assert result == "mydb.internal"
        mock_client.get_parameter.assert_called_once_with(
            Name="/prod/db_config", WithDecryption=True
        )

    def test_nested_key_navigation(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {
            "Parameter": {"Value": '{"connection": {"host": "db.internal"}}'}
        }
        with patch("boto3.client", return_value=mock_client):
            result = SsmResolver().resolve("/prod/db.connection.host")
        assert result == "db.internal"

    def test_raises_config_error_on_invalid_json(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": "not-json-at-all"}}
        with patch("boto3.client", return_value=mock_client), pytest.raises(ConfigError):
            SsmResolver().resolve("/prod/db.host")

    def test_raises_config_error_on_missing_key(self, mock_client: MagicMock) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "db"}'}}
        with patch("boto3.client", return_value=mock_client), pytest.raises(ConfigError):
            SsmResolver().resolve("/prod/db.missing_key")

    def test_env_var_expansion_then_dot_navigation(
        self, mock_client: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("ENV", "prod")
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "prod-db"}'}}
        with patch("boto3.client", return_value=mock_client):
            result = SsmResolver().resolve("/myapp/%ENV%/db_config.host")
        assert result == "prod-db"
        mock_client.get_parameter.assert_called_once_with(
            Name="/myapp/prod/db_config", WithDecryption=True
        )


class TestSsmResolverLogging:
    def test_info_log_emitted_with_ssm_path(
        self, mock_client: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_client.get_parameter.return_value = {"Parameter": {"Value": '{"host": "db.internal"}'}}
        with (
            caplog.at_level(logging.INFO, logger="loom.core.config.ssm"),
            patch("boto3.client", return_value=mock_client),
        ):
            SsmResolver().resolve("/prod/db_config.host")
        messages = [r.message for r in caplog.records]
        assert any("/prod/db_config" in msg for msg in messages), (
            f"Expected a log record containing '/prod/db_config', got: {messages}"
        )
        secret_value = "db.internal"
        assert all(secret_value not in msg for msg in messages), (
            f"Log must not contain the secret value '{secret_value}', got: {messages}"
        )

    def test_log_uses_expanded_path_not_original(
        self,
        mock_client: MagicMock,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setenv("STAGE", "prod")
        mock_client.get_parameter.return_value = {"Parameter": {"Value": "some-value"}}
        with (
            caplog.at_level(logging.INFO, logger="loom.core.config.ssm"),
            patch("boto3.client", return_value=mock_client),
        ):
            SsmResolver().resolve("/app/%STAGE%/db")
        messages = [r.message for r in caplog.records]
        assert any("/app/prod/db" in msg for msg in messages), (
            f"Expected a log record containing '/app/prod/db', got: {messages}"
        )
        assert all("%STAGE%" not in msg for msg in messages), (
            f"Log must not contain the unexpanded placeholder '%STAGE%', got: {messages}"
        )
