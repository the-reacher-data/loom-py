"""Multi-role HTTP transport for the ClickHouse driver boundary (spec §2/§3).

ClickHouse only accepts several roles as REPEATED HTTP parameters
(``role=a&role=b``); a comma-joined value is parsed as a single role name and
rejected with code 511. ``clickhouse-connect`` 0.15.1 builds the request URL
with ``urlencode(final_params)`` — without ``doseq=True`` — so a sequence value
would travel as its ``repr``. ``loom.core.sql.clickhouse._client`` fixes that at
the driver boundary; these tests pin the patch point so a future driver version
can never degrade the workaround silently.
"""

from __future__ import annotations

import inspect
from urllib.parse import urlencode

import pytest
from clickhouse_connect.driver import httpclient

from loom.core.config.errors import ConfigError
from loom.core.sql.clickhouse import ClickHouseConnectionRegistry
from loom.core.sql.clickhouse._client import supports_repeated_query_params
from tests.unit.core.sql._fakes import (
    RecordingClientFactory,
    make_connection_config,
    make_sql_config,
)


def test_the_patched_driver_symbol_still_exists() -> None:
    """Loud failure if the driver stops exposing the encoder we rebind."""
    assert callable(getattr(httpclient, "urlencode", None))


def test_the_driver_still_builds_its_url_with_the_patched_encoder() -> None:
    """Loud failure if the driver stops routing URL building through ``urlencode``."""
    source = inspect.getsource(httpclient.HttpClient._raw_request)
    assert "urlencode(final_params)" in source


def test_sequence_settings_become_repeated_http_parameters() -> None:
    """A tuple of roles is what ClickHouse needs: one parameter per role."""
    assert httpclient.urlencode({"role": ("role_a", "role_b")}) == "role=role_a&role=role_b"


def test_scalar_settings_encode_exactly_like_the_stock_encoder() -> None:
    """The workaround is backward compatible: scalars are untouched."""
    params = {"role": "role_a", "limit": 4, "readonly": 1, "query": "SELECT 1", "flag": True}
    assert httpclient.urlencode(params) == urlencode(params)


def test_capability_probe_reports_repeated_parameter_support() -> None:
    """The probe answers on the encoder the driver will actually use."""
    assert supports_repeated_query_params() is True


async def test_startup_fails_closed_when_the_transport_cannot_repeat_parameters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A multi-role allowlist without repeated-parameter support aborts startup."""
    monkeypatch.setattr(httpclient, "urlencode", urlencode)
    config = make_sql_config(
        analytics=make_connection_config(allowed_roles=("role_a", "role_b"), default_role="role_a")
    )
    registry = ClickHouseConnectionRegistry(config=config, client_factory=RecordingClientFactory())
    with pytest.raises(ConfigError, match="role"):
        async with registry:
            pass


async def test_single_role_connections_do_not_depend_on_the_workaround(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One role travels as a plain scalar, so single-role setups never need the shim."""
    monkeypatch.setattr(httpclient, "urlencode", urlencode)
    config = make_sql_config(
        analytics=make_connection_config(allowed_roles=("role_a",), default_role="role_a")
    )
    async with ClickHouseConnectionRegistry(
        config=config, client_factory=RecordingClientFactory()
    ) as registry:
        assert registry.executor("analytics") is not None
