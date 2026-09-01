"""Engine and provider resolution contract for ``loom.ai.registry`` (T030).

Pins the deployment-resolution failures of the error-codes contract:
``ENGINE_NOT_FOUND``, ``ENGINE_DUPLICATE``, ``ENGINE_API_MISMATCH``,
``PROVIDER_NOT_INSTALLED`` and ``PROVIDER_SETTING_MISSING``. All of them must
surface as ``AgentCompilationError`` carrying the corresponding code.

Entry points are simulated by replacing ``entry_points`` inside
``loom.core.plugins.entrypoints``, mirroring
``tests/unit/core/plugins/test_entrypoints.py``.
"""

from __future__ import annotations

import pytest

from loom.ai.errors import AgentCompilationError, AgentErrorCode
from loom.ai.registry import (
    require_provider_sdk,
    require_provider_setting,
    resolve_engine_provider,
)
from loom.core.plugins import entrypoints as entrypoints_module

_GROUP = "loom.ai.engines"
_ENGINE_NAME = "pydantic-ai"


class _FakeDist:
    """Minimal stand-in for ``importlib.metadata.Distribution``."""

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeEntryPoint:
    """Minimal stand-in for ``importlib.metadata.EntryPoint``."""

    def __init__(self, name: str, dist_name: str, target: object) -> None:
        self.name = name
        self.group = _GROUP
        self.dist = _FakeDist(dist_name)
        self._target = target

    def load(self) -> object:
        return self._target


class _FakeEntryPoints:
    """Stand-in for the collection returned by ``entry_points()``."""

    def __init__(self, entries: tuple[_FakeEntryPoint, ...]) -> None:
        self._entries = entries

    def select(self, *, group: str) -> tuple[_FakeEntryPoint, ...]:
        return tuple(entry for entry in self._entries if entry.group == group)


class _ProviderWithoutHandshake:
    """Loaded object lacking the ``LOOM_AI_ENGINE_API`` attribute."""


class _ProviderWithUnsupportedHandshake:
    """Loaded object announcing a handshake version this release cannot speak."""

    LOOM_AI_ENGINE_API = 99


def _install(
    monkeypatch: pytest.MonkeyPatch,
    entries: tuple[_FakeEntryPoint, ...],
) -> None:
    """Replace ``entry_points`` inside the shared loader with a fake collection."""
    monkeypatch.setattr(
        entrypoints_module,
        "entry_points",
        lambda: _FakeEntryPoints(entries),
    )


def _codes(error: AgentCompilationError) -> list[AgentErrorCode]:
    """Extract the ordered issue codes carried by a compilation error."""
    return [issue.code for issue in error.issues]


class TestEngineNotFound:
    def test_falla_con_engine_not_found_cuando_no_hay_entry_points(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        _install(monkeypatch, ())

        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)

        assert AgentErrorCode.ENGINE_NOT_FOUND in _codes(excinfo.value)


class TestEngineDuplicate:
    @pytest.fixture()
    def duplicate_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> AgentCompilationError:
        """Resolve against two distributions claiming the same engine name."""
        _install(
            monkeypatch,
            (
                _FakeEntryPoint(_ENGINE_NAME, "loom-engine-alpha", object()),
                _FakeEntryPoint(_ENGINE_NAME, "loom-engine-beta", object()),
            ),
        )
        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)
        return excinfo.value

    def test_falla_con_engine_duplicate_cuando_dos_distribuciones_lo_registran(
        self,
        duplicate_error: AgentCompilationError,
    ) -> None:
        assert AgentErrorCode.ENGINE_DUPLICATE in _codes(duplicate_error)

    def test_el_mensaje_nombra_ambas_distribuciones_cuando_hay_duplicado(
        self,
        duplicate_error: AgentCompilationError,
    ) -> None:
        message = str(duplicate_error)

        assert "loom-engine-alpha" in message and "loom-engine-beta" in message


class TestEngineApiMismatch:
    def test_falla_con_engine_api_mismatch_cuando_falta_el_handshake(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A loaded object without ``LOOM_AI_ENGINE_API`` cannot be an engine."""
        _install(
            monkeypatch,
            (_FakeEntryPoint(_ENGINE_NAME, "loom-engine-alpha", _ProviderWithoutHandshake()),),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)

        assert AgentErrorCode.ENGINE_API_MISMATCH in _codes(excinfo.value)

    def test_falla_con_engine_api_mismatch_cuando_la_version_no_esta_soportada(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Only handshake version 1 is supported by this release."""
        _install(
            monkeypatch,
            (
                _FakeEntryPoint(
                    _ENGINE_NAME,
                    "loom-engine-alpha",
                    _ProviderWithUnsupportedHandshake(),
                ),
            ),
        )

        with pytest.raises(AgentCompilationError) as excinfo:
            resolve_engine_provider(_ENGINE_NAME)

        assert AgentErrorCode.ENGINE_API_MISMATCH in _codes(excinfo.value)


class TestProviderHelpers:
    def test_falla_con_provider_not_installed_cuando_el_sdk_no_esta(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            require_provider_sdk("bedrock", "loom_nonexistent_sdk_xyz", "ai-bedrock")

        assert AgentErrorCode.PROVIDER_NOT_INSTALLED in _codes(excinfo.value)

    def test_el_mensaje_nombra_el_extra_cuando_el_sdk_no_esta(self) -> None:
        """The failure must tell the operator which extra to install."""
        with pytest.raises(AgentCompilationError) as excinfo:
            require_provider_sdk("bedrock", "loom_nonexistent_sdk_xyz", "ai-bedrock")

        assert "ai-bedrock" in str(excinfo.value)

    def test_falla_con_provider_setting_missing_cuando_falta_un_setting(self) -> None:
        with pytest.raises(AgentCompilationError) as excinfo:
            require_provider_setting("bedrock", "region", None)

        assert AgentErrorCode.PROVIDER_SETTING_MISSING in _codes(excinfo.value)

    def test_el_mensaje_nombra_el_setting_cuando_falta(self) -> None:
        """The failure must name the missing setting, not just the provider."""
        with pytest.raises(AgentCompilationError) as excinfo:
            require_provider_setting("bedrock", "region", None)

        assert "region" in str(excinfo.value)
