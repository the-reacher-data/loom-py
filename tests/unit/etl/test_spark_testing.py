from __future__ import annotations

from pathlib import Path

import pytest

from loom.etl.testing import spark as spark_testing


class TestResolveIvyDir:
    def test_explicit_dir_has_priority(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LOOM_SPARK_IVY_DIR", "/env/ivy")

        result = spark_testing._resolve_ivy_dir("/explicit/ivy")

        assert result == Path("/explicit/ivy")

    def test_uses_env_dir_when_explicit_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LOOM_SPARK_IVY_DIR", "/env/ivy")

        result = spark_testing._resolve_ivy_dir(None)

        assert result == Path("/env/ivy")

    def test_returns_none_when_no_override_is_available(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("LOOM_SPARK_IVY_DIR", raising=False)

        result = spark_testing._resolve_ivy_dir(None)

        assert result is None


class TestOfflineJarsFlag:
    def test_returns_true_when_offline_jars_are_requested(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("LOOM_SPARK_OFFLINE", "1")
        assert spark_testing._offline_jars_requested() is True

    def test_returns_false_when_the_flag_is_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("LOOM_SPARK_OFFLINE", raising=False)
        assert spark_testing._offline_jars_requested() is False


class TestPickJar:
    def test_prefers_expected_version_when_present(self, tmp_path: Path) -> None:
        old = tmp_path / "io.delta_delta-spark_2.12-3.2.0.jar"
        new = tmp_path / "io.delta_delta-spark_2.12-3.3.2.jar"
        old.touch()
        new.touch()

        result = spark_testing._pick_jar(tmp_path, "io.delta_delta-spark_2.12-", "3.2.0")

        assert result == old

    def test_falls_back_to_latest_candidate(self, tmp_path: Path) -> None:
        older = tmp_path / "io.delta_delta-storage-3.2.0.jar"
        newer = tmp_path / "io.delta_delta-storage-3.3.2.jar"
        older.touch()
        newer.touch()

        result = spark_testing._pick_jar(tmp_path, "io.delta_delta-storage-", None)

        assert result == newer
