"""Unit tests for load_config and section helpers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import msgspec
import pytest

from loom.core.config.errors import ConfigError
from loom.core.config.loader import load_config, section

# ---------------------------------------------------------------------------
# User-defined config structs (no framework base class)
# ---------------------------------------------------------------------------


class DatabaseConfig(msgspec.Struct, kw_only=True):
    url: str
    pool_size: int = 5


class CacheConfig(msgspec.Struct, kw_only=True):
    host: str = "localhost"
    port: int = 6379


@dataclass
class AppSettings:
    debug: bool = False
    log_level: str = "INFO"


# ---------------------------------------------------------------------------
# load_config — single file
# ---------------------------------------------------------------------------


def test_load_config_single_file(tmp_path: Path) -> None:
    f = tmp_path / "config.yaml"
    f.write_text("database:\n  url: sqlite:///dev.db\n  pool_size: 3\n")
    cfg = load_config(str(f))
    assert cfg.database.url == "sqlite:///dev.db"
    assert cfg.database.pool_size == 3


def test_load_config_returns_dictconfig(tmp_path: Path) -> None:
    from omegaconf import DictConfig

    f = tmp_path / "cfg.yaml"
    f.write_text("key: value\n")
    cfg = load_config(str(f))
    assert isinstance(cfg, DictConfig)


def test_load_config_dot_access(tmp_path: Path) -> None:
    f = tmp_path / "cfg.yaml"
    f.write_text("server:\n  host: 0.0.0.0\n  port: 8080\n")
    cfg = load_config(str(f))
    assert cfg.server.host == "0.0.0.0"
    assert cfg.server.port == 8080


# ---------------------------------------------------------------------------
# load_config — multiple files (merge left-to-right)
# ---------------------------------------------------------------------------


def test_load_config_multiple_files_later_overrides(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    base.write_text("debug: false\nlog_level: INFO\n")
    override = tmp_path / "local.yaml"
    override.write_text("debug: true\n")

    cfg = load_config(str(base), str(override))
    assert cfg.debug is True
    assert cfg.log_level == "INFO"


def test_load_config_three_files_merge(tmp_path: Path) -> None:
    a = tmp_path / "a.yaml"
    a.write_text("x: 1\ny: 1\n")
    b = tmp_path / "b.yaml"
    b.write_text("y: 2\nz: 2\n")
    c = tmp_path / "c.yaml"
    c.write_text("z: 3\n")

    cfg = load_config(str(a), str(b), str(c))
    assert cfg.x == 1
    assert cfg.y == 2
    assert cfg.z == 3


def test_load_config_nested_merge(tmp_path: Path) -> None:
    base = tmp_path / "base.yaml"
    base.write_text("database:\n  url: sqlite:///base.db\n  pool_size: 5\n")
    override = tmp_path / "prod.yaml"
    override.write_text("database:\n  url: postgresql://prod/mydb\n")

    cfg = load_config(str(base), str(override))
    assert cfg.database.url == "postgresql://prod/mydb"
    assert cfg.database.pool_size == 5  # from base


# ---------------------------------------------------------------------------
# load_config — env interpolation
# ---------------------------------------------------------------------------


def test_load_config_env_interpolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_URL", "postgresql+asyncpg://prod-host/prod")
    f = tmp_path / "cfg.yaml"
    f.write_text("database:\n  url: ${oc.env:DB_URL}\n")
    cfg = load_config(str(f))
    assert cfg.database.url == "postgresql+asyncpg://prod-host/prod"


# ---------------------------------------------------------------------------
# load_config — error cases
# ---------------------------------------------------------------------------


def test_load_config_no_files_raises() -> None:
    with pytest.raises(ConfigError, match="at least one"):
        load_config()


def test_load_config_file_not_found_raises() -> None:
    with pytest.raises(ConfigError, match="not found"):
        load_config("/nonexistent/path/config.yaml")


def test_load_config_one_missing_among_many_raises(tmp_path: Path) -> None:
    good = tmp_path / "good.yaml"
    good.write_text("x: 1\n")
    with pytest.raises(ConfigError, match="not found"):
        load_config(str(good), "/nonexistent/missing.yaml")


# ---------------------------------------------------------------------------
# section — happy path
# ---------------------------------------------------------------------------


def test_section_msgspec_struct(tmp_path: Path) -> None:
    f = tmp_path / "cfg.yaml"
    f.write_text("database:\n  url: sqlite:///test.db\n  pool_size: 10\n")
    cfg = load_config(str(f))
    db = section(cfg, "database", DatabaseConfig)
    assert isinstance(db, DatabaseConfig)
    assert db.url == "sqlite:///test.db"
    assert db.pool_size == 10


def test_section_defaults_applied(tmp_path: Path) -> None:
    f = tmp_path / "cfg.yaml"
    f.write_text("database:\n  url: sqlite:///dev.db\n")
    cfg = load_config(str(f))
    db = section(cfg, "database", DatabaseConfig)
    assert db.pool_size == 5  # default


def test_section_nested_key(tmp_path: Path) -> None:
    f = tmp_path / "cfg.yaml"
    f.write_text("services:\n  cache:\n    host: redis-host\n    port: 6380\n")
    cfg = load_config(str(f))
    cache = section(cfg, "services.cache", CacheConfig)
    assert cache.host == "redis-host"
    assert cache.port == 6380


def test_section_dataclass(tmp_path: Path) -> None:
    f = tmp_path / "cfg.yaml"
    f.write_text("app:\n  debug: true\n  log_level: DEBUG\n")
    cfg = load_config(str(f))
    app = section(cfg, "app", AppSettings)
    assert app.debug is True
    assert app.log_level == "DEBUG"


def test_section_env_interpolation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDIS_HOST", "my-redis")
    f = tmp_path / "cfg.yaml"
    f.write_text("cache:\n  host: ${oc.env:REDIS_HOST}\n")
    cfg = load_config(str(f))
    cache = section(cfg, "cache", CacheConfig)
    assert cache.host == "my-redis"


# ---------------------------------------------------------------------------
# section — error cases
# ---------------------------------------------------------------------------


def test_section_missing_key_raises(tmp_path: Path) -> None:
    f = tmp_path / "cfg.yaml"
    f.write_text("other: value\n")
    cfg = load_config(str(f))
    with pytest.raises(ConfigError, match="not found"):
        section(cfg, "database", DatabaseConfig)


def test_section_validation_failure_raises(tmp_path: Path) -> None:
    f = tmp_path / "cfg.yaml"
    f.write_text("database:\n  pool_size: not-a-number\n")  # url missing, pool_size wrong type
    cfg = load_config(str(f))
    with pytest.raises(ConfigError, match="validation"):
        section(cfg, "database", DatabaseConfig)


def test_section_nested_key_missing_intermediate_raises(tmp_path: Path) -> None:
    f = tmp_path / "cfg.yaml"
    f.write_text("services:\n  other: value\n")
    cfg = load_config(str(f))
    with pytest.raises(ConfigError, match="not found"):
        section(cfg, "services.cache", CacheConfig)
