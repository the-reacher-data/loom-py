"""Tests for ``resolvers=`` and the built-in resolver defaults in ``bootstrap_worker``."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from loom.celery.bootstrap import bootstrap_worker
from loom.core.config import ConfigError
from loom.core.job.job import Job
from tests.unit._resolver_stubs import MappingResolver

pytestmark = pytest.mark.usefixtures("clear_builtin_resolvers")


class _SyncJob(Job[int]):
    def execute(self, value: int = 0) -> int:
        return value * 2


def _write_worker_yaml(tmp_path: Path, broker_url: str) -> str:
    path = tmp_path / "worker.yaml"
    path.write_text(
        "celery:\n"
        f"  broker_url: {broker_url}\n"
        "  result_backend: cache+memory://\n"
        "  task_always_eager: true\n"
        "database:\n"
        "  url: sqlite+aiosqlite:///test.db\n",
        encoding="utf-8",
    )
    return str(path)


def _without_boto3(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("loom.core.config.secrets._boto3_module", None)
    monkeypatch.setattr("loom.core.config.ssm._boto3_module", None)


def test_user_resolver_reaches_the_celery_app(tmp_path: Path) -> None:
    config_path = _write_worker_yaml(tmp_path, "${stub:broker}")

    result = bootstrap_worker(
        config_path,
        jobs=[_SyncJob],
        resolvers=[MappingResolver("stub", {"broker": "memory://"})],
    )

    assert result.celery_app.conf.broker_url == "memory://"


def test_secrets_placeholder_without_boto3_names_the_extra(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_boto3(monkeypatch)
    config_path = _write_worker_yaml(tmp_path, "${secrets:/worker/broker}")

    with pytest.raises(ConfigError, match=r"loom-kernel\[config-ssm\]"):
        bootstrap_worker(config_path, jobs=[_SyncJob])


def test_no_aws_client_without_placeholders(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    boto3 = MagicMock()
    monkeypatch.setattr("loom.core.config.secrets._boto3_module", boto3)
    monkeypatch.setattr("loom.core.config.ssm._boto3_module", boto3)
    config_path = _write_worker_yaml(tmp_path, "memory://")

    result = bootstrap_worker(config_path, jobs=[_SyncJob])

    assert result.celery_app.conf.broker_url == "memory://"
    boto3.client.assert_not_called()


def test_user_secrets_resolver_wins_over_the_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _without_boto3(monkeypatch)
    config_path = _write_worker_yaml(tmp_path, "${secrets:/worker/broker}")

    result = bootstrap_worker(
        config_path,
        jobs=[_SyncJob],
        resolvers=[MappingResolver("secrets", {"/worker/broker": "memory://"})],
    )

    assert result.celery_app.conf.broker_url == "memory://"
