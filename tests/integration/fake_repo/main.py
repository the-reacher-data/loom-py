from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import FastAPI

from loom.rest.fastapi.auto import create_app

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry

_REPO_ROOT = str(Path(__file__).resolve().parents[3])


def create_app_from_config(
    config_path: str,
    repo_root: str | None = None,
    metrics_registry: CollectorRegistry | None = None,
) -> FastAPI:
    return create_app(
        config_path,
        code_path=repo_root or _REPO_ROOT,
        metrics_registry=metrics_registry,
    )
