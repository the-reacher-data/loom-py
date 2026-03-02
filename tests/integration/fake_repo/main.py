from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI

from loom.rest.fastapi.auto import create_app

_REPO_ROOT = str(Path(__file__).resolve().parents[3])


def create_app_from_config(config_path: str, repo_root: str | None = None) -> FastAPI:
    return create_app(config_path, code_path=repo_root or _REPO_ROOT)
