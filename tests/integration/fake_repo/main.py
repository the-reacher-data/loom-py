from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI

from loom.rest.fastapi.auto import create_app


def create_app_from_config(config_path: str, repo_root: str | None = None) -> FastAPI:
    code_path = repo_root or str(Path(__file__).resolve().parents[3])
    return create_app(config_path=config_path, code_path=code_path)
