"""Minimal discoverable project used by the ``create_app`` config tests.

``create_app`` needs a real importable package to discover use cases and REST
interfaces, plus a YAML file. Writing both is boilerplate every config-driven
test needs, so it lives here once.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

_MODULE_PREFIX = "loom_config_fixture_app"
_NON_IDENTIFIER = re.compile(r"\W", re.ASCII)

_APP_SOURCE = '''\
"""Minimal discoverable app used by the create_app configuration tests."""

from __future__ import annotations

from typing import Any

from loom.core.model import BaseModel, ColumnField
from loom.core.use_case.use_case import UseCase
from loom.rest.model import RestInterface, RestRoute


class ConfigRecord(BaseModel):
    __tablename__ = "config_records_fixture"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=50)


class ConfigPingUseCase(UseCase[ConfigRecord, str]):
    async def execute(self, **kwargs: Any) -> str:
        return "pong"


class ConfigPingInterface(RestInterface[str]):
    prefix = "{prefix}"
    routes = (RestRoute(use_case=ConfigPingUseCase, method="GET", path="{route_path}"),)
'''


def write_project(
    tmp_path: Path,
    *,
    rest: dict[str, Any] | None = None,
    sql: dict[str, Any] | None = None,
    observability: dict[str, Any] | None = None,
    prefix: str = "/ping",
    route_path: str = "/",
) -> str:
    """Write the fixture module plus a YAML config and return the config path.

    Args:
        tmp_path: Directory owning the generated project.
        rest: Contents of the ``app.rest`` section.
        sql: Contents of the ``sql`` section.
        observability: Contents of the ``observability`` section.
        prefix: Prefix of the generated REST interface.
        route_path: Path of its single route, relative to *prefix*.

    Returns:
        Path of the written YAML config file.
    """
    # One module per test: the interpreter caches imports by name, so a shared
    # name would serve the first test's routes to every later one.
    module = f"{_MODULE_PREFIX}_{_NON_IDENTIFIER.sub('_', tmp_path.name)}"
    source = _APP_SOURCE.format(prefix=prefix, route_path=route_path)
    (tmp_path / f"{module}.py").write_text(source, encoding="utf-8")
    config: dict[str, Any] = {
        "app": {
            "name": "config-demo",
            "code_path": ".",
            "discovery": {
                "mode": "interfaces",
                "interfaces": {"modules": [module], "warn_recommended": False},
            },
        },
        "database": {"url": "sqlite+aiosqlite:///"},
    }
    if rest is not None:
        config["app"]["rest"] = rest
    if sql is not None:
        config["sql"] = sql
    if observability is not None:
        config["observability"] = observability
    config_path = tmp_path / "app.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return str(config_path)
