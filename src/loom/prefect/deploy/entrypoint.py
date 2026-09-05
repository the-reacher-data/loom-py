"""Prefect entrypoint for ETLs declared in YAML.

Prefect rebuilds a YAML-declared flow by importing this module and reading
one attribute, ``loom.prefect.deploy.entrypoint.<attribute>``, both in the
deploy process and in the worker. The attribute is the ETL name with hyphens
replaced by underscores. The declaration file is taken from the
``LOOM_ETL_CONFIG`` environment variable: the deployer exports it around each
``from_source`` call and records it in the deployment's job variables, so the
worker sees the same value. Names starting with ``_`` raise ``AttributeError``
as in any module; a public name raises ``ConfigError`` when the variable is
unset.
"""

from __future__ import annotations

import os
from typing import Any

from loom.core.config import ConfigError
from loom.prefect.deploy._yaml_etls import EtlDeclaration, load_declaration
from loom.prefect.flow._factory import _build_etl_flow

LOOM_ETL_CONFIG = "LOOM_ETL_CONFIG"


def build_flow(declaration: EtlDeclaration) -> Any:
    """Build the Prefect flow of a YAML declaration.

    Args:
        declaration: Resolved ETL declaration.

    Returns:
        A ``@prefect.flow``-decorated callable with ``__loom_etl_meta__``
        attached, sourced from this module.
    """
    return _build_etl_flow(
        name=declaration.name,
        pipeline=declaration.pipeline,
        params_type=declaration.params_type,
        settings=declaration.settings,
        config_path=declaration.config_uri,
        source_file=__file__,
        storage_config_path=declaration.storage_config_path,
        manifest_store=None,
        flow_config=declaration.settings.retry_policy,
    )


def __getattr__(attribute: str) -> Any:
    if attribute.startswith("_"):
        raise AttributeError(attribute)
    config_uri = os.environ.get(LOOM_ETL_CONFIG)
    if not config_uri:
        raise ConfigError(
            f"{LOOM_ETL_CONFIG} is not set; it must name the YAML file declaring "
            f"the ETL behind entrypoint attribute {attribute!r}"
        )
    return build_flow(load_declaration(config_uri, attribute))


__all__ = ["LOOM_ETL_CONFIG", "build_flow"]
