"""Pluggable config resolver protocol.

Resolvers extend the YAML loader with custom ``${prefix:key}`` interpolation,
enabling secret values to be fetched from external stores (e.g. AWS SSM,
Azure Key Vault) at parse time without ever writing them to disk.

Register resolvers via :func:`~loom.core.config.loader.load_config`::

    cfg = load_config("s3://bucket/prod.yaml", resolvers=[MyResolver()])

YAML placeholders::

    storage:
      catalogs:
        main:
          token: ${myresolver:prod/databricks/token}

The resolver name becomes the placeholder prefix.  Values are resolved
when OmegaConf materialises the config — i.e. at job startup, so secret
rotation takes effect on the next run without redeployment.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ConfigResolver(Protocol):
    """Protocol for pluggable config value resolvers.

    Implementors provide a *name* (used as the OmegaConf placeholder prefix)
    and a *resolve* callable that fetches the actual value at parse time.

    Example::

        class SsmResolver:
            name = "ssm"

            def __init__(self, region: str) -> None:
                self._region = region

            def resolve(self, key: str) -> str:
                import boto3
                client = boto3.client("ssm", region_name=self._region)
                return client.get_parameter(Name=key, WithDecryption=True)[
                    "Parameter"
                ]["Value"]

        cfg = load_config("s3://bucket/prod.yaml", resolvers=[SsmResolver("eu-west-1")])
    """

    @property
    def name(self) -> str:
        """OmegaConf resolver prefix.

        Used as the placeholder prefix in YAML: ``${<name>:key}``.
        Must be unique across all registered resolvers.

        Returns:
            Resolver name string (e.g. ``"ssm"``, ``"keyvault"``).
        """
        ...

    def resolve(self, key: str) -> str:
        """Resolve *key* to its string value.

        Called by OmegaConf when materialising ``${<name>:key}``
        placeholders.  Runs at config parse time (job startup), so the
        returned value reflects the current state of the backing store.

        Args:
            key: Key portion of the placeholder after the prefix separator
                (e.g. ``"/prod/token"`` for ``${ssm:/prod/token}``).

        Returns:
            Resolved string value.
        """
        ...


__all__ = ["ConfigResolver"]
