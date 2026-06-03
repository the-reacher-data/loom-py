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

Built-in resolvers
------------------
:class:`~loom.core.config.ssm.SsmResolver` is the bundled implementation
for AWS SSM Parameter Store.  Install ``loom[config-ssm]`` to use it::

    from loom.core.config import load_config, SsmResolver

    cfg = load_config("config/prod.yaml", resolvers=[SsmResolver()])
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ConfigResolver(Protocol):
    """Protocol for pluggable config value resolvers.

    Implementors provide a *name* (used as the OmegaConf placeholder prefix)
    and a *resolve* callable that fetches the actual value at parse time.

    See :class:`~loom.core.config.ssm.SsmResolver` for the bundled AWS SSM
    implementation.  Custom resolvers only need to satisfy this two-member
    protocol::

        class VaultResolver:
            @property
            def name(self) -> str:
                return "vault"

            def resolve(self, key: str) -> str:
                return vault_client.read_secret(key)

        cfg = load_config("config/prod.yaml", resolvers=[VaultResolver()])
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
