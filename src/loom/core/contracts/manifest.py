"""Manifest attribute names for REST app discovery.

:class:`AppManifestAttr` centralises the string attribute names that
:class:`~loom.core.discovery.manifest.ManifestDiscoveryEngine` reads from
manifest modules, eliminating scattered ``getattr(module, "MODELS", [])``
calls across the codebase.

Example manifest module::

    from app.product.model import Product
    from app.product.use_cases import GetProductUseCase
    from app.product.interface import ProductInterface
    from app.product.repository import ProductRepository

    MODELS = [Product]
    USE_CASES = [GetProductUseCase]
    INTERFACES = [ProductInterface]
    REPOSITORIES = [ProductRepository]
"""

from __future__ import annotations

from enum import StrEnum


class AppManifestAttr(StrEnum):
    """Attribute names expected on a REST app manifest module.

    Values are plain strings at runtime (``StrEnum``), so they work directly
    with ``getattr``::

        models = getattr(module, AppManifestAttr.MODELS, [])
    """

    MODELS = "MODELS"
    USE_CASES = "USE_CASES"
    INTERFACES = "INTERFACES"
    REPOSITORIES = "REPOSITORIES"
