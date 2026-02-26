"""Dependency injection scopes."""

from enum import Enum


class Scope(Enum):
    """Lifetime scope for a registered dependency.

    Attributes:
        APPLICATION: Singleton — created once and reused for the entire
            process lifetime.  Suitable for stateless services, connection
            pools, and configuration objects.
        REQUEST: Per-execution — a fresh instance is created for each
            :meth:`~loom.core.di.container.LoomContainer.resolve` call.
            Suitable for repositories and unit-of-work objects.
        TRANSACTION: Reserved for transactional scope (Fase 4).
            Behaves like ``REQUEST`` in v1.
    """

    APPLICATION = "application"
    REQUEST = "request"
    TRANSACTION = "transaction"
