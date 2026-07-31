"""Configuration for the native stateless JWT authentication middleware."""

from __future__ import annotations

from loom.core.config.errors import ConfigError
from loom.core.model import LoomFrozenStruct

_DEFAULT_EXCLUDE_PATHS: tuple[str, ...] = ("/docs", "/redoc", "/openapi.json", "/metrics")
_FORBIDDEN_ALGORITHM = "none"


def _is_symmetric(algorithm: str) -> bool:
    """Return ``True`` when *algorithm* uses a shared secret (HS* family)."""
    return algorithm.upper().startswith("HS")


class JwtAuthConfig(LoomFrozenStruct, frozen=True, kw_only=True):
    """Validated settings for :class:`~loom.rest.auth.JwtAuthMiddleware`.

    Binds from the ``app.rest.auth.jwt`` config section.  Validation runs on
    construction (fail-fast): exactly one key source must be provided and the
    algorithm allowlist must be non-empty and coherent with that key source.

    Attributes:
        secret: Shared secret for symmetric algorithms (HS*).  Mutually
            exclusive with ``public_key``.
        public_key: Static PEM-encoded public key for asymmetric algorithms
            (RS*/ES*).  Mutually exclusive with ``secret``.
        algorithms: Explicit allowlist of accepted JWT algorithms.  The
            ``none`` algorithm is always forbidden.
        audience: Expected ``aud`` claim.  Validated only when set.
        issuer: Expected ``iss`` claim.  Validated only when set.
        leeway_seconds: Clock-skew tolerance applied to time-based claims.
        exclude_paths: Exact request paths that bypass authentication.

    Raises:
        ConfigError: If key sources, algorithms, or leeway are invalid.

    Example YAML::

        app:
          rest:
            auth:
              jwt:
                secret: ${oc.env:LOOM_JWT_SECRET}
                algorithms: [HS256]
    """

    secret: str | None = None
    public_key: str | None = None
    algorithms: tuple[str, ...] = ()
    audience: str | None = None
    issuer: str | None = None
    leeway_seconds: int = 0
    exclude_paths: tuple[str, ...] = _DEFAULT_EXCLUDE_PATHS

    def __repr__(self) -> str:
        secret = "***" if self.secret is not None else None
        return (
            f"JwtAuthConfig(secret={secret!r},"
            f" public_key={self.public_key!r},"
            f" algorithms={self.algorithms!r},"
            f" audience={self.audience!r},"
            f" issuer={self.issuer!r},"
            f" leeway_seconds={self.leeway_seconds!r},"
            f" exclude_paths={self.exclude_paths!r})"
        )

    def __post_init__(self) -> None:
        if (self.secret is None) == (self.public_key is None):
            raise ConfigError("JWT auth requires exactly one of 'secret' or 'public_key'.")
        if not self.algorithms:
            raise ConfigError("JWT auth requires a non-empty 'algorithms' allowlist.")
        if self.leeway_seconds < 0:
            raise ConfigError("JWT auth 'leeway_seconds' must be >= 0.")
        for algorithm in self.algorithms:
            self._validate_algorithm(algorithm)

    def _validate_algorithm(self, algorithm: str) -> None:
        if algorithm.lower() == _FORBIDDEN_ALGORITHM:
            raise ConfigError("The 'none' JWT algorithm is always forbidden.")
        if _is_symmetric(algorithm) and self.secret is None:
            raise ConfigError(f"JWT algorithm {algorithm!r} requires 'secret', not 'public_key'.")
        if not _is_symmetric(algorithm) and self.public_key is None:
            raise ConfigError(f"JWT algorithm {algorithm!r} requires 'public_key', not 'secret'.")

    @property
    def verification_key(self) -> str:
        """Return the key material used to verify token signatures.

        Returns:
            The shared ``secret`` for HS* setups, or the ``public_key`` PEM
            for RS*/ES* setups.
        """
        if self.secret is not None:
            return self.secret
        if self.public_key is not None:
            return self.public_key
        raise ConfigError("JwtAuthConfig has no key material.")  # pragma: no cover
