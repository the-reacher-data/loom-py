"""RestInterface compiler.

Validates and compiles :class:`~loom.rest.model.RestInterface` declarations
into :class:`CompiledRoute` records at startup.  No reflection or validation
occurs after compilation.

Compilation steps per interface:

1. Validate structural constraints (prefix, routes non-empty if ``auto=False``).
2. For each :class:`~loom.rest.model.RestRoute`:
   - Verify ``use_case`` has a compiled :class:`~loom.core.engine.plan.ExecutionPlan`.
   - Resolve effective pagination mode (route → interface → global).
   - Resolve effective profile policy (route → interface → global).
3. Detect and resolve ``(method, path)`` conflicts: custom routes win over
   duplicates.  Raise :class:`InterfaceCompilationError` on ambiguous conflicts
   within the same ``routes`` tuple.
4. Validate ``expose_profile`` requires a non-empty ``allowed_profiles``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from loom.core.engine.compiler import UseCaseCompiler
from loom.core.engine.metrics import MetricsAdapter
from loom.rest.model import PaginationMode, RestApiDefaults, RestInterface, RestRoute


class InterfaceCompilationError(Exception):
    """Raised when a RestInterface fails structural validation at startup.

    Args:
        message: Human-readable description of the compilation failure.

    Example::

        raise InterfaceCompilationError(
            "UserRestInterface: duplicate route (GET, /{user_id})"
        )
    """


@dataclass(frozen=True)
class CompiledRoute:
    """Fully resolved route ready for transport binding.

    Produced by :class:`RestInterfaceCompiler` for each valid route declaration.
    All policy fields are resolved — no further lookup is needed at request time.

    Args:
        interface_name: Qualified name of the originating ``RestInterface``.
        route: Original :class:`~loom.rest.model.RestRoute` declaration.
        full_path: Absolute HTTP path (``interface.prefix + route.path``).
        effective_pagination_mode: Resolved pagination strategy after applying
            route → interface → global precedence.
        effective_profile_default: Resolved default profile name.
        effective_allowed_profiles: Resolved set of allowed profiles.
        effective_expose_profile: Whether ``?profile=...`` is publicly
            accepted for this route.
        effective_allow_pagination_override: Whether callers may override
            pagination mode via query parameters for this route.
        interface_tags: OpenAPI tags inherited from the parent ``RestInterface``.
    """

    interface_name: str
    route: RestRoute
    full_path: str
    effective_pagination_mode: PaginationMode
    effective_profile_default: str
    effective_allowed_profiles: tuple[str, ...]
    effective_expose_profile: bool
    effective_allow_pagination_override: bool
    interface_tags: tuple[str, ...] = ()


class RestInterfaceCompiler:
    """Compiles RestInterface declarations into CompiledRoute records.

    Validates structure, resolves policies, and caches the result.  Designed
    to run once at startup, driven by :func:`~loom.core.bootstrap.bootstrap.bootstrap_app`
    or the FastAPI composition root.

    Args:
        use_case_compiler: Compiler that holds the cached
            :class:`~loom.core.engine.plan.ExecutionPlan` registry.
        defaults: Global REST API defaults applied when interface/route
            level is unset.  Defaults to :class:`~loom.rest.model.RestApiDefaults`
            with offset pagination.
        metrics: Optional metrics adapter.  Reserved for future use
            (HTTP metrics are emitted at request time by the router runtime).

    Example::

        compiler = RestInterfaceCompiler(use_case_compiler)
        routes = compiler.compile(UserRestInterface)
    """

    def __init__(
        self,
        use_case_compiler: UseCaseCompiler,
        defaults: RestApiDefaults | None = None,
        metrics: MetricsAdapter | None = None,
    ) -> None:
        self._uc_compiler = use_case_compiler
        self._defaults = defaults or RestApiDefaults()
        self._metrics = metrics
        self._cache: dict[type[RestInterface[Any]], list[CompiledRoute]] = {}

    def compile(self, interface: type[RestInterface[Any]]) -> list[CompiledRoute]:
        """Compile ``interface`` and return the list of :class:`CompiledRoute`.

        Compilation is idempotent: repeated calls with the same class return
        the cached result.

        Args:
            interface: ``RestInterface`` subclass to compile.

        Returns:
            Ordered list of compiled routes ready for transport binding.

        Raises:
            InterfaceCompilationError: If the interface fails validation.
        """
        if interface in self._cache:
            return self._cache[interface]

        result = self._compile_fresh(interface)
        self._cache[interface] = result
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _compile_fresh(self, interface: type[RestInterface[Any]]) -> list[CompiledRoute]:
        iface_name = interface.__qualname__
        prefix = interface.prefix.rstrip("/")

        self._validate_interface(interface)

        seen: dict[tuple[str, str], RestRoute] = {}
        compiled: list[CompiledRoute] = []

        for route in interface.routes:
            self._validate_route(iface_name, route)

            key = (route.method.upper(), route.path)
            if key in seen:
                raise InterfaceCompilationError(
                    f"{iface_name}: duplicate route "
                    f"({route.method.upper()}, {route.path!r}).  "
                    "Each (method, path) pair must be unique within a RestInterface."
                )
            seen[key] = route

            full_path = f"{prefix}{route.path}" if route.path else prefix or "/"

            compiled.append(
                CompiledRoute(
                    interface_name=iface_name,
                    route=route,
                    full_path=full_path,
                    effective_pagination_mode=self._resolve_pagination(route, interface),
                    effective_profile_default=self._resolve_profile_default(route, interface),
                    effective_allowed_profiles=self._resolve_allowed_profiles(route, interface),
                    effective_expose_profile=self._resolve_expose_profile(route, interface),
                    effective_allow_pagination_override=self._resolve_allow_pagination_override(
                        route, interface
                    ),
                    interface_tags=interface.tags,
                )
            )

            effective = compiled[-1]
            if effective.effective_expose_profile and not effective.effective_allowed_profiles:
                raise InterfaceCompilationError(
                    f"{iface_name}: route ({route.method.upper()}, {route.path!r}) "
                    "has profile exposure enabled but no allowed profiles. "
                    "Declare at least one allowed profile."
                )

        return compiled

    def _validate_interface(self, interface: type[RestInterface[Any]]) -> None:
        iface_name = interface.__qualname__
        if not interface.prefix:
            raise InterfaceCompilationError(
                f"{iface_name}: 'prefix' must be a non-empty string (e.g. '/users')."
            )
        if not interface.routes:
            raise InterfaceCompilationError(
                f"{iface_name}: 'routes' is empty.  Declare at least one RestRoute to expose."
            )

    def _validate_route(self, iface_name: str, route: RestRoute) -> None:
        if not route.method:
            raise InterfaceCompilationError(
                f"{iface_name}: RestRoute for {route.use_case.__qualname__!r} is missing 'method'."
            )
        if route.path == "" and route.path is not None:
            # path="" is treated as "/" — allowed, but warn via compilation for clarity
            pass
        plan = self._uc_compiler.get_plan(route.use_case)
        if plan is None:
            raise InterfaceCompilationError(
                f"{iface_name}: route ({route.method.upper()}, {route.path!r}) "
                f"references {route.use_case.__qualname__!r} which has not been "
                "compiled.  Pass it to bootstrap_app(use_cases=[...]) first."
            )

    def _resolve_pagination(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> PaginationMode:
        return route.pagination_mode or interface.pagination_mode or self._defaults.pagination_mode

    def _resolve_profile_default(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> str:
        return route.profile_default or interface.profile_default or self._defaults.profile_default

    def _resolve_allowed_profiles(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> tuple[str, ...]:
        if route.allowed_profiles:
            return route.allowed_profiles
        if interface.allowed_profiles:
            return interface.allowed_profiles
        return self._defaults.allowed_profiles

    def _resolve_expose_profile(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> bool:
        if route.expose_profile:
            return True
        return interface.expose_profile

    def _resolve_allow_pagination_override(
        self, route: RestRoute, interface: type[RestInterface[Any]]
    ) -> bool:
        if route.allow_pagination_override is not None:
            return route.allow_pagination_override
        if interface.allow_pagination_override is not None:
            return interface.allow_pagination_override
        return self._defaults.allow_pagination_override
