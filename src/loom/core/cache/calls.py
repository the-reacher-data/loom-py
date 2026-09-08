"""Binding of ``@cache_call`` coroutines to a configured cache.

The decorator declares and the composition root binds: a marked coroutine is
imported and tested with no configuration at all, and the ``CachedCalls`` a
container holds — built once at boot from the same ``cache:`` section the
repositories read — is what turns it into a cached call.

A cached call is a TTL cache with **no invalidation**: loom cannot know what a
coroutine depends on, so nothing tags it and nothing bumps it. It expires, or
the caller bumps ``version``.

The key is the arguments and nothing else. That is what makes an answer
shareable between two callers and between two instances of a toolset class, and
it is why a cached coroutine must be a pure function of its arguments: ambient
identity never reaches the key, and the load runs detached in its own task.
"""

from __future__ import annotations

import functools
import inspect
import math
import random
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, cast

import msgspec

from loom.core.cache._single_flight import SingleFlight
from loom.core.cache.abc.backend import CacheBackend
from loom.core.cache.abc.config import CacheConfig
from loom.core.cache.decorators import _CallPolicy, declares_cache_call
from loom.core.cache.keys import call_key
from loom.core.cache.result_codec import ResultCodec, build_call_codec
from loom.core.di.container import LoomContainer
from loom.core.logger import get_logger
from loom.core.model.introspection import resolve_type_hints

AsyncCallable = Callable[..., Awaitable[Any]]

_UNDECODABLE = object()
"""Sentinel for a payload the codec refused; ``None`` is a legitimate value."""


class _Unrenderable(Exception):
    """An argument has no canonical rendering, so the call gets no key."""


class CachedCalls(Protocol):
    """Binder that turns declared coroutines into cached ones."""

    def wrap(self, func: AsyncCallable) -> AsyncCallable:
        """Return the cached form of *func*, or *func* itself."""
        ...

    def bind(self, obj: object) -> list[AsyncCallable]:
        """Return the public coroutine methods of *obj*, cached where declared."""
        ...


def cached_calls(container: LoomContainer) -> CachedCalls:
    """Return the container's binder, or a pass-through when it has none.

    A container built outside the two bootstraps that apply the cache module
    has no binding, and :meth:`LoomContainer.resolve` raises for one. A factory
    called with such a container gets the announcing pass-through instead, so
    it never has to guess how its application was built.

    Args:
        container: Container handed to the factory.

    Returns:
        The registered binder, or a fresh pass-through.
    """
    if container.is_registered(CachedCalls):
        return cast(CachedCalls, container.resolve(CachedCalls))
    return _UnconfiguredCalls()


def _render(value: Any) -> Any:
    """Render *value* canonically, so equal arguments give one key.

    Mappings are emitted with their keys sorted, sets and frozensets as sorted
    lists, and everything else through :func:`msgspec.to_builtins`, whose
    output is walked in turn: a struct's fields, a list's items and a mapping's
    values all get the same treatment.

    Args:
        value: One bound argument, or a part of one.

    Returns:
        The canonical rendering of *value*.

    Raises:
        _Unrenderable: The value, or something nested inside it, has no
            canonical rendering.
    """
    if isinstance(value, Mapping):
        return _render_mapping(value)
    if isinstance(value, set | frozenset):
        return _render_set(value)
    if isinstance(value, list | tuple):
        return [_render(item) for item in value]
    return _render_leaf(value)


def _render_mapping(value: Mapping[Any, Any]) -> dict[Any, Any]:
    """Render a mapping with its keys sorted, recursively."""
    pairs = [(_render(key), _render(item)) for key, item in value.items()]
    pairs.sort(key=lambda pair: _encoded(pair[0]))
    try:
        return dict(pairs)
    except TypeError as error:
        raise _Unrenderable(f"unhashable mapping key: {error}") from error


def _render_set(value: set[Any] | frozenset[Any]) -> list[Any]:
    """Render a set as a list ordered by its members' encodings."""
    return sorted((_render(item) for item in value), key=_encoded)


def _render_leaf(value: Any) -> Any:
    """Render one non-container argument, descending into what msgspec builds.

    A non-finite float is refused: msgspec renders ``nan`` and the infinities
    as ``null``, which would collide with ``None`` and serve one call's answer
    to another.
    """
    try:
        rendered = msgspec.to_builtins(value)
    except (TypeError, ValueError) as error:
        raise _Unrenderable(f"no canonical rendering: {error}") from error
    if isinstance(rendered, float):
        if not math.isfinite(rendered):
            raise _Unrenderable(f"non-finite float: {rendered!r}")
        return rendered
    if isinstance(rendered, Mapping | list | tuple | set | frozenset):
        return _render(rendered)
    return rendered


def _encoded(value: Any) -> bytes:
    """Encode a rendered value, giving both a sort order and a check.

    Raises:
        _Unrenderable: The rendering cannot be encoded, so it can neither be
            ordered nor hashed.
    """
    try:
        return msgspec.json.encode(value)
    except (TypeError, ValueError, msgspec.MsgspecError) as error:
        raise _Unrenderable(f"cannot encode {type(value).__name__}: {error}") from error


@dataclass(frozen=True, slots=True)
class _CallSpec:
    """Everything about one wrapped call that is decided at wrap time.

    Attributes:
        func: The declared coroutine, unwrapped.
        policy: What ``@cache_call`` declared about it.
        codec: Codec derived from its return annotation.
        signature: Its signature, computed once.
    """

    func: AsyncCallable
    policy: _CallPolicy
    codec: ResultCodec
    signature: inspect.Signature

    def key(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        """Build the cache key of one call.

        Args:
            args: Positional arguments of the call.
            kwargs: Keyword arguments of the call.

        Returns:
            The key covering this function, this version and these arguments.

        Raises:
            _Unrenderable: An argument has no canonical rendering, or its
                rendering is not encodable — a mapping keyed by ``None`` renders
                fine and only the final encoding refuses it, and no argument
                shape may raise to the caller.
            TypeError: The arguments do not fit the signature; the body would
                raise the same error.
        """
        bound = self.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        arguments = _render(dict(bound.arguments))
        try:
            return call_key(
                module=self.func.__module__,
                qualname=self.func.__qualname__,
                version=self.policy.version,
                arguments=arguments,
            )
        except (TypeError, ValueError, msgspec.MsgspecError) as error:
            raise _Unrenderable(f"cannot encode the rendered arguments: {error}") from error


def _function_name(func: AsyncCallable) -> str:
    """Dotted ``module.qualname`` of *func*."""
    return f"{func.__module__}.{func.__qualname__}"


def _publishes_coroutine(attribute: object) -> bool:
    """Whether a class-dictionary entry publishes a coroutine.

    The raw entry is asked rather than the instance attribute, so a property
    is never evaluated. A ``staticmethod`` or ``classmethod`` entry is the
    descriptor and not the function it holds, so it is unwrapped first:
    asking the descriptor itself answers ``False`` for an async static or
    class method and would drop the tool without a word.

    Args:
        attribute: Entry read from a class ``__dict__``.

    Returns:
        Whether binding the entry to an instance yields a coroutine function.
    """
    if isinstance(attribute, staticmethod | classmethod):
        return inspect.iscoroutinefunction(attribute.__func__)
    return inspect.iscoroutinefunction(attribute)


def _log_abandoned_call(key: str, error: BaseException) -> None:
    """Report a detached load that failed with no caller left to receive it.

    The body of a cached call is arbitrary application code, so a failure the
    single flight can hand to nobody would otherwise disappear entirely.
    """
    get_logger(__name__).warning("CacheCallLoadAbandoned", key=key, error=repr(error))


def _coroutine_methods(obj: object, warn_once: _WarnOnce) -> list[AsyncCallable]:
    """Return the public coroutine methods of *obj*, base classes first.

    The MRO is walked in reverse and each class dictionary in its own order, so
    a method keeps the position of the class that first declared it and an
    override does not jump to the end. The **class attribute** is what decides,
    not the instance one: reading the instance would evaluate a property, and a
    property is not a tool. A ``staticmethod`` or ``classmethod`` is unwrapped
    before the question is asked, so an async one is published like any other
    coroutine method.

    A subclass that shadows a base's coroutine with something that is not one —
    a property, a plain ``def`` — unpublishes it, because the published object
    would not be awaitable. That is announced (``CacheCallOverriddenBySync``):
    a tool disappearing from a toolset is exactly the kind of change nobody
    notices until an agent stops using it.

    Args:
        obj: Instance whose methods are being published.
        warn_once: Announcer of the binder asking for the methods.

    Returns:
        The bound methods, in declaration order.
    """
    names: dict[str, None] = {}
    for cls in reversed(type(obj).__mro__):
        for name, attribute in cls.__dict__.items():
            if name.startswith("_"):
                continue
            if _publishes_coroutine(attribute):
                names[name] = None
            elif name in names:
                del names[name]
                warn_once.emit(
                    "CacheCallOverriddenBySync",
                    f"{cls.__qualname__}.{name}",
                    attribute=type(attribute).__name__,
                )
    return [cast(AsyncCallable, getattr(obj, name)) for name in names]


class _WarnOnce:
    """Per-instance memory of which warning was already emitted for which call.

    The scope is the binder the composition root built, not the module: two
    containers in one process warn independently, and no import mutates
    anything global.
    """

    __slots__ = ("_log", "_seen")

    def __init__(self) -> None:
        self._log = get_logger(__name__)
        self._seen: set[tuple[str, str]] = set()

    def __call__(self, event: str, func: AsyncCallable, **fields: Any) -> None:
        """Log *event* for *func* the first time it happens.

        Args:
            event: Log event name.
            func: Declared coroutine the event is about.
            **fields: Extra structured fields.
        """
        self.emit(event, _function_name(func), **fields)

    def emit(self, event: str, name: str, **fields: Any) -> None:
        """Log *event* for the dotted *name* the first time it happens.

        Args:
            event: Log event name.
            name: Dotted name of what the event is about; the memo is keyed by
                the pair, so two events about one name both get through and one
                event about two names does too.
            **fields: Extra structured fields.
        """
        if (event, name) in self._seen:
            return
        self._seen.add((event, name))
        self._log.warning(event, function=name, **fields)


class _ConfiguredCalls:
    """Binder over a live cache backend.

    Args:
        config: The decoded ``cache:`` section, for the TTLs.
        cache: Backend the entries are written to and read from.
        flight: Coalescer shared by every call this binder wraps.
        rng: Generator owned by this binder, so seeding the process-wide
            ``random`` module does not freeze every TTL in the process.
    """

    def __init__(
        self,
        config: CacheConfig,
        cache: CacheBackend,
        flight: SingleFlight,
        rng: random.Random,
    ) -> None:
        self._config = config
        self._cache = cache
        self._flight = flight
        self._rng = rng
        self._warn_once = _WarnOnce()

    def wrap(self, func: AsyncCallable) -> AsyncCallable:
        """Return the cached form of *func*.

        *func* comes back unchanged when it is already a wrapper, when it
        carries no policy, and when its return annotation yields no codec — the
        last case warns, because a deployment should learn that a marked
        coroutine runs uncached.

        Args:
            func: Coroutine, marked or not.

        Returns:
            The wrapper, or *func* itself.
        """
        if getattr(func, "__cache_call_wrapped__", False):
            return func
        policy = declares_cache_call(func)
        if policy is None:
            return func
        codec = build_call_codec(func)
        if codec is None:
            self._warn_once("CacheCallNotCacheable", func)
            return func
        spec = _CallSpec(func, policy, codec, inspect.signature(func))

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self._call(spec, args, kwargs)

        wrapper.__cache_call_wrapped__ = True  # type: ignore[attr-defined]
        # The wrapper answers for itself: ``functools.wraps`` would leave it
        # carrying the string annotations of another module, resolvable only by
        # following ``__wrapped__``.  ``include_extras=True`` is load-bearing —
        # a tool parameter's description and constraints live in
        # ``Annotated[...]``, and resolving without them strips every
        # description from the schema a toolset generates.
        wrapper.__annotations__ = resolve_type_hints(func, include_extras=True)
        return wrapper

    def bind(self, obj: object) -> list[AsyncCallable]:
        """Return the public coroutine methods of *obj*, cached where declared.

        Every public coroutine method is published, so adding a public helper
        to a toolset class publishes a tool.

        Args:
            obj: Instance to publish.

        Returns:
            The bound methods, in declaration order, base class first.
        """
        return [self.wrap(method) for method in _coroutine_methods(obj, self._warn_once)]

    async def _call(self, spec: _CallSpec, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        """Serve one call from the cache, or load it once and store it."""
        try:
            key = spec.key(args, kwargs)
        except _Unrenderable as error:
            self._warn_once("CacheCallKeyUnrenderable", spec.func, error=str(error))
            return await spec.func(*args, **kwargs)

        payload = await self._read(spec, key)
        if payload is not None:
            decoded = self._decode(spec, key, payload)
            if decoded is not _UNDECODABLE:
                return decoded
        return await self._flight.run(key, functools.partial(self._load, spec, key, args, kwargs))

    async def _read(self, spec: _CallSpec, key: str) -> Any:
        """Read the stored payload, answering a miss when the backend is down.

        A cache is an optimisation, and a Redis outage that raised here would
        fail every cached call in the process — trading a cache problem for an
        application outage, which is exactly what the write path already
        refuses to do. The transport raises whatever its client defines, so
        the guard is deliberately wide; cancellation is a ``BaseException``
        and still propagates.

        Returns:
            The stored payload, or ``None`` — the backends' miss sentinel.
        """
        try:
            return await self._cache.get_value(key)
        except Exception as error:
            self._warn_once("CacheCallReadFailed", spec.func, key=key, error=type(error).__name__)
            return None

    def _decode(self, spec: _CallSpec, key: str, payload: Any) -> Any:
        """Decode a cached payload, or report it unusable.

        A payload written before the declared type gained a field no longer
        fits it, and nothing about the entry changes when the type does.
        Raising here would fail every caller of a warm key for a whole TTL, in
        a cache that by decision has no invalidation, so the entry is treated
        as a miss and the load that follows overwrites it.

        Returns:
            The decoded value, or :data:`_UNDECODABLE`, which is not ``None``
            because ``None`` is a value a codec can legitimately return.
        """
        try:
            return spec.codec.decode(payload)
        # ``pydantic.ValidationError`` is a ``ValueError``; naming it would
        # import pydantic eagerly, which this package must not do.  Only the
        # class name is logged: pydantic renders ``input_value=`` into its text,
        # so the error itself would carry another caller's cached answer into a
        # WARNING record.
        except (msgspec.ValidationError, ValueError, TypeError) as error:
            self._warn_once(
                "CacheCallPayloadMismatch", spec.func, key=key, error=type(error).__name__
            )
            return _UNDECODABLE

    async def _load(
        self,
        spec: _CallSpec,
        key: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> Any:
        """Run the body once and store its result, unless the policy refuses.

        The encoded value is returned rather than the body's own object, so a
        hit and a miss answer the same shape — except when ``unless`` is
        truthy, the one case where the caller sees what the body built.
        """
        result = await spec.func(*args, **kwargs)
        if spec.policy.unless is not None and spec.policy.unless(result):
            return result
        encoded = spec.codec.encode(result)
        try:
            await self._cache.set_value(key, encoded.payload, ttl=self._write_ttl(spec.policy))
        except Exception as error:
            # Divergence from ``CachedRepository``, which lets this propagate:
            # the answer is already produced, and a cache write must not fail a
            # call that succeeded.  Wider than ``CacheWriteError``, which the
            # gateway raises only for a value its serializer refused: a backend
            # that is unreachable raises whatever its transport defines, and
            # that must degrade the call to an uncached one rather than fail
            # it.  Only the store is inside the guard — the body's own errors
            # were raised before it and propagate untouched.
            self._warn_once(
                "CacheCallWriteFailed",
                spec.func,
                key=key,
                value_type=type(result).__name__,
                error=repr(error),
            )
        return encoded.value

    def _write_ttl(self, policy: _CallPolicy) -> int:
        """Resolve the declared TTL and spread it inside the configured jitter.

        ``ttl_key`` shares the ``ttl:`` namespace with entity overrides, so a
        key equal to an entity name deliberately shares that entity's TTL. The
        spread is applied where the value reaches the backend, so a burst of
        writes does not expire at the same instant; with ``ttl_jitter`` at zero
        the resolved value is written as it is.
        """
        ttl = (
            self._config.default_ttl
            if policy.ttl_key is None
            else self._config.ttl_for_single(policy.ttl_key)
        )
        jitter = self._config.ttl_jitter
        if jitter <= 0.0:
            return ttl
        spread = ttl * jitter
        return max(1, round(ttl + self._rng.uniform(-spread, spread)))


class _UnconfiguredCalls:
    """Pass-through binder for a deployment with no usable ``cache:`` section.

    It announces, once per marked coroutine it is asked to bind, that the
    coroutine runs uncached, so the deployment learns at boot rather than from
    its bill.
    """

    __slots__ = ("_warn_once",)

    def __init__(self) -> None:
        self._warn_once = _WarnOnce()

    def wrap(self, func: AsyncCallable) -> AsyncCallable:
        """Return *func* unchanged, announcing it when it is marked.

        Args:
            func: Coroutine, marked or not.

        Returns:
            *func* itself.
        """
        if declares_cache_call(func) is not None:
            self._warn_once("CacheCallNotConfigured", func)
        return func

    def bind(self, obj: object) -> list[AsyncCallable]:
        """Return the public coroutine methods of *obj*, all uncached.

        Args:
            obj: Instance to publish.

        Returns:
            The bound methods, in declaration order, base class first.
        """
        return [self.wrap(method) for method in _coroutine_methods(obj, self._warn_once)]
