"""The cache module wraps every ``@cached`` repository the registration module builds.

AC2, AC3 and AC4 of spec 009: the decorator hook fires once per model
whichever module is loaded first, the wrapped instance is shared by every key
of the model, invalidation lands in the counter alias, not the data one, and a
deployment without the section learns at boot which marked classes run uncached.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Sequence
from contextlib import ExitStack
from typing import Any

import pytest
from aiocache import caches

from loom.core.cache import CacheConfig, CachedRepository
from loom.core.cache.decorators import cached
from loom.core.cache.wiring import CacheGateways, cache_module, cache_module_for
from loom.core.config import ConfigContext
from loom.core.di.container import LoomContainer, ResolutionError
from loom.core.repository import (
    RepositoryBuildContext,
    RepositoryDecorator,
    RepositoryRegistration,
    build_repository_registration_module,
)
from loom.core.repository.abc.repo_for import Listable, Readable

from ._doubles import (
    MEMORY_BACKEND,
    SERIALIZED_BACKEND,
    CachedWidgetRepository,
    CodeWidget,
    Widget,
    WidgetUpdate,
    WritableRepository,
    open_transaction_with_channel,
    registered_repository,
)


def _backend(alias: str) -> Any:
    """Return the aiocache instance behind ``alias`` (typed ``object`` by aiocache's stubs)."""
    return caches.get(alias)


Module = Callable[[LoomContainer], None]


class PlainCodeWidgetRepository(
    WritableRepository[CodeWidget], Readable[CodeWidget], Listable[CodeWidget]
):
    """Unmarked double with the same capability keys as the cached one."""

    def __init__(self, rows: Sequence[CodeWidget] = ()) -> None:
        super().__init__(rows, CodeWidget)


class _SpyBuilder:
    """Repository builder that keeps every instance it built."""

    def __init__(self) -> None:
        self.built: list[Any] = []

    def __call__(
        self, context: RepositoryBuildContext, registration: RepositoryRegistration
    ) -> Any:
        rows = [Widget(id=1, name="one")] if registration.model is Widget else []
        repository = registration.repository_type(rows)
        self.built.append(repository)
        return repository


class _Tagged:
    """What a user-registered decorator returns."""

    def __init__(self, repository: Any) -> None:
        self.repository = repository


def _user_decorator(repository: Any) -> Any:
    return _Tagged(repository)


class _TaggingDecorator:
    """User-registered decorator whose class name the warning must report."""

    def __call__(self, repository: Any) -> Any:
        return _Tagged(repository)


@cached
class ReadOnlyWidgetRepository:
    """Marked double that only reads, so the wrapper would advertise writes it lacks."""

    def __init__(self, rows: Sequence[Widget] = ()) -> None:
        self.rows = list(rows)

    async def get_by_id(self, obj_id: int, profile: str = "default") -> Widget | None:
        return next((row for row in self.rows if row.id == obj_id), None)


def _single_alias() -> CacheConfig:
    return CacheConfig(aiocache_config={"default": dict(SERIALIZED_BACKEND)})


def _two_aliases() -> CacheConfig:
    return CacheConfig(
        aiocache_alias="data",
        counter_alias="counters",
        aiocache_config={"data": dict(SERIALIZED_BACKEND), "counters": dict(MEMORY_BACKEND)},
    )


@pytest.fixture
def builder() -> _SpyBuilder:
    return _SpyBuilder()


@pytest.fixture
def repository_module(builder: _SpyBuilder) -> Iterator[Module]:
    with ExitStack() as stack:
        stack.enter_context(registered_repository(Widget, CachedWidgetRepository))
        stack.enter_context(registered_repository(CodeWidget, PlainCodeWidgetRepository))
        yield build_repository_registration_module(
            models=(),
            explicit_models=(Widget, CodeWidget),
            build_registered_repository=builder,
        )


def _container(*modules: Module) -> LoomContainer:
    container = LoomContainer()
    for module in modules:
        module(container)
    return container


def _in_either_order(first: Module, second: Module, cache_first: bool) -> LoomContainer:
    return _container(second, first) if cache_first else _container(first, second)


class TestDecoration:
    @pytest.mark.parametrize("cache_first", [False, True])
    def test_a_marked_repository_is_served_wrapped(
        self, repository_module: Module, cache_first: bool
    ) -> None:
        container = _in_either_order(repository_module, cache_module(_single_alias()), cache_first)

        assert isinstance(container.resolve_repo(Widget), CachedRepository)

    def test_every_key_of_the_model_resolves_the_same_instance(
        self, repository_module: Module
    ) -> None:
        container = _container(repository_module, cache_module(_single_alias()))

        primary = container.resolve_repo(Widget)

        assert container.resolve(Readable[Widget]) is primary
        assert container.resolve(Listable[Widget]) is primary

    @pytest.mark.parametrize("cache_first", [False, True])
    def test_an_unmarked_repository_is_served_bare(
        self, repository_module: Module, cache_first: bool
    ) -> None:
        container = _in_either_order(repository_module, cache_module(_single_alias()), cache_first)

        primary = container.resolve_repo(CodeWidget)

        assert isinstance(primary, PlainCodeWidgetRepository)
        assert container.resolve(Readable[CodeWidget]) is primary
        assert container.resolve(Listable[CodeWidget]) is primary

    def test_a_decorator_registered_before_the_cache_module_is_kept(
        self, repository_module: Module
    ) -> None:
        container = LoomContainer()
        container.register_instance(RepositoryDecorator, _user_decorator)
        cache_module(_single_alias())(container)
        repository_module(container)

        assert container.resolve(RepositoryDecorator) is _user_decorator
        assert isinstance(container.resolve_repo(Widget), _Tagged)

    def test_names_the_decorator_that_already_held_the_slot(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = LoomContainer()
        container.register_instance(RepositoryDecorator, _TaggingDecorator())

        with caplog.at_level(logging.WARNING, logger="loom.core.cache.wiring"):
            cache_module(_single_alias())(container)

        warnings = [
            record
            for record in caplog.records
            if "CacheDecoratorAlreadyRegistered" in record.getMessage()
        ]
        assert len(warnings) == 1
        assert "_TaggingDecorator" in warnings[0].getMessage()

    def test_two_aliases_open_two_gateways(self) -> None:
        container = _container(cache_module(_two_aliases()))

        gateways = container.resolve(CacheGateways)

        assert gateways.counters is not gateways.data
        assert list(gateways.distinct()) == [gateways.data, gateways.counters]

    def test_one_alias_opens_a_single_gateway(self) -> None:
        container = _container(cache_module(_single_alias()))

        gateways = container.resolve(CacheGateways)

        assert gateways.counters is gateways.data
        assert list(gateways.distinct()) == [gateways.data]


class TestIncompleteRepository:
    def test_refuses_a_marked_repository_that_cannot_be_delegated_to(self) -> None:
        with registered_repository(Widget, ReadOnlyWidgetRepository):
            module = build_repository_registration_module(
                models=(),
                explicit_models=(Widget,),
                build_registered_repository=_SpyBuilder(),
            )
            container = _container(module, cache_module(_single_alias()))

            with pytest.raises(RuntimeError, match="ReadOnlyWidgetRepository") as info:
                container.resolve_repo(Widget)

        assert "create" in str(info.value)
        assert "list_paginated" in str(info.value)

    def test_boot_validation_names_the_offending_repository(self) -> None:
        """``validate()`` wraps the refusal, so the aborted boot still names the class."""
        with registered_repository(Widget, ReadOnlyWidgetRepository):
            module = build_repository_registration_module(
                models=(),
                explicit_models=(Widget,),
                build_registered_repository=_SpyBuilder(),
            )
            container = _container(module, cache_module(_single_alias()))

            with pytest.raises(ResolutionError, match="ReadOnlyWidgetRepository") as info:
                container.validate()

        assert "create" in str(info.value)
        assert "list_paginated" in str(info.value)


class TestInvalidationThroughTheContainer:
    async def test_the_second_read_is_served_from_the_cache(
        self, repository_module: Module, builder: _SpyBuilder
    ) -> None:
        container = _container(repository_module, cache_module(_two_aliases()))
        wrapped = container.resolve_repo(Widget)
        inner = builder.built[0]
        assert isinstance(inner, CachedWidgetRepository)

        await wrapped.get_by_id(1)
        await wrapped.get_by_id(1)

        assert inner.get_by_id_calls == 1

    async def test_a_committed_write_sends_the_next_read_through(
        self, repository_module: Module, builder: _SpyBuilder
    ) -> None:
        container = _container(repository_module, cache_module(_two_aliases()))
        wrapped = container.resolve_repo(Widget)
        inner = builder.built[0]
        await wrapped.get_by_id(1)

        with open_transaction_with_channel() as channel:
            await wrapped.update(1, WidgetUpdate(name="uno"))
        await channel.drain(committed=True)
        await wrapped.get_by_id(1)

        assert inner.get_by_id_calls == 2
        assert await _backend("counters").exists("tag:widget:id:1")
        assert await _backend("counters").exists("tag:widget:list")
        assert not await _backend("data").exists("tag:widget:id:1")
        assert not await _backend("data").exists("tag:widget:list")


def _not_configured_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.levelno == logging.WARNING and "CacheNotConfigured" in record.getMessage()
    ]


class TestCacheNotConfigured:
    @pytest.fixture(autouse=True)
    def _capture_wiring_warnings(self, caplog: pytest.LogCaptureFixture) -> None:
        caplog.set_level(logging.WARNING, logger="loom.core.cache.wiring")

    @pytest.mark.parametrize(
        "raw",
        [pytest.param({}, id="absent"), pytest.param({"cache": {"enabled": False}}, id="disabled")],
    )
    def test_boot_warns_once_for_a_marked_class_without_the_section(
        self, repository_module: Module, caplog: pytest.LogCaptureFixture, raw: dict[str, Any]
    ) -> None:
        container = _container(repository_module, cache_module_for(ConfigContext.from_dict(raw)))

        container.validate()

        warnings = _not_configured_warnings(caplog)
        assert len(warnings) == 1
        assert "CachedWidgetRepository" in warnings[0].getMessage()
        assert isinstance(container.resolve_repo(Widget), CachedWidgetRepository)

    def test_resolving_after_validate_warns_no_further(
        self, repository_module: Module, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = _container(repository_module, cache_module_for(ConfigContext.from_dict({})))
        container.validate()
        caplog.clear()

        container.resolve_repo(Widget)
        container.resolve(Readable[Widget])

        assert _not_configured_warnings(caplog) == []

    def test_an_unmarked_class_is_never_announced(
        self, repository_module: Module, caplog: pytest.LogCaptureFixture
    ) -> None:
        container = _container(repository_module, cache_module_for(ConfigContext.from_dict({})))
        container.validate()

        assert all(
            "PlainCodeWidgetRepository" not in record.getMessage()
            for record in _not_configured_warnings(caplog)
        )

    def test_an_enabled_section_wraps_and_stays_quiet(
        self, repository_module: Module, caplog: pytest.LogCaptureFixture
    ) -> None:
        ctx = ConfigContext.from_dict(
            {"cache": {"aiocache_config": {"default": dict(SERIALIZED_BACKEND)}}}
        )
        container = _container(repository_module, cache_module_for(ctx))

        container.validate()

        assert isinstance(container.resolve_repo(Widget), CachedRepository)
        assert _not_configured_warnings(caplog) == []
