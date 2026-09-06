from __future__ import annotations

from typing import Any

import pytest

from loom.core.repository.abc import Creatable, Readable

from .conftest import SEED, BackendCase, require


class _Abort(Exception):
    pass


async def test_commit_makes_writes_visible(case: BackendCase, repository: Any) -> None:
    require(case, Creatable, Readable)

    async with case.unit_of_work(repository):
        created = await repository.create(SEED[0])

    assert await repository.get_by_id(created.id) == created


async def test_rollback_discards_writes(case: BackendCase, repository: Any) -> None:
    require(case, Creatable, Readable)
    if not case.transactions:
        pytest.skip(f"{case.name} does not roll back writes")

    with pytest.raises(_Abort):
        async with case.unit_of_work(repository):
            await repository.create(SEED[0])
            raise _Abort

    assert await repository.get_by_id(SEED[0].id) is None
