"""``DynamoUnitOfWork``: no-op, writes autocommit per item — the signal stays closed."""

from __future__ import annotations

import pytest

from loom.core.repository.dynamodb.uow import DynamoUnitOfWorkFactory
from loom.core.transaction import in_atomic_transaction

pytestmark = pytest.mark.asyncio


async def test_leaves_the_atomic_transaction_signal_closed() -> None:
    """F03: writes autocommit here, so the cache bump must stay inline."""
    factory = DynamoUnitOfWorkFactory()

    async with factory.create():
        assert in_atomic_transaction() is False

    assert in_atomic_transaction() is False
