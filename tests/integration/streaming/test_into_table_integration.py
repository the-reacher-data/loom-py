"""Integration tests for IntoTable — SQLAlchemy backend against real SQLite.

Each test uses a file-backed SQLite database in ``tmp_path`` so that the
partition's own engine and the assertion engine share the same data.
"""

from __future__ import annotations

from pathlib import Path

import msgspec
from sqlalchemy import create_engine, text

from loom.streaming import Backend, IntoTable


class _OrderRow(msgspec.Struct):
    order_id: int
    amount: float
    status: str


def _make_db(tmp_path: Path, ddl: str) -> str:
    url = f"sqlite:///{tmp_path / 'sink.db'}"
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text(ddl))
    engine.dispose()
    return url


class TestIntoTableSQLAlchemyIntegration:
    def test_write_batch_inserts_rows(self, tmp_path: Path) -> None:
        url = _make_db(
            tmp_path,
            "CREATE TABLE orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        node = IntoTable(payload=_OrderRow, table="orders", backend=Backend.SQLALCHEMY)
        partition = node.build_partition({"url": url}, worker_index=0, worker_count=1)

        partition.write_batch(
            [
                _OrderRow(order_id=1, amount=99.9, status="shipped"),
                _OrderRow(order_id=2, amount=49.5, status="pending"),
            ]
        )
        partition.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT order_id, amount, status FROM orders ORDER BY order_id")
            ).fetchall()
        engine.dispose()

        assert len(rows) == 2
        assert rows[0] == (1, 99.9, "shipped")
        assert rows[1] == (2, 49.5, "pending")

    def test_empty_batch_is_noop(self, tmp_path: Path) -> None:
        url = _make_db(
            tmp_path,
            "CREATE TABLE orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        node = IntoTable(payload=_OrderRow, table="orders")
        partition = node.build_partition({"url": url}, worker_index=0, worker_count=1)
        partition.write_batch([])
        partition.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        engine.dispose()

        assert count == 0

    def test_config_table_overrides_dsl_table(self, tmp_path: Path) -> None:
        url = _make_db(
            tmp_path,
            "CREATE TABLE sink_orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        node = IntoTable(payload=_OrderRow, table="orders")
        partition = node.build_partition(
            {"url": url, "table": "sink_orders"}, worker_index=0, worker_count=1
        )
        partition.write_batch([_OrderRow(order_id=10, amount=5.0, status="done")])
        partition.close()

        engine = create_engine(url)
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT order_id FROM sink_orders")).fetchall()
        engine.dispose()

        assert rows == [(10,)]
