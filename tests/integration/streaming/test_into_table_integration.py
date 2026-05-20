"""Integration tests for IntoTable — SQLAlchemy backend against real SQLite.

Each test uses a file-backed SQLite database in ``tmp_path`` so that the
partition's own engine and the assertion engine share the same data.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import polars as pl
import pytest
from sqlalchemy import create_engine, event, text

from loom.core.async_bridge import AsyncBridge
from loom.core.config import ConfigContext
from loom.core.model import LoomStruct
from loom.core.repository.sqlalchemy.session_manager import SessionManager
from loom.streaming import Backend, FromTopic, IntoTable, Process, StreamFlow
from loom.streaming.nodes import _table as _table_module
from loom.streaming.testing import StreamingTestRunner


class _OrderRow(LoomStruct):
    order_id: int
    amount: float
    status: str


def _make_db(tmp_path: Path, ddl: str) -> Path:
    path = tmp_path / "sink.db"
    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as conn:
        conn.execute(text(ddl))
    engine.dispose()
    return path


def _sync_url(path: Path) -> str:
    return f"sqlite:///{path}"


def _async_url(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path}"


class TestIntoTableSQLAlchemyIntegration:
    def test_write_batch_inserts_rows(self, tmp_path: Path) -> None:
        path = _make_db(
            tmp_path,
            "CREATE TABLE orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        bridge = AsyncBridge()
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        node = IntoTable(payload=_OrderRow, table="orders", backend=Backend.SQLALCHEMY)
        partition = node.build_partition(
            {"chunk_size": 500},
            worker_index=0,
            worker_count=1,
            bridge=bridge,
            session_manager=session_manager,
        )

        try:
            partition.write_batch(
                [
                    _OrderRow(order_id=1, amount=99.9, status="shipped"),
                    _OrderRow(order_id=2, amount=49.5, status="pending"),
                ]
            )
            partition.close()
        finally:
            bridge.run(session_manager.dispose())
            bridge.shutdown()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT order_id, amount, status FROM orders ORDER BY order_id")
            ).fetchall()
        engine.dispose()

        assert len(rows) == 2
        assert rows[0] == (1, 99.9, "shipped")
        assert rows[1] == (2, 49.5, "pending")

    def test_empty_batch_is_noop(self, tmp_path: Path) -> None:
        path = _make_db(
            tmp_path,
            "CREATE TABLE orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        bridge = AsyncBridge()
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        node = IntoTable(payload=_OrderRow, table="orders")
        partition = node.build_partition(
            {"chunk_size": 500},
            worker_index=0,
            worker_count=1,
            bridge=bridge,
            session_manager=session_manager,
        )
        try:
            partition.write_batch([])
            partition.close()
        finally:
            bridge.run(session_manager.dispose())
            bridge.shutdown()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        engine.dispose()

        assert count == 0

    def test_config_table_overrides_dsl_table(self, tmp_path: Path) -> None:
        path = _make_db(
            tmp_path,
            "CREATE TABLE sink_orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        bridge = AsyncBridge()
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        node = IntoTable(payload=_OrderRow, table="orders")
        partition = node.build_partition(
            {"table": "sink_orders", "chunk_size": 500},
            worker_index=0,
            worker_count=1,
            bridge=bridge,
            session_manager=session_manager,
        )
        try:
            partition.write_batch([_OrderRow(order_id=10, amount=5.0, status="done")])
            partition.close()
        finally:
            bridge.run(session_manager.dispose())
            bridge.shutdown()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            rows = conn.execute(text("SELECT order_id FROM sink_orders")).fetchall()
        engine.dispose()

        assert list(rows) == [(10,)]

    def test_multiple_kafka_events_flush_once_when_buffer_is_retained(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        path = _make_db(
            tmp_path,
            "CREATE TABLE orders (order_id INTEGER, amount REAL, status TEXT)",
        )
        session_manager = SessionManager.from_config({"url": _async_url(path)})
        insert_calls: list[int] = []

        @event.listens_for(session_manager.engine.sync_engine, "before_cursor_execute")
        def _count_inserts(
            conn: object,
            cursor: object,
            statement: str,
            parameters: object,
            context: object,
            executemany: bool,
        ) -> None:
            del conn, cursor, parameters, context
            if statement.lstrip().upper().startswith("INSERT INTO ORDERS"):
                insert_calls.append(1)

        monkeypatch.setattr(
            SessionManager,
            "from_config",
            classmethod(lambda cls, config, **kwargs: session_manager),
        )

        flow: StreamFlow[_OrderRow, _OrderRow] = StreamFlow(
            name="orders_sink_flow",
            source=FromTopic("orders.raw", payload=_OrderRow),
            process=Process(
                cast(
                    Any,
                    IntoTable(
                        payload=_OrderRow,
                        table="orders",
                        backend=Backend.SQLALCHEMY,
                        name="orders_sink",
                    ),
                ),
            ),
        )
        config = ConfigContext.from_dict(
            {
                "kafka": {
                    "consumer": {
                        "brokers": ["localhost:9092"],
                        "group_id": "test",
                        "topics": ["orders.raw"],
                    }
                },
                "database": {
                    "warehouse": {
                        "url": _async_url(path),
                    },
                },
                "streaming": {
                    "sinks": {
                        "orders_sink": {
                            "database": "warehouse",
                            "table": "orders",
                            "chunk_size": 500,
                            "buffer_max_records": 10_000,
                            "buffer_max_bytes": 10_000_000,
                            "buffer_max_age_s": 9999.0,
                        }
                    }
                },
            }
        )

        runner = StreamingTestRunner.from_flow(flow, config=config)
        runner.with_payloads(
            [
                _OrderRow(order_id=1, amount=10.0, status="new"),
                _OrderRow(order_id=2, amount=20.0, status="paid"),
                _OrderRow(order_id=3, amount=30.0, status="sent"),
            ]
        ).run()

        engine = create_engine(_sync_url(path))
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT order_id, amount, status FROM orders ORDER BY order_id")
            ).fetchall()
        engine.dispose()

        assert len(rows) == 3
        assert len(insert_calls) == 1

    def test_multiple_kafka_events_flush_once_for_delta(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        pytest.importorskip("deltalake")

        uri = str(tmp_path / "events_delta")
        write_calls: list[int] = []
        original_write_deltalake = cast(Any, _table_module).write_deltalake

        def _counted_write_deltalake(*args: Any, **kwargs: Any) -> Any:
            write_calls.append(1)
            return original_write_deltalake(*args, **kwargs)

        monkeypatch.setattr(cast(Any, _table_module), "write_deltalake", _counted_write_deltalake)

        flow: StreamFlow[_OrderRow, _OrderRow] = StreamFlow(
            name="events_delta_sink_flow",
            source=FromTopic("events.raw", payload=_OrderRow),
            process=Process(
                cast(
                    Any,
                    IntoTable(
                        payload=_OrderRow,
                        table="events",
                        backend=Backend.DELTA,
                        name="events_sink",
                    ),
                )
            ),
        )
        config = ConfigContext.from_dict(
            {
                "kafka": {
                    "consumer": {
                        "brokers": ["localhost:9092"],
                        "group_id": "test",
                        "topics": ["events.raw"],
                    }
                },
                "streaming": {
                    "sinks": {
                        "events_sink": {
                            "uri": uri,
                            "buffer_max_records": 10_000,
                            "buffer_max_bytes": 10_000_000,
                            "buffer_max_age_s": 9999.0,
                            "spool_max_bytes": 1024,
                        }
                    }
                },
            }
        )

        runner = StreamingTestRunner.from_flow(flow, config=config)
        runner.with_payloads(
            [
                _OrderRow(order_id=1, amount=10.0, status="new"),
                _OrderRow(order_id=2, amount=20.0, status="paid"),
                _OrderRow(order_id=3, amount=30.0, status="sent"),
            ]
        ).run()

        frame = pl.scan_delta(uri).sort("order_id").collect()

        assert frame.to_dicts() == [
            {"order_id": 1, "amount": 10.0, "status": "new"},
            {"order_id": 2, "amount": 20.0, "status": "paid"},
            {"order_id": 3, "amount": 30.0, "status": "sent"},
        ]
        assert len(write_calls) == 1
