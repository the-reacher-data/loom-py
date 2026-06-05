"""Route stdlib log records to the right Prefect TaskRun.

Loom steps emit logs through Python's standard ``logging`` module (or
``structlog`` which is configured to feed stdlib). With the synthetic
``TaskRun`` pattern those logs never reach the Prefect UI because the
step's logic does not run inside a ``@prefect.task`` context.

This module bridges that gap **without** coupling steps to Prefect:

- A ``ContextVar`` tracks the active ``task_run_id`` (set by
  :class:`PrefectTaskRunObserver` while a STEP is running).
- :class:`PrefectLogBridgeHandler` reads that ContextVar in ``emit`` and
  ships each ``LogRecord`` to Prefect via ``client.create_logs(...)``,
  tagged with the right ``task_run_id`` (or ``flow_run_id`` when no step
  is active).
- A background worker thread drains a queue in small batches so the
  hot path never blocks on the Prefect API.

The handler is installed once per flow run from
:func:`loom.prefect.flow._body.build_flow_body` and uninstalled in a
``finally`` block.
"""

from __future__ import annotations

import contextlib
import logging
import queue
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

_current_task_run_id: ContextVar[uuid.UUID | None] = ContextVar(
    "loom_prefect_task_run_id",
    default=None,
)
_current_flow_run_id: ContextVar[uuid.UUID | None] = ContextVar(
    "loom_prefect_flow_run_id",
    default=None,
)


@contextmanager
def bind_task_run(task_run_id: uuid.UUID) -> Iterator[None]:
    """Bind *task_run_id* as the active step's TaskRun for the current context.

    Records emitted while this context manager is active are routed to
    the given TaskRun. Outside it (between steps, or in observer code
    that runs after STEP_END) records fall back to the flow run.

    Args:
        task_run_id: The Prefect TaskRun UUID returned by
            ``client.create_task_run(...)``.
    """
    token = _current_task_run_id.set(task_run_id)
    try:
        yield
    finally:
        _current_task_run_id.reset(token)


def current_task_run_id() -> uuid.UUID | None:
    """Return the active TaskRun id, or ``None`` if no step is running."""
    return _current_task_run_id.get()


def current_flow_run_id() -> uuid.UUID | None:
    """Return the active flow run id set by :func:`install_log_bridge`."""
    return _current_flow_run_id.get()


# ---------------------------------------------------------------------------
# Handler + worker
# ---------------------------------------------------------------------------


class PrefectLogBridgeHandler(logging.Handler):
    """``logging.Handler`` that ships records to the Prefect logs API.

    Records carry the current ``task_run_id`` if one is bound; otherwise
    they fall back to the installed ``flow_run_id``.

    A daemon worker thread drains the internal queue and posts batches
    of up to :data:`_BATCH_SIZE` records every :data:`_FLUSH_SECONDS` so
    the hot path never blocks on the API. Errors during emit/post are
    routed through ``logging.Handler.handleError`` and never raised back
    to the caller — losing a log line is preferable to crashing a step.
    """

    _BATCH_SIZE = 100
    _FLUSH_SECONDS = 1.0
    _SENTINEL: Any = object()
    _MAX_ATTEMPTS = 3
    _BACKOFF_BASE_SECONDS = 1.0

    def __init__(self) -> None:
        super().__init__(level=logging.NOTSET)
        self._queue: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._worker: threading.Thread | None = None
        self._closed = False
        self.dropped_logs = 0
        self._sleep = time.sleep

    def start(self) -> None:
        """Spawn the background drain worker. Idempotent."""
        if self._worker is not None:
            return
        self._worker = threading.Thread(
            target=self._drain_loop,
            name="loom-prefect-log-bridge",
            daemon=True,
        )
        self._worker.start()

    # ------------------------------------------------------------------
    # Handler API
    # ------------------------------------------------------------------

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D401 - inherited
        if self._closed:
            return
        try:
            payload = self._record_to_payload(record)
        except Exception:  # noqa: BLE001
            self.handleError(record)
            return
        if payload is None:
            return
        self._queue.put_nowait(payload)

    def close(self) -> None:  # noqa: D401 - inherited
        """Drain pending logs and stop the worker."""
        if self._closed:
            return
        self._closed = True
        self._queue.put_nowait(self._SENTINEL)
        worker = self._worker
        if worker is not None:
            worker.join(timeout=5.0)
        super().close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _record_to_payload(self, record: logging.LogRecord) -> dict[str, Any] | None:
        task_run_id = _current_task_run_id.get()
        flow_run_id = _current_flow_run_id.get()
        # Without any binding we have nowhere to send the record.
        if task_run_id is None and flow_run_id is None:
            return None
        payload: dict[str, Any] = {
            "name": record.name,
            "level": record.levelno,
            "message": record.getMessage(),
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC),
        }
        if task_run_id is not None:
            payload["task_run_id"] = task_run_id
        else:
            payload["flow_run_id"] = flow_run_id
        return payload

    def _drain_loop(self) -> None:
        from loom.prefect._async import run_sync  # noqa: PLC0415

        batch: list[dict[str, Any]] = []
        while True:
            try:
                item = self._queue.get(timeout=self._FLUSH_SECONDS)
            except queue.Empty:
                if batch:
                    self._flush_with_retry(batch, run_sync_fn=run_sync)
                    batch = []
                continue
            if item is self._SENTINEL:
                if batch:
                    self._flush_with_retry(batch, run_sync_fn=run_sync)
                while True:
                    try:
                        leftover = self._queue.get_nowait()
                    except queue.Empty:
                        return
                    if leftover is self._SENTINEL:
                        continue
                    self._flush_with_retry([leftover], run_sync_fn=run_sync)
                return
            batch.append(item)
            if len(batch) >= self._BATCH_SIZE:
                self._flush_with_retry(batch, run_sync_fn=run_sync)
                batch = []

    def _flush_with_retry(self, batch: list[dict[str, Any]], run_sync_fn: Any) -> None:
        for attempt in range(1, self._MAX_ATTEMPTS + 1):
            try:
                self._post_batch(batch, run_sync_fn)
                return
            except Exception:  # noqa: BLE001
                if attempt >= self._MAX_ATTEMPTS:
                    break
                self._sleep(self._BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
        self.dropped_logs += len(batch)
        with contextlib.suppress(Exception):
            self.handleError(logging.LogRecord(__name__, logging.ERROR, "", 0, "", None, None))

    def _post_batch(self, batch: list[dict[str, Any]], run_sync_fn: Any) -> None:
        from prefect.client.orchestration import get_client  # noqa: PLC0415
        from prefect.client.schemas.actions import LogCreate  # noqa: PLC0415

        logs = [LogCreate(**payload) for payload in batch]

        async def _send() -> None:
            async with get_client() as client:
                await client.create_logs(logs)

        run_sync_fn(_send())


# ---------------------------------------------------------------------------
# Install / uninstall
# ---------------------------------------------------------------------------

_handler_lock = threading.Lock()
_installed: PrefectLogBridgeHandler | None = None
_flow_run_token: Any = None


def install_log_bridge(flow_run_id: uuid.UUID | None) -> None:
    """Attach the bridge handler to the root logger and bind *flow_run_id*.

    Idempotent: a second call within the same process is a no-op. The
    handler is removed by :func:`uninstall_log_bridge` (typically from a
    ``finally`` block in the flow body).

    Args:
        flow_run_id: The Prefect FlowRun id used as fallback target when
            no TaskRun is bound. ``None`` is accepted and disables the
            install entirely (so unit tests / standalone runs don't pay
            any cost).
    """
    global _installed, _flow_run_token
    if flow_run_id is None:
        return
    with _handler_lock:
        if _installed is not None:
            return
        handler = PrefectLogBridgeHandler()
        handler.start()
        logging.getLogger().addHandler(handler)
        _installed = handler
        _flow_run_token = _current_flow_run_id.set(flow_run_id)


def uninstall_log_bridge() -> None:
    """Detach the bridge handler and clear the flow_run_id binding."""
    global _installed, _flow_run_token
    with _handler_lock:
        if _installed is None:
            return
        logging.getLogger().removeHandler(_installed)
        _installed.close()
        _installed = None
        if _flow_run_token is not None:
            # Token may have been created in another context — best effort.
            with contextlib.suppress(ValueError):
                _current_flow_run_id.reset(_flow_run_token)
            _flow_run_token = None


__all__ = [
    "PrefectLogBridgeHandler",
    "bind_task_run",
    "current_flow_run_id",
    "current_task_run_id",
    "install_log_bridge",
    "uninstall_log_bridge",
]
