from __future__ import annotations

from typing import Any

from loom.core.job.callback import JobCallback, NullJobCallback

# ---------------------------------------------------------------------------
# NullJobCallback satisfies the protocol
# ---------------------------------------------------------------------------


def test_null_callback_satisfies_protocol() -> None:
    assert isinstance(NullJobCallback(), JobCallback)


def test_null_callback_on_success_does_not_raise() -> None:
    cb = NullJobCallback()
    cb.on_success("job-1", result=42, user_id=99)


def test_null_callback_on_failure_does_not_raise() -> None:
    cb = NullJobCallback()
    cb.on_failure("job-1", exc_type="ValueError", exc_msg="boom", user_id=99)


def test_null_callback_on_success_returns_none() -> None:
    assert NullJobCallback().on_success("j", result=None) is None


def test_null_callback_on_failure_returns_none() -> None:
    assert NullJobCallback().on_failure("j", exc_type="E", exc_msg="m") is None


# ---------------------------------------------------------------------------
# Async implementations also satisfy the protocol (structural check)
# ---------------------------------------------------------------------------


def test_async_on_success_satisfies_protocol() -> None:
    class AsyncCallback:
        async def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
            # Intentionally empty: protocol conformance test only.
            return None

        async def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
            # Intentionally empty: protocol conformance test only.
            return None

    assert isinstance(AsyncCallback(), JobCallback)


def test_partial_implementation_does_not_satisfy_protocol() -> None:
    class MissingOnFailure:
        def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
            # Intentionally empty: the test asserts missing on_failure.
            return None

    assert not isinstance(MissingOnFailure(), JobCallback)


# ---------------------------------------------------------------------------
# Custom sync callback
# ---------------------------------------------------------------------------


def test_custom_sync_callback_satisfies_protocol() -> None:
    calls: list[str] = []

    class RecordingCallback:
        def on_success(self, job_id: str, result: Any, **ctx: Any) -> None:
            calls.append(f"success:{job_id}")

        def on_failure(self, job_id: str, exc_type: str, exc_msg: str, **ctx: Any) -> None:
            calls.append(f"failure:{job_id}")

    cb = RecordingCallback()
    assert isinstance(cb, JobCallback)
    cb.on_success("j1", result=1)
    cb.on_failure("j2", exc_type="E", exc_msg="m")
    assert calls == ["success:j1", "failure:j2"]
