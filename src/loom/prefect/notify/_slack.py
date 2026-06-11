"""Slack webhook notifier."""

from __future__ import annotations

import json as _json
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from loom.core.logger import get_logger
from loom.prefect.notify._event import NotifyEvent

_log = get_logger(__name__)


HttpPost = Callable[..., Any]


class SlackNotifier:
    """Post terminal flow events to a Slack incoming webhook.

    Args:
        webhook_url: Slack incoming webhook URL.
        on_failure: Post when state is ``"Failed"`` or ``"Crashed"``.
        on_completion: Post when state is ``"Completed"``.
        channel: Optional channel override (``"#data-alerts"``). When
            ``None``, Slack uses the webhook's default channel.
        http_post: Injected HTTP poster used by tests. Defaults to
            ``urllib.request``-backed implementation that has no extra
            runtime dependency.
    """

    def __init__(
        self,
        *,
        webhook_url: str,
        on_failure: bool = True,
        on_completion: bool = False,
        channel: str | None = None,
        http_post: HttpPost | None = None,
    ) -> None:
        self._webhook_url = webhook_url
        self._on_failure = on_failure
        self._on_completion = on_completion
        self._channel = channel
        self._http_post = http_post or _default_http_post

    def notify(self, event: NotifyEvent) -> None:
        """Post *event* to Slack when filters match. Errors are swallowed."""
        if not self._should_post(event.state):
            return
        payload = self._render(event)
        try:
            self._http_post(self._webhook_url, json=payload, timeout=5.0)
        except (urllib.error.URLError, TimeoutError, RuntimeError):
            _log.warning(
                "Slack post failed",
                flow=event.flow_name,
                run=event.flow_run_name,
                state=event.state,
                exc_info=True,
            )

    def _should_post(self, state: str) -> bool:
        if self._on_failure and state in {"Failed", "Crashed"}:
            return True
        return bool(self._on_completion and state == "Completed")

    def _render(self, event: NotifyEvent) -> dict[str, Any]:
        icon = ":x:" if event.state in {"Failed", "Crashed"} else ":white_check_mark:"

        header = f"{icon} *{event.flow_name}* → `{event.state}`"

        meta_parts = [f"run: `{event.flow_run_name}`"]
        if event.env:
            meta_parts.append(f"env: `{event.env}`")
        if event.correlation_id:
            meta_parts.append(f"correlation: `{event.correlation_id}`")
        if event.duration_seconds is not None:
            meta_parts.append(f"duration: `{_fmt_duration(event.duration_seconds)}`")

        lines = [header, "  ".join(meta_parts)]
        if event.flow_run_url:
            lines.append(f"<{event.flow_run_url}|Open in Prefect>")
        if event.message:
            lines.append(f"```{event.message}```")

        payload: dict[str, Any] = {"text": "\n".join(lines)}
        if self._channel:
            payload["channel"] = self._channel
        return payload


def _fmt_duration(seconds: float) -> str:
    """Format seconds as a human-readable duration string."""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    m, s = divmod(s, 60)
    if m < 60:
        return f"{m}m {s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h {m:02d}m {s:02d}s"


def _default_http_post(url: str, *, json: dict[str, Any], timeout: float) -> None:
    data = _json.dumps(json).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
            resp.read()
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Slack webhook POST failed: {exc}") from exc


__all__ = ["SlackNotifier"]
