"""Tests for ``loom.prefect.notify`` notification subsystem."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from loom.prefect.notify import (
    NotifyEvent,
    SlackNotifier,
    build_notifiers,
)


def _make_event(state: str = "Failed", flow_name: str = "etl-x") -> NotifyEvent:
    return NotifyEvent(
        flow_name=flow_name,
        flow_run_name="run-1",
        flow_run_url="https://prefect/runs/1",
        state=state,
        correlation_id="corr-1",
        message="boom",
    )


class TestSlackNotifier:
    def test_posts_to_webhook_on_matching_state(self) -> None:
        http = MagicMock()
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/X",
            on_failure=True,
            http_post=http,
        )
        notifier.notify(_make_event(state="Failed"))
        assert http.call_count == 1
        url, _, kwargs = http.call_args.args[0], http.call_args.args, http.call_args.kwargs
        assert url == "https://hooks.slack.com/X"
        assert "etl-x" in kwargs["json"]["text"]
        assert "Failed" in kwargs["json"]["text"]

    def test_does_not_post_when_state_filter_excludes_event(self) -> None:
        http = MagicMock()
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/X",
            on_failure=True,
            on_completion=False,
            http_post=http,
        )
        notifier.notify(_make_event(state="Completed"))
        assert http.call_count == 0

    def test_posts_on_completion_when_enabled(self) -> None:
        http = MagicMock()
        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/X",
            on_failure=False,
            on_completion=True,
            http_post=http,
        )
        notifier.notify(_make_event(state="Completed"))
        assert http.call_count == 1

    def test_swallows_http_failure(self) -> None:
        def _boom(*_: Any, **__: Any) -> None:
            raise RuntimeError("network down")

        notifier = SlackNotifier(
            webhook_url="https://hooks.slack.com/X",
            on_failure=True,
            http_post=_boom,
        )
        notifier.notify(_make_event(state="Failed"))  # must not raise


class TestBuildNotifiers:
    def test_returns_empty_when_block_missing(self) -> None:
        assert build_notifiers(None) == ()
        assert build_notifiers([]) == ()

    def test_builds_slack_notifier_from_yaml_block(self) -> None:
        block = [
            {
                "kind": "slack",
                "webhook_url": "https://hooks.slack.com/abc",
                "on_failure": True,
                "on_completion": False,
            }
        ]
        notifiers = build_notifiers(block)
        assert len(notifiers) == 1
        assert isinstance(notifiers[0], SlackNotifier)

    def test_rejects_unknown_kind(self) -> None:
        with pytest.raises(ValueError, match="unknown notifier kind"):
            build_notifiers([{"kind": "carrier-pigeon", "address": "north"}])

    def test_rejects_missing_kind(self) -> None:
        with pytest.raises(ValueError, match="kind"):
            build_notifiers([{"webhook_url": "x"}])

    def test_skips_slack_when_webhook_url_empty(self) -> None:
        block = [{"kind": "slack", "webhook_url": "", "on_failure": True}]
        assert build_notifiers(block) == ()

    def test_skips_slack_when_webhook_url_whitespace(self) -> None:
        block = [{"kind": "slack", "webhook_url": "   ", "on_failure": True}]
        assert build_notifiers(block) == ()

    def test_supports_multiple_notifiers(self) -> None:
        block = [
            {"kind": "slack", "webhook_url": "https://a", "on_failure": True},
            {"kind": "slack", "webhook_url": "https://b", "on_completion": True},
        ]
        notifiers = build_notifiers(block)
        assert len(notifiers) == 2
