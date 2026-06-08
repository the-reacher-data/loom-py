"""Tests for loom.prefect._placeholders.resolve_placeholder.

DSL: ${today}, ${yesterday}, ${today±Nd}, ${now}, ${now±Nd|Nh|Nm}.
Anything else that looks like a placeholder must raise ValueError.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from loom.prefect._placeholders import resolve_placeholder


def test_today_returns_today_utc_date() -> None:
    assert resolve_placeholder("${today}") == date.today()


def test_yesterday_returns_today_minus_one_day() -> None:
    assert resolve_placeholder("${yesterday}") == date.today() - timedelta(days=1)


def test_today_plus_offset() -> None:
    assert resolve_placeholder("${today+3d}") == date.today() + timedelta(days=3)


def test_today_minus_offset() -> None:
    assert resolve_placeholder("${today-7d}") == date.today() - timedelta(days=7)


def test_now_returns_utc_datetime_close_to_now() -> None:
    result = resolve_placeholder("${now}")
    assert isinstance(result, datetime)
    assert result.tzinfo is not None
    delta = abs((datetime.now(UTC) - result).total_seconds())
    assert delta < 5.0


def test_now_minus_hours() -> None:
    result = resolve_placeholder("${now-1h}")
    expected = datetime.now(UTC) - timedelta(hours=1)
    assert isinstance(result, datetime)
    assert abs((expected - result).total_seconds()) < 5.0


def test_now_plus_minutes() -> None:
    result = resolve_placeholder("${now+15m}")
    expected = datetime.now(UTC) + timedelta(minutes=15)
    assert abs((expected - result).total_seconds()) < 5.0


def test_now_minus_days() -> None:
    result = resolve_placeholder("${now-2d}")
    expected = datetime.now(UTC) - timedelta(days=2)
    assert abs((expected - result).total_seconds()) < 5.0


@pytest.mark.parametrize(
    "bad",
    [
        "${tomorrow}",
        "${today+5h}",
        "${yesterday-1d}",
        "${now-1}",
        "${nows}",
        "${today+}",
        "${now+abc}",
    ],
)
def test_invalid_placeholders_raise_value_error(bad: str) -> None:
    with pytest.raises(ValueError):
        resolve_placeholder(bad)


def test_non_placeholder_string_passes_through() -> None:
    assert resolve_placeholder("hello world") == "hello world"


def test_literal_iso_date_string_passes_through() -> None:
    assert resolve_placeholder("2026-05-15") == "2026-05-15"


def test_int_passes_through() -> None:
    assert resolve_placeholder(42) == 42


def test_list_passes_through() -> None:
    assert resolve_placeholder(["ES", "FR"]) == ["ES", "FR"]


def test_none_passes_through() -> None:
    assert resolve_placeholder(None) is None
