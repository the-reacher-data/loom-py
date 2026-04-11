"""Unit tests for backend format dispatch helper."""

from __future__ import annotations

import pytest

from loom.etl.backends._format_registry import resolve_format_handler
from loom.etl.declarative._format import Format


def test_resolve_format_handler_accepts_enum() -> None:
    handlers = {Format.CSV: "csv-handler"}
    assert resolve_format_handler(Format.CSV, handlers) == "csv-handler"


def test_resolve_format_handler_accepts_string_value() -> None:
    handlers = {Format.JSON: "json-handler"}
    assert resolve_format_handler("json", handlers) == "json-handler"


def test_resolve_format_handler_raises_for_unregistered_format() -> None:
    handlers = {Format.CSV: "csv-handler"}
    with pytest.raises(ValueError, match="Unsupported format: json"):
        resolve_format_handler(Format.JSON, handlers)


def test_resolve_format_handler_raises_for_unknown_string() -> None:
    handlers = {Format.CSV: "csv-handler"}
    with pytest.raises(ValueError, match="'xml' is not a valid Format"):
        resolve_format_handler("xml", handlers)
