"""Module that fails at import time, referenced by dotted path from a test."""

raise RuntimeError("boom at import")
