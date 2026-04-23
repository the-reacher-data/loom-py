"""Streaming testing helpers built on top of Bytewax's testing sinks/sources.

Public API
----------
* :class:`StreamingTestRunner` — test-oriented runner with injectable input,
  captured output, and explicit error branch capture.
"""

from loom.streaming.testing._runner import StreamingTestRunner

__all__ = ["StreamingTestRunner"]
