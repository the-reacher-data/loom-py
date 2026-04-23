"""Core primitives for the streaming DSL.

No dependencies on other loom.streaming sub-packages.
"""

from loom.streaming.core._errors import ErrorEnvelope, ErrorKind
from loom.streaming.core._message import Message, MessageMeta, StreamPayload

__all__ = [
    "ErrorEnvelope",
    "ErrorKind",
    "Message",
    "MessageMeta",
    "StreamPayload",
]
