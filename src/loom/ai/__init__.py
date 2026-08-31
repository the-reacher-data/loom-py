"""Loom AI pillar.

Package root for the AI agent layer. It stays import-light on purpose: no
optional engine extra may be imported here, so ``import loom.ai`` succeeds on a
base installation, and engine types are never re-exported (FR-056).
"""

from __future__ import annotations

from loom.ai.abc import (
    AgentEngine,
    AgentEngineProvider,
    AgentEvent,
    AgentResult,
    AgentUsage,
    DepsFactory,
    ErrorEvent,
    FinalEvent,
    HealthStatus,
    TextDeltaEvent,
    ToolCallEvent,
    ToolResultEvent,
    ToolsetFactory,
)
from loom.ai.config import A2AConfig, AgentEndpointConfig, AiConfig
from loom.ai.inference import InferenceTarget
from loom.ai.runtime import AgentHealth, AgentRunError, AgentRuntime

__all__ = [
    "A2AConfig",
    "AgentEndpointConfig",
    "AgentEngine",
    "AgentEngineProvider",
    "AgentEvent",
    "AgentHealth",
    "AgentResult",
    "AgentRunError",
    "AgentRuntime",
    "AgentUsage",
    "AiConfig",
    "DepsFactory",
    "ErrorEvent",
    "FinalEvent",
    "HealthStatus",
    "InferenceTarget",
    "TextDeltaEvent",
    "ToolCallEvent",
    "ToolResultEvent",
    "ToolsetFactory",
]
