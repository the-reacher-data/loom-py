"""Published JSON Schema for authored agent artifacts.

The document is emitted from the Tier-1 structs rather than read from disk, and
every pattern, default, minimum and maximum comes from the constants in
:mod:`loom.ai.declarative._v1`, so the schema cannot drift from the structs it
describes.

Two places where the emitted document is deliberately ahead of the committed
``contracts/agent-spec-v1.schema.json`` file:

* ``$defs.policies`` carries ``run_timeout_ms`` (FR-033a).
* the ``sql`` capability requires ``max_rows`` and ``max_result_bytes``
  (FR-046b), because an unbounded query is not representable.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from types import MappingProxyType
from typing import Any, Final

from ._envelope import LATEST_SPEC_VERSION
from ._v1 import (
    AGENT_NAME_PATTERN,
    DEFAULT_MODEL_ROLE,
    MAX_ITERATIONS_DEFAULT,
    MAX_ITERATIONS_MAX,
    MAX_ITERATIONS_MIN,
    MODEL_ROLE_PATTERN,
    RETRIES_DEFAULT,
    RETRIES_MAX,
    RETRIES_MIN,
    RUN_TIMEOUT_MS_DEFAULT,
    RUN_TIMEOUT_MS_MAX,
    RUN_TIMEOUT_MS_MIN,
    SPEC_VERSION_V1,
    SYMBOL_REF_PATTERN,
    TOOL_TIMEOUT_MS_DEFAULT,
    TOOL_TIMEOUT_MS_MAX,
    TOOL_TIMEOUT_MS_MIN,
)


def _v1_properties() -> dict[str, Any]:
    return {
        "spec_version": {
            "const": SPEC_VERSION_V1,
            "description": "Format version. Read first, before any other field.",
        },
        "name": {
            "type": "string",
            "pattern": AGENT_NAME_PATTERN,
            "description": ("Unique agent name within the application. Published in the A2A card."),
        },
        "description": {
            "type": "string",
            "minLength": 1,
            "description": "What the agent does. PUBLIC: published in the A2A card.",
        },
        "instructions": {
            "type": "string",
            "minLength": 1,
            "description": (
                "Instructions the agent follows. NEVER published. Must not encode authorization."
            ),
        },
        "model_role": {
            "type": "string",
            "pattern": MODEL_ROLE_PATTERN,
            "default": DEFAULT_MODEL_ROLE,
            "description": (
                "Logical model role, bound to a concrete provider and model by "
                "deployment configuration. Never a vendor name or model id."
            ),
        },
        "output": {"$ref": "#/$defs/output"},
        "capabilities": {
            "type": "array",
            "default": [],
            "items": {"$ref": "#/$defs/capability"},
        },
        "policies": {"$ref": "#/$defs/policies"},
        "metadata": {
            "type": "object",
            "additionalProperties": {"type": "string"},
            "default": {},
        },
    }


def _v1_output_def() -> dict[str, Any]:
    return {
        "oneOf": [
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "schema"],
                "properties": {
                    "kind": {"const": "json_schema"},
                    "schema": {
                        "type": "object",
                        "description": (
                            "JSON Schema object describing the required answer. "
                            "Canonical form; what a generator emits."
                        ),
                    },
                },
            },
            {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "ref"],
                "properties": {
                    "kind": {"const": "type_ref"},
                    "ref": {
                        "type": "string",
                        "pattern": SYMBOL_REF_PATTERN,
                        "description": (
                            "Shortcut for hand-written applications: module:Symbol "
                            "resolved at compile time."
                        ),
                    },
                },
            },
        ]
    }


def _v1_usecase_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "keys"],
        "properties": {
            "kind": {"const": "usecase"},
            "keys": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "minLength": 1},
                "description": (
                    "Explicitly granted business operation keys. Never expanded automatically."
                ),
            },
        },
    }


def _v1_sql_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "connection", "max_rows", "max_result_bytes"],
        "properties": {
            "kind": {"const": "sql"},
            "connection": {
                "type": "string",
                "minLength": 1,
                "description": "Named connection. Compilation fails unless it is read-only.",
            },
            "max_rows": {"type": "integer", "minimum": 1},
            "max_result_bytes": {"type": "integer", "minimum": 1},
        },
    }


def _v1_mcp_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "url"],
        "properties": {
            "kind": {"const": "mcp"},
            "url": {"type": "string", "format": "uri"},
            "tool_filter": {"$ref": "#/$defs/tool_filter"},
            "headers_ref": {
                "type": "string",
                "description": (
                    "Reference to headers resolved from deployment configuration. "
                    "Never inline credentials."
                ),
            },
        },
    }


def _v1_skills_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "refs"],
        "properties": {
            "kind": {"const": "skills"},
            "refs": {
                "type": "array",
                "minItems": 1,
                "items": {"type": "string", "pattern": SYMBOL_REF_PATTERN},
                "description": (
                    "Package references relative to skills_root. "
                    "Absolute filesystem paths are not representable."
                ),
            },
        },
    }


def _v1_python_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "factory"],
        "properties": {
            "kind": {"const": "python"},
            "factory": {
                "type": "string",
                "pattern": SYMBOL_REF_PATTERN,
                "description": (
                    "module:factory satisfying the ToolsetFactory protocol. "
                    "A factory, never a constructed object."
                ),
            },
        },
    }


def _v1_a2a_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "url"],
        "properties": {
            "kind": {"const": "a2a"},
            "url": {
                "type": "string",
                "format": "uri",
                "description": "Remote A2A agent to delegate to.",
            },
            "skills": {"type": "array", "items": {"type": "string"}},
        },
    }


def _v1_defs() -> dict[str, Any]:
    return {
        "output": _v1_output_def(),
        "capability": {
            "oneOf": [
                _v1_usecase_capability(),
                _v1_sql_capability(),
                _v1_mcp_capability(),
                _v1_skills_capability(),
                _v1_python_capability(),
                _v1_a2a_capability(),
            ]
        },
        "tool_filter": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "include": {"type": "array", "items": {"type": "string"}, "default": []},
                "exclude": {"type": "array", "items": {"type": "string"}, "default": []},
            },
        },
        "policies": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "retries": {
                    "type": "integer",
                    "minimum": RETRIES_MIN,
                    "maximum": RETRIES_MAX,
                    "default": RETRIES_DEFAULT,
                },
                "tool_timeout_ms": {
                    "type": "integer",
                    "minimum": TOOL_TIMEOUT_MS_MIN,
                    "maximum": TOOL_TIMEOUT_MS_MAX,
                    "default": TOOL_TIMEOUT_MS_DEFAULT,
                },
                "max_iterations": {
                    "type": "integer",
                    "minimum": MAX_ITERATIONS_MIN,
                    "maximum": MAX_ITERATIONS_MAX,
                    "default": MAX_ITERATIONS_DEFAULT,
                },
                "run_timeout_ms": {
                    "type": "integer",
                    "minimum": RUN_TIMEOUT_MS_MIN,
                    "maximum": RUN_TIMEOUT_MS_MAX,
                    "default": RUN_TIMEOUT_MS_DEFAULT,
                },
            },
        },
    }


def _v1_schema() -> dict[str, Any]:
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://loom.dev/schemas/agent-spec/v1.json",
        "title": "Loom AgentSpec v1",
        "description": (
            "Declarative, engine-agnostic and vendor-agnostic agent definition. "
            "Published per spec version (FR-009) so generators and editors can "
            "validate without the platform runtime."
        ),
        "type": "object",
        "additionalProperties": False,
        "required": ["spec_version", "name", "description", "instructions", "output"],
        "properties": _v1_properties(),
        "$defs": _v1_defs(),
    }


_SCHEMA_BUILDERS: Final[Mapping[int, Callable[[], dict[str, Any]]]] = MappingProxyType(
    {1: _v1_schema}
)


def agent_spec_json_schema(spec_version: int = LATEST_SPEC_VERSION) -> dict[str, Any]:
    """Return the published JSON Schema document for a spec version.

    The document is rebuilt on every call, so callers may mutate the result
    freely without affecting anyone else.

    Args:
        spec_version: Spec version whose schema is wanted.

    Returns:
        The JSON Schema document, ready to serialise or hand to a validator.

    Raises:
        ValueError: If no schema is published for that version.

    Example:
        >>> agent_spec_json_schema(1)["title"]
        'Loom AgentSpec v1'
    """
    builder = _SCHEMA_BUILDERS.get(spec_version)
    if builder is None:
        known = ", ".join(str(version) for version in sorted(_SCHEMA_BUILDERS))
        raise ValueError(
            f"no JSON Schema published for spec version {spec_version}; known: {known}"
        )
    return builder()
