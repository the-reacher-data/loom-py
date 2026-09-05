"""Published JSON Schema for authored agent artifacts.

The document is emitted from the Tier-1 structs rather than read from disk, and
every pattern, default, minimum and maximum comes from the constants in
:mod:`loom.ai.declarative._v1`, so the schema cannot drift from the structs it
describes.

The emitted document is byte-for-byte the schema file **shipped inside the
distribution** (``loom/ai/declarative/schemas/agent-spec-v1.schema.json``), and
a test asserts it: the contract is published to generators and editors, so a
drift between the schema they validate against and the structs loom decodes
with would only surface as a decoding failure at deploy time.

The file is shipped rather than only emitted because the consumer that needs it
most — a generator that writes ``.agent.yaml`` files — must be able to validate
its output without importing loom, and often without installing it at all: the
file can be extracted from the wheel or the sdist and handed to any JSON Schema
validator in any language.  :func:`agent_spec_schema_path` locates it for the
callers that *do* have loom installed.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
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
    NATIVE_TOOLS,
    RETRIES_DEFAULT,
    RETRIES_MAX,
    RETRIES_MIN,
    RUN_TIMEOUT_MS_DEFAULT,
    RUN_TIMEOUT_MS_MAX,
    RUN_TIMEOUT_MS_MIN,
    SKILLS_LIBRARY_PATTERN,
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
        "on_output": {"$ref": "#/$defs/on_output"},
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


def _v1_name_filter() -> dict[str, Any]:
    """Flat include/exclude properties shared by mcp, skills and a2a."""
    return {
        "include": {
            "type": "array",
            "items": {"type": "string"},
            "default": [],
            "description": "Names or glob patterns to expose. Empty means all.",
        },
        "exclude": {
            "type": "array",
            "items": {"type": "string"},
            "default": [],
            "description": "Names or glob patterns to omit.",
        },
    }


def _v1_mcp_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "server"],
        "properties": {
            "kind": {
                "const": "mcp",
                "description": "Tools from a named remote tool server.",
            },
            "server": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Named remote tool server, resolved from ai.mcp_servers in "
                    "deployment configuration. The artifact names the server; the "
                    "deployment knows where it is and how to authenticate to it."
                ),
            },
            **_v1_name_filter(),
        },
    }


def _v1_skills_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "library"],
        "properties": {
            "kind": {
                "const": "skills",
                "description": "Packaged prompt material from a skill library.",
            },
            "library": {
                "type": "string",
                "minLength": 1,
                "pattern": SKILLS_LIBRARY_PATTERN,
                "description": (
                    "Skill library. './name' resolves beside this artifact and travels "
                    "with it; a bare name resolves against ai.skills_root. '..' is not "
                    "representable, so a library can never escape its own directory."
                ),
            },
            **_v1_name_filter(),
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
        "required": ["kind", "agent"],
        "properties": {
            "kind": {
                "const": "a2a",
                "description": "Delegation to a named remote agent.",
            },
            "agent": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Named remote agent, resolved from ai.a2a_agents in deployment configuration."
                ),
            },
            **_v1_name_filter(),
        },
    }


def _v1_native_capability() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["kind", "tool"],
        "properties": {
            "kind": {
                "const": "native",
                "description": "Tool the model provider runs in its own infrastructure.",
            },
            "tool": {
                "type": "string",
                "enum": list(NATIVE_TOOLS),
                "description": (
                    "Provider tool by its stable name. Whether the model bound to "
                    "model_role admits it is checked at compile time."
                ),
            },
        },
    }


def _v1_output_hook_def() -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["usecase"],
        "properties": {
            "usecase": {
                "type": "string",
                "minLength": 1,
                "description": (
                    "Use-case key of the registry executed once per completed run "
                    "with the validated output. Not a tool: the model never sees it, "
                    "and the same key must not also be granted as a usecase capability."
                ),
            },
        },
    }


def _v1_defs() -> dict[str, Any]:
    return {
        "output": _v1_output_def(),
        "on_output": _v1_output_hook_def(),
        "capability": {
            "oneOf": [
                _v1_usecase_capability(),
                _v1_sql_capability(),
                _v1_mcp_capability(),
                _v1_skills_capability(),
                _v1_python_capability(),
                _v1_a2a_capability(),
                _v1_native_capability(),
            ]
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


SCHEMA_FILENAMES: Final[Mapping[int, str]] = MappingProxyType({1: "agent-spec-v1.schema.json"})
"""File name each published spec version is shipped under, inside ``schemas/``."""

_SCHEMA_DIRECTORY: Final[Path] = Path(__file__).parent / "schemas"


def agent_spec_schema_path(spec_version: int = LATEST_SPEC_VERSION) -> Path:
    """Locate the JSON Schema file shipped in the installed distribution.

    The file is the byte-for-byte twin of :func:`agent_spec_json_schema`, kept
    honest by a test.  Prefer this over the emitter when the schema must be
    handed to a tool that reads a *path* — an editor, a linter, a CI validation
    step in another language.

    Args:
        spec_version: Spec version whose schema file is wanted.

    Returns:
        Absolute path of the shipped schema file.

    Raises:
        ValueError: If no schema is published for that version.
        FileNotFoundError: If the distribution was built without its schema
            data files, which is a packaging defect rather than a usage error.

    Example:
        >>> agent_spec_schema_path(1).name
        'agent-spec-v1.schema.json'
    """
    filename = SCHEMA_FILENAMES.get(spec_version)
    if filename is None:
        known = ", ".join(str(version) for version in sorted(SCHEMA_FILENAMES))
        raise ValueError(
            f"no JSON Schema published for spec version {spec_version}; known: {known}"
        )
    path = _SCHEMA_DIRECTORY / filename
    if not path.is_file():
        raise FileNotFoundError(
            f"the installed distribution ships no schema file at {path}; "
            f"rebuild the package so 'schemas/*.json' is included"
        )
    return path
