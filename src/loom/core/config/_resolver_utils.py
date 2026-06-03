"""Shared utilities for config resolvers (SSM, Secrets Manager, etc.)."""

from __future__ import annotations

import json
import os
import re

from loom.core.config.errors import ConfigError

_ENV_VAR_PATTERN = re.compile(r"%([A-Z][A-Z0-9_]*)%")


def _expand_env_vars(key: str) -> str:
    """Expand ``%VAR_NAME%`` placeholders in *key* using environment variables.

    Only uppercase identifiers matching ``[A-Z][A-Z0-9_]*`` are expanded.
    The ``%VAR%`` syntax avoids conflicts with OmegaConf's ANTLR grammar,
    which rejects bare ``{`` inside ``${resolver:...}`` interpolations.

    Args:
        key: Resolver path, possibly containing ``%VAR_NAME%`` tokens.

    Returns:
        Path with all tokens replaced by their environment variable values.

    Raises:
        ConfigError: When a referenced variable is absent from the environment.
    """
    if not _ENV_VAR_PATTERN.search(key):
        return key

    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            raise ConfigError(
                f"Env var {var_name!r} not found in environment "
                f"(referenced in resolver path {key!r})"
            )
        return value

    return _ENV_VAR_PATTERN.sub(_replace, key)


def _split_resolver_key(key: str) -> tuple[str, list[str]]:
    """Split a resolver key into the base path and JSON navigation keys.

    The first dot after the last '/' separator marks the start of a JSON key
    path. Both SSM and Secrets Manager use '/' as path separator, so a dot
    is unambiguously a JSON navigation token.

    Note: ARN-style SecretIds (``arn:aws:secretsmanager:...``) are not
    supported for dot-notation — use short names instead.

    **Limitation — env-var values with dots in the final path segment:**
    If a ``%VAR%`` token expands to a value that contains a dot *and* that
    token appears in the last path segment (after the final ``/``), the dot
    will be interpreted as a JSON navigation separator, producing a confusing
    ``ConfigError``. Example: ``/app/svc/%ENV%`` where ``ENV=db.prod`` becomes
    ``/app/svc/db`` with JSON key ``["prod"]``.  Use env-var tokens only in
    path segments that are guaranteed to be dot-free (environment names,
    region codes, etc.).

    Args:
        key: Expanded resolver key, e.g. ``"/app/prod/db_config.host"``.

    Returns:
        A 2-tuple ``(base_path, json_keys)`` where *base_path* is the path
        to fetch and *json_keys* is a list of dict keys to navigate
        (empty list when no dot-notation is present).
    """
    if not key:
        return key, []
    last_slash = key.rfind("/")
    after_last_slash = key[last_slash + 1 :] if last_slash != -1 else key
    dot_pos = after_last_slash.find(".")
    if dot_pos == -1:
        return key, []
    base_end = (last_slash + 1 + dot_pos) if last_slash != -1 else dot_pos
    json_tail = key[base_end + 1 :]
    return key[:base_end], [k for k in json_tail.split(".") if k]


def _navigate_json(raw: str, keys: list[str], path: str) -> object:
    """Parse *raw* as JSON and navigate to the value at *keys*.

    Args:
        raw: Raw string value from the resolver backend.
        keys: Ordered list of dict keys to traverse.
        path: Original path, used only in error messages.

    Returns:
        The value at the navigated position. May be any JSON-compatible type.

    Raises:
        ConfigError: When *raw* is not valid JSON, or a key is absent.
    """
    try:
        data: object = json.loads(raw)
    except json.JSONDecodeError as exc:
        # Suppress chain (from None) — JSONDecodeError.doc holds the raw secret
        # and would be exposed via __cause__ in tracebacks / APM serialisation.
        raise ConfigError(
            f"Resolver path {path!r} value is not valid JSON "
            f"(required for key navigation): {exc.msg} at position {exc.pos}"
        ) from None
    current: object = data
    for k in keys:
        if not isinstance(current, dict) or k not in current:
            raise ConfigError(f"Key {k!r} not found at resolver path {path!r}")
        current = current[k]
    return current


__all__ = ["_expand_env_vars", "_split_resolver_key", "_navigate_json"]
