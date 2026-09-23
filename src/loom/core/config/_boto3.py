"""The AWS SDK (``config-ssm`` extra): the one module of ``loom.core.config`` importing boto3."""

from __future__ import annotations

from typing import Any

import boto3  # type: ignore[import-untyped]

__all__ = ["client"]


def client(service: str, *, region: str | None) -> Any:
    """Build one boto3 client.

    Args:
        service: Service name, ``"ssm"`` or ``"secretsmanager"``.
        region: AWS region, or ``None`` to let boto3 resolve its own.

    Returns:
        The client; typed ``Any`` because boto3 ships no stubs.
    """
    return boto3.client(service, region_name=region)
