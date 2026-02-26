from __future__ import annotations

from typing import Protocol


class PersistenceBackend(Protocol):
    """Contract for compiling domain Struct models into persistence layer types."""

    def compile_model(self, model_cls: type) -> type: ...

    def get_compiled(self, model_cls: type) -> type | None: ...
