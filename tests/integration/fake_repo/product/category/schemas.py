from __future__ import annotations

import msgspec


class CreateCategory(msgspec.Struct):
    name: str
