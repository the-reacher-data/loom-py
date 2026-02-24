from __future__ import annotations

from typing import Annotated

from loom.core.model import BaseModel, Field, Integer, String


class Category(BaseModel):
    __tablename__ = "categories"

    id: Annotated[int, Integer, Field(primary_key=True, autoincrement=True)]
    name: Annotated[str, String(100)]
