from __future__ import annotations

from loom.core.model import BaseModel, ColumnField


class Category(BaseModel):
    __tablename__ = "categories"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    name: str = ColumnField(length=100)
