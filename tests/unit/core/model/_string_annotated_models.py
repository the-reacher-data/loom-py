from __future__ import annotations

from typing import Annotated

from loom.core.model import BaseModel, Cardinality, ColumnField, ProjectionField, RelationField


class Crate(BaseModel):
    __tablename__ = "annotation_resolution_crates"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    items: list[dict[str, object]] = RelationField(
        foreign_key="crate_id",
        cardinality=Cardinality.ONE_TO_MANY,
    )
    weight: Annotated[float, "kg"] = ProjectionField(loader=None, default=0.0)
