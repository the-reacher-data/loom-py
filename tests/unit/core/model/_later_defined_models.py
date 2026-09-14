import msgspec

from loom.core.model import BaseModel, Cardinality, ColumnField, RelationField


class Reader(BaseModel):
    __tablename__ = "annotation_resolution_readers"
    id: int = ColumnField(primary_key=True, autoincrement=True)
    loans: list[Loan] = RelationField(
        foreign_key="reader_id",
        cardinality=Cardinality.ONE_TO_MANY,
    )


class Loan(msgspec.Struct):
    reader_id: int
