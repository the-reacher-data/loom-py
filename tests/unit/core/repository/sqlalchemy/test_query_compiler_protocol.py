from __future__ import annotations

from loom.core.backend.sqlalchemy import compile_all, get_compiled
from loom.core.model import BaseModel, ColumnField
from loom.core.repository.abc import FilterGroup, FilterOp, FilterSpec, SortSpec
from loom.core.repository.abc.query_compiler import QueryCompiler
from loom.core.repository.sqlalchemy.query_compiler.compiler import QuerySpecCompiler
from loom.core.repository.sqlalchemy.query_compiler.errors import UnsafeFilterError


class _Row(BaseModel):
    __tablename__ = "protocol_rows"

    id: int = ColumnField(primary_key=True, autoincrement=True)
    rank: int = ColumnField()


def _compiler(allowed: frozenset[str] = frozenset()) -> QuerySpecCompiler:
    compile_all(_Row)
    sa_model = get_compiled(_Row)
    assert sa_model is not None
    return QuerySpecCompiler(
        sa_model, sa_model.id, allowed, backend="sqlalchemy", model_name="_Row"
    )


def test_sqlalchemy_compiler_satisfies_query_compiler() -> None:
    assert isinstance(_compiler(), QueryCompiler)


def test_compile_filter_and_sort_delegate_to_sqlalchemy_clauses() -> None:
    compiler = _compiler()

    clause = compiler.compile_filter(FilterGroup(filters=(FilterSpec("rank", FilterOp.GT, 1),)))
    order = compiler.compile_sort((SortSpec("rank", "DESC"),))

    assert str(clause) == "protocol_rows.rank > :rank_1"
    assert [str(item) for item in order] == ["protocol_rows.rank DESC"]


def test_compile_filter_honours_allowed_fields() -> None:
    compiler = _compiler(frozenset({"id"}))

    try:
        compiler.compile_filter(FilterGroup(filters=(FilterSpec("rank", FilterOp.EQ, 1),)))
    except UnsafeFilterError:
        return
    raise AssertionError("allowed_fields was not enforced")
