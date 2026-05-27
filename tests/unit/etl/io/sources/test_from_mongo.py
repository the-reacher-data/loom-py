"""TDD Red — tests for FromMongo builder and MongoLookupSourceSpec.

All tests in this file MUST FAIL with ImportError / ModuleNotFoundError until
loom/etl/io/sources/_mongo.py is implemented.
"""

from __future__ import annotations

import msgspec
import pytest

from loom.etl.declarative.source._from import FromTemp
from loom.etl.declarative.source._specs import SourceKind, TempSourceSpec
from loom.etl.io.sources._mongo import FromMongo, MongoLookupSourceSpec  # noqa: F401

# ---------------------------------------------------------------------------
# Minimal schema for tests that require a msgspec.Struct schema
# ---------------------------------------------------------------------------


class SomeSchema(msgspec.Struct):
    id: str
    name: str | None = None


# ---------------------------------------------------------------------------
# Test classes
# ---------------------------------------------------------------------------


class TestWhereIdInFromTemp:
    def test_where_id_in_fromtemp_produces_spec(self) -> None:
        """where_id_in(FromTemp('ids')) yields MongoLookupSourceSpec.

        Verifies id_source is a TempSourceSpec with the correct temp_name.
        """
        builder = FromMongo("motos").where_id_in(FromTemp("ids"), id_col="document_id")
        spec = builder._to_spec("motos")

        assert isinstance(spec, MongoLookupSourceSpec)
        assert isinstance(spec.id_source, TempSourceSpec)
        assert spec.id_source.temp_name == "ids"
        assert spec.id_col == "document_id"


class TestSpecKind:
    def test_spec_kind_is_mongo_lookup(self) -> None:
        """spec.kind must equal SourceKind.MONGO_LOOKUP."""
        spec = FromMongo("motos").where_id_in(FromTemp("ids"))._to_spec("motos")
        assert spec.kind == SourceKind.MONGO_LOOKUP


class TestSchemaStoredInSpec:
    def test_schema_stored_in_spec(self) -> None:
        """.with_schema(SomeSchema) stores the class on spec.schema_type."""
        spec = (
            FromMongo("motos")
            .where_id_in(FromTemp("ids"))
            .with_schema(SomeSchema)
            ._to_spec("motos")
        )
        assert spec.schema_type is SomeSchema


class TestOnExtraFields:
    def test_on_extra_fields_stored_in_spec(self) -> None:
        """.on_extra_fields('capture') sets spec.extra_fields_mode to 'capture'."""
        spec = (
            FromMongo("motos")
            .where_id_in(FromTemp("ids"))
            .on_extra_fields("capture")
            ._to_spec("motos")
        )
        assert spec.extra_fields_mode == "capture"


class TestIdSourceCannotBeMongoLookupSpec:
    def test_id_source_cannot_be_mongo_lookup_spec(self) -> None:
        """Passing a MongoLookupSourceSpec as where_id_in source must raise TypeError.

        This enforces the C2 constraint: id_source type excludes MongoLookupSourceSpec
        to prevent unbounded recursion.  The builder must reject it at call time.
        """
        # Build a real MongoLookupSourceSpec to attempt as the id_source
        inner_spec = FromMongo("inner").where_id_in(FromTemp("some_ids"))._to_spec("inner")
        assert isinstance(inner_spec, MongoLookupSourceSpec)

        with pytest.raises(TypeError):
            FromMongo("outer").where_id_in(inner_spec)


class TestBatchSize:
    def test_batch_size_default_is_1000(self) -> None:
        """Default batch_size on MongoLookupSourceSpec must be 1000."""
        spec = FromMongo("motos").where_id_in(FromTemp("ids"))._to_spec("motos")
        assert spec.batch_size == 1000

    def test_batch_size_configurable(self) -> None:
        """.batch_size(500) sets spec.batch_size to 500."""
        spec = FromMongo("motos").where_id_in(FromTemp("ids")).batch_size(500)._to_spec("motos")
        assert spec.batch_size == 500


class TestRepr:
    def test_repr_includes_collection(self) -> None:
        """repr(FromMongo('motos')) must contain the collection name."""
        assert "motos" in repr(FromMongo("motos"))
