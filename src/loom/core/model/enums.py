from __future__ import annotations

from enum import StrEnum


class OnDelete(StrEnum):
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    RESTRICT = "RESTRICT"
    NO_ACTION = "NO ACTION"


class OnUpdate(StrEnum):
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    RESTRICT = "RESTRICT"
    NO_ACTION = "NO ACTION"


class Cardinality(StrEnum):
    ONE_TO_ONE = "one_to_one"
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"


class ServerDefault(StrEnum):
    NOW = "now"
    UUID4 = "uuid4"


class ServerOnUpdate(StrEnum):
    NOW = "now"
