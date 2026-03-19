import sys
import typing
from typing import TYPE_CHECKING, Any, ClassVar

import msgspec
from msgspec import UNSET, UnsetType

from loom.core.model.field import ColumnFieldSpec
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation
from loom.core.model.struct import LoomStruct

if TYPE_CHECKING:

    class _StructMeta(type):
        pass
else:
    _StructMeta = type(msgspec.Struct)


def _resolve_annotation(annotation: Any, namespace: dict[str, Any]) -> Any:
    """Resolve a stringified annotation to its actual type."""
    if isinstance(annotation, str):
        eval_ns: dict[str, Any] = {}
        eval_ns.update(namespace)
        eval_ns.setdefault("typing", typing)
        return typing.ForwardRef(annotation)._evaluate(eval_ns, eval_ns, frozenset[str]())
    return annotation


def _build_eval_namespace(namespace: dict[str, Any]) -> dict[str, Any]:
    """Build annotation evaluation namespace from module globals + class locals."""
    eval_ns: dict[str, Any] = {}
    module = namespace.get("__module__")
    if module:
        mod = sys.modules.get(module)
        if mod:
            eval_ns.update(vars(mod))
    eval_ns.update(namespace)
    return eval_ns


def _resolve_column_default(spec: ColumnFieldSpec) -> Any:
    """Resolve runtime default assigned to a model field in class namespace."""
    if spec.field.default is not msgspec.UNSET:
        return spec.field.default
    if spec.field.primary_key and spec.field.autoincrement:
        # Avoid propagating msgspec.UNSET into INSERT bind parameters.
        return None
    return UNSET


def _mark_optional_unset(
    attr_name: str,
    annotations: dict[str, Any],
    eval_ns: dict[str, Any],
) -> None:
    """Widen annotation to include ``UnsetType`` for lazy-loaded fields."""
    if attr_name not in annotations:
        return
    resolved = _resolve_annotation(annotations[attr_name], eval_ns)
    annotations[attr_name] = resolved | UnsetType


class LoomStructMeta(_StructMeta):
    """Metaclass that intercepts ``Relation`` and ``Projection`` assignments
    before ``StructMeta`` processes the class body.

    Each intercepted attribute is replaced with ``UNSET`` as its default,
    and the type annotation is widened to ``T | UnsetType`` so that
    ``omit_defaults=True`` strips unloaded fields from serialisation output.
    """

    def __new__(
        cls,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        **kwargs: Any,
    ) -> Any:
        kwargs.setdefault("frozen", True)
        kwargs.setdefault("kw_only", True)
        kwargs.setdefault("omit_defaults", True)
        kwargs.setdefault("rename", "camel")

        columns: dict[str, ColumnFieldSpec] = {}
        relations: dict[str, Relation] = {}
        projections: dict[str, Projection] = {}
        annotations: dict[str, Any] = namespace.get("__annotations__", {})

        eval_ns = _build_eval_namespace(namespace)

        for attr_name in list(namespace):
            value = namespace[attr_name]

            if isinstance(value, ColumnFieldSpec):
                columns[attr_name] = value
                namespace[attr_name] = _resolve_column_default(value)
                continue

            if isinstance(value, Relation):
                relations[attr_name] = value
                namespace[attr_name] = UNSET
                _mark_optional_unset(attr_name, annotations, eval_ns)
                continue

            if isinstance(value, Projection):
                projections[attr_name] = value
                namespace[attr_name] = UNSET
                _mark_optional_unset(attr_name, annotations, eval_ns)

        namespace["__annotations__"] = annotations
        struct_cls: Any = super().__new__(cls, name, bases, namespace, **kwargs)
        struct_cls.__loom_columns__ = columns
        struct_cls.__loom_relations__ = relations
        struct_cls.__loom_projections__ = projections
        return struct_cls


class BaseModel(LoomStruct, metaclass=LoomStructMeta):  # type: ignore[metaclass]  # LoomStructMeta + msgspec StructMeta combo not supported by mypy
    """Base for all loom domain models.

    Subclasses must declare ``__tablename__`` and provide typed attributes.
    Column metadata is optional via ``ColumnField(...)``.
    """

    __tablename__: ClassVar[str]
