import sys
import typing
from typing import TYPE_CHECKING, Any, ClassVar

import msgspec
from msgspec import UNSET, UnsetType

from loom.core.model.field import ColumnFieldSpec
from loom.core.model.projection import Projection
from loom.core.model.relation import Relation

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

        # Build evaluation context from the module where the class is defined
        eval_ns: dict[str, Any] = {}
        module = namespace.get("__module__")
        if module:
            mod = sys.modules.get(module)
            if mod:
                eval_ns.update(vars(mod))
        eval_ns.update(namespace)

        for attr_name in list(namespace):
            value = namespace[attr_name]
            if isinstance(value, ColumnFieldSpec):
                columns[attr_name] = value
                if value.field.default is not msgspec.UNSET:
                    namespace[attr_name] = value.field.default
                elif value.field.primary_key and value.field.autoincrement:
                    # Avoid propagating msgspec.UNSET into INSERT bind parameters.
                    namespace[attr_name] = None
                else:
                    namespace[attr_name] = UNSET
            elif isinstance(value, Relation):
                relations[attr_name] = value
                namespace[attr_name] = UNSET
                if attr_name in annotations:
                    resolved = _resolve_annotation(annotations[attr_name], eval_ns)
                    annotations[attr_name] = resolved | UnsetType
            elif isinstance(value, Projection):
                projections[attr_name] = value
                namespace[attr_name] = UNSET
                if attr_name in annotations:
                    resolved = _resolve_annotation(annotations[attr_name], eval_ns)
                    annotations[attr_name] = resolved | UnsetType

        namespace["__annotations__"] = annotations
        struct_cls: Any = super().__new__(cls, name, bases, namespace, **kwargs)
        struct_cls.__loom_columns__ = columns
        struct_cls.__loom_relations__ = relations
        struct_cls.__loom_projections__ = projections
        return struct_cls


if TYPE_CHECKING:
    class BaseModel(msgspec.Struct):
        """Typing-only base model to avoid metaclass noise in mypy."""

        __tablename__: ClassVar[str]
else:
    class BaseModel(msgspec.Struct, metaclass=LoomStructMeta):
        """Base for all loom domain models.

        Subclasses must declare ``__tablename__`` and provide typed attributes.
        Column metadata is optional via ``ColumnField(...)``.
        """

        __tablename__: ClassVar[str]
