import sys
import typing
from typing import Any, ClassVar

import msgspec
from msgspec import UNSET, UnsetType
from msgspec._core import StructMeta

from loom.core.model.projection import Projection
from loom.core.model.relation import Relation


def _resolve_annotation(annotation: Any, namespace: dict[str, Any]) -> Any:
    """Resolve a stringified annotation to its actual type."""
    if isinstance(annotation, str):
        eval_ns: dict[str, Any] = {}
        eval_ns.update(namespace)
        eval_ns.setdefault("typing", typing)
        return typing.ForwardRef(annotation)._evaluate(eval_ns, eval_ns, frozenset[str]())
    return annotation


class LoomStructMeta(StructMeta):
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
    ) -> "LoomStructMeta":
        kwargs.setdefault("frozen", True)
        kwargs.setdefault("kw_only", True)
        kwargs.setdefault("omit_defaults", True)
        kwargs.setdefault("rename", "camel")

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
            if isinstance(value, Relation):
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
        struct_cls = super().__new__(cls, name, bases, namespace, **kwargs)
        struct_cls.__loom_relations__ = relations  # type: ignore[attr-defined]
        struct_cls.__loom_projections__ = projections  # type: ignore[attr-defined]
        return struct_cls  # type: ignore[return-value]


class BaseModel(msgspec.Struct, metaclass=LoomStructMeta):
    """Base for all loom domain models.

    Subclasses must declare ``__tablename__`` and annotate fields with
    ``Annotated[T, ColumnType, Field(...)]``.
    """

    __tablename__: ClassVar[str]
