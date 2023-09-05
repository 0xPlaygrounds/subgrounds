from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pipe import where

from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.utils import identity

from .fieldpath import FieldPath, SyntheticField

if TYPE_CHECKING:
    from .subgraph import Subgraph


@dataclass
class Object:
    _subgraph: Subgraph
    _object: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta

    def __init__(
        self, subgraph: Subgraph, object: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta
    ) -> None:
        self._subgraph = subgraph
        self._object = object

        for field_ in self._object.fields:
            super().__setattr__(field_.name, self._select(field_.name))

    @property
    def _schema(self) -> SchemaMeta:
        return self._subgraph._schema

    def _select(self, name: str) -> FieldPath:
        """Selects the field from ``self`` with name ``name`` and returns the field
        as a :class:`FieldPath`.

        Args:
          name: The name of the field

        Raises:
          TypeError: _description_

        Returns:
          _description_
        """

        field = self._object.field(name)

        match self._schema.type_of_typeref(field.type_):
            case (
                TypeMeta.ObjectMeta()
                | TypeMeta.InterfaceMeta()
                | TypeMeta.EnumMeta()
                | TypeMeta.ScalarMeta() as type_
            ):
                return FieldPath(
                    self._subgraph,
                    TypeRef.Named(name=self._object.name, kind="OBJECT"),
                    field.type_,
                    [(None, field)],
                )

            case TypeMeta.T as type_:
                raise TypeError(
                    f"Object: Unexpected type {type_.name}"
                    f" when selection {name} on {self}"
                )

        assert False  # Suppress mypy missing return statement warning

    def _add_field(self, name: str, fpath: FieldPath) -> None:
        sfield = SyntheticField(identity, fpath._type, fpath)
        self._subgraph._add_synthetic_field(self._object, name, sfield)

    def _add_sfield(self, name: str, sfield: SyntheticField) -> None:
        # TODO: Add check to make sure obj has the deps of sfield
        # obj_fields = [field.name for field in obj.object_.fields]
        # sfield_deps = [fpath.leaf.name for fpath in sfield.deps]
        # for dep in sfield_deps:
        #   if dep not in obj_fields:
        #     raise Exception(
        #       f'SyntheticField {obj.object_.name}.{name}: {obj.object_.name}'
        #        ' does not have the field {dep}'
        #     )

        def f(
            obj_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta, path: list[str]
        ) -> None:
            match path:
                case [field_name, *rest]:
                    try:
                        field: TypeMeta.FieldMeta = next(
                            obj_.fields | where(lambda field: field.name == field_name)
                        )

                        f(self._schema.type_map[field.type_.name], rest)
                    except StopIteration:
                        raise Exception(
                            f"SyntheticField {self._object.name}.{name}: {obj_.name}"
                            f" does not have the field {field_name}"
                        )
                case []:
                    return

        for fpath in sfield._deps:
            f(self._object, fpath._name_path())

        self._subgraph._add_synthetic_field(self._object, name, sfield)

    # ================================================================
    # Overloaded magic functions
    # ================================================================
    # Field selection
    def __getattribute__(self, __name: str) -> Any:
        # Small hack to get code completion to work while allowing updates to FieldPath
        # (i.e.: setting arguments)
        try:
            match super().__getattribute__(__name):
                case FieldPath() | SyntheticField() | None:
                    return self._select(__name)
                case value:
                    return value
        except AttributeError:
            return self._select(__name)

    def __setattr__(
        self, __name: str, __value: SyntheticField | FieldPath | Any
    ) -> None:
        match __value:
            case SyntheticField() as sfield:
                self._add_sfield(__name, sfield)
                super().__setattr__(__name, self._select(__name))
            case FieldPath() as fpath:
                self._add_field(__name, fpath)
                super().__setattr__(__name, self._select(__name))
            case _:
                super().__setattr__(__name, __value)
