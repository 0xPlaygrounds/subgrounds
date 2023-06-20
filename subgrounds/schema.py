""" Schema data structure module

This module contains various data structures in the form of dataclasses that
are used to represent GraphQL schemas in Subgrounds using an AST-like approach.
"""

from __future__ import annotations

import warnings
from typing import Annotated, Literal

from pipe import map, where
from pydantic import BaseModel as PydanticBaseModel
from pydantic import Field, root_validator

from .errors import SchemaError

warnings.simplefilter("default")


class BaseModel(PydanticBaseModel):
    class Config:
        allow_population_by_field_name = True


class TypeRef:
    class T(BaseModel):
        """Base class of all types of type references."""

        @property
        def name(self) -> str:
            raise NotImplementedError

        @property
        def is_list(self) -> bool:
            return False

        @property
        def is_non_null(self) -> bool:
            return False

    class Named(T):
        name_: str = Field(alias="name")
        kind: str

        @property
        def name(self) -> str:
            return self.name_

    class NonNull(T):
        inner: TypeRef_T = Field(alias="ofType")
        kind: Literal["NON_NULL"] = "NON_NULL"

        @property
        def name(self) -> str:
            return self.inner.name

        @property
        def is_list(self) -> bool:
            return self.inner.is_list

        @property
        def is_non_null(self) -> bool:
            return True

    class List(T):
        inner: TypeRef_T = Field(alias="ofType")
        kind: Literal["LIST"] = "LIST"

        @property
        def name(self) -> str:
            return self.inner.name

        @property
        def is_list(self) -> bool:
            return True

        @property
        def is_non_null(self) -> bool:
            return self.inner.is_non_null

    @staticmethod
    def non_null(name: str, kind: str = "SCALAR") -> TypeRef_T:
        return TypeRef.NonNull(inner=TypeRef.Named(name=name, kind=kind))

    @staticmethod
    def non_null_list(name: str, kind: str = "SCALAR") -> TypeRef_T:
        return TypeRef.NonNull(
            inner=TypeRef.List(
                inner=TypeRef.NonNull(inner=TypeRef.Named(name=name, kind=kind))
            )
        )

    @staticmethod
    def root_type_name(type_: TypeRef.T) -> str:
        return type_.name

    @staticmethod
    def is_non_null(type_: TypeRef.T) -> bool:
        return type_.is_non_null

    @staticmethod
    def is_list(type_: TypeRef.T) -> bool:
        return type_.is_list

    @staticmethod
    def graphql(type_: TypeRef.T) -> str:
        match type_:
            case TypeRef.Named(name=name):
                return name
            case TypeRef.NonNull(inner=t):
                return f"{TypeRef.graphql(t)}!"
            case TypeRef.List(inner=t):
                return f"[{TypeRef.graphql(t)}]"

        assert False


TypeRef_ListOrNonNull = Annotated[
    TypeRef.List | TypeRef.NonNull, Field(discriminator="kind")
]

TypeRef_T = TypeRef_ListOrNonNull | TypeRef.Named


class TypeMeta:
    class T(BaseModel):
        """Base class of all GraphQL schema types."""

        name: str
        description: str | None = ""

        @property
        def is_object(self) -> bool:
            return False

    class ArgumentMeta(T):
        """Class representing a field argument definition."""

        type_: TypeRef_T = Field(alias="type")
        default_value: str | None = Field(alias="defaultValue")

    class FieldMeta(T):
        """Class representing an object field definition."""

        arguments: list[TypeMeta.ArgumentMeta] = Field(alias="args")
        type_: TypeRef_T = Field(alias="type")

        def has_arg(self, argname: str) -> bool:
            try:
                next(self.arguments | where(lambda arg: arg.name == argname))
                return True
            except StopIteration:
                return False

        def type_of_arg(self: TypeMeta.FieldMeta, argname: str) -> TypeRef.T:
            try:
                return next(
                    self.arguments
                    | where(lambda argmeta: argmeta.name == argname)
                    | map(lambda arg: arg.type_)
                )
            except StopIteration:
                raise SchemaError(
                    f"TypeMeta.FieldMeta.type_of_arg: no argument named {argname}"
                    f" for field {self.name}"
                )

    class ScalarMeta(T):
        kind: Literal["SCALAR"] = "SCALAR"
        """ Class representing an scalar definition."""

    class ObjectMeta(T):
        """Class representing an object definition."""

        kind: Literal["OBJECT"] = "OBJECT"
        fields: list[TypeMeta.FieldMeta]
        interfaces_: list[dict] = Field(alias="interfaces", default_factory=list)

        # interfaces: list[str] = Field(default_factory=list)

        @property
        def interfaces(self) -> list[str]:
            return list(self.interfaces_ | map(lambda intf: intf["name"]))

        @property
        def is_object(self) -> bool:
            return True

        def field(self, fname: str) -> TypeMeta.FieldMeta:
            """Returns the field definition of object :attr:`self` with name
             :attr:`fname`, if any.

            Args:
                fname: The name of the desired field definition

            Raises:
                KeyError: If no field named :attr:`fname` is defined for object
                  :attr:`self`.

            Returns:
                The field definition
            """

            try:
                return next(self.fields | where(lambda fmeta: fmeta.name == fname))
            except StopIteration:
                raise KeyError(
                    f"TypeMeta.ObjectMeta.field: no field named {fname} for"
                    f" interface {self.name}"
                )

        def type_of_field(self, fname: str) -> TypeRef.T:
            """Returns the type reference of the field of object :attr:`self`
             with name :attr:`fname`, if any.

            Args:
                fname: The name of the desired field type

            Raises:
                KeyError: If no field named :attr:`fname` is defined for object
                 :attr:`self`.

            Returns:
                The field type reference
            """

            try:
                return next(
                    self.fields
                    | where(lambda fmeta: fmeta.name == fname)
                    | map(lambda fmeta: fmeta.type_)
                )
            except StopIteration:
                raise KeyError(
                    f"TypeMeta.ObjectMeta.type_of_field: no field named {fname}"
                    f" for object {self.name}"
                )

    class EnumValueMeta(T):
        """Class representing an enum value definition."""

        pass

    class EnumMeta(T):
        """Class representing an enum definition."""

        kind: Literal["ENUM"] = "ENUM"
        values: list[TypeMeta.EnumValueMeta] = Field(alias="enumValues")

    class InterfaceMeta(T):
        """Class representing an interface definition."""

        kind: Literal["INTERFACE"] = "INTERFACE"
        fields: list[TypeMeta.FieldMeta]

        @property
        def is_object(self) -> bool:
            return False

        def field(self, fname: str) -> TypeMeta.FieldMeta:
            """Returns the field definition of interface `self` with name `fname`,
             if any.

            Args:
                self: The interface type
                fname: The name of the desired field definition

            Raises:
                KeyError: If no field named :attr:`fname` is defined for interface
                  :attr:`self`.

            Returns:
                The field definition
            """

            try:
                return next(self.fields | where(lambda fmeta: fmeta.name == fname))
            except StopIteration:
                raise KeyError(
                    f"TypeMeta.InterfaceMeta.field: no field named {fname}"
                    f" for interface {self.name}"
                )

    class UnionMeta(T):
        """Class representing an union definition."""

        kind: Literal["UNION"] = "UNION"
        types: list[str] = Field(alias="possibleTypes")

    class InputObjectMeta(T):
        """Class representing an input object definition."""

        kind: Literal["INPUT_OBJECT"] = "INPUT_OBJECT"
        input_fields: list[TypeMeta.ArgumentMeta] = Field(alias="inputFields")

        def type_of_input_field(self, fname: str) -> TypeRef.T:
            """Returns the type reference of the input field named `fname` in the
            input object `self`, if any.

            Args:
                fname: The name of the input field

            Raises:
                KeyError: If `fname` is not an input field of input object `self`

            Returns:
                The type reference for input field `fname`
            """

            try:
                return next(
                    self.input_fields
                    | where(lambda infield: infield.name == fname)
                    | map(lambda infield: infield.type_)
                )
            except StopIteration:
                raise KeyError(
                    f"TypeMeta.InputObjectMeta.type_of_input_field: no input field"
                    f" named {fname} for input object {self.name}"
                )


TypeMeta_T = Annotated[
    TypeMeta.ScalarMeta | TypeMeta.ObjectMeta
    # | TypeMeta.EnumValueMeta
    | TypeMeta.EnumMeta
    | TypeMeta.InterfaceMeta
    | TypeMeta.UnionMeta
    | TypeMeta.InputObjectMeta,
    Field(discriminator="kind"),
]

# Pydantic stuff
TypeRef.Named.update_forward_refs()
TypeRef.List.update_forward_refs()
TypeRef.NonNull.update_forward_refs()

TypeMeta.ArgumentMeta.update_forward_refs()
TypeMeta.FieldMeta.update_forward_refs()
TypeMeta.ScalarMeta.update_forward_refs()
TypeMeta.ObjectMeta.update_forward_refs()
TypeMeta.EnumValueMeta.update_forward_refs()
TypeMeta.EnumMeta.update_forward_refs()
TypeMeta.InterfaceMeta.update_forward_refs()
TypeMeta.UnionMeta.update_forward_refs()
TypeMeta.InputObjectMeta.update_forward_refs()


class SchemaMeta(BaseModel):
    """Class representing a GraphQL schema.

    Contains all type definitions."""

    query_type_: dict[str, str] = Field(alias="queryType")
    types: list[TypeMeta_T]
    type_map: dict[str, TypeMeta_T] = Field(default_factory=dict)
    mutation_type_: dict[str, str] | None = Field(alias="mutationType", default=None)
    subscription_type_: dict[str, str] | None = Field(
        alias="subscriptionType", default=None
    )

    @property
    def query_type(self) -> str:
        return self.query_type_["name"]

    @property
    def mutation_type(self) -> str | None:
        if self.mutation_type_:
            return self.mutation_type_["name"]

        return None

    @property
    def subscription_type(self) -> str | None:
        if self.subscription_type_:
            return self.subscription_type_["name"]

        return None

    # @validator("type_map", "types", always=True, pre=True)
    @root_validator(allow_reuse=True)
    def type_map_generator(cls, values):
        if "types" in values and len(values["types"]) > 0:
            values["type_map"] = {type_.name: type_ for type_ in values["types"]}

        return values

    def type_of_typeref(self, typeref: TypeRef.T) -> TypeMeta_T:
        """Returns the type information of the type reference `typeref`

        Args:
            typeref: The type reference pointing to the type of interest.

        Raises:
            KeyError: If the type reference refers to a non-existant type

        Returns:
            Type information
        """

        tname = TypeRef.root_type_name(typeref)

        try:
            return self.type_map[tname]
        except KeyError:
            raise KeyError(
                f"SchemaMeta.type_of_typeref: No type named {typeref.name} in schema!"
            )

    def type_of(
        self: SchemaMeta, tmeta: TypeMeta.ArgumentMeta | TypeMeta.FieldMeta
    ) -> TypeMeta_T:
        """Returns the argument or field definition's underlying type"""

        return self.type_of_typeref(tmeta.type_)

    def type_of_input_object_meta(
        self, tmeta: TypeMeta.InputObjectMeta, args: list[str]
    ) -> TypeRef.T:
        """Recursively finds the nested type"""

        if len(args) < 1:
            raise Exception("type_of_input_object_meta: TODO")

        type_ref = tmeta.type_of_input_field(args.pop(0))
        match self.type_of_typeref(type_ref):
            case TypeMeta.InputObjectMeta() as tmeta:
                return self.type_of_input_object_meta(tmeta, args)

            case _:
                return type_ref
