""" Query data structure module

This module contains various data structures in the form of dataclasses that
are used to represent GraphQL queries in Subgrounds using an AST-like approach.
To the extent possible, these dataclasses are immutable (i.e.: :attr:`frozen=True`)
to enforce a functional programming style and reduce side-effects.

A typical Subgrounds request will have the following dataclass hierarchy:

.. code-block:: none

  DataRequest
  └── Document
      └── Query
          ├── VariableDefinition
          │   └── InputValue
          └── Selection
              ├── Argument
              │   └── InputValue
              └── Selection
"""

from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass, field, replace
from functools import partial, reduce
from typing import Any, Callable, Iterator, Literal, Protocol, TypeVar

from pipe import map, take, traverse, where
from typing_extensions import Self

from .errors import SubgroundsError
from .schema import SchemaMeta, TypeMeta, TypeRef
from .utils import (
    extract_data,
    filter_map,
    filter_none,
    identity,
    merge,
    rel_complement,
    union,
)

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")


# ================================================================
# Query definitions, data structures and types
# ================================================================
class InputValue:
    class T(Protocol):
        @property
        def graphql(self) -> str:
            """Returns a GraphQL string representation of the input value

            Returns:
              The GraphQL string representation of the input value
            """
            ...

        @property
        def is_variable(self) -> bool:
            """Returns True i.f.f. the input value is of type Variable

            Returns:
              True i.f.f. the input value is of type Variable, otherwise False
            """
            ...

        @property
        def is_number(self) -> bool:
            """Returns True i.f.f. the input value is of type Float or Int

            Returns:
              True i.f.f. the input value is of type Float or Int, otherwise False
            """
            ...

        def iter(self) -> Iterator[InputValue.T]:
            ...

    @dataclass(frozen=True)
    class Null:
        @property
        def graphql(self) -> str:
            return "null"

        @property
        def is_variable(self) -> bool:
            return False

        @property
        def is_number(self) -> bool:
            return False

        def iter(self) -> Iterator[InputValue.T]:
            yield self

    @dataclass(frozen=True)
    class Int:
        value: int

        @property
        def graphql(self) -> str:
            return str(self.value)

        @property
        def is_variable(self) -> bool:
            return False

        @property
        def is_number(self) -> bool:
            return True

        def iter(self) -> Iterator[InputValue.T]:
            yield self

    @dataclass(frozen=True)
    class Float:
        value: float

        @property
        def graphql(self) -> str:
            return str(self.value)

        @property
        def is_variable(self) -> bool:
            return False

        @property
        def is_number(self) -> bool:
            return True

        def iter(self) -> Iterator[InputValue.T]:
            yield self

    @dataclass(frozen=True)
    class String:
        value: str

        @property
        def graphql(self) -> str:
            return f'"{self.value}"'

        @property
        def is_variable(self) -> bool:
            return False

        @property
        def is_number(self) -> bool:
            return False

        def iter(self) -> Iterator[InputValue.T]:
            yield self

    @dataclass(frozen=True)
    class Boolean:
        value: bool

        @property
        def graphql(self) -> str:
            return str(self.value).lower()

        @property
        def is_variable(self) -> bool:
            return False

        @property
        def is_number(self) -> bool:
            return False

        def iter(self) -> Iterator[InputValue.T]:
            yield self

    @dataclass(frozen=True)
    class Enum:
        value: str

        @property
        def graphql(self) -> str:
            return self.value

        @property
        def is_variable(self) -> bool:
            return False

        @property
        def is_number(self) -> bool:
            return False

        def iter(self) -> Iterator[InputValue.T]:
            yield self

    @dataclass(frozen=True)
    class Variable:
        name: str

        @property
        def graphql(self) -> str:
            return f"${self.name}"

        @property
        def is_variable(self) -> bool:
            return True

        @property
        def is_number(self) -> bool:
            return False

        def iter(self) -> Iterator[InputValue.T]:
            yield self

    @dataclass(frozen=True)
    class List:
        value: list[InputValue.T]

        @property
        def graphql(self) -> str:
            return f"[{', '.join([val.graphql for val in self.value])}]"

        @property
        def is_variable(self) -> bool:
            return False

        @property
        def is_number(self) -> bool:
            return False

        def iter(self) -> Iterator[InputValue.T]:
            yield self
            for val in self.value:
                yield from val.iter()

    @dataclass(frozen=True)
    class Object:
        value: dict[str, InputValue.T]

        @property
        def graphql(self) -> str:
            field_strings = [
                f"{key}: {value.graphql}" for key, value in self.value.items()
            ]
            fields = ", ".join(field_strings)
            return "{" + fields + "}"

        @property
        def is_variable(self) -> bool:
            return False

        @property
        def is_number(self) -> bool:
            return False

        def iter(self) -> Iterator[InputValue.T]:
            yield self
            for val in self.value.values():
                yield from val.iter()


@dataclass(frozen=True)
class VariableDefinition:
    """Representation of a GraphQL variable definition

    Attributes:
      name: Name of the argument
      type_: GraphQL type of the argument
      default: Default value of the variable. Defaults to None.
    """

    name: str
    type_: TypeRef.T
    default: InputValue.T | None = None

    @property
    def graphql(self) -> str:
        """Returns the GraphQL string representation of the variable definition

        Example:

        >>> vardef = VariableDefinition(
        ...   name='foo',
        ...   type_=TypeRef.NonNull(TypeRef.Named(name="Int", kind="SCALAR")),
        ...   default=InputValue.Int(100)
        ... )
        >>> print(vardef.graphql)
        $foo: Int! = 100

        Returns:
          The GraphQL string representation of the variable definition
        """

        if self.default is None:
            return f"${self.name}: {TypeRef.graphql(self.type_)}"
        else:
            return (
                f"${self.name}: {TypeRef.graphql(self.type_)} = {self.default.graphql}"
            )


@dataclass(frozen=True)
class Argument:
    name: str
    value: InputValue.T

    @property
    def graphql(self) -> str:
        return f"{self.name}: {self.value.graphql}"

    def iter(self) -> Iterator[InputValue.T]:
        yield from self.value.iter()

    def iter_vars(self) -> Iterator[InputValue.Variable]:
        for iv in self.value.iter():
            match iv:
                case InputValue.Variable():
                    yield iv
                case _:
                    continue

    def for_all(self, predicate: Callable[[InputValue.T], bool]) -> bool:
        return all(self.iter() | map(predicate))

    def for_all_vars(self, predicate: Callable[[InputValue.Variable], bool]) -> bool:
        return all(self.iter_vars() | map(predicate))

    def exists(self, predicate: Callable[[InputValue.T], bool]) -> bool:
        return any(self.iter() | map(predicate))

    def exists_vars(self, predicate: Callable[[InputValue.Variable], bool]) -> bool:
        return any(self.iter_vars() | map(predicate))

    def find(self, predicate: Callable[[InputValue.T], bool]) -> InputValue.T | None:
        try:
            return next(self.iter() | where(predicate))
        except StopIteration:
            return None

    def find_var(
        self, predicate: Callable[[InputValue.Variable], bool]
    ) -> InputValue.T | None:
        try:
            return next(self.iter_vars() | where(predicate))
        except StopIteration:
            return None

    def all_defined(self, variables: Iterator[str]) -> bool:
        return self.for_all_vars(lambda var: var.name in variables)


@dataclass(frozen=True)
class Selection:
    """Represents a GraphQL field selection.

    Attributes:
      fmeta: The type definition of the field being selected.
      alias: The alias of the field selection. Defaults to None.
      arguments: The arguments, if any, of the field selection. Defaults to [].
      selection: The inner field selections, if any. Defaults to [].
    """

    fmeta: TypeMeta.FieldMeta
    alias: str | None = None
    arguments: list[Argument] = field(default_factory=list)
    selection: list[Selection] = field(default_factory=list)

    @property
    def key(self):
        if self.alias is not None:
            return self.alias
        else:
            return self.fmeta.name

    @property
    def args_graphql(self) -> str:
        if self.arguments:
            return f'({", ".join([arg.graphql for arg in self.arguments])})'
        else:
            return ""

    def graphql(self, level: int = 0) -> str:
        indent = "  " * level

        if self.alias:
            alias_str = f"{self.alias}: "
        else:
            alias_str = ""

        match (self.selection):
            case None | []:
                return f"{indent}{alias_str}{self.fmeta.name}{self.args_graphql}"
            case inner_selection:
                inner_str = "\n".join(
                    [f.graphql(level=level + 1) for f in inner_selection]
                )
                return (
                    f"{indent}{alias_str}{self.fmeta.name}{self.args_graphql}"
                    f" {{\n{inner_str}\n{indent}}}"
                )  # TODO: string templates w/ `.format`?

    @property
    def data_path(self) -> list[str]:
        match self:
            case Selection(TypeMeta.FieldMeta(name), None, _, []) | Selection(
                TypeMeta.FieldMeta(_), str() as name, _, []
            ):
                return [name]
            case Selection(
                TypeMeta.FieldMeta(name), None, _, [inner_select, *_]
            ) | Selection(TypeMeta.FieldMeta(_), str() as name, _, [inner_select, *_]):
                return [name] + inner_select.data_path

        assert False  # Suppress mypy missing return statement warning

    @property
    def data_paths(self) -> list[list[str]]:
        def f(select: Selection, keys: list[str] = []):
            match select:
                case Selection(TypeMeta.FieldMeta(name=name), None, _, []) | Selection(
                    TypeMeta.FieldMeta(name=_), str() as name, _, []
                ):
                    yield [*keys, name]
                case Selection(
                    TypeMeta.FieldMeta(name=name), None, _, inner
                ) | Selection(TypeMeta.FieldMeta(name=_), str() as name, _, inner):
                    for select in inner:
                        yield from f(select, keys=[*keys, name])

        return list(f(self))

    # ================================================================
    # Generic functions
    # ================================================================
    def iter(self) -> Iterator[Selection]:
        """Returns an iterator over all ``Selections`` of the current selection tree."""

        yield self
        for inner in self.selection:
            yield from inner.iter()

    def iter_args(self, recurse: bool = True) -> Iterator[Argument]:
        """Returns an iterator over all ``Arguments`` of the current ``Selection``.

        If ``recurse == True``, then the iterator also includes ``Arguments`` of
        inner ``Selections``.
        """

        for arg in self.arguments:
            yield arg

        if recurse:
            for inner in self.selection:
                yield from inner.iter_args()

    def filter(self, predicate: Callable[[Selection], bool]) -> Selection | None:
        """Returns a new ``Selection`` object containing all attributes of the current
        ``Selection`` if ``predicate(self) == True`` and ``None`` otherwise. The
        function if also applied recursively to inner ``Selections``.

        Args:
          predicate: _description_

        Returns:
          _description_
        """

        if predicate(self):
            return Selection(
                fmeta=self.fmeta,
                alias=self.alias,
                arguments=self.arguments,
                selection=list(
                    self.selection
                    | filter_map(partial(Selection.filter, predicate=predicate))
                ),
            )

        return None

    def filter_args(
        self, predicate: Callable[[Argument], bool], recurse: bool = True
    ) -> Selection:
        """Returns a new ``Selection`` object which contains all attributes of
        the current ``Selection`` except for ``Arguments`` for which
        ``predicate(arg) == True``.

        If ``recurse == True``, then the function is applied recursively to
        inner ``Selections``

        Args:
          predicate: _description_
          recurse: _description_. Defaults to True.

        Returns:
          _description_
        """
        return Selection(
            fmeta=self.fmeta,
            alias=self.alias,
            arguments=list(self.arguments | where(predicate)),
            selection=list(
                self.selection
                | map(partial(Selection.filter_args, predicate=predicate))
            )
            if recurse
            else self.selection,
        )

    def map(
        self,
        map_f: Callable[[Selection], Selection],
        priority: Literal["self"] | Literal["children"] = "self",
    ) -> Selection:
        """Returns a new ``Selection`` object containing the same selection tree
        as the current ``Selection`` where each ``Selection`` object ``s`` is
        ``map_f(s)``

        Args:
          map_f: Mapping function to apply to each ``Selection``

        Returns:
          _description_
        """

        match priority:
            case "self":
                new_selection = map_f(self)

                return Selection(
                    fmeta=new_selection.fmeta,
                    alias=new_selection.alias,
                    arguments=new_selection.arguments,
                    selection=list(
                        self.selection
                        | map(partial(Selection.map, map_f=map_f, priority=priority))
                    ),
                )

            case "children":
                new_selection = Selection(
                    fmeta=self.fmeta,
                    alias=self.alias,
                    arguments=self.arguments,
                    selection=list(
                        self.selection
                        | map(partial(Selection.map, map_f=map_f, priority=priority))
                    ),
                )

                return map_f(new_selection)

            case _:
                raise SubgroundsError(f"map: invalid priority {priority}")

    def map_args(
        self,
        map_f: Callable[[Argument], Argument | list[Argument]],
        recurse: bool = True,
    ) -> Selection:
        """Replaces each ``Argument`` ``arg`` in the current ``Selection`` with
         ``map_f(arg)`` and returns a new ``Selection`` object containing
         the modified arguments.

        If ``recurse == True``, then the function is applied recursively to inner
        ``Selections``.

        Args:
          map_f: _description_
          recurse: _description_. Defaults to True.

        Returns:
          _description_
        """

        return Selection(
            fmeta=self.fmeta,
            alias=self.alias,
            arguments=list(self.arguments | map(map_f)),
            selection=list(
                self.selection
                | map(partial(Selection.map_args, map_f=map_f))
                | traverse
            )
            if recurse
            else self.selection,
        )

    def filter_map(
        self, map_f: Callable[[Selection], Selection | None]
    ) -> Selection | None:
        new_selection = map_f(self)

        if new_selection is not None:
            return Selection(
                fmeta=new_selection.fmeta,
                alias=new_selection.alias,
                arguments=new_selection.arguments,
                selection=list(
                    self.selection
                    | filter_map(partial(Selection.filter_map, map_f=map_f))
                ),
            )

        return None

    def filter_map_args(
        self,
        map_f: Callable[[Argument], Argument | list[Argument | None]],
        recurse: bool = True,
    ) -> Selection:
        return Selection(
            fmeta=self.fmeta,
            alias=self.alias,
            arguments=list(self.arguments | filter_map(map_f) | traverse),
            selection=list(
                self.selection | map(partial(Selection.filter_map_args, map_f=map_f))
            )
            if recurse
            else self.selection,
        )

    def for_all(self, predicate: Callable[[Selection], bool]) -> bool:
        if predicate(self):
            return all(
                self.selection | map(partial(Selection.for_all, predicate=predicate))
            )

        return False

    def for_all_args(
        self, predicate: Callable[[Argument], bool], recurse: bool = True
    ) -> bool:
        if all(self.arguments | map(predicate)):
            if recurse:
                return all(
                    self.selection
                    | map(partial(Selection.for_all_args, predicate=predicate))
                )

            return True

        return False

    def exists(self, predicate: Callable[[Selection], bool]) -> bool:
        if predicate(self):
            return True

        return any(self.selection | map(partial(Selection.exists, predicate=predicate)))

    def exists_args(
        self, predicate: Callable[[Argument], bool], recurse: bool = True
    ) -> bool:
        if any(self.arguments | map(predicate)):
            return True

        if recurse:
            return any(
                self.selection
                | map(partial(Selection.exists_args, predicate=predicate))
            )

        return False

    def find(self, predicate: Callable[[Selection], bool]) -> Selection | None:
        try:
            return next(self.iter() | where(predicate))
        except StopIteration:
            return None

    def find_args(
        self, predicate: Callable[[Argument], bool], recurse: bool = True
    ) -> Argument | None:
        try:
            return next(self.iter_args(recurse=recurse) | where(predicate))
        except StopIteration:
            return None

    def find_all(self, predicate: Callable[[Selection], bool]) -> list[Selection]:
        return list(self.iter() | where(predicate))

    def find_all_args(
        self, predicate: Callable[[Argument], bool], recurse: bool = True
    ) -> list[Argument]:
        return list(self.iter_args(recurse=recurse) | where(predicate))

    T = TypeVar("T")

    def fold(
        self,
        fold_f: Callable[[Selection, list[Selection], list[T]], T | list[T]],
        parents: list[Selection] = [],
    ) -> T | list[T]:
        inner = list(
            self.selection
            | map(partial(Selection.fold, fold_f=fold_f, parents=[*parents, self]))
            | traverse
        )
        return fold_f(self, parents, inner)

    def contains_list(self) -> bool:
        """Returns True i.f.f. the selection :attr:`self` selects a field of type
        list.

        Args:
          self: The selection to traverse

        Returns:
          True if selection or nested selections selects a list field. False otherwise.
        """

        return self.exists(lambda select: select.fmeta.type_.is_list)

    def split(self) -> list[Selection]:
        """Returns a list of selections where each of the selections corresponds
        to a single selection path from the root to a leaf for each leaf selected
        in :attr:`self`.

        Example (simplified, does not show all attributes):

        >>> select = Selection('foo', inner=[
        ...   Selection('bar', inner=[
        ...     Selection('field0', inner=[]),
        ...     Selection('field1', inner=[]),
        ...   ]),
        ...   Selection('x', inner=[])
        ... ])
        >>> split(select)
        [
          Selection(
            'foo', inner=[Selection('bar', inner=[Selection('field0', inner=[])])]),
          Selection(
            'foo', inner=[Selection('bar', inner=[Selection('field1', inner=[])])]),
          Selection(
            'foo', inner=[Selection('x', inner=[])]),
        ]

        Args:
          self: The selection to split

        Returns:
          The split selections
        """

        match self:
            case Selection(_, _, _, [] | None):
                return [self]
            case Selection(fmeta, alias, args, inner_select):
                return list(
                    inner_select
                    | map(Selection.split)
                    | traverse
                    | map(
                        lambda inner_select: Selection(fmeta, alias, args, inner_select)
                    )
                )

    def extract_data(self: Selection, data: dict | list[dict]) -> list[Any] | Any:
        return extract_data(self.data_path, data)

    def add(self, new_selections: Selection | list[Selection]) -> Selection:
        """Returns a new selection consisting of a copy of :attr:`self` expanded
        with the selection(s) :attr:`new_selections`. It is assumed that
        :attr:`new_selections` are inner selections of the root selection
        :attr:`self`.

        Args:
          self: The Selection object to be expanded
          new_selections: A single or multiple Selection object(s) to be added to
            :attr:`self`

        Returns:
          The resulting new selection, i.e.: :attr:`self` expanded with
            :attr:`new_selections`
        """

        match new_selections:
            case Selection() as new_selection:
                return self.add([new_selection])

            case list() as new_selections:
                return Selection(
                    fmeta=self.fmeta,
                    alias=self.alias,
                    arguments=self.arguments,
                    selection=union(
                        self.selection,
                        new_selections,
                        key=lambda select: select.fmeta.name,
                        combine=Selection.combine,
                    ),
                )

    def remove(self, to_remove: Selection | list[Selection]) -> Selection:
        """Returns a new Selection object consisting of a copy of :attr:`self`
        without the selections in :attr:`selections_to_remove`.

        Args:
          to_remove: The selection(s) to remove from :attr:`self`

        Returns:
          The new trimmed down selection, i.e.: :attr:`self` without
            :attr:`selections_to_remove`
        """

        def combine(
            select: Selection, selection_to_remove: Selection
        ) -> Selection | None:
            if selection_to_remove.selection == []:
                return None
            else:
                return select.remove(selection_to_remove.selection)

        # TODO: Resolve mypy complaints
        match to_remove:
            case Selection() as to_remove:
                return self.remove([to_remove])

            case list() as to_remove:
                return Selection(
                    fmeta=self.fmeta,
                    alias=self.alias,
                    arguments=self.arguments,
                    selection=filter_none(
                        union(
                            self.selection,
                            to_remove,
                            key=lambda s: s.fmeta.name,
                            combine=combine,
                        )
                    ),
                )

    def variable_args(self, recurse: bool = True) -> Iterator[Argument]:
        """Returns all arguments in the current selection which have been given a
        variable as value.

        If ``recurse == True``, then the function is applied recursively to inner
        selections.

        Args:
          recurse: _description_. Defaults to True.

        Returns:
          _description_
        """
        for arg in self.iter_args(recurse=recurse):
            if type(arg.value) == InputValue.Variable:
                yield arg

    def infer_variable_definitions(self: Selection) -> list[VariableDefinition]:
        def vardef_of_arg(arg):
            match arg:
                case Argument(name=argname, value=InputValue.Variable(name=varname)):
                    return VariableDefinition(varname, self.fmeta.type_of(argname))

                case Argument(name=argname, value=InputValue.Object(fields)):
                    return list(
                        fields.values()
                        | where(lambda iv: type(iv) == InputValue.Variable)
                        | map(
                            lambda iv: VariableDefinition(
                                iv.name, self.fmeta.type_of(argname)
                            )
                        )
                    )

                case _:
                    return []

        var_defs = list(self.arguments | map(vardef_of_arg) | traverse)

        return (
            list(self.selection | map(Selection.infer_variable_definitions) | traverse)
            + var_defs
        )

    def combine(self: Selection, other: Selection) -> Selection:
        if self.key != other.key:
            raise SubgroundsError(f"Selection.combine: {self.key} != {other.key}")

        return Selection(
            fmeta=self.fmeta,
            alias=self.alias,
            arguments=self.arguments,
            selection=filter_none(
                union(
                    self.selection,
                    other.selection,
                    key=lambda select: select.fmeta.name,
                    combine=Selection.combine,
                )
            ),
        )

    @staticmethod
    def merge(selections: list[Selection]) -> list[Selection]:
        """Returns a list of Selection objects resulting from merging
        :attr:`selections` to the extent possible.

        Args:
          selections The selections to be merged

        Returns:
          _description_
        """

        def f(selections: list[Selection], other: Selection) -> list[Selection]:
            try:
                next(selections | where(lambda select: select.key == other.key))
                return list(
                    selections
                    | map(
                        lambda select: Selection.combine(select, other)
                        if select.key == other.key
                        else select
                    )
                )
            except StopIteration:
                return selections + [other]

        return reduce(f, selections, [])

    def contains(self, other: Selection) -> bool:
        """Returns True i.f.f. the Selection :attr:`other` is a subtree of the
        Selection :attr:`self` and False otherwise

        Args:
          self: The selection
          other: The subselection

        Returns:
          True i.f.f. :attr:`other` is in :attr:`self`
        """

        if (
            self.fmeta == other.fmeta
            and rel_complement(
                other.selection, self.selection, key=lambda s: s.fmeta.name
            )
            == []
        ):
            return all(
                other.selection
                | map(
                    lambda s: Selection.contains(
                        next(
                            filter(
                                lambda s_: s.fmeta.name == s_.fmeta.name, self.selection
                            )
                        ),
                        s,
                    )
                )
            )
        else:
            return False

    def contains_argument(self, argname: str, recurse: bool = True) -> bool:
        """Returns True i.f.f. there is an Argument object in :attr:`self` named
        :attr:`argname`. If :attr:`recurse` is True, then the method also checks the
        nested selections for an argument named :attr:`argname`.

        Args:
          argname: The name of the argument
          recurse: Flag indicating whether or not the method should be run recursively
            on nested selections. Defaults to True.

        Returns:
          True i.f.f. there is an argument named :attr:`argname` in :attr:`self`
        """

        return self.exists_args(lambda arg: arg.name == argname, recurse=recurse)

    def get_argument(self, argname: str, recurse: bool = True) -> Argument | None:
        """Returns an Argument object corresponding to the argument in the Selection
        object :attr:`select` with name :attr:`argname`. If :attr:`select` does not
        contain such an argument and :attr:`recurse` is True, then the function is
        called recursively on :attr:`select`'s inner selections. If no such argument
        is found in :attr:`select` or its inner selections, then the function raises
        an exception.

        Args:
          argname: The name of the argument to find
          recurse: Flag indicating whether or not the method should be run recursively
            on nested selections. Defaults to True.

        Raises:
          KeyError: If no argument named :attr:`argname` exists in the selection
            :attr:`self`.

        Returns:
          The argument in :attr:`select` with name :attr:`argname` (if any).
        """

        return self.find_args(lambda arg: arg.name == argname, recurse=recurse)

    def get_argument_by_variable(
        self, varname: str, recurse: bool = True
    ) -> Argument | None:
        """Returns an Argument object corresponding to the argument in the Selection
        object :attr:`select` whose value is a variable named :attr:`varname`. If
        :attr:`select` does not contain such an argument and :attr:`recurse` is True,
        then the function is called recursively on :attr:`select`'s inner selections.
        If no such argument is found in :attr:`select` or its inner selections, then
        the function raises an exception

        Args:
          varname: The name of the variable to find
          recurse: Flag indicating whether or not the method should be run recursively.
            Defaults to True.

        Raises:
          KeyError: If no argument with variable value named :attr:`varname` exists
            in the selection :attr:`self`.

        Returns:
          The argument in :attr:`select` with variable value named :attr:`varname`
            if it exists
        """

        return self.find_args(
            lambda arg: arg.exists_vars(lambda var: var.name == varname),
            recurse=recurse,
        )

    # TODO: Replace substitute_arg calls by map_args call
    def substitute_arg(
        self, argname: str, replacement: Argument | list[Argument], recurse: bool = True
    ) -> Selection:
        """Returns a new Selection object containing the same data as :attr:`self`
        with the argument named :attr:`argname` replaced with :attr:`replacement`.
        If :attr:`recurse` is True, then the method is called recursively on
        :attr:`self`'s inner selections and the substitution is also applied to the
        latter.

        Args:
          argname: The name of the argument to substitute.
          replacement: The argument(s) replacement
          recurse: Flag indicating whether or not the method should be run recursively.
            Defaults to True.

        Returns:
          _description_
        """

        return self.map_args(
            lambda arg: replacement if arg.name == argname else arg, recurse=recurse
        )

    def select(self: Selection, other: Selection) -> Selection:
        if other.selection == []:
            return self
        else:
            return Selection(
                fmeta=self.fmeta,
                alias=self.alias,
                arguments=self.arguments,
                selection=list(
                    other.selection
                    | map(
                        lambda s: next(
                            self.selection
                            | where(lambda s_: s_.fmeta.name == s.fmeta.name)
                            | map(lambda s_: Selection.select(s_, s))
                            | take(1)
                        )
                    )
                ),
            )

    def prune_undefined(self, variables: Iterator[str]) -> Selection | None:
        """Return a new ``Selection`` containing the subtree of the current
        ``Selection`` where all argument ``InputValues`` are defined,
        i.e.: each argument's ``InputValue`` is either
            1) not of type ``InputValue.Variable``
            or
            2) of type ``InputValue.Variable``
            and
            the variable name is contained in ``variables``.

        Args:
          variables: An iterator over defined variables

        Returns:
          A new pruned ``Selection`` object
        """

        return self.filter(
            lambda select: select.for_all_args(
                lambda arg: arg.all_defined(variables), recurse=False
            )
        )


@dataclass(frozen=True)
class Query:
    name: str | None = None
    selection: list[Selection] = field(default_factory=list)

    # Variables as query arguments, not the values of those variables
    variables: list[VariableDefinition] = field(default_factory=list)

    @property
    def graphql(self) -> str:
        """Returns a string containing a GraphQL query matching the current query

        Returns:
          The string containing the GraphQL query
        """

        selection_str = "\n".join(
            [select.graphql(level=1) for select in self.selection]
        )

        if len(self.variables) > 0:
            args_str = f'({", ".join([vardef.graphql for vardef in self.variables])})'
        else:
            args_str = ""

        return f"query{args_str} {{\n{selection_str}\n}}"

    # ================================================================
    # Generic functions
    # ================================================================
    def iter(self) -> Iterator[Selection]:
        """Returns an iterator over all ``Selections`` of the selection tree of
        the current ``Query``."""

        for select in self.selection:
            yield from select.iter()

    def iter_args(self) -> Iterator[Argument]:
        """Returns an iterator over all ``Arguments`` of the selection tree of
        the current ``Query``."""

        for select in self.selection:
            yield from select.iter_args()

    def iter_vardefs(self) -> Iterator[VariableDefinition]:
        """Returns an iterator over all ``VariableDefinitions`` of the selection tree of
        the current ``Query``."""

        for vardef in self.variables:
            yield vardef

    def filter(self, predicate: Callable[[Selection], bool]) -> Query:
        """Returns a new ``Query`` object containing all selections ``s`` that satisfy
        ``predicate(s) == True``.

        Args:
          predicate: _description_

        Returns:
          _description_
        """

        return Query(
            name=self.name,
            selection=list(
                self.selection
                | filter_map(partial(Selection.filter, predicate=predicate))
            ),
            variables=self.variables,
        )

    def filter_args(self, predicate: Callable[[Argument], bool]) -> Query:
        """Returns a new ``Query`` object containing all selections arguments ``arg``
        that satisfy ``predicate(arg) == True``.

        Args:
          predicate: _description_

        Returns:
          _description_
        """

        return Query(
            name=self.name,
            selection=list(
                self.selection
                | map(partial(Selection.filter_args, predicate=predicate))
            ),
            variables=self.variables,
        )

    def filter_vardefs(self, predicate: Callable[[VariableDefinition], bool]) -> Query:
        return Query(
            name=self.name,
            selection=self.selection,
            variables=list(self.variables | where(predicate)),
        )

    def map(
        self,
        map_f: Callable[[Selection], Selection],
        priority: Literal["self"] | Literal["children"] = "self",
    ) -> Query:
        """Applies the function ``map_f`` to each ``Selection`` in the current
         ``Query`` and returns a new ``Query`` object containing the resulting
         ``Selections``.

        Args:
          map_f: Mapping function to apply to each ``Selection``

        Returns:
          _description_
        """

        return Query(
            name=self.name,
            selection=list(
                self.selection
                | map(partial(Selection.map, map_f=map_f, priority=priority))
            ),
            variables=self.variables,
        )

    def map_args(self, map_f: Callable[[Argument], Argument]) -> Query:
        """Applies the function ``map_f`` to each ``Argument`` in the current
         ``Query`` and returns a new ``Query`` object containing the resulting
         ``Arguments``.

        Args:
          map_f: _description_

        Returns:
          _description_
        """

        return Query(
            name=self.name,
            selection=list(
                self.selection | map(partial(Selection.map_args, map_f=map_f))
            ),
            variables=self.variables,
        )

    def map_vardefs(
        self, map_f: Callable[[VariableDefinition], VariableDefinition]
    ) -> Query:
        return Query(
            name=self.name,
            selection=self.selection,
            variables=list(self.variables | map(map_f)),
        )

    def filter_map(self, map_f: Callable[[Selection], Selection | None]) -> Query:
        return Query(
            name=self.name,
            selection=list(
                self.selection | filter_map(partial(Selection.filter_map, map_f=map_f))
            ),
            variables=self.variables,
        )

    def filter_map_args(self, map_f: Callable[[Argument], Argument | None]) -> Query:
        return Query(
            name=self.name,
            selection=list(
                self.selection | map(partial(Selection.filter_map_args, map_f=map_f))
            ),
            variables=self.variables,
        )

    def filter_map_vardefs(
        self, map_f: Callable[[VariableDefinition], VariableDefinition | None]
    ) -> Query:
        return Query(
            name=self.name,
            selection=self.selection,
            variables=list(self.variables | filter_map(map_f)),
        )

    def for_all(self, predicate: Callable[[Selection], bool]) -> bool:
        return all(
            self.selection | map(partial(Selection.for_all, predicate=predicate))
        )

    def for_all_args(self, predicate: Callable[[Argument], bool]) -> bool:
        return all(
            self.selection | map(partial(Selection.for_all_args, predicate=predicate))
        )

    def for_all_vardefs(self, predicate: Callable[[VariableDefinition], bool]) -> bool:
        return all(self.variables | map(predicate))

    def exists(self, predicate: Callable[[Selection], bool]) -> bool:
        return any(self.selection | map(partial(Selection.exists, predicate=predicate)))

    def exists_args(self, predicate: Callable[[Argument], bool]) -> bool:
        return any(
            self.selection | map(partial(Selection.exists_args, predicate=predicate))
        )

    def exists_vardefs(self, predicate: Callable[[VariableDefinition], bool]) -> bool:
        return any(self.variables | map(predicate))

    def find(self, predicate: Callable[[Selection], bool]) -> Selection | None:
        try:
            return next(self.iter() | where(predicate))
        except StopIteration:
            return None

    def find_args(self, predicate: Callable[[Argument], bool]) -> Argument | None:
        try:
            return next(self.iter_args() | where(predicate))
        except StopIteration:
            return None

    def find_vardefs(
        self, predicate: Callable[[VariableDefinition], bool]
    ) -> VariableDefinition | None:
        try:
            return next(self.iter_vardefs() | where(predicate))
        except StopIteration:
            return None

    T = TypeVar("T")

    def fold(
        self,
        fold_f: Callable[[Selection, list[Selection], list[T]], T | list[T]],
    ) -> list[T]:
        return list(self.selection | map(partial(Selection.fold, fold_f=fold_f)))

    def infer_variable_definitions(self: Query) -> list[VariableDefinition]:
        return list(
            self.selection | map(Selection.infer_variable_definitions) | traverse
        )

    def add(self: Query, other: Query | Selection | list[Selection]) -> Query:
        """Returns a new Query containing all selections in :attr:'self' along with
        the new selections in :attr:`other`

        Args:
          self: The query to which new selection(s) or query are to be added
          other: The new selection(s) or query to be added to the query

        Returns:
          A new `Query` objects containing all selections
        """

        match other:
            case Selection() as new_selection:
                return self.add([new_selection])

            case Query() as query:
                return self.add(query.selection)

            case list() as new_selections:
                return Query(
                    name=self.name,
                    selection=union(
                        self.selection,
                        new_selections,
                        key=lambda select: select.key,
                        combine=Selection.combine,
                    ),
                )

    def add_vardefs(self, vardefs: list[VariableDefinition]) -> Query:
        return Query(
            name=self.name,
            selection=self.selection,
            variables=union(
                self.variables,
                vardefs,
                key=lambda vardef: vardef.name,
                combine=lambda _, x: x,
            ),
        )

    def remove(self: Query, other: Query | Selection | list[Selection]) -> Query:
        """Returns a new :class:`Query` object containing all selections in
        :attr:`self` minus the subquery or selection(s) specified in :attr:`other`.

        Note: :attr:`other` does not need to be a "full" selection (i.e.: a
        selection all the way to leaves of the GraphQL schema).

        Example:

        >>> og_selection = Selection(
        ...     TypeMeta.FieldMeta(
        ...         'pair',
        ...         description="",
        ...         args=[],
        ...         type=TypeRef.non_null_list("Pair", kind="OBJECT")
        ...     ),
        ...     None,
        ...     [],
        ...     [
        ...         Selection(
        ...             TypeMeta.FieldMeta(
        ...                 'token0',
        ...                 description="",
        ...                 args=[],
        ...                 type=TypeRef.Named(name="Token", kind="OBJECT")
        ...             ),
        ...             None,
        ...             [],
        ...             [
        ...                 Selection(
        ...                     TypeMeta.FieldMeta(
        ...                         'id',
        ...                         description="",
        ...                         args=[],
        ...                         type=TypeRef.Named(name="String", kind="SCALAR")
        ...                     ),
        ...                     None,
        ...                     [],
        ...                     []
        ...                 ),
        ...                 Selection(
        ...                     TypeMeta.FieldMeta(
        ...                         'name',
        ...                         description="",
        ...                         args=[],
        ...                         type=TypeRef.Named(name="String", kind="SCALAR")
        ...                     ),
        ...                     None,
        ...                     [],
        ...                     []
        ...                 ),
        ...                 Selection(
        ...                     TypeMeta.FieldMeta(
        ...                         'symbol',
        ...                         description="",
        ...                         args=[],
        ...                         type=TypeRef.Named(name="String", kind="SCALAR")
        ...                     ),
        ...                     None,
        ...                     [],
        ...                     []
        ...                 ),
        ...             ]
        ...         )
        ...     ]
        ... )
        >>> selection_to_remove = Selection(
        ...     TypeMeta.FieldMeta(
        ...         'token0',
        ...         description="",
        ...         args=[],
        ...         type=TypeRef.Named(name="Token", kind="OBJECT")
        ...     ),
        ...     None,
        ...     [],
        ...     []
        ... )
        >>> og_selection.remove(selection_to_remove)
        Selection(
            TypeMeta.FieldMeta(
                'pair',
                description="",
                args=[],
                type=TypeRef.non_null_list("Pair", kind="OBJECT")
            ),
            None,
            [],
            []
        )

        Args:
          query (Query): The query to which a selection has to be removed
          other (Query | Selection | list[Selection]): The subquery or
            selection(s) to remove from :attr:`self`

        Returns:
          Query: A new `Query` object containing the original query selections
            minus :attr:`other`
        """

        def combine(
            select: Selection, selection_to_remove: Selection
        ) -> Selection | None:
            if selection_to_remove.selection == []:
                return None
            else:
                return select.remove(selection_to_remove.selection)

        match other:
            case Selection() as to_remove:
                return self.remove([to_remove])

            case Query() as to_remove:
                return self.remove(to_remove.selection)

            case list() as to_remove:
                return Query(
                    name=self.name,
                    selection=filter_none(
                        union(
                            self.selection,
                            to_remove,
                            key=lambda s: s.fmeta.name,
                            combine=combine,
                        )
                    ),
                    variables=self.variables,
                )

    @staticmethod
    def transform(
        query: Query,
        variable_f: Callable[[VariableDefinition], VariableDefinition] = identity,
        selection_f: Callable[[Selection], Selection] = identity,
    ) -> Query:
        return reduce(
            Query.add,
            query.selection | map(selection_f) | traverse,
            Query(
                name=query.name,
                variables=list(query.variables | map(variable_f) | traverse),
            ),
        )

    def contains_selection(self: Query, selection: Selection) -> bool:
        """Returns True i.f.f. the selection tree :attr:`selection` is present in
        :attr:`query`.

        Args:
          query (Query): A query object
          selection (Selection): The selection to be found (or not) in :attr:`query`

        Returns:
          bool: True if the :attr:`selection` is present in :attr:`query`, False
            otherwise.
        """

        return any(
            self.selection | map(lambda select: Selection.contains(select, selection))
        )

    def contains_argument(self, argname: str) -> bool:
        return self.exists_args(lambda arg: arg.name == argname)

    def get_argument(self, argname: str) -> Argument | None:
        return self.find_args(lambda arg: arg.name == argname)

    # TODO: Replace substitute_arg calls by map_args call
    @staticmethod
    def substitute_arg(
        query: Query, arg_name: str, replacement: Argument | list[Argument]
    ) -> Query:
        return Query(
            name=query.name,
            selection=list(
                query.selection
                | map(
                    partial(
                        Selection.substitute_arg,
                        arg_name=arg_name,
                        replacement=replacement,
                    )
                )
            ),
            variables=query.variables,
        )

    # TODO: Cleanup select
    @staticmethod
    def contains(query: Query, other: Query) -> bool:
        """Returns True i.f.f. all selections in `other` are contained in `query`.
        In other words, returns true i.f.f. `other` is a subset of `query`.

        Note: `other` does not need to include "full" selections
        (i.e.: selections all the way to leaves of the GraphQL schema).

        Args:
          query (Query): The query that is to be checked
          other (Query): The query that has to be in `query`

        Returns:
          bool: True i.f.f. all selections in `other` are contained in `query`,
           otherwise False
        """

        return all(other.selection | map(partial(Query.contains_selection, query)))

    # TODO: Cleanup select
    @staticmethod
    def select(query: Query, other: Query) -> Query:
        """Returns a new Query

        Args:
            query (Query): [description]
            other (Query): [description]

        Returns:
            Query: [description]
        """

        return Query(
            name=query.name,
            selection=list(
                other.selection
                | map(
                    lambda s: next(
                        query.selection
                        | where(lambda s_: s_.fmeta.name == s.fmeta.name)
                        | map(lambda s_: Selection.select(s_, s))
                        | take(1)
                    )
                )
            ),
            variables=query.variables,
        )

    def prune_undefined(self, variables: Iterator[str]) -> Query:
        return self.filter_map(
            partial(Selection.prune_undefined, variables=variables)
        ).filter_vardefs(lambda vardef: vardef.name in variables)


@dataclass(frozen=True)
class Fragment:
    name: str
    type_: TypeRef.T
    selection: list[Selection] = field(default_factory=list)

    # Variables as fragment arguments, not the values of those variables
    variables: list[VariableDefinition] = field(default_factory=list)

    @property
    def graphql(self):
        selection_str = "\n".join(select.graphql(level=1) for select in self.selection)
        root_type_name = TypeRef.root_type_name(self.type_)
        return f"fragment {self.name} on {root_type_name} {{\n{selection_str}\n}}"

    # TODO: Cleanup combine
    @staticmethod
    def combine(frag: Fragment, other: Fragment) -> Fragment:
        raise NotImplementedError("Fragment.combine")

    # TODO: Cleanup transform
    @staticmethod
    def transform(frag: Fragment, f: Callable[[Selection], Selection]) -> Fragment:
        return Fragment(
            name=frag.name, type_=frag.type_, selection=list(frag.selection | map(f))
        )


@dataclass(frozen=True)
class Document:
    url: str
    query: Query
    fragments: list[Fragment] = field(default_factory=list)

    # A list of variable assignments. For non-repeating queries
    # the list would be of length 1 (i.e.: only one set of query variable
    # assignments)
    variables: dict[str, Any] = field(default_factory=dict)

    @property
    def graphql(self):
        return "\n".join(
            [self.query.graphql, *list(self.fragments | map(lambda frag: frag.graphql))]
        )

    @staticmethod
    def mk_single_query(url: str, query: Query) -> Document:
        return Document(url, query)

    def filter(self, predicate: Callable[[Selection], bool]) -> Document:
        return Document(
            url=self.url,
            query=self.query.filter(predicate),
            fragments=self.fragments,  # TODO: Add filtering to fragments
            variables=self.variables,
        )

    def filter_args(self, predicate: Callable[[Argument], bool]) -> Document:
        return Document(
            url=self.url,
            query=self.query.filter_args(predicate),
            fragments=self.fragments,  # TODO: Add filtering to fragments
            variables=self.variables,
        )

    def map(self, map_f: Callable[[Selection], Selection]) -> Document:
        """Applies the function ``map_f`` to each ``Selection`` in the current
        ``Document`` and returns a new ``Document`` object containing the
        resulting ``Selections``.

        Args:
            map_f (Callable[[Selection], Selection]): Mapping function to apply
              to each ``Selection``

        Returns:
            Query: _description_
        """

        return Document(
            url=self.url,
            query=self.query.map(map_f),
            fragments=self.fragments,  # TODO: Add mapping to fragments
            variables=self.variables,
        )

    def map_args(self, map_f: Callable[[Argument], Argument]) -> Document:
        """Applies the function ``map_f`` to each ``Argument`` in the current
        ``Document`` and returns a new ``Document`` object containing the resulting
        ``Arguments``.

        Args:
            map_f (Callable[[Argument], Argument]): _description_

        Returns:
            Selection: _description_
        """
        return Document(
            url=self.url,
            query=self.query.map_args(map_f),
            fragments=self.fragments,  # TODO: Add mapping to fragments
            variables=self.variables,
        )

    def filter_map(self, map_f: Callable[[Selection], Selection | None]) -> Document:
        return Document(
            url=self.url,
            query=self.query.filter_map(map_f),
            fragments=self.fragments,  # TODO: Add mapping to fragments
            variables=self.variables,
        )

    # TODO: Cleanup combine
    @staticmethod
    def combine(doc: Document, other: Document) -> Document:
        return Document(
            url=doc.url,
            query=doc.query.combine(other.query),
            fragments=union(
                doc.fragments,
                other.fragments,
                key=lambda frag: frag.name,
                combine=Fragment.combine,
            ),
        )

    # TODO: Cleanup transform
    @staticmethod
    def transform(
        doc: Document,
        query_f: Callable[[Query], Query] = identity,
        fragment_f: Callable[[Fragment], Fragment] = identity,
    ) -> Document:
        return Document(
            url=doc.url,
            query=query_f(doc.query),
            fragments=list(doc.fragments | map(fragment_f)),
            variables=doc.variables,
        )

    def prune_undefined(self, variables: Iterator[str]) -> Document:
        """Returns a new ``Document`` object that contains the subset of the current
        ``Document``'s query containing only the ``Selections`` for which all its
        arguments are defined (i.e.: either constants or variables in ``variables``).

        Args:
            variables (dict[str, Any]): _description_

        Returns:
            Document: _description_
        """
        return Document(
            url=self.url,
            query=self.query.prune_undefined(variables),
            fragments=self.fragments,
            variables=variables,
        )


@dataclass(frozen=True)
class DocumentResponse:
    # TODO: update to recursive json dict w/ pydantic 2.0
    url: str
    data: dict[str, Any] = field(default_factory=dict)

    def combine(self: Self, other: Self):
        return replace(self, data=merge(self.data, other.data))


@dataclass(frozen=True)
class DataRequest:
    documents: list[Document] = field(default_factory=list)

    @property
    def graphql(self):
        return "\n".join(list(self.documents | map(lambda doc: doc.graphql)))

    @classmethod
    def combine(cls, req: DataRequest, other: DataRequest):
        return cls(
            documents=union(
                req.documents,
                other.documents,
                key=lambda doc: doc.url,
                combine=Document.combine,
            )
        )

    @classmethod
    def transform(cls, req: DataRequest, f: Callable[[Document], Document]):
        return cls(documents=list(req.documents | map(f)))

    @classmethod
    def single_query(cls, url: str, query: Query):
        return cls([Document(url, query)])

    @classmethod
    def single_document(cls, doc: Document):
        return cls([doc])

    def add_documents(self, docs: Document | list[Document]):
        return replace(self, documents=list((self.documents, docs) | traverse))


@dataclass(frozen=True)
class DataResponse:
    responses: list[DocumentResponse] = field(default_factory=list)

    def add_responses(self, resps: DocumentResponse | list[DocumentResponse]):
        return replace(self, responses=list((self.responses, resps) | traverse))


# ================================================================
# Utility functions
# ================================================================
def selections_of_object(
    schema: SchemaMeta, object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta
):
    """Returns generator of Selection objects that selects all non-list fields of
    GraphQL Object of Interface :attr:`object_`.

    Args:
      schema (SchemaMeta): _description_
      object_ (TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta): _description_

    Yields:
      _type_: _description_
    """
    for fmeta in object_.fields:
        if not fmeta.type_.is_list and len(fmeta.arguments) == 0:
            match schema.type_of_typeref(fmeta.type_):
                case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as inner_object:
                    yield Selection(
                        fmeta, selection=[Selection(inner_object.field("id"))]
                    )
                case _:
                    yield Selection(fmeta)


def input_value_of_argument(
    schema: SchemaMeta, argmeta: TypeMeta.ArgumentMeta, value: Any
) -> InputValue:
    def fmt_value(type_ref: TypeRef.T, value: Any, non_null=False):
        match (type_ref, schema.type_map[TypeRef.root_type_name(type_ref)], value):
            # Only allow Null values when non_null=True
            case (_, _, None):
                if not non_null:
                    return InputValue.Null()
                else:
                    raise TypeError(f"Argument {argmeta.name} cannot be None!")

            # If type is non_null, recurse with non_null=True
            case (TypeRef.NonNull(inner=t), _, _):
                return fmt_value(t, value, non_null=True)

            case (TypeRef.Named(name="ID"), _, str()):
                return InputValue.String(value)

            case (TypeRef.Named(name="Int"), _, int()):
                return InputValue.Int(value)
            case (TypeRef.Named(name="BigInt"), _, int()):
                return InputValue.String(str(value))

            case (TypeRef.Named(name="Float"), _, int() | float()):
                return InputValue.Float(float(value))
            case (TypeRef.Named(name="BigDecimal"), _, int() | float()):
                return InputValue.String(str(float(value)))

            case (TypeRef.Named(name="String" | "Bytes"), _, str()):
                return InputValue.String(value)
            case (TypeRef.Named(), TypeMeta.EnumMeta(), str()):
                return InputValue.Enum(value)

            case (TypeRef.Named(name="Boolean"), _, bool()):
                return InputValue.Boolean(value)

            case (TypeRef.List(inner=t), _, list()):
                return InputValue.List([fmt_value(t, val, non_null) for val in value])

            case (TypeRef.Named(), TypeMeta.InputObjectMeta() as input_object, dict()):
                return InputValue.Object(
                    {
                        key: fmt_value(
                            input_object.type_of_input_field(key), val, non_null
                        )
                        for key, val in value.items()
                    }
                )

            case (value, typ, non_null):
                raise TypeError(f"mk_input_value({value}, {typ}, {non_null})")

    return fmt_value(argmeta.type_, value)


def arguments_of_field_args(
    schema: SchemaMeta, field: TypeMeta.FieldMeta, args: dict[str, Any] | None
) -> list[Argument]:
    if args is None:
        args = {}

    def f(arg_meta: TypeMeta.ArgumentMeta) -> Argument | None:
        if arg_meta.name in args:
            return Argument(
                arg_meta.name,
                input_value_of_argument(schema, arg_meta, args[arg_meta.name]),
            )
        else:
            if (arg_meta.default_value) or (not TypeRef.is_non_null(arg_meta.type_)):
                return None
            else:
                raise TypeError(
                    f"arguments_of_field_args: Argument {arg_meta.name} of field"
                    f" {field.name} is required but not provided!"
                )

    # TODO: Add warnings if arguments are not used

    match field:
        case TypeMeta.FieldMeta() as field:
            args = [f(arg_meta) for arg_meta in field.arguments]
            return list(filter(lambda arg: arg is not None, args))
        case _:
            raise TypeError(
                f"arguments_of_field_args: TypeMeta {field.name}"
                " is not of type FieldMeta"
            )
