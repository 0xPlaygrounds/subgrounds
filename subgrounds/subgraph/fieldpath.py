from __future__ import annotations

import logging
import operator
import warnings
from dataclasses import dataclass
from datetime import datetime
from functools import partial, reduce
from hashlib import blake2b
from typing import TYPE_CHECKING, Any, Callable, ClassVar

from pipe import map, traverse
from typing_extensions import Self  # 3.10 support

from subgrounds.query import Query, Selection, arguments_of_field_args
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.utils import extract_data

from .filter import Filter

if TYPE_CHECKING:
    from .subgraph import Subgraph

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")

FPATH_DEPTH_LIMIT: int = 4


def typeref_of_binary_op(
    op: str, t1: TypeRef.T, t2: int | float | str | bool | FieldPath | SyntheticField
):
    def f_typeref(t1, t2):
        match (op, TypeRef.root_type_name(t1), TypeRef.root_type_name(t2)):
            case ("add", "String" | "Bytes", "String" | "Bytes"):
                return TypeRef.Named(name="String", kind="SCALAR")

            case (
                "add" | "sub" | "mul" | "div" | "pow" | "mod",
                "BigInt" | "Int",
                "BigInt" | "Int",
            ):
                return TypeRef.Named(name="Int", kind="SCALAR")
            case (
                "add" | "sub" | "mul" | "div" | "pow",
                "BigInt" | "Int",
                "BigDecimal" | "Float",
            ):
                return TypeRef.Named(name="Float", kind="SCALAR")
            case (
                "add" | "sub" | "mul" | "div" | "pow",
                "BigDecimal" | "Float",
                "BigInt" | "Int" | "BigDecimal" | "Float",
            ):
                return TypeRef.Named(name="Float", kind="SCALAR")

            case _ as args:
                raise Exception(
                    f"typeref_of_binary_op: f_typeref: unhandled arguments {args}"
                )

    def f_const(t1, const):
        match (op, TypeRef.root_type_name(t1), const):
            case ("add", "String" | "Bytes", str()):
                return TypeRef.Named(name="String", kind="SCALAR")

            case (
                "add" | "sub" | "mul" | "div" | "pow" | "mod",
                "BigInt" | "Int",
                int(),
            ):
                return TypeRef.Named(name="Int", kind="SCALAR")
            case ("add" | "sub" | "mul" | "div" | "pow", "BigInt" | "Int", float()):
                return TypeRef.Named(name="Float", kind="SCALAR")
            case (
                "add" | "sub" | "mul" | "div" | "pow",
                "BigDecimal" | "Float",
                int() | float(),
            ):
                return TypeRef.Named(name="Float", kind="SCALAR")

            case _ as args:
                raise Exception(
                    f"typeref_of_binary_op: f_typeref: unhandled arguments {args}"
                )

    match t2:
        case int() | float() | str() | bool() as constant:
            return f_const(t1, constant)
        case FieldPath() | SyntheticField() as field:
            return f_typeref(t1, field._type)


def type_ref_of_unary_op(op: str, t: TypeRef.T):
    match (op, TypeRef.root_type_name(t)):
        case ("abs", "BigInt" | "Int"):
            return TypeRef.Named(name="Int", kind="SCALAR")
        case ("abs", "BigDecimal" | "Float"):
            return TypeRef.Named(name="Float", kind="SCALAR")

        case ("neg", "BigInt" | "Int"):
            return TypeRef.Named(name="Int", kind="SCALAR")
        case ("neg", "BigDecimal" | "Float"):
            return TypeRef.Named(name="Float", kind="SCALAR")

        case _ as args:
            raise Exception(
                f"typeref_of_binary_op: f_typeref: unhandled arguments {args}"
            )


class FieldOperatorMixin:
    _subgraph: Subgraph
    _type: TypeRef.T

    def __add__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            operator.add, typeref_of_binary_op("add", self._type, other), [self, other]
        )

    def __radd__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            lambda x, y: operator.add(y, x),
            typeref_of_binary_op("add", self._type, other),
            [self, other],
        )

    def __sub__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            operator.sub, typeref_of_binary_op("sub", self._type, other), [self, other]
        )

    def __rsub__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            lambda x, y: operator.sub(y, x),
            typeref_of_binary_op("sub", self._type, other),
            [self, other],
        )

    def __mul__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            operator.mul, typeref_of_binary_op("mul", self._type, other), [self, other]
        )

    def __rmul__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            lambda x, y: operator.mul(y, x),
            typeref_of_binary_op("mul", self._type, other),
            [self, other],
        )

    def __truediv__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            operator.truediv,
            typeref_of_binary_op("div", self._type, other),
            [self, other],
        )

    def __rtruediv__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            lambda x, y: operator.truediv(y, x),
            typeref_of_binary_op("div", self._type, other),
            [self, other],
        )

    def __floordiv__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            operator.floordiv,
            typeref_of_binary_op("div", self._type, other),
            [self, other],
        )

    def __rfloordiv__(self, other: Any) -> SyntheticField:
        return SyntheticField(
            lambda x, y: operator.floordiv(y, x),
            typeref_of_binary_op("div", self._type, other),
            [self, other],
        )

    def __pow__(self, rhs: Any) -> SyntheticField:
        return SyntheticField(
            operator.pow, typeref_of_binary_op("pow", self._type, rhs), [self, rhs]
        )

    def __rpow__(self, lhs: Any) -> SyntheticField:
        return SyntheticField(
            lambda x, y: operator.pow(y, x),
            typeref_of_binary_op("pow", self._type, lhs),
            [self, lhs],
        )

    def __mod__(self, rhs: Any) -> SyntheticField:
        return SyntheticField(
            operator.mod, typeref_of_binary_op("mod", self._type, rhs), [self, rhs]
        )

    def __rmod__(self, lhs: Any) -> SyntheticField:
        return SyntheticField(
            lambda x, y: operator.mod(y, x),
            typeref_of_binary_op("mod", self._type, lhs),
            [self, lhs],
        )

    def __neg__(self) -> SyntheticField:
        return SyntheticField(
            operator.neg, type_ref_of_unary_op("neg", self._type), self
        )

    def __abs__(self) -> SyntheticField:
        return SyntheticField(
            operator.abs, type_ref_of_unary_op("abs", self._type), self
        )


def fieldpaths_of_object(
    subgraph: Subgraph, object_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta
):
    """Returns generator of FieldPath objects that selects all non-list fields of
    GraphQL Object of Interface :attr:`object_`.

    Args:
      schema: _description_
      object_: _description_

    Yields:
      _description_
    """
    for fmeta in object_.fields:
        if not fmeta.type_.is_list and len(fmeta.arguments) == 0:
            match subgraph._schema.type_of_typeref(fmeta.type_):
                case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta():
                    yield subgraph.__getattribute__(object_.name).__getattribute__(
                        fmeta.name
                    ).id

                case _:
                    yield subgraph.__getattribute__(object_.name).__getattribute__(
                        fmeta.name
                    )


@dataclass
class FieldPath(FieldOperatorMixin):
    _subgraph: Subgraph
    _root_type: TypeRef.T
    _type: TypeRef.T
    _path: list[tuple[dict[str, Any] | None, TypeMeta.FieldMeta]]

    # Purely for testing
    __test_mode: ClassVar[bool] = False

    def __init__(
        self,
        subgraph: Subgraph,
        root_type: TypeRef.T,
        type_: TypeRef.T,
        path: list[tuple[dict[str, Any] | None, TypeMeta.FieldMeta]],
    ) -> None:
        self._subgraph = subgraph
        self._root_type = root_type
        self._type = type_
        self._path = path

        # Add fields as attributes if leaf is object
        match self._subgraph._schema.type_of(self._leaf):
            case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as type_ if len(
                self._path
            ) < FPATH_DEPTH_LIMIT:
                # We generate fieldpaths up to depth 8
                for fmeta in type_.fields:
                    path = self._path.copy()
                    path.append((None, fmeta))

                    super().__setattr__(
                        fmeta.name,
                        FieldPath(
                            subgraph=self._subgraph,
                            root_type=self._root_type,
                            type_=fmeta.type_,
                            path=path,
                        ),
                    )

            case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as type_ if len(
                self._path
            ) == FPATH_DEPTH_LIMIT:
                for fmeta in type_.fields:
                    # NOTE: We set the attribute to None on purpose since we want code
                    # completion to work while avoiding infinite loops caused by cycles
                    # in the GraphQL schema. The attribute itself will be initialized
                    # on __getattribute_
                    super().__setattr__(fmeta.name, None)

            case _:
                pass

    @property
    def _schema(self) -> SchemaMeta:
        return self._subgraph._schema

    @property
    def _root(self) -> TypeMeta.FieldMeta:
        """Returns the type information of the root field of the current
        :class:`FieldPath`

        Returns:
          Type information of the root field of the current :class:`FieldPath`
        """
        return self._path[0][1]

    @property
    def _leaf(self) -> TypeMeta.FieldMeta:
        """Returns the type information of the leaf field of the current
        :class:`FieldPath`

        Returns:
          Type information of the leaf field of the current :class:`FieldPath`
        """
        return self._path[-1][1]

    @staticmethod
    def _hash(msg: str) -> str:
        h = blake2b(digest_size=8)
        h.update(msg.encode("UTF-8"))
        return "x" + h.hexdigest()

    @staticmethod
    def _merge(fpaths: list[FieldPath]) -> list[Selection]:
        """Returns a Selection tree containing all selection paths in `fpaths`.
        Note: Assumes that all fieldpaths in `fpaths` belong to the same subgraph

        Args:
          fpaths: _description_

        Returns:
          _description_
        """

        query = reduce(Query.add, fpaths | map(FieldPath._selection), Query())
        return query.selection

    def _name_path(self, use_aliases: bool = False) -> list[str]:
        """Returns a list of strings correspoding to the names of all fields
        selected in the current :class:`FieldPath`. If :attr:`use_aliases` is True,
        then if a field has an automatically generated alias, the alias will be
        returned.

        Args:
          use_aliases: Flag indicating wether of not to use the fields' automatically
            generated alias (if present). Defaults to False.

        Returns:
          List of field names selected in the current :class:`FieldPath`
        """

        def gen_alias(ele: tuple[dict[str, Any] | None, TypeMeta.FieldMeta]) -> str:
            if ele[0] != {} and ele[0] is not None:
                return FieldPath._hash(ele[1].name + str(ele[0]))
            else:
                return ele[1].name

        return list(
            self._path | map(lambda ele: gen_alias(ele) if use_aliases else ele[1].name)
        )

    def _name(self, use_aliases: bool = False) -> str:
        """Generates the name of the current :class:`FieldPath` using the names of
        the fields it selects. If :attr:`use_aliases` is True, then if a field has
        an automatically generated alias, the alias will be used.

        Args:
          use_aliases: Flag indicating wether of not to use the fields' automatically
            generated alias (if present). Defaults to False.

        Returns:
          The generated name of the current :class:`FieldPath`.
        """

        return "_".join(self._name_path(use_aliases=use_aliases))

    def _auto_select(self) -> FieldPath | list[FieldPath]:
        match self._subgraph._schema.type_of_typeref(self._leaf.type_):
            case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as obj:
                return list(
                    fieldpaths_of_object(self._subgraph, obj)
                    | map(partial(FieldPath._extend, self))
                )

            case _:
                return self

    def _extract_data(
        self, data: dict[str, Any] | list[dict[str, Any]]
    ) -> list[Any] | Any:
        """Extract the data corresponding to the current :class:`FieldPath` from
        the dictionary :attr:`data`.

        Args:
          data: Data dictionary that contains the data corresponding to the current
            :class:`FieldPath`.

        Returns:
          Data corresponding to the current :class:`FieldPath`.
        """
        return extract_data(self._name_path(use_aliases=True), data)

    def _selection(self) -> Selection | list[Selection]:
        """Returns a selection or list of selections corresponding to the current
        :class:`FieldPath`.

        Returns:
          Selection | list[Selection]: _description_
        """

        def f(
            path: list[tuple[dict[str, Any] | None, TypeMeta.FieldMeta]]
        ) -> list[Selection]:
            match path:
                case [
                    (args, TypeMeta.FieldMeta() as fmeta),
                    *rest,
                ] if args == {} or args is None:
                    return [Selection(fmeta, selection=f(rest))]

                case [(args, TypeMeta.FieldMeta() as fmeta), *rest]:
                    return [
                        Selection(
                            fmeta,
                            # TODO: Revisit this
                            alias=FieldPath._hash(fmeta.name + str(args)),
                            arguments=arguments_of_field_args(
                                self._subgraph._schema, fmeta, args
                            ),
                            selection=f(rest),
                        )
                    ]

                case []:
                    return []

            assert False  # Suppress mypy missing return statement warning

        return f(self._path)[0]

    def _set_arguments(
        self, args: dict[str, Any], selection: list[FieldPath] = []
    ) -> FieldPath | list[FieldPath]:
        """Set the arguments to the leaf of the current :class:`FieldPath`. The
        method returns the :attr:`self`.

        Args:
          args: _description_
          selection: _description_. Defaults to [].

        Returns:
          _description_
        """

        def fmt_arg(name, raw_arg):
            match (name, raw_arg):
                case ("where", [Filter(), *_] as filters):
                    return Filter.to_dict(filters)
                case ("orderBy", FieldPath() as fpath):
                    if paths := fpath._name_path():
                        return "__".join(paths)
                    raise Exception(
                        f"Cannot use empty paths as orderBy argument {fpath}"
                    )
                case _:
                    return raw_arg

        match self._leaf:
            case TypeMeta.FieldMeta():
                args = {key: fmt_arg(key, val) for key, val in args.items()}
                self._path[-1] = (args, self._path[-1][1])
                if len(selection) > 0:
                    return list(selection | map(partial(FieldPath._extend, self)))
                else:
                    return self
            case _:
                raise TypeError(f"Unexpected type for FieldPath {self}")

    def _select(self, name: str) -> FieldPath:
        """Returns a new FieldPath corresponding to the FieldPath `self` extended with
        an additional selection on the field named `name`.

        Args:
          name: The name of the field to expand on the leaf of `fpath`

        Raises:
          TypeError: [description]
          TypeError: [description]
          TypeError: [description]

        Returns:
          A new FieldPath containing `fpath` extended with the field named `name`
        """
        match self._schema.type_of_typeref(self._type):
            # If the FieldPath fpath
            case TypeMeta.EnumMeta() | TypeMeta.ScalarMeta():
                raise TypeError(
                    f"FieldPath: path {self} ends with a scalar field!"
                    f" cannot select field {name}"
                )

            case TypeMeta.ObjectMeta() | TypeMeta.InterfaceMeta() as obj:
                field = obj.field(name)

                match self._schema.type_of_typeref(field.type_):
                    case (
                        TypeMeta.ObjectMeta()
                        | TypeMeta.InterfaceMeta()
                        | TypeMeta.EnumMeta()
                        | TypeMeta.ScalarMeta()
                    ):
                        # Copy current path and append newly selected field
                        path = self._path.copy()
                        path.append((None, field))

                        # Return new FieldPath
                        return FieldPath(
                            subgraph=self._subgraph,
                            root_type=self._root_type,
                            type_=field.type_,
                            path=path,
                        )
                    case _:
                        raise TypeError(
                            f"FieldPath: field {name} is not a valid field for object"
                            f" {self._type.name} at path {self}"
                        )

            case _:
                raise TypeError(
                    f"FieldPath: Unexpected type {self._type.name}"
                    f" when selection {name} on {self}"
                )

    def _extend(self, ext: FieldPath) -> FieldPath:
        """Extends the current :class:`FieldPath` with the :class:`FieldPath`
        :attr:`ext`. :attr:`ext` must start where the current :class:`FieldPath` ends.

        Args:
          ext: The :class:`FieldPath` representing the extension

        Raises:
          TypeError: [description]
          TypeError: [description]
          TypeError: [description]

        Returns:
          A new :class:`FieldPath` containing the initial current :class:`FieldPath`
            extended with :attr:`ext`
        """
        match self._leaf:
            case TypeMeta.FieldMeta() as fmeta:
                match self._schema.type_of_typeref(fmeta.type_):
                    case TypeMeta.ObjectMeta(name=name) | TypeMeta.InterfaceMeta(
                        name=name
                    ):
                        if name == ext._root_type.name:
                            return FieldPath(
                                subgraph=self._subgraph,
                                root_type=self._root_type,
                                type_=ext._type,
                                path=self._path + ext._path,
                            )
                        else:
                            raise TypeError(
                                f"extend: FieldPath {ext} does not start at the"
                                f" same type from where FieldPath {self} ends"
                            )
                    case _:
                        raise TypeError(f"extend: FieldPath {self} is not object field")
            case _:
                raise TypeError(f"extend: FieldPath {self} is not an object field")

    # ================================================================
    # Overloaded magic functions
    # ================================================================
    # When setting arguments
    def __call__(self, **kwargs: Any) -> Any:
        """Sets field arguments and expand subfields. The updated FieldPath is returned.

        Example:
        >>> aaveV2 = sg.load_subgraph(
        ...     "https://api.thegraph.com/subgraphs/name/aave/protocol-v2"
        ... )
        >>> query = aaveV2.Query.borrows(
        ...   first=10,
        ...   order_by=aaveV2.Borrow.timestamp,
        ...   order_direction="desc",
        ...   selection=[
        ...     aaveV2.Borrow.id,
        ...     aaveV2.Borrow.timestamp,
        ...     aaveV2.Borrow.amount
        ...   ]
        ... )

        Returns:
          The updated field path if :attr:`selection` is not specified, or a list of
            fieldpaths when :attr:`selection` is specified.
        """
        selection = kwargs.pop("selection", [])
        return self._set_arguments(kwargs, selection)

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

    # Filtering
    def __eq__(self, value: FieldPath | Any) -> Filter | bool:
        if FieldPath.__test_mode:
            # Purely used for testing so that assertEqual works
            return (
                self._subgraph == value._subgraph
                and self._type == value._type
                and self._path == value._path
            )
        else:
            return Filter.mk_filter(self, Filter.Operator.EQ, value)

    def __ne__(self, value: Any) -> Filter:
        return Filter.mk_filter(self, Filter.Operator.NEQ, value)

    def __lt__(self, value: Any) -> Filter:
        return Filter.mk_filter(self, Filter.Operator.LT, value)

    def __gt__(self, value: Any) -> Filter:
        return Filter.mk_filter(self, Filter.Operator.GT, value)

    def __le__(self, value: Any) -> Filter:
        return Filter.mk_filter(self, Filter.Operator.LTE, value)

    def __ge__(self, value: Any) -> Filter:
        return Filter.mk_filter(self, Filter.Operator.GTE, value)

    # Utility
    def __str__(self) -> str:
        return ".".join(self._path | map(lambda ele: ele[1].name))

    def __repr__(self) -> str:
        vars = f"{self._subgraph._url}, {self._root_type.name}, {self._name_path()}"
        return f"FieldPath({vars})"


@dataclass
class SyntheticField(FieldOperatorMixin):
    STRING: ClassVar[TypeRef.Named] = TypeRef.Named(name="String", kind="SCALAR")
    INT: ClassVar[TypeRef.Named] = TypeRef.Named(name="Int", kind="SCALAR")
    FLOAT: ClassVar[TypeRef.Named] = TypeRef.Named(name="Float", kind="SCALAR")
    BOOL: ClassVar[TypeRef.Named] = TypeRef.Named(name="Boolean", kind="SCALAR")

    _counter: ClassVar[int] = 0

    _f: Callable
    _type: TypeRef.T
    _default: Any
    _deps: list[FieldPath]

    def __init__(
        self,
        f: Callable,
        type_: TypeRef.T,
        deps: list[FieldPath | SyntheticField] | FieldPath | SyntheticField,
        default: Any = None,
    ) -> None:
        deps = list([deps] | traverse)

        def mk_deps(
            deps: list[FieldPath | SyntheticField],
            f: Callable,
            acc: list[tuple[Callable | None, int]] = [],
        ) -> tuple[Callable, list[FieldPath]]:
            """If all dependencies are field paths, then this function does nothing.
             If the dependencies contain one or more other synthetic fields, as is the
             case when chaining binary operators, then the synthetic field tree is
             flattened to a single synthetic field containing all leaf dependencies.

            Args:
              deps: Initial dependencies for synthetic field
              f: Function to apply to the values of those dependencies
              acc: Accumulator. Defaults to [].

            Returns:
              A tuple containing the potentially modified function and dependency list.
            """

            match deps:
                case []:

                    def new_f(*args):
                        new_args = []
                        _counter = 0
                        for f_, deps in acc:
                            match (f_, deps):
                                case (None, FieldPath()):
                                    new_args.append(args[_counter])
                                    _counter += 1
                                case (
                                    None,
                                    int() | float() | str() | bool() as constant,
                                ):
                                    new_args.append(constant)
                                case (f_, list() as deps):
                                    new_args.append(
                                        f_(*args[_counter : _counter + len(deps)])
                                    )
                                    _counter += len(deps)

                        return f(*new_args)

                    new_deps = []
                    for _, deps in acc:
                        match deps:
                            case FieldPath() as dep:
                                new_deps.append(dep)
                            case int() | float() | str() | bool():
                                pass
                            case list() as deps:
                                new_deps = new_deps + deps

                    return (new_f, new_deps)

                case [SyntheticField(_f=inner_f, _deps=inner_deps), *rest]:
                    acc.append((inner_f, inner_deps))
                    return mk_deps(rest, f, acc)

                case [FieldPath() as dep, *rest]:
                    acc.append((None, dep))
                    return mk_deps(rest, f, acc)

                case [int() | float() | str() | bool() as constant, *rest]:
                    acc.append((None, constant))
                    return mk_deps(rest, f, acc)

                case _ as deps:
                    raise TypeError(f"mk_deps: unexpected argument {deps}")

        (f, deps) = mk_deps(deps, f)
        self._f = f
        self._type = type_
        self._default = (
            default if default is not None else SyntheticField.default_of_type(type_)
        )
        self._deps = deps

        SyntheticField._counter += 1

    @staticmethod
    def default_of_type(type_: TypeRef.T):
        match type_.name:
            case "String":
                return ""
            case "Int":
                return 0
            case "Float":
                return 0.0
            case "Boolean":
                return False
            case _:
                return 0

    @classmethod
    def constant(cls, value: str | int | float | bool) -> SyntheticField:
        """Returns a constant ``SyntheticField`` with value ``value``.
        Useful for injecting additional static data to a schema or merging entities.

        Args:
          value: The constant field's value

        Returns:
          The constant ``SyntheticField``

        Example:

        .. code-block:: python

            >>> from subgrounds import Subgrounds, SyntheticField
            >>> sg = Subgrounds()
            >>> univ3 = sg.load_subgraph(
            ...     "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"
            ... )

            # Create constant SyntheticFields
            >>> univ3.Mint.tx_type = SyntheticField.constant('MINT')
            >>> univ3.Burn.tx_type = SyntheticField.constant('BURN')

            # Last 10 mints and burns
            >>> mints = univ3.Query.mints(
            ...     first=10,
            ...     orderBy=univ3.Mint.timestamp,
            ...     orderDirection='desc'
            ... )
            >>> burns = univ3.Query.burns(
            ...     first=10,
            ...     orderBy=univ3.Burn.timestamp,
            ...     orderDirection='desc'
            ... )

            # Query mints and burns. Notice that we merge the two entity tables by
            # setting `concat=True` and overwriting the column names (columns must
            # match the `FieldPaths`)
            >>> df = sg.query_df(
            ...     [
            ...         mints.transaction.id,
            ...         mints.timestamp,
            ...         mints.tx_type,
            ...         mints.origin,
            ...         mints.amountUSD,
            ...         burns.transaction.id,
            ...         burns.timestamp,
            ...         burns.tx_type,
            ...         burns.origin,
            ...         burns.amountUSD,
            ...     ],
            ...     columns=['tx_hash', 'timestamp', 'tx_type', 'origin', 'amount_USD'],
            ...     concat=True
            ..  )

            # Sort the DataFrame (output is truncated)
            >>> df.sort_values(by=['timestamp'], ascending=False)
             tx_hash   timestamp    tx_type  origin     amount_USD
            0xcbe1...  1656016553    MINT  0x3435....  7.879784e+03
            0xddda...  1656016284    MINT  0xc747....  5.110840e+04
            0xa767...  1656016284    BURN  0xd40d....  2.804573e+05
            0xc132...  1656015853    MINT  0xc747....  5.122569e+04
            0x1444...  1656015773    MINT  0xd11a....  8.897983e+03
            0x3315...  1656015693    MINT  0xb7dd....  0.000000e+00
            0xcc71...  1656015278    BURN  0xa7c4....  1.254942e+06
            0x7bbf...  1656015111    MINT  0xac56....  3.432075e+04
            0xea21...  1656014785    BURN  0x0709....  2.059106e+04
            0x3bd3...  1656014120    MINT  0x5099....  2.517578e+03
            0x1ea5...  1656014018    BURN  0x5099....  0.000000e+00
            0xb9d3...  1656013998    MINT  0x22df....  8.365750e+04
            0xc5e3...  1656013946    BURN  0xac56....  3.363809e+04
            0x7c73...  1656013913    MINT  0x4ce6....  4.837287e+04
            0x95cf...  1656013728    BURN  0x4ce6....  5.110010e+04
            0x76dd...  1656013599    MINT  0x234a....  5.363896e+03
            0x47e5...  1656013580    BURN  0xaf0f....  0.000000e+00
            0xe20e...  1656013455    BURN  0xaf0f....  0.000000e+00
            0xac3e...  1656013237    BURN  0x665d....  2.254100e+05
            0x01c3...  1656013034    BURN  0x0084....  0.000000e+00
        """

        match value:
            case str():
                return cls(lambda: value, cls.STRING, [])
            case int():
                return cls(lambda: value, cls.INT, [])
            case float():
                return cls(lambda: value, cls.FLOAT, [])
            case bool():
                return cls(lambda: value, cls.BOOL, [])

    @classmethod
    def datetime_of_timestamp(cls, timestamp: FieldPath | SyntheticField) -> Self:
        """Returns a ``SyntheticField`` that will transform the ``FieldPath``
        ``timestamp`` into a human-readable ISO8601 string.

        Args:
          timestamp: A ``FieldPath`` representing a Unix timestamp field.

        Returns:
          An ISO8601 datetime string ``SyntheticField``.

        Example:

        .. code-block:: python

            >>> from subgrounds import Subgrounds, SyntheticField
            >>> sg = Subgrounds()
            >>> univ3 = sg.load_subgraph(
            ...     "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"
            ... )

            # Create datetime SyntheticField
            >>> univ3.Swap.datetime = SyntheticField.datetime_of_timestamp(
            ...     univ3.Swap.timestamp
            ... )

            # Query 100 swaps
            >>> sg.query_df([
            ...     univ3.Query.swaps.timestamp,
            ...     univ3.Query.swaps.datetime,
            ... ])
                swaps_timestamp       swaps_datetime
            0        1625105710  2021-06-30 22:15:10
            1        1629253724  2021-08-17 22:28:44
            2        1647333277  2022-03-15 04:34:37
            3        1630801974  2021-09-04 20:32:54
            4        1653240241  2022-05-22 13:24:01
            ..              ...                  ...
            95       1646128326  2022-03-01 04:52:06
            96       1646128326  2022-03-01 04:52:06
            97       1626416555  2021-07-16 02:22:35
            98       1626416555  2021-07-16 02:22:35
            99       1625837291  2021-07-09 09:28:11
        """

        return SyntheticField(
            lambda timestamp: str(datetime.fromtimestamp(timestamp)),
            SyntheticField.STRING,
            timestamp,
        )

    @classmethod
    def map(
        cls,
        dict: dict[Any, Any],
        type_: TypeRef.T,
        fpath: FieldPath | SyntheticField,
        default: Any | None = None,
    ) -> Self:
        """Returns a SyntheticField that will map the values of ``fpath`` using the
        key value pairs in ``dict``. If a value is not in the dictionary, then
        ``default`` will be used instead.

        Args:
          dict: The dictionary containing the key value pairs used to map
            ``fpath``'s values
          type_: The type of the resulting field
          fpath: The FieldPath whose values will be mapped using the dictionary
          default: Default value used when a value is not in the provided dictionary

        Returns:
            A map SyntheticField

        Example:

        .. code-block:: python

            >>> from subgrounds import Subgrounds, SyntheticField
            >>> sg = Subgrounds()
            >>> univ3 = sg.load_subgraph(
            ...     'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3'
            ... )

            # Hand-crafted mapping of pool addresses to symbol
            >>> pooladdr_symbol_map = {
            ...     '0x5777d92f208679db4b9778590fa3cab3ac9e2168': 'DAI/USDC-001',
            ...     '0x6c6bc977e13df9b0de53b251522280bb72383700': 'DAI/USDC-005',
            ...     '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8': 'USDC/ETH-030',
            ...     '0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640': 'USDC/ETH-005',
            ... }

            # Create map SyntheticField using our dictionary with 'UNKNOWN' as the
            # default value
            >>> univ3.Pool.symbol = SyntheticField.map(
            ...     pooladdr_symbol_map,
            ...     SyntheticField.STRING,
            ...     univ3.Pool.id,
            ...     'UNKNOWN'
            ... )

            # Query top 10 pools by TVL
            >>> pools = univ3.Query.pools(
            ...     first=10,
            ...     orderBy=univ3.Pool.totalValueLockedUSD,
            ...     orderDirection='desc'
            ... )
            >>> sg.query_df([
            ...     pools.id,
            ...     pools.symbol
            ... ])
                                                 pools_id  pools_symbol
            0  0xa850478adaace4c08fc61de44d8cf3b64f359bec       UNKNOWN
            1  0x5777d92f208679db4b9778590fa3cab3ac9e2168  DAI/USDC-001
            2  0x6c6bc977e13df9b0de53b251522280bb72383700  DAI/USDC-005
            3  0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8  USDC/ETH-030
            4  0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640  USDC/ETH-005
            5  0x3416cf6c708da44db2624d63ea0aaef7113527c6       UNKNOWN
            6  0xcbcdf9626bc03e24f779434178a73a0b4bad62ed       UNKNOWN
            7  0xc63b0708e2f7e69cb8a1df0e1389a98c35a76d52       UNKNOWN
            8  0x4585fe77225b41b697c938b018e2ac67ac5a20c0       UNKNOWN
            9  0x4e68ccd3e89f51c3074ca5072bbac773960dfa36       UNKNOWN
        """

        return SyntheticField(
            lambda value: dict[value] if value in dict else default, type_, fpath
        )
