""" Helper functions and classes used by Subgrounds' own pagination strategies.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from functools import reduce
from itertools import count
from typing import Any, cast

from pipe import map, reverse, skip, traverse

from subgrounds.query import (
    Argument,
    Document,
    InputValue,
    Selection,
    VariableDefinition,
)
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.utils import accumulate

from .utils import merge_input_value_object_metas

DEFAULT_NUM_ENTITIES = 100


@dataclass(frozen=True)
class PaginationNode:
    """Class representing the pagination config for a single GraphQL list field.

    Attributes:
      node_idx: Index of PaginationNode, used to label pagination arguments for
        this node.
      filter_field : Name of the node's filter field, e.g.: if ``filter_name`` is
        ``timestamp_gt``, then :attr:`filter_field` is ``timestamp``
      first_value: Initial value of the ``first`` argument
      skip_value: Initial value of the ``skip`` argument
      filter_value: Initial value of the filter argument
        (i.e.: ``where: {filter: FILTER_VALUE}``)
      filter_value_type: Type of the filter value
      key_path: Location in the list field to which this pagination node refers to in
        the initial query
      inner: Nested pagination nodes (if any).
    """

    node_idx: int
    filter_field: str

    first_value: int
    skip_value: int
    filter_value: Any
    filter_value_type: TypeRef.T

    key_path: list[str]
    inner: list[PaginationNode] = field(default_factory=list)

    def get_vardefs(self) -> list[VariableDefinition]:
        """Returns a list of variable definitions corresponding to this pagination
        node's pagination arguments as well as the variable definitions related
        to any nested pagination nodes.

        Returns:
          _description_
        """
        vardefs = [
            VariableDefinition(
                f"first{self.node_idx}", TypeRef.Named(name="Int", kind="SCALAR")
            ),
            VariableDefinition(
                f"skip{self.node_idx}", TypeRef.Named(name="Int", kind="SCALAR")
            ),
            VariableDefinition(
                f"lastOrderingValue{self.node_idx}", self.filter_value_type
            ),
        ]

        nested_vardefs = list(self.inner | map(PaginationNode.get_vardefs) | traverse)

        return nested_vardefs + vardefs


PAGINATION_ARGS: set[str] = {"first", "skip", "where", "orderBy", "orderDirection"}


def is_pagination_node(schema: SchemaMeta, selection: Selection) -> bool:
    return (
        selection.fmeta.type_.is_list
        and schema.type_of_typeref(selection.fmeta.type_).is_object
    )


def get_orderBy_value(selection: Selection) -> str:
    order_by_val = selection.find_args(lambda arg: arg.name == "orderBy", recurse=False)
    if order_by_val is None:
        return "id"

    return order_by_val.value.value


def get_orderDirection_value(selection: Selection) -> str:
    order_direction_arg = selection.find_args(
        lambda arg: arg.name == "orderDirection", recurse=False
    )
    if order_direction_arg is None:
        return "asc"

    return order_direction_arg.value.value


def get_filtering_args(selection: Selection) -> list[str]:
    orderBy_val = get_orderBy_value(selection)
    orderDirection_val = get_orderDirection_value(selection)

    *names, last = orderBy_val.split("__")

    return [*names, "{}_{}".format(last, "gt" if orderDirection_val == "asc" else "lt")]


def get_filtering_value(selection: Selection) -> Any | None:
    def recurse_filtering_values(
        filtering_args: list[str],
        obj: InputValue.Object,
    ) -> Any | None:
        if not filtering_args:
            return None

        if (filtering_arg := filtering_args.pop(0)) not in obj.value:
            return None

        match obj.value[filtering_arg]:
            case InputValue.Object() as obj:
                return recurse_filtering_values(filtering_args, obj)
            case value:
                return value.value

    where_arg = selection.find_args(lambda arg: arg.name == "where", recurse=False)
    if where_arg is None:
        return None

    return recurse_filtering_values(
        get_filtering_args(selection), cast(InputValue.Object, where_arg.value)
    )


def generate_pagination_nodes(
    schema: SchemaMeta, document: Document
) -> list[PaginationNode]:
    counter = count()

    def fold_f(
        current: Selection, parents: list[Selection], children: list[PaginationNode]
    ) -> PaginationNode | list[PaginationNode]:
        if not is_pagination_node(schema, current):
            return children
        idx = next(counter)

        orderBy_val = get_orderBy_value(current)
        filtering_args = get_filtering_args(current)

        t: TypeRef.T = current.fmeta.type_of_arg("where")
        where_arg_type: TypeMeta.InputObjectMeta = schema.type_of_typeref(t)
        filtering_arg_type: TypeRef.T = schema.type_of_input_object_meta(
            where_arg_type, filtering_args
        )

        return PaginationNode(
            node_idx=idx,
            filter_field=orderBy_val,
            first_value=(
                current.find_args(
                    lambda arg: arg.name == "first", recurse=False
                ).value.value
                if current.exists_args(lambda arg: arg.name == "first", recurse=False)
                else DEFAULT_NUM_ENTITIES
            ),
            skip_value=(
                current.find_args(
                    lambda arg: arg.name == "skip", recurse=False
                ).value.value
                if current.exists_args(lambda arg: arg.name == "skip", recurse=False)
                else 0
            ),
            filter_value=get_filtering_value(current),
            filter_value_type=filtering_arg_type,
            key_path=[select.key for select in [*parents, current]],
            inner=children,
        )

    return list(document.query.fold(fold_f) | traverse)


def normalize(
    schema: SchemaMeta,
    document: Document,
    pagination_nodes: list[PaginationNode],
):
    """Inject various graphql components to "normalize" the query for pagination.

    When we paginate a query, we inject custom filtering based on the order by values.
    We also add GraphQL variables so that `PaginationStrategy` only need to change those
     to perform pagination.

    The main process for normalization begins by recursively adjusting `Selection` nodes
     within the Query tree. We only apply the following steps if the node needs to be
     paginated.

    > Note, these steps always check the current selection and will merge new values
     and selections onto whats currently there.

    1. Ensure `id` is on the `Selection`
    2. Replace `first` argument value by `$firstX`
    3. Replace `skip` argument value by `$skipX`
    4. With the `orderBy` (default being `id`), generate where filtering arguments
      a) These are used to filter out values when paginating
    5. Set `where` filtering values (deep union / merge)
    """

    counter = count()

    def map_f(current: Selection) -> Selection:
        if not is_pagination_node(schema, current):
            return current

        idx = next(counter)

        where_value: dict[str, Any] = cast(
            dict,
            current.find_args(
                lambda arg: arg.name == "where", recurse=False
            ).value.value  # type: ignore
            if current.exists_args(lambda arg: arg.name == "where", recurse=False)
            else {},
        )

        orderBy_value = get_orderBy_value(current)

        # Build out a nested dictionary of `InputValue.Object` containing the
        #  `lastOrderingValue{idx}` used for pagination. We do this by iterating through
        #  the args backwards starting from the innermost and nesting them in dicts.
        # We need to add a trailing `_` to outer arguments for `InputValue.Object` for
        #  where clauses to work properly.
        *args, innermost_arg = get_filtering_args(current)
        where_filtering_args: InputValue.Object = reduce(
            lambda inner, outer_arg: InputValue.Object({f"{outer_arg}_": inner}),
            args | reverse,
            InputValue.Object(
                {innermost_arg: InputValue.Variable(f"lastOrderingValue{idx}")}
            ),
        )

        pagination_args = [
            Argument(name="first", value=InputValue.Variable(f"first{idx}")),
            Argument(name="skip", value=InputValue.Variable(f"skip{idx}")),
            Argument(name="orderBy", value=InputValue.Enum(orderBy_value)),
            Argument(
                name="orderDirection",
                value=InputValue.Enum(get_orderDirection_value(current)),
            ),
            Argument(
                name="where",
                value=InputValue.Object(
                    merge_input_value_object_metas(
                        where_filtering_args.value, where_value
                    )
                ),
            ),
        ]

        # This ensures `id` always exists
        current = current.add(
            Selection(
                fmeta=TypeMeta.FieldMeta(
                    name="id",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="String", kind="SCALAR"),
                )
            )
        )

        # Using nested orderBy values (tabulated by "__"), gather the type of the field
        #  from the schema.
        orderBy_values = orderBy_value.split("__")

        # Note, we skip the first type since that is the type of the current `Selection`
        orderBy_types = (
            orderBy_values
            | accumulate(
                func=lambda curr, val: (
                    schema.type_of_typeref(curr).type_of_field(val)
                ),
                initial=schema.type_of_typeref(current.fmeta.type_),
            )
            | skip(1)
        )

        # Generate a `Selection` tree to add to the current selection.
        # This tree is generated from the orderBy values and types as nested `FieldMeta`
        #  constructed through `reduce` by placing singular `Selection` objects in the
        #  `selection` field on the `Selection`.
        current = current.add(
            reduce(
                lambda current, new: replace(current, selection=[new]),
                zip(orderBy_values, orderBy_types)
                | map(
                    lambda tup: Selection(
                        fmeta=TypeMeta.FieldMeta(
                            name=tup[0],
                            description="",
                            args=[],
                            type=tup[1],
                        ),
                    )
                ),
            )
        )

        # add the other arguments that aren't used for pagination
        pagination_args += current.find_all_args(
            lambda arg: arg.name not in PAGINATION_ARGS, recurse=False
        )

        return replace(current, arguments=pagination_args)

    query = document.query.map(map_f, priority="children")
    vardefs = list(pagination_nodes | map(PaginationNode.get_vardefs) | traverse)

    # TODO: normalize document fragments
    return replace(document, query=query.add_vardefs(vardefs))


def prune_doc(document: Document, args: dict[str, Any]) -> Document:
    def prune_where_arg(where_arg: Argument) -> Argument:
        input_val: InputValue.Object = where_arg.value
        return Argument(
            name=where_arg.name,
            value=InputValue.Object(
                {
                    name: val
                    for name, val in input_val.value.items()
                    if not val.is_variable or (val.is_variable and val.name in args)
                }
            ),
        )

    return (
        document.map_args(
            lambda arg: prune_where_arg(arg) if arg.name == "where" else arg
        )
        .filter_args(
            lambda arg: (
                arg.find_var(lambda var: "lastOrderingValue" in var.name).name in args
                if arg.exists_vars(lambda var: "lastOrderingValue" in var.name)
                else True
            )
        )
        .prune_undefined(args)
    )
