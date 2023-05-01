""" Helper functions and classes used by Subgrounds' own pagination strategies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import count
from typing import Any, cast

from pipe import map, traverse

from subgrounds.pagination.utils import DEFAULT_NUM_ENTITIES
from subgrounds.query import (
    Argument,
    Document,
    InputValue,
    Selection,
    VariableDefinition,
)
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef


@dataclass(frozen=True)
class PaginationNode:
    """Class representing the pagination config for a single GraphQL list field.

    Attributes:
      node_idx (int): Index of PaginationNode, used to label pagination arguments
        for this node.
      filter_field (str): Name of the node's filter field, e.g.: if
        ``filter_name`` is ``timestamp_gt``, then :attr:`filter_field`
        is ``timestamp``
      first_value (int): Initial value of the ``first`` argument
      skip_value (int): Initial value of the ``skip`` argument
      filter_value (Any): Initial value of the filter argument
        (i.e.: ``where: {filter: FILTER_VALUE}``)
      filter_value_type (TypeRef.T): Type of the filter value
      key_path (list[str]): Location in the list field to which this pagination
        node refers to in the initial query
      inner (list[PaginationNode]): Nested pagination nodes (if any).
    """

    node_idx: int
    filter_field: str

    first_value: int
    skip_value: int
    filter_value: Any
    filter_value_type: TypeRef.T

    key_path: list[str]
    inner: list[PaginationNode] = field(default_factory=list)

    def get_vardefs(self: PaginationNode) -> list[VariableDefinition]:
        """Returns a list of variable definitions corresponding to this pagination
        node's pagination arguments as well as the variable definitions related
        to any nested pagination nodes.

        Args:
          self (PaginationNode): The current PaginationNode

        Returns:
          list[VariableDefinition]: _description_
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
    else:
        return order_by_val.value.value


def get_orderDirection_value(selection: Selection) -> str:
    order_direction_arg = selection.find_args(
        lambda arg: arg.name == "orderDirection", recurse=False
    )

    if order_direction_arg is None:
        return "asc"
    else:
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


""" normalize:

For each selection:
1. If `id` not selected, add `id` to selection
2. Replace `first` argument value by `$firstX`
3. Replace `skip` argument value by `$skipX`
4. If `orderBy` argument is not defined, add argument with value `asc`
5. If `where` argument specified 
"""


def generate_pagination_nodes(
    schema: SchemaMeta, document: Document
) -> list[PaginationNode]:
    counter = count()

    def fold_f(
        current: Selection, parents: list[Selection], children: list[PaginationNode]
    ) -> list[PaginationNode]:
        if is_pagination_node(schema, current):
            idx = next(counter)

            orderBy_val = get_orderBy_value(current)
            filtering_arg = get_filtering_args(current).pop()

            t: TypeRef.T = current.fmeta.type_of_arg("where")
            where_arg_type: TypeMeta.InputObjectMeta = schema.type_of_typeref(t)
            filtering_arg_type: TypeRef.T = where_arg_type.type_of_input_field(
                filtering_arg
            )

            return PaginationNode(
                node_idx=idx,
                filter_field=orderBy_val,
                first_value=(
                    current.find_args(
                        lambda arg: arg.name == "first", recurse=False
                    ).value.value
                    if current.exists_args(
                        lambda arg: arg.name == "first", recurse=False
                    )
                    else DEFAULT_NUM_ENTITIES
                ),
                skip_value=(
                    current.find_args(
                        lambda arg: arg.name == "skip", recurse=False
                    ).value.value
                    if current.exists_args(
                        lambda arg: arg.name == "skip", recurse=False
                    )
                    else 0
                ),
                filter_value=get_filtering_value(current),
                filter_value_type=filtering_arg_type,
                key_path=[select.key for select in [*parents, current]],
                inner=children,
            )
        else:
            return children

    return list(document.query.fold(fold_f) | traverse)


def normalize(
    schema: SchemaMeta, document: Document, pagination_nodes: list[PaginationNode]
):
    counter = count()

    def map_f(
        current: Selection,
    ) -> Selection:
        if is_pagination_node(schema, current):
            idx = next(counter)

            where_value: dict[str, Any] = (
                current.find_args(
                    lambda arg: arg.name == "where", recurse=False
                ).value.value
                if current.exists_args(lambda arg: arg.name == "where", recurse=False)
                else {}
            )

            orderBy_value = get_orderBy_value(current)

            where_filtering_args = None
            for arg in reversed(get_filtering_args(current)):
                if where_filtering_args is None:
                    where_filtering_args = {
                        arg: InputValue.Variable(f"lastOrderingValue{idx}")
                    }
                else:
                    where_filtering_args = {arg: where_filtering_args}

            where_filtering_args = cast(dict[str, Any], where_filtering_args)

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
                    value=InputValue.Object(where_value | where_filtering_args),
                ),
            ]

            selections = current.selection
            if not any(selections | map(lambda s: s.fmeta.name == "id")):
                selections.append(
                    Selection(
                        fmeta=TypeMeta.FieldMeta(
                            name="id",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="String", kind="SCALAR"),
                        )
                    )
                )

            if orderBy_value != "id" and not any(
                selections | map(lambda s: s.fmeta.name == orderBy_value)
            ):
                current_type: TypeMeta.ObjectMeta = schema.type_of_typeref(
                    current.fmeta.type_
                )
                selections.append(
                    Selection(
                        fmeta=TypeMeta.FieldMeta(
                            name=orderBy_value,
                            description="",
                            args=[],
                            type=current_type.type_of_field(orderBy_value),
                        )
                    )
                )

            return Selection(
                fmeta=current.fmeta,
                alias=current.alias,
                arguments=pagination_args
                + current.find_all_args(
                    lambda arg: arg.name not in PAGINATION_ARGS, recurse=False
                ),
                selection=selections,
            )
        else:
            return current

    query = document.query.map(map_f, priority="children")

    vardefs = list(pagination_nodes | map(PaginationNode.get_vardefs) | traverse)

    return Document(
        url=document.url,
        query=query.add_vardefs(vardefs),
        fragments=document.fragments,  # TODO: normalize fragments
        variables=document.variables,
    )


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
