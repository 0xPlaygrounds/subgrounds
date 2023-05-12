from typing import Any

import pytest

import tests.queries as queries
from subgrounds.pagination.preprocess import (
    PaginationNode,
    generate_pagination_nodes,
    normalize,
    prune_doc,
)
from subgrounds.query import (
    Argument,
    Document,
    InputValue,
    Query,
    Selection,
    VariableDefinition,
)
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef


@pytest.mark.parametrize(
    ["document", "expected"],
    [
        (
            queries.doc0(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
        (
            queries.doc1(),
            [
                PaginationNode(
                    1,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [
                        PaginationNode(
                            0,
                            "id",
                            100,
                            0,
                            None,
                            TypeRef.Named(name="String", kind="SCALAR"),
                            ["pairs", "swaps"],
                            [],
                        )
                    ],
                )
            ],
        ),
        (
            queries.doc2(),
            [
                PaginationNode(
                    1,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [
                        PaginationNode(
                            0,
                            "id",
                            100,
                            0,
                            None,
                            TypeRef.Named(name="String", kind="SCALAR"),
                            ["pairs", "swaps"],
                            [],
                        )
                    ],
                ),
                PaginationNode(
                    2,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["swaps"],
                    [],
                ),
            ],
        ),
        (
            queries.doc3(),
            [
                PaginationNode(
                    0,
                    "id",
                    455,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
        (
            queries.doc4(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    10,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
        (
            queries.doc5(),
            [
                PaginationNode(
                    0,
                    "createdAtTimestamp",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
        (
            queries.doc6(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
        (
            queries.doc7(),
            [
                PaginationNode(
                    0,
                    "createdAtTimestamp",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
        (
            queries.doc8(),
            [
                PaginationNode(
                    1,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [
                        PaginationNode(
                            0,
                            "id",
                            100,
                            0,
                            None,
                            TypeRef.Named(name="String", kind="SCALAR"),
                            ["pairs", "foo", "swaps"],
                            [],
                        )
                    ],
                )
            ],
        ),
        (queries.doc9(), []),
        (
            queries.doc10(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pair", "swaps"],
                    [],
                )
            ],
        ),
        (
            queries.doc11(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
        (
            queries.doc12(),
            [
                PaginationNode(
                    0,
                    "createdAtTimestamp",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
        (
            queries.doc13(),
            [
                PaginationNode(
                    0,
                    "createdAtTimestamp",
                    100,
                    0,
                    10000,
                    TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
        ),
    ],
)
def test_gen_pagination_nodes(
    schema: SchemaMeta, document: Document, expected: PaginationNode
):
    assert generate_pagination_nodes(schema, document) == expected


@pytest.mark.parametrize(
    ["document", "pagination_nodes", "expected"],
    [
        (
            queries.doc0(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                )
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc1(),
            [
                PaginationNode(
                    1,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [
                        PaginationNode(
                            0,
                            "id",
                            100,
                            0,
                            None,
                            TypeRef.Named(name="String", kind="SCALAR"),
                            ["pairs", "swaps"],
                            [],
                        )
                    ],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first1")),
                                Argument("skip", InputValue.Variable("skip1")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue1"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="swaps",
                                        description="",
                                        args=[
                                            TypeMeta.ArgumentMeta(
                                                name="first",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="skip",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="where",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Swap_filter",
                                                    kind="INPUT_OBJECT",
                                                ),
                                                defaultValue=None,
                                            ),
                                        ],
                                        type=TypeRef.non_null_list(
                                            "Swap", kind="OBJECT"
                                        ),
                                    ),
                                    arguments=[
                                        Argument(
                                            "first", InputValue.Variable("first0")
                                        ),
                                        Argument("skip", InputValue.Variable("skip0")),
                                        Argument("orderBy", InputValue.Enum("id")),
                                        Argument(
                                            "orderDirection", InputValue.Enum("asc")
                                        ),
                                        Argument(
                                            "where",
                                            InputValue.Object(
                                                {
                                                    "id_gt": InputValue.Variable(
                                                        "lastOrderingValue0"
                                                    )
                                                }
                                            ),
                                        ),
                                    ],
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="id",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            )
                                        )
                                    ],
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue1",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc2(),
            [
                PaginationNode(
                    1,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [
                        PaginationNode(
                            0,
                            "id",
                            100,
                            0,
                            None,
                            TypeRef.Named(name="String", kind="SCALAR"),
                            ["pairs", "swaps"],
                            [],
                        )
                    ],
                ),
                PaginationNode(
                    2,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["swaps"],
                    [],
                ),
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first1")),
                                Argument("skip", InputValue.Variable("skip1")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue1"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="swaps",
                                        description="",
                                        args=[
                                            TypeMeta.ArgumentMeta(
                                                name="first",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="skip",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="where",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Swap_filter",
                                                    kind="INPUT_OBJECT",
                                                ),
                                                defaultValue=None,
                                            ),
                                        ],
                                        type=TypeRef.non_null_list(
                                            "Swap", kind="OBJECT"
                                        ),
                                    ),
                                    arguments=[
                                        Argument(
                                            "first", InputValue.Variable("first0")
                                        ),
                                        Argument("skip", InputValue.Variable("skip0")),
                                        Argument("orderBy", InputValue.Enum("id")),
                                        Argument(
                                            "orderDirection", InputValue.Enum("asc")
                                        ),
                                        Argument(
                                            "where",
                                            InputValue.Object(
                                                {
                                                    "id_gt": InputValue.Variable(
                                                        "lastOrderingValue0"
                                                    )
                                                }
                                            ),
                                        ),
                                    ],
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="id",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            )
                                        )
                                    ],
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        ),
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="swaps",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Swap_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first2")),
                                Argument("skip", InputValue.Variable("skip2")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue2"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                )
                            ],
                        ),
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue1",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first2", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip2", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue2",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc3(),
            [
                PaginationNode(
                    0,
                    "id",
                    455,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                )
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc4(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    10,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                )
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc5(),
            [
                PaginationNode(
                    0,
                    "createdAtTimestamp",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument(
                                    "orderBy", InputValue.Enum("createdAtTimestamp")
                                ),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "createdAtTimestamp_gt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="createdAtTimestamp",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigInt", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="BigInt", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc6(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("desc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_lt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc7(),
            [
                PaginationNode(
                    0,
                    "createdAtTimestamp",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument(
                                    "orderBy", InputValue.Enum("createdAtTimestamp")
                                ),
                                Argument("orderDirection", InputValue.Enum("desc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "createdAtTimestamp_lt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="createdAtTimestamp",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigInt", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="BigInt", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc8(),
            [
                PaginationNode(
                    1,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [
                        PaginationNode(
                            0,
                            "id",
                            100,
                            0,
                            None,
                            TypeRef.Named(name="String", kind="SCALAR"),
                            ["pairs", "foo", "swaps"],
                            [],
                        )
                    ],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first1")),
                                Argument("skip", InputValue.Variable("skip1")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue1"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="foo",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(name="Foo", kind="OBJECT"),
                                    ),
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="swaps",
                                                description="",
                                                args=[
                                                    TypeMeta.ArgumentMeta(
                                                        name="first",
                                                        description="",
                                                        type=TypeRef.Named(
                                                            name="Int", kind="SCALAR"
                                                        ),
                                                        defaultValue=None,
                                                    ),
                                                    TypeMeta.ArgumentMeta(
                                                        name="skip",
                                                        description="",
                                                        type=TypeRef.Named(
                                                            name="Int", kind="SCALAR"
                                                        ),
                                                        defaultValue=None,
                                                    ),
                                                    TypeMeta.ArgumentMeta(
                                                        name="where",
                                                        description="",
                                                        type=TypeRef.Named(
                                                            name="Swap_filter",
                                                            kind="INPUT_OBJECT",
                                                        ),
                                                        defaultValue=None,
                                                    ),
                                                ],
                                                type=TypeRef.non_null_list(
                                                    "Swap", kind="OBJECT"
                                                ),
                                            ),
                                            arguments=[
                                                Argument(
                                                    "first",
                                                    InputValue.Variable("first0"),
                                                ),
                                                Argument(
                                                    "skip", InputValue.Variable("skip0")
                                                ),
                                                Argument(
                                                    "orderBy", InputValue.Enum("id")
                                                ),
                                                Argument(
                                                    "orderDirection",
                                                    InputValue.Enum("asc"),
                                                ),
                                                Argument(
                                                    "where",
                                                    InputValue.Object(
                                                        {
                                                            "id_gt": InputValue.Variable(
                                                                "lastOrderingValue0"
                                                            )
                                                        }
                                                    ),
                                                ),
                                            ],
                                            selection=[
                                                Selection(
                                                    fmeta=TypeMeta.FieldMeta(
                                                        name="id",
                                                        description="",
                                                        args=[],
                                                        type=TypeRef.Named(
                                                            name="String", kind="SCALAR"
                                                        ),
                                                    )
                                                )
                                            ],
                                        )
                                    ],
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue1",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc10(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pair", "swaps"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pair",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="id",
                                        description="",
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                        defaultValue=None,
                                    )
                                ],
                                type=TypeRef.Named(name="Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument(
                                    "id",
                                    InputValue.String(
                                        "0x786b582a0bbcac5d192d7e039f49c116ac5f05a8"
                                    ),
                                )
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="swaps",
                                        description="",
                                        args=[
                                            TypeMeta.ArgumentMeta(
                                                name="first",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="skip",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="where",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Swap_filter",
                                                    kind="INPUT_OBJECT",
                                                ),
                                                defaultValue=None,
                                            ),
                                        ],
                                        type=TypeRef.non_null_list(
                                            "Swap", kind="OBJECT"
                                        ),
                                    ),
                                    arguments=[
                                        Argument(
                                            "first", InputValue.Variable("first0")
                                        ),
                                        Argument("skip", InputValue.Variable("skip0")),
                                        Argument("orderBy", InputValue.Enum("id")),
                                        Argument(
                                            "orderDirection", InputValue.Enum("asc")
                                        ),
                                        Argument(
                                            "where",
                                            InputValue.Object(
                                                {
                                                    "id_gt": InputValue.Variable(
                                                        "lastOrderingValue0"
                                                    )
                                                }
                                            ),
                                        ),
                                    ],
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="id",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            )
                                        )
                                    ],
                                )
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc11(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            ),
                                            "createdAtTimestamp_gt": InputValue.Int(
                                                10000
                                            ),
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc12(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument(
                                    "orderBy", InputValue.Enum("createdAtTimestamp")
                                ),
                                Argument("orderDirection", InputValue.Enum("desc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "createdAtTimestamp_lt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            ),
                                            "createdAtTimestamp_gt": InputValue.Int(
                                                10000
                                            ),
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="createdAtTimestamp",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigInt", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc13(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument(
                                    "orderBy", InputValue.Enum("createdAtTimestamp")
                                ),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "createdAtTimestamp_gt": InputValue.Variable(
                                                "lastOrderingValue0"
                                            ),
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="createdAtTimestamp",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigInt", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
        (
            queries.doc14(),
            [
                PaginationNode(
                    0,
                    "id",
                    100,
                    0,
                    None,
                    TypeRef.Named(name="String", kind="SCALAR"),
                    ["pairs"],
                    [],
                )
            ],
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first0")),
                                Argument("skip", InputValue.Variable("skip0")),
                                Argument("orderBy", InputValue.Enum("token0__symbol")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "token0": InputValue.Object(
                                                {
                                                    "id": InputValue.String(
                                                        "0x000000000000000000000000000000000000dEaD"
                                                    ),
                                                    "symbol_gt": InputValue.Variable(
                                                        "lastOrderingValue0"
                                                    )
                                                }
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="name",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="token0",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="symbol",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            ),
                                        )
                                    ],
                                ),
                            ],
                        )
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
        ),
    ],
)
def test_normalize_doc(
    schema: SchemaMeta,
    document: Document,
    pagination_nodes: list[PaginationNode],
    expected: PaginationNode,
):
    assert normalize(schema, document, pagination_nodes) == expected


@pytest.mark.parametrize(
    ["document", "args", "pruned"],
    [
        (
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first1")),
                                Argument("skip", InputValue.Variable("skip1")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue1"
                                            ),
                                            "createdAtTimestamp_gt": InputValue.Int(
                                                10000
                                            ),
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="swaps",
                                        description="",
                                        args=[
                                            TypeMeta.ArgumentMeta(
                                                name="first",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="skip",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="where",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Swap_filter",
                                                    kind="INPUT_OBJECT",
                                                ),
                                                defaultValue=None,
                                            ),
                                        ],
                                        type=TypeRef.non_null_list(
                                            "Swap", kind="OBJECT"
                                        ),
                                    ),
                                    arguments=[
                                        Argument(
                                            "first", InputValue.Variable("first0")
                                        ),
                                        Argument("skip", InputValue.Variable("skip0")),
                                        Argument("orderBy", InputValue.Enum("id")),
                                        Argument(
                                            "orderDirection", InputValue.Enum("asc")
                                        ),
                                        Argument(
                                            "where",
                                            InputValue.Object(
                                                {
                                                    "id_gt": InputValue.Variable(
                                                        "lastOrderingValue0"
                                                    ),
                                                    "createdAtTimestamp_gt": InputValue.Int(
                                                        10000
                                                    ),
                                                }
                                            ),
                                        ),
                                    ],
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="id",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            )
                                        )
                                    ],
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        ),
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="swaps",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Swap_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first2")),
                                Argument("skip", InputValue.Variable("skip2")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue2"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                )
                            ],
                        ),
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue1",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first2", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip2", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue2",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
            {
                "first0": 10,
                "skip0": 0,
                "lastOrderingValue0": 10,
                "first1": 10,
                "skip1": 0,
                "lastOrderingValue1": 10,
            },
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first1")),
                                Argument("skip", InputValue.Variable("skip1")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue1"
                                            ),
                                            "createdAtTimestamp_gt": InputValue.Int(
                                                10000
                                            ),
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="swaps",
                                        description="",
                                        args=[
                                            TypeMeta.ArgumentMeta(
                                                name="first",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="skip",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="where",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Swap_filter",
                                                    kind="INPUT_OBJECT",
                                                ),
                                                defaultValue=None,
                                            ),
                                        ],
                                        type=TypeRef.non_null_list(
                                            "Swap", kind="OBJECT"
                                        ),
                                    ),
                                    arguments=[
                                        Argument(
                                            "first", InputValue.Variable("first0")
                                        ),
                                        Argument("skip", InputValue.Variable("skip0")),
                                        Argument("orderBy", InputValue.Enum("id")),
                                        Argument(
                                            "orderDirection", InputValue.Enum("asc")
                                        ),
                                        Argument(
                                            "where",
                                            InputValue.Object(
                                                {
                                                    "id_gt": InputValue.Variable(
                                                        "lastOrderingValue0"
                                                    ),
                                                    "createdAtTimestamp_gt": InputValue.Int(
                                                        10000
                                                    ),
                                                }
                                            ),
                                        ),
                                    ],
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="id",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            )
                                        )
                                    ],
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        ),
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue1",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
                variables={
                    "first0": 10,
                    "skip0": 0,
                    "lastOrderingValue0": 10,
                    "first1": 10,
                    "skip1": 0,
                    "lastOrderingValue1": 10,
                },
            ),
        ),
        (
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first1")),
                                Argument("skip", InputValue.Variable("skip1")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue1"
                                            ),
                                            "createdAtTimestamp_gt": InputValue.Int(
                                                10000
                                            ),
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="swaps",
                                        description="",
                                        args=[
                                            TypeMeta.ArgumentMeta(
                                                name="first",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="skip",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="where",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Swap_filter",
                                                    kind="INPUT_OBJECT",
                                                ),
                                                defaultValue=None,
                                            ),
                                        ],
                                        type=TypeRef.non_null_list(
                                            "Swap", kind="OBJECT"
                                        ),
                                    ),
                                    arguments=[
                                        Argument(
                                            "first", InputValue.Variable("first0")
                                        ),
                                        Argument("skip", InputValue.Variable("skip0")),
                                        Argument("orderBy", InputValue.Enum("id")),
                                        Argument(
                                            "orderDirection", InputValue.Enum("asc")
                                        ),
                                        Argument(
                                            "where",
                                            InputValue.Object(
                                                {
                                                    "id_gt": InputValue.Variable(
                                                        "lastOrderingValue0"
                                                    ),
                                                    "createdAtTimestamp_gt": InputValue.Int(
                                                        10000
                                                    ),
                                                }
                                            ),
                                        ),
                                    ],
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="id",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            )
                                        )
                                    ],
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        ),
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="swaps",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Swap_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first2")),
                                Argument("skip", InputValue.Variable("skip2")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "id_gt": InputValue.Variable(
                                                "lastOrderingValue2"
                                            )
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                )
                            ],
                        ),
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue0",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue1",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        VariableDefinition(
                            "first2", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip2", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "lastOrderingValue2",
                            TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                    ],
                ),
            ),
            {
                "first0": 10,
                "skip0": 0,
                "first1": 10,
                "skip1": 0,
            },
            Document(
                url="www.abc.xyz/graphql",
                query=Query(
                    name=None,
                    selection=[
                        Selection(
                            fmeta=TypeMeta.FieldMeta(
                                name="pairs",
                                description="",
                                args=[
                                    TypeMeta.ArgumentMeta(
                                        name="first",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="skip",
                                        description="",
                                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                                        defaultValue=None,
                                    ),
                                    TypeMeta.ArgumentMeta(
                                        name="where",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Pair_filter", kind="INPUT_OBJECT"
                                        ),
                                        defaultValue=None,
                                    ),
                                ],
                                type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                            ),
                            arguments=[
                                Argument("first", InputValue.Variable("first1")),
                                Argument("skip", InputValue.Variable("skip1")),
                                Argument("orderBy", InputValue.Enum("id")),
                                Argument("orderDirection", InputValue.Enum("asc")),
                                Argument(
                                    "where",
                                    InputValue.Object(
                                        {
                                            "createdAtTimestamp_gt": InputValue.Int(
                                                10000
                                            ),
                                        }
                                    ),
                                ),
                            ],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="swaps",
                                        description="",
                                        args=[
                                            TypeMeta.ArgumentMeta(
                                                name="first",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="skip",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Int", kind="SCALAR"
                                                ),
                                                defaultValue=None,
                                            ),
                                            TypeMeta.ArgumentMeta(
                                                name="where",
                                                description="",
                                                type=TypeRef.Named(
                                                    name="Swap_filter",
                                                    kind="INPUT_OBJECT",
                                                ),
                                                defaultValue=None,
                                            ),
                                        ],
                                        type=TypeRef.non_null_list(
                                            "Swap", kind="OBJECT"
                                        ),
                                    ),
                                    arguments=[
                                        Argument(
                                            "first", InputValue.Variable("first0")
                                        ),
                                        Argument("skip", InputValue.Variable("skip0")),
                                        Argument("orderBy", InputValue.Enum("id")),
                                        Argument(
                                            "orderDirection", InputValue.Enum("asc")
                                        ),
                                        Argument(
                                            "where",
                                            InputValue.Object(
                                                {
                                                    "createdAtTimestamp_gt": InputValue.Int(
                                                        10000
                                                    ),
                                                }
                                            ),
                                        ),
                                    ],
                                    selection=[
                                        Selection(
                                            fmeta=TypeMeta.FieldMeta(
                                                name="id",
                                                description="",
                                                args=[],
                                                type=TypeRef.Named(
                                                    name="String", kind="SCALAR"
                                                ),
                                            )
                                        )
                                    ],
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        ),
                    ],
                    variables=[
                        VariableDefinition(
                            "first0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip0", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "first1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                        VariableDefinition(
                            "skip1", TypeRef.Named(name="Int", kind="SCALAR")
                        ),
                    ],
                ),
                variables={
                    "first0": 10,
                    "skip0": 0,
                    "first1": 10,
                    "skip1": 0,
                },
            ),
        ),
    ],
)
def test_prune_doc(document: Document, args: dict[str, Any], pruned: Document):
    assert prune_doc(document, args) == pruned
