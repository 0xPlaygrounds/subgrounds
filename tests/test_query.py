import pytest

from subgrounds.query import Argument, InputValue, Query, Selection, VariableDefinition
from subgrounds.schema import TypeMeta, TypeRef

# ================================================================
# Query printing tests
# ================================================================


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (
            Query(
                None,
                [
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
                                TypeMeta.ArgumentMeta(
                                    name="orderBy",
                                    description="",
                                    type=TypeRef.Named(
                                        name="Pair_orderBy", kind="ENUM"
                                    ),
                                    defaultValue=None,
                                ),
                                TypeMeta.ArgumentMeta(
                                    name="orderDirection",
                                    description="",
                                    type=TypeRef.Named(
                                        name="OrderDirection", kind="ENUM"
                                    ),
                                    defaultValue=None,
                                ),
                            ],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        arguments=[
                            Argument("first", InputValue.Int(100)),
                            Argument(
                                "where",
                                InputValue.Object(
                                    {"reserveUSD_lt": InputValue.String("10.0")}
                                ),
                            ),
                            Argument("orderBy", InputValue.Enum("reserveUSD")),
                            Argument("orderDirection", InputValue.Enum("desc")),
                        ],
                        selection=[
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                )
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                selection=[
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        )
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
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
                                TypeMeta.FieldMeta(
                                    name="token1",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                selection=[
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        )
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
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
                    )
                ],
            ),
            """query {
  pairs(first: 100, where: {reserveUSD_lt: "10.0"}, orderBy: reserveUSD, orderDirection: desc) {
    id
    token0 {
      name
      symbol
    }
    token1 {
      name
      symbol
    }
  }
}""",
        ),
        (
            Query(
                None,
                [
                    Selection(
                        fmeta=TypeMeta.FieldMeta(
                            name="token",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Token"),
                        ),
                        alias=None,
                        arguments=[Argument("id", InputValue.Variable("tokenId"))],
                        selection=[
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [VariableDefinition("tokenId", TypeRef.non_null("String"))],
            ),
            """query($tokenId: String!) {
  token(id: $tokenId) {
    id
    name
    symbol
  }
}""",
        ),
    ],
)
def test_graphql_compilation(test_input: Query, expected: str):
    assert test_input.graphql == expected


# ================================================================
# Selection class tests
# ================================================================


@pytest.mark.parametrize(
    "selection1, selection2, expected",
    [
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [Argument("first", InputValue.Int(100))],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="amount1In",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                ),
                None,
                [],
                [],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [Argument("first", InputValue.Int(100))],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="token0",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="name",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="symbol",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            [
                Selection(
                    TypeMeta.FieldMeta(
                        name="amount1In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Float", kind="SCALAR"),
                    ),
                    None,
                    [],
                    [],
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                    ),
                    None,
                    [],
                    [],
                ),
            ],
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="timestamp",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Int", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
        ),
    ],
)
def test_selection_add(selection1, selection2, expected):
    assert Selection.add(selection1, selection2) == expected


@pytest.mark.parametrize(
    "selection1, selection2, expected",
    [
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="timestamp",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Int", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="timestamp",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Int", kind="SCALAR"),
                ),
                None,
                [],
                [],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="token0",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="name",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="symbol",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="String", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="token0",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                ),
                None,
                [],
                [],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [],
            ),
        ),
    ],
)
def test_selection_remove(selection1, selection2, expected):
    assert Selection.remove(selection1, selection2) == expected


@pytest.mark.parametrize(
    "selection1, selection2, expected",
    [
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            True,
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="timestamp",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            False,
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [
                    Argument("first", InputValue.Int(100)),
                ],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            True,
        ),
        (
            Selection(
                fmeta=TypeMeta.FieldMeta(
                    name="log",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="String", kind="SCALAR"),
                ),
                alias=None,
                arguments=[],
                selection=[],
            ),
            Selection(
                fmeta=TypeMeta.FieldMeta(
                    name="log",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="String", kind="SCALAR"),
                ),
                alias=None,
                arguments=[],
                selection=[],
            ),
            True,
        ),
    ],
)
def test_selection_contains(selection1, selection2, expected):
    assert Selection.contains(selection1, selection2) == expected


@pytest.mark.parametrize(
    "selection1, selection2, expected",
    [
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount0In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [],
                    )
                ],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
        ),
    ],
)
def test_selection_select(selection1, selection2, expected):
    assert Selection.select(selection1, selection2) == expected


@pytest.mark.parametrize(
    "selections, expected",
    [
        (
            [
                Selection(
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Token", kind="OBJECT"),
                            ),
                            None,
                            [],
                            [
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                            ],
                        )
                    ],
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Token", kind="OBJECT"),
                            ),
                            None,
                            [],
                            [
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="name",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                            ],
                        )
                    ],
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Token", kind="OBJECT"),
                            ),
                            None,
                            [],
                            [
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="symbol",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                            ],
                        )
                    ],
                ),
            ],
            [
                Selection(
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Token", kind="OBJECT"),
                            ),
                            None,
                            [],
                            [
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="name",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="symbol",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                            ],
                        )
                    ],
                )
            ],
        ),
        (
            [
                Selection(
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            ),
                            None,
                            [],
                            [],
                        ),
                    ],
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Token", kind="OBJECT"),
                            ),
                            None,
                            [],
                            [
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                            ],
                        )
                    ],
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Token", kind="OBJECT"),
                            ),
                            None,
                            [],
                            [
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="name",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                            ],
                        )
                    ],
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Token", kind="OBJECT"),
                            ),
                            None,
                            [],
                            [
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="symbol",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                            ],
                        )
                    ],
                ),
            ],
            [
                Selection(
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            ),
                            None,
                            [],
                            [],
                        ),
                    ],
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="token0",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Token", kind="OBJECT"),
                            ),
                            None,
                            [],
                            [
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="name",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="symbol",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    ),
                                    None,
                                    [],
                                    [],
                                ),
                            ],
                        )
                    ],
                ),
            ],
        ),
    ],
)
def test_selection_merge(selections, expected):
    assert Selection.merge(selections) == expected


@pytest.mark.parametrize(
    "selection, expected",
    [
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            True,
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Token"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            True,
        ),
        (
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="id",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            False,
        ),
    ],
)
def test_selection_contains_list(selection, expected):
    assert Selection.contains_list(selection) == expected


# ================================================================
# Query class tests
# ================================================================


@pytest.mark.parametrize(
    "query, other, expected",
    [
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="amount1In",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Float", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    )
                ],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount1In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
        ),
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="id",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="id",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
        ),
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
            [
                Selection(
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    None,
                    [],
                    [
                        Selection(
                            TypeMeta.FieldMeta(
                                name="amount1In",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Float", kind="SCALAR"),
                            ),
                            None,
                            [],
                            [],
                        ),
                        Selection(
                            TypeMeta.FieldMeta(
                                name="timestamp",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Int", kind="SCALAR"),
                            ),
                            None,
                            [],
                            [],
                        ),
                    ],
                )
            ],
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount1In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="timestamp",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Int", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
        ),
    ],
)
def test_query_add(query, other, expected):
    assert Query.add(query, other) == expected


@pytest.mark.parametrize(
    "query, other, expected",
    [
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount1In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="timestamp",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Int", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="swaps",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="timestamp",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Int", kind="SCALAR"),
                        ),
                        None,
                        [],
                        [],
                    ),
                ],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount1In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
        ),
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="id",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
            Selection(
                TypeMeta.FieldMeta(
                    name="pair",
                    description="",
                    args=[],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
                None,
                [],
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="token0",
                            description="",
                            args=[],
                            type=TypeRef.Named(name="Token", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="name",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="symbol",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="String", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="id",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
        ),
    ],
)
def test_query_remove(query, other, expected):
    assert Query.remove(query, other) == expected


@pytest.mark.parametrize(
    "query, other, expected",
    [
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount1In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="swaps",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="amount0In",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                                ),
                                None,
                                [],
                                [],
                            ),
                        ],
                    )
                ],
                [],
            ),
        ),
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="id",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [],
                            )
                        ],
                    )
                ],
                [],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="id",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
        ),
    ],
)
def test_query_select(query, other, expected):
    assert Query.select(query, other) == expected


@pytest.mark.parametrize(
    "query, other, expected",
    [
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="id",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [],
                            )
                        ],
                    )
                ],
                [],
            ),
            True,
        ),
        (
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="id",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
            Query(
                None,
                [
                    Selection(
                        TypeMeta.FieldMeta(
                            name="pair",
                            description="",
                            args=[],
                            type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                        ),
                        None,
                        [],
                        [
                            Selection(
                                TypeMeta.FieldMeta(
                                    name="token0",
                                    description="",
                                    args=[],
                                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                                ),
                                None,
                                [],
                                [
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="name",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                    Selection(
                                        TypeMeta.FieldMeta(
                                            name="symbol",
                                            description="",
                                            args=[],
                                            type=TypeRef.Named(
                                                name="String", kind="SCALAR"
                                            ),
                                        ),
                                        None,
                                        [],
                                        [],
                                    ),
                                ],
                            )
                        ],
                    )
                ],
                [],
            ),
            True,
        ),
    ],
)
def test_query_contains(query, other, expected):
    assert Query.contains(query, other) == expected
