from subgrounds import Subgrounds
from subgrounds.query import Argument, DataRequest, InputValue, Query, Selection
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef
from subgrounds.subgraph import FieldPath, Filter, Object, Subgraph, SyntheticField
from subgrounds.utils import identity
from tests.conftest import fieldpath_test_mode


def test_add_synthetic_field_1(subgraph: Subgraph):
    sfield = SyntheticField(identity, SyntheticField.FLOAT, subgraph.Pair.reserveUSD)

    expected = SchemaMeta(
        queryType={"name": "Query"},
        types=[],
        type_map={
            "ID": TypeMeta.ScalarMeta(name="ID", description=""),
            "Int": TypeMeta.ScalarMeta(name="Int", description="", kind="SCALAR"),
            "Float": TypeMeta.ScalarMeta(name="Float", description="", kind="SCALAR"),
            "BigInt": TypeMeta.ScalarMeta(name="BigInt", description="", kind="SCALAR"),
            "BigDecimal": TypeMeta.ScalarMeta(
                name="BigDecimal", description="", kind="SCALAR"
            ),
            "String": TypeMeta.ScalarMeta(name="String", description="", kind="SCALAR"),
            "OrderDirection": TypeMeta.EnumMeta(
                name="OrderDirection",
                description="",
                enumValues=[
                    TypeMeta.EnumValueMeta(name="asc", description=""),
                    TypeMeta.EnumValueMeta(name="desc", description=""),
                ],
            ),
            "Query": TypeMeta.ObjectMeta(
                name="Query",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
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
                                type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
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
                            TypeMeta.ArgumentMeta(
                                name="orderBy",
                                description="",
                                type=TypeRef.Named(name="Swap_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="id",
                                description="",
                                type=TypeRef.non_null("ID", kind="SCALAR"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.Named(name="Pair", kind="OBJECT"),
                    ),
                ],
            ),
            "Swap": TypeMeta.ObjectMeta(
                name="Swap",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount0In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount0Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount1In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount1Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                ],
            ),
            "Token": TypeMeta.ObjectMeta(
                name="Token",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="name",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="symbol",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="decimals",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                    ),
                ],
            ),
            "Token_filter": TypeMeta.InputObjectMeta(
                name="Token_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
            "Pair": TypeMeta.ObjectMeta(
                name="Pair",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="token0",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="token1",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="reserveUSD",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="priceToken0",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="priceToken1",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="createdAtTimestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="reserveCAD",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Float", kind="SCALAR"),
                    ),
                ],
            ),
            "Pair_filter": TypeMeta.InputObjectMeta(
                name="Pair_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token0",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token0_",
                        description="",
                        type=TypeRef.Named(name="Token_filter", kind="OBJECT"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token1",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token1_",
                        description="",
                        type=TypeRef.Named(name="Token_filter", kind="OBJECT"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="reserveUSD_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="reserveUSD_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken0_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken0_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken1_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken1_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="createdAtTimestamp_lt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="createdAtTimestamp_gt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
            "Pair_orderBy": TypeMeta.EnumMeta(
                name="Pair_orderBy",
                description="",
                enumValues=[
                    TypeMeta.EnumValueMeta(name="id", description=""),
                    TypeMeta.EnumValueMeta(name="reserveUSD", description=""),
                    TypeMeta.EnumValueMeta(name="createdAtTimestamp", description=""),
                    TypeMeta.EnumValueMeta(name="token0__symbol", description=""),
                    TypeMeta.EnumValueMeta(name="token1__symbol", description=""),
                ],
            ),
            "Swap_filter": TypeMeta.InputObjectMeta(
                name="Swap_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp_gt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp_lt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount0In",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount0Out",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount1In",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount1Out",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
        },
    )

    Pair = subgraph.Pair
    Pair.reserveCAD = sfield

    assert subgraph._schema == expected


def test_add_synthetic_field_2(subgraph: Subgraph):
    expected = SchemaMeta(
        queryType={"name": "Query"},
        types=[],
        type_map={
            "ID": TypeMeta.ScalarMeta(name="ID", description=""),
            "Int": TypeMeta.ScalarMeta(name="Int", description="", kind="SCALAR"),
            "Float": TypeMeta.ScalarMeta(name="Float", description="", kind="SCALAR"),
            "BigInt": TypeMeta.ScalarMeta(name="BigInt", description="", kind="SCALAR"),
            "BigDecimal": TypeMeta.ScalarMeta(
                name="BigDecimal", description="", kind="SCALAR"
            ),
            "String": TypeMeta.ScalarMeta(name="String", description="", kind="SCALAR"),
            "OrderDirection": TypeMeta.EnumMeta(
                name="OrderDirection",
                description="",
                enumValues=[
                    TypeMeta.EnumValueMeta(name="asc", description=""),
                    TypeMeta.EnumValueMeta(name="desc", description=""),
                ],
            ),
            "Query": TypeMeta.ObjectMeta(
                name="Query",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
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
                                type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
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
                            TypeMeta.ArgumentMeta(
                                name="orderBy",
                                description="",
                                type=TypeRef.Named(name="Swap_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="id",
                                description="",
                                type=TypeRef.non_null("ID", kind="SCALAR"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.Named(name="Pair", kind="OBJECT"),
                    ),
                ],
            ),
            "Swap": TypeMeta.ObjectMeta(
                name="Swap",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount0In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount0Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount1In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount1Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                ],
            ),
            "Token": TypeMeta.ObjectMeta(
                name="Token",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="name",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="symbol",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="decimals",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                    ),
                ],
            ),
            "Token_filter": TypeMeta.InputObjectMeta(
                name="Token_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
            "Pair": TypeMeta.ObjectMeta(
                name="Pair",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="token0",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="token1",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="reserveUSD",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="priceToken0",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="priceToken1",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="createdAtTimestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="reserveCAD",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Float", kind="SCALAR"),
                    ),
                ],
            ),
            "Pair_filter": TypeMeta.InputObjectMeta(
                name="Pair_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token0",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token0_",
                        description="",
                        type=TypeRef.Named(name="Token_filter", kind="OBJECT"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token1",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token1_",
                        description="",
                        type=TypeRef.Named(name="Token_filter", kind="OBJECT"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="reserveUSD_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="reserveUSD_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken0_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken0_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken1_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken1_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="createdAtTimestamp_lt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="createdAtTimestamp_gt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
            "Pair_orderBy": TypeMeta.EnumMeta(
                name="Pair_orderBy",
                description="",
                enumValues=[
                    TypeMeta.EnumValueMeta(name="id", description=""),
                    TypeMeta.EnumValueMeta(name="reserveUSD", description=""),
                    TypeMeta.EnumValueMeta(name="createdAtTimestamp", description=""),
                    TypeMeta.EnumValueMeta(name="token0__symbol", description=""),
                    TypeMeta.EnumValueMeta(name="token1__symbol", description=""),
                ],
            ),
            "Swap_filter": TypeMeta.InputObjectMeta(
                name="Swap_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp_gt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp_lt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount0In",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount0Out",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount1In",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount1Out",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
        },
    )

    Pair = subgraph.Pair
    Pair.reserveCAD = Pair.reserveUSD * 1.3

    assert subgraph._schema == expected


def test_add_synthetic_field_3(subgraph: Subgraph):
    expected = SchemaMeta(
        queryType={"name": "Query"},
        types=[],
        type_map={
            "ID": TypeMeta.ScalarMeta(name="ID", description=""),
            "Int": TypeMeta.ScalarMeta(name="Int", description="", kind="SCALAR"),
            "Float": TypeMeta.ScalarMeta(name="Float", description="", kind="SCALAR"),
            "BigInt": TypeMeta.ScalarMeta(name="BigInt", description="", kind="SCALAR"),
            "BigDecimal": TypeMeta.ScalarMeta(
                name="BigDecimal", description="", kind="SCALAR"
            ),
            "String": TypeMeta.ScalarMeta(name="String", description="", kind="SCALAR"),
            "OrderDirection": TypeMeta.EnumMeta(
                name="OrderDirection",
                description="",
                enumValues=[
                    TypeMeta.EnumValueMeta(name="asc", description=""),
                    TypeMeta.EnumValueMeta(name="desc", description=""),
                ],
            ),
            "Query": TypeMeta.ObjectMeta(
                name="Query",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
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
                                type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
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
                            TypeMeta.ArgumentMeta(
                                name="orderBy",
                                description="",
                                type=TypeRef.Named(name="Swap_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="id",
                                description="",
                                type=TypeRef.non_null("ID", kind="SCALAR"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.Named(name="Pair", kind="OBJECT"),
                    ),
                ],
            ),
            "Swap": TypeMeta.ObjectMeta(
                name="Swap",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount0In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount0Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount1In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount1Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                ],
            ),
            "Token": TypeMeta.ObjectMeta(
                name="Token",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="name",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="symbol",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="decimals",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                    ),
                ],
            ),
            "Token_filter": TypeMeta.InputObjectMeta(
                name="Token_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
            "Pair": TypeMeta.ObjectMeta(
                name="Pair",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="token0",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="token1",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="reserveUSD",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="priceToken0",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="priceToken1",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="createdAtTimestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="reserveCAD",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Float", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="reserveEUR",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Float", kind="SCALAR"),
                    ),
                ],
            ),
            "Pair_filter": TypeMeta.InputObjectMeta(
                name="Pair_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token0",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token0_",
                        description="",
                        type=TypeRef.Named(name="Token_filter", kind="OBJECT"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token1",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token1_",
                        description="",
                        type=TypeRef.Named(name="Token_filter", kind="OBJECT"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="reserveUSD_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="reserveUSD_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken0_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken0_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken1_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken1_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="createdAtTimestamp_lt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="createdAtTimestamp_gt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
            "Pair_orderBy": TypeMeta.EnumMeta(
                name="Pair_orderBy",
                description="",
                enumValues=[
                    TypeMeta.EnumValueMeta(name="id", description=""),
                    TypeMeta.EnumValueMeta(name="reserveUSD", description=""),
                    TypeMeta.EnumValueMeta(name="createdAtTimestamp", description=""),
                    TypeMeta.EnumValueMeta(name="token0__symbol", description=""),
                    TypeMeta.EnumValueMeta(name="token1__symbol", description=""),
                ],
            ),
            "Swap_filter": TypeMeta.InputObjectMeta(
                name="Swap_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp_gt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp_lt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount0In",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount0Out",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount1In",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount1Out",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
        },
    )

    Pair = subgraph.Pair
    Pair.reserveCAD = Pair.reserveUSD * 1.3
    Pair.reserveEUR = Pair.reserveCAD * 1.5

    assert subgraph._schema == expected


def test_add_synthetic_field_4(subgraph: Subgraph):
    expected = SchemaMeta(
        queryType={"name": "Query"},
        types=[],
        type_map={
            "ID": TypeMeta.ScalarMeta(name="ID", description=""),
            "Int": TypeMeta.ScalarMeta(name="Int", description="", kind="SCALAR"),
            "Float": TypeMeta.ScalarMeta(name="Float", description="", kind="SCALAR"),
            "BigInt": TypeMeta.ScalarMeta(name="BigInt", description="", kind="SCALAR"),
            "BigDecimal": TypeMeta.ScalarMeta(
                name="BigDecimal", description="", kind="SCALAR"
            ),
            "String": TypeMeta.ScalarMeta(name="String", description="", kind="SCALAR"),
            "OrderDirection": TypeMeta.EnumMeta(
                name="OrderDirection",
                description="",
                enumValues=[
                    TypeMeta.EnumValueMeta(name="asc", description=""),
                    TypeMeta.EnumValueMeta(name="desc", description=""),
                ],
            ),
            "Query": TypeMeta.ObjectMeta(
                name="Query",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
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
                                type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
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
                            TypeMeta.ArgumentMeta(
                                name="orderBy",
                                description="",
                                type=TypeRef.Named(name="Swap_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="pair",
                        description="",
                        args=[
                            TypeMeta.ArgumentMeta(
                                name="id",
                                description="",
                                type=TypeRef.non_null("ID", kind="SCALAR"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.Named(name="Pair", kind="OBJECT"),
                    ),
                ],
            ),
            "Swap": TypeMeta.ObjectMeta(
                name="Swap",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount0In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount0Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount1In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="amount1Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                ],
            ),
            "Token": TypeMeta.ObjectMeta(
                name="Token",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="name",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="symbol",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="decimals",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Int", kind="SCALAR"),
                    ),
                ],
            ),
            "Token_filter": TypeMeta.InputObjectMeta(
                name="Token_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="symbol_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
            "Pair": TypeMeta.ObjectMeta(
                name="Pair",
                description="",
                fields=[
                    TypeMeta.FieldMeta(
                        name="id",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="token0",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="token1",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                    TypeMeta.FieldMeta(
                        name="reserveUSD",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="priceToken0",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="priceToken1",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="createdAtTimestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                    TypeMeta.FieldMeta(
                        name="tokenString",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                ],
            ),
            "Pair_filter": TypeMeta.InputObjectMeta(
                name="Pair_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token0",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token0_",
                        description="",
                        type=TypeRef.Named(name="Token_filter", kind="OBJECT"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token1",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="token1_",
                        description="",
                        type=TypeRef.Named(name="Token_filter", kind="OBJECT"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="reserveUSD_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="reserveUSD_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken0_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken0_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken1_lt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="priceToken1_gt",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="createdAtTimestamp_lt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="createdAtTimestamp_gt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
            "Pair_orderBy": TypeMeta.EnumMeta(
                name="Pair_orderBy",
                description="",
                enumValues=[
                    TypeMeta.EnumValueMeta(name="id", description=""),
                    TypeMeta.EnumValueMeta(name="reserveUSD", description=""),
                    TypeMeta.EnumValueMeta(name="createdAtTimestamp", description=""),
                    TypeMeta.EnumValueMeta(name="token0__symbol", description=""),
                    TypeMeta.EnumValueMeta(name="token1__symbol", description=""),
                ],
            ),
            "Swap_filter": TypeMeta.InputObjectMeta(
                name="Swap_filter",
                description="",
                inputFields=[
                    TypeMeta.ArgumentMeta(
                        name="id",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_gt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="id_lt",
                        description="",
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp_gt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="timestamp_lt",
                        description="",
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount0In",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount0Out",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount1In",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                    TypeMeta.ArgumentMeta(
                        name="amount1Out",
                        description="",
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                        defaultValue=None,
                    ),
                ],
            ),
        },
    )

    Pair = subgraph.Pair
    Pair.tokenString = (
        Pair.token0.symbol
        + "_"
        + Pair.token0.id
        + "_"
        + Pair.token1.symbol
        + "_"
        + Pair.token1.id
    )

    assert subgraph._schema == expected


def test_object(subgraph: Subgraph):
    object_ = TypeMeta.ObjectMeta(
        name="Pair",
        description="",
        fields=[
            TypeMeta.FieldMeta(
                name="id",
                description="",
                args=[],
                type=TypeRef.Named(name="String", kind="SCALAR"),
            ),
            TypeMeta.FieldMeta(
                name="token0",
                description="",
                args=[],
                type=TypeRef.Named(name="Token", kind="OBJECT"),
            ),
            TypeMeta.FieldMeta(
                name="token1",
                description="",
                args=[],
                type=TypeRef.Named(name="Token", kind="OBJECT"),
            ),
            TypeMeta.FieldMeta(
                name="reserveUSD",
                description="",
                args=[],
                type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
            ),
            TypeMeta.FieldMeta(
                name="priceToken0",
                description="",
                args=[],
                type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
            ),
            TypeMeta.FieldMeta(
                name="priceToken1",
                description="",
                args=[],
                type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
            ),
            TypeMeta.FieldMeta(
                name="createdAtTimestamp",
                description="",
                args=[],
                type=TypeRef.Named(name="BigInt", kind="SCALAR"),
            ),
        ],
    )

    expected = Object(subgraph, object_)

    Pair = subgraph.Pair

    with fieldpath_test_mode():
        assert Pair == expected


def test_field_path_1(subgraph: Subgraph):
    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Pair", kind="OBJECT"),
        TypeRef.Named(name="Token", kind="OBJECT"),
        [
            (
                None,
                TypeMeta.FieldMeta(
                    name="token0",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                ),
            ),
        ],
    )

    fpath = subgraph.Pair.token0

    FieldPath.__test_mode = True
    assert fpath == expected


def test_field_path_2(subgraph: Subgraph):
    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Pair", kind="OBJECT"),
        TypeRef.Named(name="String", kind="SCALAR"),
        [
            (
                None,
                TypeMeta.FieldMeta(
                    name="id",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="String", kind="SCALAR"),
                ),
            ),
        ],
    )

    fpath = subgraph.Pair.id

    with fieldpath_test_mode():
        assert fpath == expected


def test_field_path_3(subgraph: Subgraph):
    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Pair", kind="OBJECT"),
        TypeRef.Named(name="String", kind="SCALAR"),
        [
            (
                None,
                TypeMeta.FieldMeta(
                    name="token0",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                ),
            ),
            (
                None,
                TypeMeta.FieldMeta(
                    name="id",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="String", kind="SCALAR"),
                ),
            ),
        ],
    )

    fpath = subgraph.Pair.token0.id

    with fieldpath_test_mode():
        assert fpath == expected


def test_synthetic_field_path_1(subgraph: Subgraph):
    sfield = SyntheticField(identity, SyntheticField.FLOAT, subgraph.Pair.reserveUSD)

    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Pair", kind="OBJECT"),
        TypeRef.Named(name="Float", kind="SCALAR"),
        [
            (
                None,
                TypeMeta.FieldMeta(
                    name="reserveCAD",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Float", kind="SCALAR"),
                ),
            ),
        ],
    )

    subgraph.Pair.reserveCAD = sfield
    fpath = subgraph.Pair.reserveCAD

    with fieldpath_test_mode():
        assert fpath == expected


def test_synthetic_field_path_2(subgraph: Subgraph):
    sfield = SyntheticField(identity, SyntheticField.STRING, [], "Le Token")

    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Token", kind="OBJECT"),
        TypeRef.Named(name="String", kind="SCALAR"),
        [
            (
                None,
                TypeMeta.FieldMeta(
                    name="token0",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                ),
            ),
            (
                None,
                TypeMeta.FieldMeta(
                    name="frenchName",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="String", kind="SCALAR"),
                ),
            ),
        ],
    )

    subgraph.Token.frenchName = sfield
    fpath = subgraph.Pair.token0.frenchName

    with fieldpath_test_mode():
        assert fpath == expected


def test_synthetic_field_path_3(subgraph: Subgraph):
    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Pair", kind="OBJECT"),
        TypeRef.Named(name="String", kind="SCALAR"),
        [
            (
                None,
                TypeMeta.FieldMeta(
                    name="token0Id",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="String", kind="SCALAR"),
                ),
            )
        ],
    )

    subgraph.Pair.token0Id = subgraph.Pair.token0.id
    fpath = subgraph.Pair.token0Id

    with fieldpath_test_mode():
        assert fpath == expected


def test_filter_1(subgraph: Subgraph):
    expected = Filter(
        TypeMeta.FieldMeta(
            name="reserveUSD",
            description="",
            args=[],
            type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
        ),
        Filter.Operator.GT,
        100,
    )

    filter_ = subgraph.Pair.reserveUSD > 100

    with fieldpath_test_mode():
        assert filter_ == expected


def test_field_path_args_1(subgraph: Subgraph):
    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Query", kind="OBJECT"),
        TypeRef.non_null_list("Pair", kind="OBJECT"),
        [
            (
                {
                    "first": 100,
                    "where": {"reserveUSD_lt": 10},
                    "orderBy": "reserveUSD",
                    "orderDirection": "desc",
                },
                TypeMeta.FieldMeta(
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
                            type=TypeRef.Named(name="Pair_filter", kind="INPUT_OBJECT"),
                            defaultValue=None,
                        ),
                        TypeMeta.ArgumentMeta(
                            name="orderBy",
                            description="",
                            type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                            defaultValue=None,
                        ),
                        TypeMeta.ArgumentMeta(
                            name="orderDirection",
                            description="",
                            type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                            defaultValue=None,
                        ),
                    ],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
            ),
        ],
    )

    fpath = subgraph.Query.pairs(
        first=100,
        where={"reserveUSD_lt": 10},
        orderBy="reserveUSD",
        orderDirection="desc",
    )

    with fieldpath_test_mode():
        assert fpath == expected


def test_field_path_args_2(subgraph: Subgraph):
    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Query", kind="OBJECT"),
        TypeRef.non_null_list("Pair", kind="OBJECT"),
        [
            (
                {"first": 100, "where": {"reserveUSD_gt": 100.0, "token0": "abcd"}},
                TypeMeta.FieldMeta(
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
                            type=TypeRef.Named(name="Pair_filter", kind="INPUT_OBJECT"),
                            defaultValue=None,
                        ),
                        TypeMeta.ArgumentMeta(
                            name="orderBy",
                            description="",
                            type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                            defaultValue=None,
                        ),
                        TypeMeta.ArgumentMeta(
                            name="orderDirection",
                            description="",
                            type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                            defaultValue=None,
                        ),
                    ],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
            )
        ],
    )

    fpath = subgraph.Query.pairs(
        first=100,
        where=[subgraph.Pair.reserveUSD > 100, subgraph.Pair.token0 == "abcd"],
    )

    with fieldpath_test_mode():
        assert fpath == expected


def test_field_path_args_3(subgraph: Subgraph):
    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Query", kind="OBJECT"),
        TypeRef.non_null_list("Pair", kind="OBJECT"),
        [
            (
                {"first": 100, "orderBy": "reserveUSD"},
                TypeMeta.FieldMeta(
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
                            type=TypeRef.Named(name="Pair_filter", kind="INPUT_OBJECT"),
                            defaultValue=None,
                        ),
                        TypeMeta.ArgumentMeta(
                            name="orderBy",
                            description="",
                            type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                            defaultValue=None,
                        ),
                        TypeMeta.ArgumentMeta(
                            name="orderDirection",
                            description="",
                            type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                            defaultValue=None,
                        ),
                    ],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
            ),
        ],
    )

    fpath = subgraph.Query.pairs(first=100, orderBy=subgraph.Pair.reserveUSD)

    with fieldpath_test_mode():
        assert fpath == expected


def test_field_path_extend_1(subgraph: Subgraph):
    expected = FieldPath(
        subgraph,
        TypeRef.Named(name="Query", kind="OBJECT"),
        TypeRef.Named(name="String", kind="SCALAR"),
        [
            (
                None,
                TypeMeta.FieldMeta(
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
                            type=TypeRef.Named(name="Pair_filter", kind="INPUT_OBJECT"),
                            defaultValue=None,
                        ),
                        TypeMeta.ArgumentMeta(
                            name="orderBy",
                            description="",
                            type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                            defaultValue=None,
                        ),
                        TypeMeta.ArgumentMeta(
                            name="orderDirection",
                            description="",
                            type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                            defaultValue=None,
                        ),
                    ],
                    type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                ),
            ),
            (
                None,
                TypeMeta.FieldMeta(
                    name="token0",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="Token", kind="OBJECT"),
                ),
            ),
            (
                None,
                TypeMeta.FieldMeta(
                    name="symbol",
                    description="",
                    args=[],
                    type=TypeRef.Named(name="String", kind="SCALAR"),
                ),
            ),
        ],
    )

    fpath1 = subgraph.Query.pairs
    fpath2 = subgraph.Pair.token0.symbol

    fpath = FieldPath._extend(fpath1, fpath2)

    with fieldpath_test_mode():
        assert fpath == expected


def test_mk_request_1(subgraph: Subgraph):
    expected = DataRequest.single_query(
        subgraph._url,
        Query(
            selection=[
                Selection(
                    TypeMeta.FieldMeta(
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
                                type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    alias="x7ecb1bc5fd9e0dcf",
                    arguments=[Argument("first", InputValue.Int(10))],
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
                                        name="symbol",
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
                )
            ]
        ),
    )

    sg = Subgrounds()

    pairs = subgraph.Query.pairs(first=10)

    req = sg.mk_request([pairs.id, pairs.token0.symbol])

    with fieldpath_test_mode():
        assert req == expected


def test_mk_request_2(sg: Subgrounds, subgraph: Subgraph):
    expected = DataRequest.single_query(
        subgraph._url,
        Query(
            selection=[
                Selection(
                    TypeMeta.FieldMeta(
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
                                type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Pair", kind="OBJECT"),
                    ),
                    alias="x7ecb1bc5fd9e0dcf",
                    arguments=[Argument("first", InputValue.Int(10))],
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
                                name="token0Id",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="String", kind="SCALAR"),
                            )
                        ),
                    ],
                )
            ]
        ),
    )

    Pair = subgraph.Pair
    Pair.token0Id = Pair.token0.id

    pairs = subgraph.Query.pairs(first=10)

    req = sg.mk_request([pairs.id, pairs.token0Id])

    with fieldpath_test_mode():
        assert req == expected


def test_mk_request_3(sg: Subgrounds, subgraph: Subgraph):
    expected = DataRequest.single_query(
        subgraph._url,
        Query(
            selection=[
                Selection(
                    TypeMeta.FieldMeta(
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
                            TypeMeta.ArgumentMeta(
                                name="orderBy",
                                description="",
                                type=TypeRef.Named(name="Swap_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    selection=[
                        Selection(
                            TypeMeta.FieldMeta(
                                name="timestamp",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                            )
                        ),
                        Selection(
                            TypeMeta.FieldMeta(
                                name="price",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Float", kind="SCALAR"),
                            )
                        ),
                    ],
                )
            ]
        ),
    )

    Swap = subgraph.Swap
    Swap.price = abs(Swap.amount0In - Swap.amount0Out) / abs(
        Swap.amount1In - Swap.amount1Out
    )

    req = sg.mk_request([subgraph.Query.swaps.timestamp, subgraph.Query.swaps.price])

    with fieldpath_test_mode():
        assert req == expected


def test_mk_request_4(sg: Subgrounds, subgraph: Subgraph):
    expected = DataRequest.single_query(
        subgraph._url,
        Query(
            selection=[
                Selection(
                    TypeMeta.FieldMeta(
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
                            TypeMeta.ArgumentMeta(
                                name="orderBy",
                                description="",
                                type=TypeRef.Named(name="Swap_orderBy", kind="ENUM"),
                                defaultValue=None,
                            ),
                            TypeMeta.ArgumentMeta(
                                name="orderDirection",
                                description="",
                                type=TypeRef.Named(name="OrderDirection", kind="ENUM"),
                                defaultValue=None,
                            ),
                        ],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                    selection=[
                        Selection(
                            TypeMeta.FieldMeta(
                                name="timestamp",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                            )
                        ),
                        Selection(
                            TypeMeta.FieldMeta(
                                name="my_value",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="Float", kind="SCALAR"),
                            )
                        ),
                    ],
                )
            ]
        ),
    )

    Swap = subgraph.Swap
    Swap.my_value = Swap.amount0In / 10**Swap.amount0Out

    req = sg.mk_request([subgraph.Query.swaps.timestamp, subgraph.Query.swaps.my_value])

    with fieldpath_test_mode():
        assert req == expected


def test_synthetic_field_1(subgraph: Subgraph):
    Swap = subgraph.Swap

    expected_deps = [Swap.amount0In]

    sfield = SyntheticField(lambda x: x * 10, SyntheticField.INT, Swap.amount0In)

    assert sfield._f(1) == 10
    assert sfield._deps == expected_deps


def test_synthetic_field_2(subgraph: Subgraph):
    Swap = subgraph.Swap

    expected_deps = [Swap.amount0In, Swap.amount0Out]

    sfield: SyntheticField = Swap.amount0In - Swap.amount0Out

    assert sfield._f(10, 0) == 10
    assert sfield._deps == expected_deps


def test_synthetic_field_3(subgraph: Subgraph):
    Swap = subgraph.Swap

    expected_deps = [
        Swap.amount0In,
        Swap.amount0Out,
        Swap.amount1In,
    ]

    sfield: SyntheticField = Swap.amount0In - Swap.amount0Out + Swap.amount1In

    assert sfield._f(10, 0, 4) == 14
    assert sfield._deps == expected_deps


def test_synthetic_field_5(subgraph: Subgraph):
    Swap = subgraph.Swap

    expected_deps = [Swap.amount0In, Swap.amount0Out, Swap.amount1In, Swap.amount1Out]

    sfield: SyntheticField = abs(Swap.amount0In - Swap.amount0Out) / abs(
        Swap.amount1In - Swap.amount1Out
    )

    assert sfield._f(10, 0, 0, 20) == 0.5
    assert sfield._deps == expected_deps


def test_synthetic_field_6(subgraph: Subgraph):
    Swap = subgraph.Swap

    expected_deps = [
        Swap.amount0In,
        Swap.amount0Out,
    ]

    sfield: SyntheticField = Swap.amount0In / 10**Swap.amount0Out

    assert sfield._f(100, 2) == 1
    assert sfield._deps == expected_deps
