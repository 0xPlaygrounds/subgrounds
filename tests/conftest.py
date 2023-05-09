import json

import pytest

from subgrounds import Subgrounds
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef  # input_value_of_argument
from subgrounds.subgraph import FieldPath, Subgraph


@pytest.fixture
def pairs_fieldmeta():
    return TypeMeta.FieldMeta(
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
    )


@pytest.fixture
def pair_fieldmeta():
    return TypeMeta.FieldMeta(
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
    )


@pytest.fixture
def swaps_fieldmeta():
    return TypeMeta.FieldMeta(
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
                type=TypeRef.Named(name="Swap_filter", kind="INPUT_OBJECT"),
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
    )


@pytest.fixture
def swap_objectmeta():
    return TypeMeta.ObjectMeta(
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
    )


@pytest.fixture
def token_objectmeta():
    return TypeMeta.ObjectMeta(
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
    )


@pytest.fixture
def pair_objectmeta():
    return TypeMeta.ObjectMeta(
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


@pytest.fixture
def schema(
    pairs_fieldmeta,
    pair_fieldmeta,
    swaps_fieldmeta,
    swap_objectmeta,
    token_objectmeta,
    pair_objectmeta,
):
    return SchemaMeta(
        queryType={"name": "Query"},
        types=[],
        type_map={
            "ID": TypeMeta.ScalarMeta(name="ID", description=""),
            "Int": TypeMeta.ScalarMeta(name="Int", description=""),
            "Float": TypeMeta.ScalarMeta(name="Float", description=""),
            "BigInt": TypeMeta.ScalarMeta(name="BigInt", description=""),
            "BigDecimal": TypeMeta.ScalarMeta(name="BigDecimal", description=""),
            "String": TypeMeta.ScalarMeta(name="String", description=""),
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
                    pairs_fieldmeta,
                    swaps_fieldmeta,
                    pair_fieldmeta,
                ],
            ),
            "Swap": swap_objectmeta,
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
            "Token": token_objectmeta,
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
            "Pair": pair_objectmeta,
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
        },
    )


@pytest.fixture
def subgraph(schema):
    return Subgraph("www.abc.xyz/graphql", schema)


@pytest.fixture
def subgraph_diff_url(schema):
    return Subgraph("www.foo.xyz/graphql", schema)


@pytest.fixture
def sg(subgraph: Subgraph, subgraph_diff_url: Subgraph):
    return Subgrounds(
        subgraphs={subgraph._url: subgraph, subgraph_diff_url._url: subgraph_diff_url}
    )


@pytest.fixture
def klima_bridged_carbon_subgraph(mocker, sg: Subgrounds):
    with open("tests/schemas/cujowolf_polygon-bridged-carbon.json", "r") as f:
        schema = json.load(f)

    mocker.patch("subgrounds.client.get_schema", return_value=schema)
    return sg.load_subgraph(
        "https://api.thegraph.com/subgraphs/name/cujowolf/polygon-bridged-carbon"
    )


@pytest.fixture
def univ2_subgraph(mocker, sg: Subgrounds):
    with open("tests/schemas/uniswap_uniswap-v2.json", "r") as f:
        schema = json.load(f)

    mocker.patch("subgrounds.client.get_schema", return_value=schema)
    return sg.load_subgraph(
        "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
    )


@pytest.fixture
def univ3_subgraph(mocker, sg: Subgrounds):
    with open("tests/schemas/uniswap_uniswap-v3.json", "r") as f:
        schema = json.load(f)

    mocker.patch("subgrounds.client.get_schema", return_value=schema)
    return sg.load_subgraph(
        "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3"
    )


@pytest.fixture
def curve_subgraph(mocker, sg: Subgrounds):
    with open("tests/schemas/gvladika_curve.json", "r") as f:
        schema = json.load(f)

    mocker.patch("subgrounds.client.get_schema", return_value=schema)
    return sg.load_subgraph("https://api.thegraph.com/subgraphs/name/gvladika/curve")


def identity(x):
    return x


def fieldpath_test_mode(func):
    def wrapper(*args, **kwargs):
        FieldPath.__test_mode = True
        func(*args, **kwargs)

    return wrapper
