# import unittest


from subgrounds.query import (
    Argument,
    DataRequest,
    Document,
    InputValue,
    Query,
    Selection,
)
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgrounds import Subgrounds

# from tests.conftest import *


def test_mk_request_1(subgraph):
    expected = DataRequest.single_query(
        "www.abc.xyz/graphql",
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
                                name="reserveUSD",
                                description="",
                                args=[],
                                type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                            )
                        ),
                    ],
                )
            ]
        ),
    )

    app = Subgrounds()

    pairs = subgraph.Query.pairs(first=10)

    req = app.mk_request([pairs.id, pairs.reserveUSD])

    assert req == expected


def test_mk_request_2(subgraph, subgraph_diff_url):
    expected = DataRequest(
        documents=[
            Document(
                "www.abc.xyz/graphql",
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
                            alias="x7ecb1bc5fd9e0dcf",
                            arguments=[Argument("first", InputValue.Int(10))],
                            selection=[
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="reserveUSD",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ]
                ),
            ),
            Document(
                "www.foo.xyz/graphql",
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
                            alias="x7ecb1bc5fd9e0dcf",
                            arguments=[Argument("first", InputValue.Int(10))],
                            selection=[
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    TypeMeta.FieldMeta(
                                        name="token0Id",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="String", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ]
                ),
            ),
        ]
    )

    pairs = subgraph.Query.pairs(first=10)

    Pair = subgraph_diff_url.Pair
    Pair.token0Id = Pair.token0.id

    app = Subgrounds()

    req = app.mk_request(
        [
            pairs.id,
            pairs.reserveUSD,
            *subgraph_diff_url.Query.pairs(
                first=10,
                selection=[subgraph_diff_url.Pair.id, subgraph_diff_url.Pair.token0Id],
            ),
        ]
    )

    assert req == expected
