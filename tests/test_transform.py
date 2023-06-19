from typing import Any, Callable

import pytest

from subgrounds.query import DataRequest, Document, DocumentResponse, Query, Selection
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.subgraph import Object, Subgraph
from subgrounds.subgraph.fieldpath import FieldPath, SyntheticField
from subgrounds.subgrounds import Subgrounds
from subgrounds.transform import DocumentTransform, LocalSyntheticField, TypeTransform


@pytest.fixture
def datarequest(subgraph: Subgraph):
    return DataRequest(
        documents=[
            Document(
                url=subgraph._url,
                query=Query(
                    None,
                    [
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
                                    TypeMeta.ArgumentMeta(
                                        name="orderBy",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Swap_orderBy", kind="ENUM"
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
                                type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                            ),
                            alias=None,
                            arguments=[],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="amount0In",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="amount0Out",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="amount1In",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="amount1Out",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                ),
            )
        ]
    )


@pytest.fixture
def response():
    return DocumentResponse(
        url="www.abc.xyz/graphql",
        data={
            "swaps": [
                {
                    "amount0In": "0.25",
                    "amount0Out": "0.0",
                    "amount1In": "0.0",
                    "amount1Out": "89820.904371079570860909",
                    "id": "0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0",
                    "timestamp": "1638554699",
                }
            ]
        },
    )


@pytest.mark.parametrize(
    "transforms, expected",
    [
        (
            [
                TypeTransform(
                    TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    lambda bigdecimal: float(bigdecimal),
                )
            ],
            [
                {
                    "swaps": [
                        {
                            "amount0In": 0.25,
                            "amount0Out": 0.0,
                            "amount1In": 0.0,
                            "amount1Out": 89820.904371079570860909,
                            "id": "0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0",
                            "timestamp": "1638554699",
                        }
                    ]
                }
            ],
        ),
    ],
)
def test_typetransform_roundtrip(
    mocker,
    datarequest: DataRequest,
    response: DocumentResponse,
    subgraph: Subgraph,
    transforms: list[DocumentTransform],
    expected: list[dict[str, Any]],
) -> None:
    mocker.patch("subgrounds.client.query", return_value=response)

    subgraph._transforms = transforms
    sg = Subgrounds(global_transforms=[], subgraphs={subgraph._url: subgraph})

    data = sg.execute(datarequest)

    assert data == expected


def test_localsyntheticfield_literal_roundtrip1(
    mocker,
    response: list[dict[str, Any]],
    subgraph: Subgraph,
):
    mocker.patch("subgrounds.client.query", return_value=response)

    expected = [
        {
            "swaps": [
                {
                    "amount0In": 0.25,
                    "amount0Out": 0.0,
                    "amount1In": 0.0,
                    "amount1Out": 89820.904371079570860909,
                    "price0": 359283.61748431827,
                    "id": "0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0",
                    "timestamp": "1638554699",
                }
            ]
        }
    ]

    datarequest = DataRequest(
        documents=[
            Document(
                url=subgraph._url,
                query=Query(
                    None,
                    [
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
                                    TypeMeta.ArgumentMeta(
                                        name="orderBy",
                                        description="",
                                        type=TypeRef.Named(
                                            name="Swap_orderBy", kind="ENUM"
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
                                type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                            ),
                            alias=None,
                            arguments=[],
                            selection=[
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="amount0In",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="amount0Out",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="amount1In",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="amount1Out",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                                Selection(
                                    fmeta=TypeMeta.FieldMeta(
                                        name="price0",
                                        description="",
                                        args=[],
                                        type=TypeRef.Named(
                                            name="BigDecimal", kind="SCALAR"
                                        ),
                                    )
                                ),
                            ],
                        )
                    ],
                ),
            )
        ]
    )

    subgraph_transforms = [
        LocalSyntheticField(
            subgraph=subgraph,
            fmeta=TypeMeta.FieldMeta(
                name="price0", description="", args=[], type=TypeRef.non_null("Float")
            ),
            type_=TypeMeta.ObjectMeta(
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
            f=lambda in0, out0, in1, out1: abs(in1 - out1) / abs(in0 - out0),
            default=0.0,
            args=[
                Selection(
                    TypeMeta.FieldMeta(
                        name="amount0In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    )
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="amount0Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    )
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="amount1In",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    )
                ),
                Selection(
                    TypeMeta.FieldMeta(
                        name="amount1Out",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    )
                ),
            ],
        ),
        TypeTransform(
            TypeRef.Named(name="BigDecimal", kind="SCALAR"),
            lambda bigdecimal: float(bigdecimal),
        ),
    ]

    subgraph._transforms = subgraph_transforms
    sg = Subgrounds(global_transforms=[], subgraphs={subgraph._url: subgraph})
    data = sg.execute(datarequest)

    assert data == expected


@pytest.mark.parametrize(
    ["response", "expected", "synthfield_f", "object_f", "fpaths_f"],
    [
        (
            DocumentResponse(
                url="www.abc.xyz/graphql",
                data={
                    "swaps": [
                        {
                            "amount0In": "0.25",
                            "amount0Out": "0.0",
                            "amount1In": "0.0",
                            "amount1Out": "89820.904371079570860909",
                            "id": "0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0",
                            "timestamp": "1638554699",
                        }
                    ]
                },
            ),
            [
                {
                    "swaps": [
                        {
                            "amount0In": 0.25,
                            "amount0Out": 0.0,
                            "amount1In": 0.0,
                            "amount1Out": 89820.904371079570860909,
                            "synthfield": 359283.61748431827,
                            "id": "0xf457e61e2aa310c8a7f01570bf96f24323fc317925c42f2a33d2061e1944df4d-0",
                            "timestamp": "1638554699",
                        }
                    ]
                }
            ],
            lambda subgraph: abs(subgraph.Swap.amount1In - subgraph.Swap.amount1Out)
            / abs(subgraph.Swap.amount0In - subgraph.Swap.amount0Out),
            lambda subgraph: subgraph.Swap,
            lambda subgraph: [subgraph.Query.swaps.synthfield],
        ),
        (
            DocumentResponse(
                url="www.abc.xyz/graphql", data={"xa6436bbcebab2a86": None}
            ),
            [{"xa6436bbcebab2a86": {"id": None, "synthfield": "FOO"}}],
            lambda _: SyntheticField.constant("FOO"),
            lambda subgraph: subgraph.Pair,
            lambda subgraph: [
                subgraph.Query.pair(id="ABC").id,
                subgraph.Query.pair(id="ABC").synthfield,
            ],
        ),
    ],
)
def test_localsyntheticfield_toplevel_roundtrip(
    mocker,
    response: list[dict[str, Any]],
    expected: list[dict[str, Any]],
    synthfield_f: Callable[[Subgraph], SyntheticField],
    object_f: Callable[[Subgraph], Object],
    fpaths_f: Callable[[Subgraph], list[FieldPath]],
    subgraph: Subgraph,
):
    mocker.patch("subgrounds.client.query", return_value=response)

    sg = Subgrounds(global_transforms=[], subgraphs={subgraph._url: subgraph})

    object_f(subgraph).synthfield = synthfield_f(subgraph)

    req = sg.mk_request(fpaths_f(subgraph))

    data = sg.execute(req)

    assert data == expected
