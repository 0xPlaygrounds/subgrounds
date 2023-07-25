import random
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from pytest_mock import MockerFixture

from subgrounds import AsyncSubgrounds, Subgrounds
from subgrounds.client import SubgroundsBase
from subgrounds.pagination.strategies import PAGE_SIZE
from subgrounds.query import DataRequest, DataResponse, DocumentResponse
from subgrounds.schema import TypeRef
from subgrounds.subgraph import Subgraph
from subgrounds.transform import DocumentTransform, TypeTransform

SEED = 1

BASE_DATA_RAW = {
    "amount0In": "0.25",
    "amount0Out": "0.0",
    "amount1In": "0.0",
    "amount1Out": "89820.904371079570860909",
    "timestamp": "1638554699",
}

BASE_DATA = {
    "amount0In": 0.25,
    "amount0Out": 0.0,
    "amount1In": 0.0,
    "amount1Out": 89820.904371079570860909,
    "timestamp": "1638554699",
}


def make_datarequest0(sg: Subgrounds, subgraph: Subgraph) -> DataRequest:
    swaps = subgraph.Query.swaps  # type: ignore
    return sg.mk_request(
        [
            swaps.amount0In,
            swaps.amount0Out,
            swaps.amount1In,
            swaps.amount1Out,
            swaps.id,
            swaps.timestamp,
        ]
    )


def make_datarequest1(sg: Subgrounds, subgraph: Subgraph) -> DataRequest:
    swaps = subgraph.Query.swaps(first=PAGE_SIZE * 2)  # type: ignore
    return sg.mk_request(
        [
            swaps.amount0In,
            swaps.amount0Out,
            swaps.amount1In,
            swaps.amount1Out,
            swaps.id,
            swaps.timestamp,
        ]
    )


def make_datarequest2(sg: Subgrounds, subgraph: Subgraph) -> DataRequest:
    swaps = subgraph.Query.swaps  # type: ignore
    pairs = subgraph.Query.pairs  # type: ignore

    req1 = sg.mk_request(
        [
            swaps.amount0In,
            swaps.amount0Out,
            swaps.amount1In,
            swaps.amount1Out,
            swaps.id,
            swaps.timestamp,
        ]
    )
    req2 = sg.mk_request(
        [pairs.id, pairs.reserveUSD],
    )

    return req1.add_documents(req2.documents)


def make_fetchresponse0(*args, **kwargs) -> dict[str, Any]:
    return {"swaps": [{"id": hex(random.getrandbits(32))} | BASE_DATA_RAW]}


def make_fetchresponse1(*args, **kwargs) -> dict[str, Any]:
    return {
        "xa11d4bcf61e1567a": [
            {"id": hex(random.getrandbits(32))} | BASE_DATA_RAW
            for _ in range(PAGE_SIZE)
        ]
    }


def make_fetchresponse2(*args, **kwargs) -> dict[str, Any]:
    # I'm sorry about this
    match args[2]["query"].split("\n")[1].strip().split("(")[0]:
        case "swaps":
            return {"swaps": [{"id": hex(random.getrandbits(32))} | BASE_DATA_RAW]}
        case "pairs":
            return {"pairs": [{"id": hex(random.getrandbits(32)), "reserveUSD": 10}]}
        case _:
            assert False


def make_docexpected0():
    return DataResponse(
        responses=[
            DocumentResponse(
                url="www.abc.xyz/graphql",
                data={"swaps": [{"id": hex(random.getrandbits(32))} | BASE_DATA]},
            )
        ]
    )


def make_docexpected1():
    return DataResponse(
        responses=[
            DocumentResponse(
                url="www.abc.xyz/graphql",
                data={
                    "xa11d4bcf61e1567a": [
                        {"id": hex(random.getrandbits(32))} | BASE_DATA
                        for _ in range(PAGE_SIZE * 2)
                    ]
                },
            )
        ]
    )


def make_docexpected2():
    return DataResponse(
        responses=[
            DocumentResponse(
                url="www.abc.xyz/graphql",
                data={"swaps": [{"id": hex(random.getrandbits(32))} | BASE_DATA]},
            ),
            DocumentResponse(
                url="www.abc.xyz/graphql",
                data={"pairs": [{"id": hex(random.getrandbits(32)), "reserveUSD": 10}]},
            ),
        ]
    )


def make_iter_docexpected0():
    yield DocumentResponse(
        url="www.abc.xyz/graphql",
        data={"swaps": [{"id": hex(random.getrandbits(32))} | BASE_DATA]},
    )


def make_iter_docexpected1():
    yield DocumentResponse(
        url="www.abc.xyz/graphql",
        data={
            "xa11d4bcf61e1567a": [
                {"id": hex(random.getrandbits(32))} | BASE_DATA
                for _ in range(PAGE_SIZE)
            ]
        },
    )
    yield DocumentResponse(
        url="www.abc.xyz/graphql",
        data={
            "xa11d4bcf61e1567a": [
                {"id": hex(random.getrandbits(32))} | BASE_DATA
                for _ in range(PAGE_SIZE)
            ]
        },
    )


def make_iter_docexpected2():
    yield DocumentResponse(
        url="www.abc.xyz/graphql",
        data={"xa11d4bcf61e1567a": [{"id": hex(random.getrandbits(32))} | BASE_DATA]},
    ),
    yield DocumentResponse(
        url="www.abc.xyz/graphql",
        data={
            "xa11d4bcf61e1567a": [{"id": hex(random.getrandbits(32)), "reserveUSD": 10}]
        },
    ),


@pytest.mark.parametrize(
    ["response_f", "transforms", "datarequest_f", "expected_f"],
    [
        (
            make_fetchresponse0,
            [
                TypeTransform(
                    TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    lambda bigdecimal: float(bigdecimal),
                )
            ],
            make_datarequest0,
            make_docexpected0,
        ),
        (
            make_fetchresponse1,
            [
                TypeTransform(
                    TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    lambda bigdecimal: float(bigdecimal),
                )
            ],
            make_datarequest1,
            make_docexpected1,
        ),
        (
            make_fetchresponse2,
            [
                TypeTransform(
                    TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    lambda bigdecimal: float(bigdecimal),
                )
            ],
            make_datarequest2,
            make_docexpected2,
        ),
    ],
)
def test_execute_roundtrip(
    mocker: MockerFixture,
    datarequest_f: Callable[[Subgrounds, Subgraph], DataRequest],
    response_f: Callable[..., dict[str, Any]],
    subgraph: Subgraph,
    transforms: list[DocumentTransform],
    expected_f: Callable[..., DocumentResponse],
) -> None:
    random.seed(SEED)
    mocker.patch.object(Subgrounds, "_fetch", new_callable=lambda *args: response_f)

    subgraph._transforms = transforms
    sg = Subgrounds(global_transforms=[], subgraphs={subgraph._url: subgraph})

    req = datarequest_f(sg, subgraph)
    actual = sg.execute(req)

    random.seed(SEED)
    expected = expected_f()

    assert type(actual) is type(expected)

    assert actual == expected


@pytest.mark.parametrize(
    ["response_f", "transforms", "datarequest_f", "expected_f"],
    [
        (
            make_fetchresponse0,
            [
                TypeTransform(
                    TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    lambda bigdecimal: float(bigdecimal),
                )
            ],
            make_datarequest0,
            make_docexpected0,
        ),
        (
            make_fetchresponse1,
            [
                TypeTransform(
                    TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    lambda bigdecimal: float(bigdecimal),
                )
            ],
            make_datarequest1,
            make_docexpected1,
        ),
    ],
)
async def test_async_execute_roundtrip(
    mocker: MockerFixture,
    datarequest_f: Callable[[SubgroundsBase, Subgraph], DataRequest],
    response_f: Callable[..., dict[str, Any]],
    subgraph: Subgraph,
    transforms: list[DocumentTransform],
    expected_f: Callable[..., DocumentResponse],
) -> None:
    random.seed(SEED)

    def _lambda():  # honestly, I don't know why this is like this..
        async def inner(*args):
            return response_f()

        return inner

    mocker.patch.object(AsyncSubgrounds, "_fetch", new_callable=_lambda)

    subgraph._transforms = transforms
    sg = AsyncSubgrounds(global_transforms=[], subgraphs={subgraph._url: subgraph})

    req = datarequest_f(sg, subgraph)
    actual = await sg.execute(req)

    random.seed(SEED)
    expected = expected_f()

    assert type(actual) is type(expected)

    assert actual == expected


@pytest.mark.parametrize(
    ["response_f", "transforms", "datarequest_f", "expected_f"],
    [
        (
            make_fetchresponse0,
            [
                TypeTransform(
                    TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    lambda bigdecimal: float(bigdecimal),
                )
            ],
            make_datarequest0,
            make_iter_docexpected0,
        ),
        (
            make_fetchresponse1,
            [
                TypeTransform(
                    TypeRef.Named(name="BigDecimal", kind="SCALAR"),
                    lambda bigdecimal: float(bigdecimal),
                )
            ],
            make_datarequest1,
            make_iter_docexpected1,
        ),
        # (
        #     make_fetchresponse2,
        #     [
        #         TypeTransform(
        #             TypeRef.Named(name="BigDecimal", kind="SCALAR"),
        #             lambda bigdecimal: float(bigdecimal),
        #         )
        #     ],
        #     make_datarequest2,
        #     make_iter_docexpected2,
        # ),
    ],
)
def test_execute_iter_roundtrip(
    mocker,
    datarequest_f: Callable[[Subgrounds, Subgraph], DataRequest],
    response_f: Callable[..., dict[str, Any]],
    subgraph: Subgraph,
    transforms: list[DocumentTransform],
    expected_f: Callable[..., Iterator[DocumentResponse]],
) -> None:
    random.seed(SEED)

    mocker.patch.object(Subgrounds, "_fetch", new_callable=lambda *args: response_f)
    subgraph._transforms = transforms
    sg = Subgrounds(global_transforms=[], subgraphs={subgraph._url: subgraph})

    req = datarequest_f(sg, subgraph)
    actual = list(sg.execute_iter(req))

    random.seed(SEED)
    expected = list(expected_f())

    assert len(actual) == len(expected)

    for data, expected_data in zip(actual, expected):
        assert type(data) is type(expected_data)
        assert data == expected_data
