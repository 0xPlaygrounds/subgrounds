from pprint import pprint
from random import randint
from typing import Any, Optional, Tuple, Type

import pytest

from subgrounds.pagination.pagination import PaginationStrategy
from subgrounds.pagination.preprocess import PaginationNode
from subgrounds.pagination.strategies import (
    LegacyStrategyArgGenerator,
    ShallowStrategyArgGenerator,
    StopPagination,
)
from subgrounds.schema import TypeRef


def generate_swaps(pair_id, n):
    for i in range(n):
        yield {"id": f"swap_{pair_id}{i}", "timestamp": i}


def generate_users(pair_id, n):
    for i in range(n):
        yield {"id": f"user_{pair_id}{i}", "volume": i}


def generate_pairs(n):
    for i in range(n):
        yield {
            "id": f"pair{i}",
            "swaps": list(generate_swaps(f"pair{i}", randint(0, 100))),
        }


def __test_args(
    strategy: PaginationStrategy,
    expected: list[dict[str, Any]],
    data_and_exception: list[Tuple[dict[str, Any], Optional[Type]]],
):
    args_ = strategy.step()
    for (args, (data, exn)) in zip(expected, data_and_exception):
        assert args_ == args

        if exn is not None:
            with pytest.raises(exn):
                strategy.step(data)
        else:
            args_ = strategy.step(data)


@pytest.mark.parametrize(
    ["page_nodes", "data_and_exception", "expected"],
    [
        # Test cursor, 1 pagination node, no args, 2 pages
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1100,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 900))}, None),
                ({"swaps": []}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 0},
                {"first0": 200, "skip0": 0, "lastOrderingValue0": "swap_a899"},
            ],
        ),
        # Test cursor, 1 pagination node, no args, 1 page
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1100,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 10))}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 0},
            ],
        ),
        # Test cursor, 1 pagination node, no args, 1 page below limit
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=100,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 100))}, StopPagination),
            ],
            [
                {"first0": 100, "skip0": 0},
            ],
        ),
        # Test cursor, nested pagination nodes, no args
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=4,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["pairs"],
                    inner=[
                        PaginationNode(
                            node_idx=1,
                            filter_field="timestamp",
                            first_value=7000,
                            skip_value=0,
                            filter_value=None,
                            filter_value_type=TypeRef.Named(
                                name="BigInt", kind="SCALAR"
                            ),
                            key_path=["pairs", "swaps"],
                            inner=[],
                        )
                    ],
                )
            ],
            [
                (
                    {"pairs": [{"id": "a", "swaps": list(generate_swaps("a", 900))}]},
                    None,
                ),
                (
                    {"pairs": [{"id": "a", "swaps": list(generate_swaps("a", 100))}]},
                    None,
                ),
                (
                    {"pairs": [{"id": "b", "swaps": list(generate_swaps("b", 100))}]},
                    None,
                ),
                (
                    {"pairs": [{"id": "c", "swaps": list(generate_swaps("c", 10))}]},
                    None,
                ),
                (
                    {"pairs": [{"id": "d", "swaps": list(generate_swaps("d", 0))}]},
                    StopPagination,
                ),
            ],
            [
                {"first0": 1, "skip0": 0, "first1": 900, "skip1": 0},
                {
                    "first0": 1,
                    "skip0": 0,
                    "first1": 900,
                    "skip1": 0,
                    "lastOrderingValue1": 899,
                },
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "a",
                    "first1": 900,
                    "skip1": 0,
                },
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "b",
                    "first1": 900,
                    "skip1": 0,
                },
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "c",
                    "first1": 900,
                    "skip1": 0,
                },
            ],
        ),
        # Test cursor, nested pagination nodes with neighbors, no args
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=4,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["pairs"],
                    inner=[
                        PaginationNode(
                            node_idx=1,
                            filter_field="timestamp",
                            first_value=7000,
                            skip_value=0,
                            filter_value=None,
                            filter_value_type=TypeRef.Named(
                                name="BigInt", kind="SCALAR"
                            ),
                            key_path=["pairs", "swaps"],
                            inner=[],
                        ),
                        PaginationNode(
                            node_idx=2,
                            filter_field="volume",
                            first_value=10,
                            skip_value=0,
                            filter_value=None,
                            filter_value_type=TypeRef.Named(
                                name="BigInt", kind="SCALAR"
                            ),
                            key_path=["pairs", "users"],
                            inner=[],
                        ),
                    ],
                )
            ],
            [
                (
                    {"pairs": [{"id": "a", "swaps": list(generate_swaps("a", 900))}]},
                    None,
                ),
                (
                    {"pairs": [{"id": "a", "swaps": list(generate_swaps("a", 100))}]},
                    None,
                ),
                ({"pairs": [{"id": "a", "users": list(generate_users("a", 9))}]}, None),
                (
                    {"pairs": [{"id": "b", "swaps": list(generate_swaps("b", 100))}]},
                    None,
                ),
                ({"pairs": [{"id": "b", "users": list(generate_users("b", 9))}]}, None),
                (
                    {"pairs": [{"id": "c", "swaps": list(generate_swaps("c", 10))}]},
                    None,
                ),
                (
                    {"pairs": [{"id": "c", "users": list(generate_users("c", 10))}]},
                    None,
                ),
                ({"pairs": [{"id": "d", "swaps": list(generate_swaps("d", 0))}]}, None),
                (
                    {"pairs": [{"id": "d", "users": list(generate_users("d", 5))}]},
                    StopPagination,
                ),
            ],
            [
                {"first0": 1, "skip0": 0, "first1": 900, "skip1": 0},
                {
                    "first0": 1,
                    "skip0": 0,
                    "first1": 900,
                    "skip1": 0,
                    "lastOrderingValue1": 899,
                },
                {"first0": 1, "skip0": 0, "first2": 10, "skip2": 0},
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "a",
                    "first1": 900,
                    "skip1": 0,
                },
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "a",
                    "first2": 10,
                    "skip2": 0,
                },
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "b",
                    "first1": 900,
                    "skip1": 0,
                },
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "b",
                    "first2": 10,
                    "skip2": 0,
                },
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "c",
                    "first1": 900,
                    "skip1": 0,
                },
                {
                    "first0": 1,
                    "skip0": 0,
                    "lastOrderingValue0": "c",
                    "first2": 10,
                    "skip2": 0,
                },
            ],
        ),
        # Test cursor, 1 pagination node, `skip` argument specified
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1500,
                    skip_value=10,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 900))}, None),
                ({"swaps": list(generate_swaps("a", 500))}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 10},
                {"first0": 600, "skip0": 0, "lastOrderingValue0": "swap_a899"},
            ],
        ),
        # Test cursor, 1 pagination node, `where` argument specified
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1500,
                    skip_value=0,
                    filter_value="0",
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 900))}, None),
                ({"swaps": list(generate_swaps("a", 500))}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 0, "lastOrderingValue0": "0"},
                {"first0": 600, "skip0": 0, "lastOrderingValue0": "swap_a899"},
            ],
        ),
    ],
)
def test_legacy_strategy(
    page_nodes: list[PaginationNode],
    data_and_exception: list[Tuple[dict[str, Any], Optional[Exception]]],
    expected: list[dict[str, Any]],
):
    strategy = LegacyStrategyArgGenerator(page_nodes)
    __test_args(strategy, expected, data_and_exception)


@pytest.mark.parametrize(
    ["page_nodes", "data_and_exception", "expected"],
    [
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1100,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 900))}, None),
                ({"swaps": []}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 0},
                {"first0": 200, "skip0": 0, "lastOrderingValue0": "swap_a899"},
            ],
        ),
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1100,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 10))}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 0},
            ],
        ),
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=100,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 100))}, StopPagination),
            ],
            [{"first0": 100, "skip0": 0}],
        ),
        # Test cursor, nested pagination nodes, no args
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1000,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["pairs"],
                    inner=[
                        PaginationNode(
                            node_idx=1,
                            filter_field="timestamp",
                            first_value=7000,
                            skip_value=0,
                            filter_value=None,
                            filter_value_type=TypeRef.Named(
                                name="BigInt", kind="SCALAR"
                            ),
                            key_path=["pairs", "swaps"],
                            inner=[],
                        )
                    ],
                )
            ],
            [
                ({"pairs": list(generate_pairs(900))}, None),
                ({"pairs": list(generate_pairs(100))}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 0, "first1": 900, "skip1": 0},
                {
                    "first0": 100,
                    "skip0": 0,
                    "lastOrderingValue0": "pair899",
                    "first1": 900,
                    "skip1": 0,
                },
            ],
        ),
        # Test cursor, nested pagination nodes with neighbors, no args
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=4,
                    skip_value=0,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["pairs"],
                    inner=[
                        PaginationNode(
                            node_idx=1,
                            filter_field="timestamp",
                            first_value=7000,
                            skip_value=0,
                            filter_value=None,
                            filter_value_type=TypeRef.Named(
                                name="BigInt", kind="SCALAR"
                            ),
                            key_path=["pairs", "swaps"],
                            inner=[],
                        ),
                        PaginationNode(
                            node_idx=2,
                            filter_field="volume",
                            first_value=10,
                            skip_value=0,
                            filter_value=None,
                            filter_value_type=TypeRef.Named(
                                name="BigInt", kind="SCALAR"
                            ),
                            key_path=["pairs", "users"],
                            inner=[],
                        ),
                    ],
                )
            ],
            [
                (
                    {
                        "pairs": [
                            {
                                "id": "a",
                                "swaps": list(generate_swaps("a", 900)),
                                "users": list(generate_users("a", 9)),
                            },
                            {
                                "id": "b",
                                "swaps": list(generate_swaps("b", 900)),
                                "users": list(generate_users("b", 9)),
                            },
                            {
                                "id": "c",
                                "swaps": list(generate_swaps("c", 900)),
                                "users": list(generate_users("c", 9)),
                            },
                            {
                                "id": "d",
                                "swaps": list(generate_swaps("d", 900)),
                                "users": list(generate_users("d", 9)),
                            },
                        ]
                    },
                    StopPagination,
                )
            ],
            [
                {
                    "first0": 4,
                    "skip0": 0,
                    "first1": 900,
                    "skip1": 0,
                    "first2": 10,
                    "skip2": 0,
                },
            ],
        ),
        # Test cursor, 1 pagination node, `skip` argument specified
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1500,
                    skip_value=10,
                    filter_value=None,
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 900))}, None),
                ({"swaps": list(generate_swaps("a", 500))}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 10},
                {"first0": 600, "skip0": 0, "lastOrderingValue0": "swap_a899"},
            ],
        ),
        # Test cursor, 1 pagination node, `where` argument specified
        (
            [
                PaginationNode(
                    node_idx=0,
                    filter_field="id",
                    first_value=1500,
                    skip_value=0,
                    filter_value="0",
                    filter_value_type=TypeRef.Named(name="String", kind="SCALAR"),
                    key_path=["swaps"],
                    inner=[],
                )
            ],
            [
                ({"swaps": list(generate_swaps("a", 900))}, None),
                ({"swaps": list(generate_swaps("a", 600))}, StopPagination),
            ],
            [
                {"first0": 900, "skip0": 0, "lastOrderingValue0": "0"},
                {"first0": 600, "skip0": 0, "lastOrderingValue0": "swap_a899"},
            ],
        ),
    ],
)
def test_greedy_strategy(
    page_nodes: list[PaginationNode],
    data_and_exception: list[Tuple[dict[str, Any], Optional[Exception]]],
    expected: list[dict[str, Any]],
):
    strategy = ShallowStrategyArgGenerator(page_nodes)
    __test_args(strategy, expected, data_and_exception)
