import operator as op
from dataclasses import dataclass

import pytest

from subgrounds.utils import (
    extract_data,
    flatten_dict,
    intersection,
    rel_complement,
    union,
)
from tests.conftest import identity


@dataclass
class Foo:
    id: int
    txt: str


@pytest.mark.parametrize(
    "l1, l2, key, expected",
    [
        ([1, 2, 3], [3, 4, 5], identity, [1, 2]),
        ([3, 4], [3, 4, 5], identity, []),
        (
            [Foo(1, "hello"), Foo(2, "bob"), Foo(6, "world!")],
            [Foo(5, "abcd"), Foo(6, "message")],
            op.attrgetter("id"),
            [Foo(1, "hello"), Foo(2, "bob")],
        ),
    ],
)
def test_rel_complement(l1, l2, key, expected):
    assert rel_complement(l1, l2, key=key) == expected


@pytest.mark.parametrize(
    "l1, l2, key, combine, expected",
    [
        ([1, 2, 3], [3, 4, 5], identity, lambda x, _: x, [3]),
        ([1, 2, 3], [3, 4, 5], identity, op.add, [6]),
        ([1, 2], [3, 4, 5], identity, lambda x, _: x, []),
        (
            [Foo(1, "hello"), Foo(2, "bob"), Foo(6, "world!")],
            [Foo(5, "abcd"), Foo(6, "message")],
            op.attrgetter("id"),
            lambda x, y: Foo(x.id, f"{x.txt} {y.txt}"),
            [Foo(6, "world! message")],
        ),
    ],
)
def test_intersection(l1, l2, key, combine, expected):
    assert intersection(l1, l2, key=key, combine=combine) == expected


@pytest.mark.parametrize(
    "l1, l2, key, combine, expected",
    [
        ([1, 2, 3], [3, 4, 5], identity, lambda x, _: x, [1, 2, 3, 4, 5]),
        ([1, 2, 3], [3, 4, 5], identity, op.add, [1, 2, 6, 4, 5]),
        (
            [Foo(1, "hello"), Foo(2, "bob"), Foo(6, "world!")],
            [Foo(5, "abcd"), Foo(6, "message")],
            op.attrgetter("id"),
            lambda x, y: Foo(x.id, f"{x.txt} {y.txt}"),
            [Foo(1, "hello"), Foo(2, "bob"), Foo(6, "world! message"), Foo(5, "abcd")],
        ),
    ],
)
def test_union(l1, l2, key, combine, expected):
    assert union(l1, l2, key=key, combine=combine) == expected


@pytest.mark.parametrize(
    "test_input, path, expected",
    [
        (
            {
                "a": [
                    {"b": {"c": 1}},
                    {"b": {"c": 2}},
                    {"b": {"c": 3}},
                ]
            },
            ["a", "b", "c"],
            [1, 2, 3],
        ),
        ({"a": []}, ["a", "b", "c"], []),
        ({"a": []}, ["a"], []),
    ],
)
def test_extract_data(test_input, path, expected):
    assert extract_data(path, test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [
        (
            {
                "x": 1,
                "a": {"b": {"c": 10}},
                "d": {"e": "hello", "f": "world"},
                "foo": True,
            },
            {"x": 1, "a_b_c": 10, "d_e": "hello", "d_f": "world", "foo": True},
        )
    ],
)
def tests_flatten_dict(test_input, expected):
    assert flatten_dict(test_input) == expected
