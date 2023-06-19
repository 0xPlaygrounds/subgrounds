import json

import pytest

from subgrounds.query import (
    Argument,
    InputValue,
    arguments_of_field_args,
    input_value_of_argument,
)
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef

# from tests.conftest import *


@pytest.mark.parametrize(
    "argmeta, argvalue, expected",
    [
        (
            TypeMeta.ArgumentMeta(
                name="where",
                description="",
                type=TypeRef.Named(name="Pair_filter", kind="INPUT_OBJECT"),
                defaultValue=None,
            ),
            {"reserveUSD_lt": 10},
            InputValue.Object({"reserveUSD_lt": InputValue.String("10.0")}),
        ),
        (
            TypeMeta.ArgumentMeta(
                name="orderBy",
                description="",
                type=TypeRef.Named(name="Pair_orderBy", kind="ENUM"),
                defaultValue=None,
            ),
            "reserveUSD",
            InputValue.Enum("reserveUSD"),
        ),
        (
            TypeMeta.ArgumentMeta(
                name="id",
                description="",
                type=TypeRef.Named(name="String", kind="SCALAR"),
                defaultValue=None,
            ),
            "abc",
            InputValue.String("abc"),
        ),
    ],
)
def test_input_value_of_argument(schema, argmeta, argvalue, expected):
    assert input_value_of_argument(schema, argmeta, argvalue) == expected


def test_arguments_of_field(schema, pairs_fieldmeta):
    field = pairs_fieldmeta

    expected = [
        Argument("first", InputValue.Int(100)),
        Argument(
            "where", InputValue.Object({"reserveUSD_lt": InputValue.String("10.0")})
        ),
        Argument("orderBy", InputValue.Enum("reserveUSD")),
        Argument("orderDirection", InputValue.Enum("desc")),
    ]

    raw_args = {
        "first": 100,
        "where": {"reserveUSD_lt": 10},
        "orderBy": "reserveUSD",
        "orderDirection": "desc",
    }

    args = arguments_of_field_args(schema, field, raw_args)

    assert args == expected


@pytest.mark.parametrize(
    ["raw_schema_path"],
    [
        ("tests/schemas/balancer.json",),
        ("tests/schemas/bentobox.json",),
        ("tests/schemas/compound-v2.json",),
        ("tests/schemas/cujowolf_polygon-bridged-carbon.json",),
        ("tests/schemas/enzyme.json",),
        ("tests/schemas/exchange.json",),
        ("tests/schemas/gvladika_curve.json",),
        ("tests/schemas/livepeer.json",),
        ("tests/schemas/marketplace.json",),
        ("tests/schemas/protocol-v2.json",),
        ("tests/schemas/synthetix.json",),
        ("tests/schemas/uniswap_uniswap-v2.json",),
        ("tests/schemas/uniswap_uniswap-v3.json",),
        ("tests/schemas/uniswap-v2.json",),
        ("tests/schemas/uniswap-v3.json",),
        ("tests/schemas/usdc.json",),
    ],
)
def test_parsing(raw_schema_path: str):
    with open(raw_schema_path, "r") as f:
        raw_schema = json.load(f)

    try:
        SchemaMeta(**raw_schema["__schema"])
    except ZeroDivisionError:
        assert False
