import polars as pl
from pandas.testing import assert_frame_equal

from subgrounds import FieldPath, Subgraph
from subgrounds.dataframe_utils import df_of_json
from subgrounds.schema import TypeMeta, TypeRef


def test_df_of_json_1(klima_bridged_carbon_subgraph: Subgraph):
    expected = pl.DataFrame(
        data={
            "carbonOffsets_retirements_value": [1, 2, 3, 4, 5],
            "carbonOffsets_tokenAddress": [
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
            ],
        }
    )

    fpaths = [
        FieldPath(
            klima_bridged_carbon_subgraph,
            TypeRef.non_null_list("CarbonOffset"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="carbonOffsets",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list(name="CarbonOffset", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="retirements",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list(name="Retirement", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="value",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            klima_bridged_carbon_subgraph,
            TypeRef.non_null_list("CarbonOffset"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="carbonOffsets",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list(name="CarbonOffset", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="tokenAddress",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                ),
            ],
        ),
    ]

    json = {
        "carbonOffsets": [
            {
                "tokenAddress": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "retirements": [
                    {"value": 1},
                    {"value": 2},
                    {"value": 3},
                    {"value": 4},
                ],
            },
            {
                "tokenAddress": "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
                "retirements": [
                    {"value": 5},
                ],
            },
            {
                "tokenAddress": "0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27",
                "retirements": [],
            },
        ]
    }

    assert_frame_equal(df_of_json(json), expected)


def test_df_of_json_3(univ3_subgraph: Subgraph):
    expected = pl.DataFrame(
        data={
            "swaps_timestamp": [
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
            ],
            "swaps_pool_id": [
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
                "0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424",
                "0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x11b815efb8f581194ae79006d24e0d814b7697f6",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x60594a405d53811d3bc4766596efd80fd545a270",
            ],
            "swaps_token0_symbol": [
                "USDC",
                "DAI",
                "gOHM",
                "USDC",
                "STC",
                "MIM",
                "USDC",
                "WETH",
                "USDC",
                "DAI",
            ],
            "swaps_token1_symbol": [
                "WETH",
                "WETH",
                "WETH",
                "WETH",
                "WETH",
                "USDC",
                "WETH",
                "USDT",
                "WETH",
                "WETH",
            ],
        }
    )

    json = [
        {
            "swaps": [
                {
                    "pool": {"id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "USDC"},
                    "token1": {"symbol": "WETH"},
                },
                {
                    "pool": {"id": "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "DAI"},
                    "token1": {"symbol": "WETH"},
                },
                {
                    "pool": {"id": "0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "gOHM"},
                    "token1": {"symbol": "WETH"},
                },
                {
                    "pool": {"id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "USDC"},
                    "token1": {"symbol": "WETH"},
                },
                {
                    "pool": {"id": "0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "STC"},
                    "token1": {"symbol": "WETH"},
                },
                {
                    "pool": {"id": "0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "MIM"},
                    "token1": {"symbol": "USDC"},
                },
                {
                    "pool": {"id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "USDC"},
                    "token1": {"symbol": "WETH"},
                },
                {
                    "pool": {"id": "0x11b815efb8f581194ae79006d24e0d814b7697f6"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "WETH"},
                    "token1": {"symbol": "USDT"},
                },
                {
                    "pool": {"id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "USDC"},
                    "token1": {"symbol": "WETH"},
                },
                {
                    "pool": {"id": "0x60594a405d53811d3bc4766596efd80fd545a270"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "DAI"},
                    "token1": {"symbol": "WETH"},
                },
            ]
        }
    ]

    fpaths = [
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pool",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Pool", kind="OBJECT"),
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token1",
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
        ),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)


def test_df_of_json_flat_1(klima_bridged_carbon_subgraph: Subgraph):
    expected = pl.DataFrame(
        data={
            "foo_a": ["hello"],
            "foo_b": ["world"],
            "foo_c_x": [10],
            "bar_d": [20],
            "bar_e": [False],
        }
    )

    json = [
        {
            "foo": {"a": "hello", "b": "world", "c": {"x": 10}},
            "bar": {"d": 20, "e": False},
        }
    ]

    fpaths = [
        FieldPath(
            klima_bridged_carbon_subgraph,
            TypeRef.Named(name="Foo", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="foo",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Foo", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="a",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            klima_bridged_carbon_subgraph,
            TypeRef.Named(name="Foo", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="foo",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Foo", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="b",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            klima_bridged_carbon_subgraph,
            TypeRef.Named(name="Foo", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="foo",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Foo", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="c",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="C", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="x",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            klima_bridged_carbon_subgraph,
            TypeRef.Named(name="Bar", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="bar",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Bar", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="d",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            klima_bridged_carbon_subgraph,
            TypeRef.Named(name="Bar", kind="OBJECT"),
            TypeRef.Named(name="Boolean", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="bar",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Bar", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="e",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Boolean", kind="SCALAR"),
                    ),
                ),
            ],
        ),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)


def test_df_of_json_flat_2(univ3_subgraph: Subgraph):
    expected = pl.DataFrame(
        data=[
            {
                "token_id": "0xdbdb4d16eda451d0503b854cf79d55697f90c8df",
                "token_name": "Alchemix",
                "token_symbol": "ALCX",
            }
        ]
    )

    json = [
        {
            "token": {
                "id": "0xdbdb4d16eda451d0503b854cf79d55697f90c8df",
                "name": "Alchemix",
                "symbol": "ALCX",
            }
        }
    ]

    fpaths = [
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Token", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token",
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Token", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="name",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Token", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token",
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
        ),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)


def test_df_of_json_sfield(univ3_subgraph: Subgraph):
    expected = pl.DataFrame(
        data={
            "swaps_timestamp": [
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
                1643206992,
            ],
            "swaps_pool_id": [
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8",
                "0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424",
                "0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x11b815efb8f581194ae79006d24e0d814b7697f6",
                "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640",
                "0x60594a405d53811d3bc4766596efd80fd545a270",
            ],
            "swaps_token0_symbol": [
                "USDC",
                "DAI",
                "gOHM",
                "USDC",
                "STC",
                "MIM",
                "USDC",
                "WETH",
                "USDC",
                "DAI",
            ],
            "swaps_token1_symbol": [
                "WETH",
                "WETH",
                "WETH",
                "WETH",
                "WETH",
                "USDC",
                "WETH",
                "USDT",
                "WETH",
                "WETH",
            ],
            "swaps_price": [
                2658.5258552452533,
                2671.2712442099437,
                0.514571064347756,
                2653.578717169152,
                509832.97241621936,
                0.9994968933919802,
                2654.7704568332915,
                0.00037697663801254737,
                2661.9363854642856,
                2657.060762848602,
            ],
        }
    )

    json = [
        {
            "swaps": [
                {
                    "amount0": -233931.515098,
                    "amount1": 87.99294339622642,
                    "pool": {"id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "USDC"},
                    "token1": {"symbol": "WETH"},
                    "price": 2658.5258552452533,
                },
                {
                    "amount0": 2298.7018250845344,
                    "amount1": -0.8605272976553902,
                    "pool": {"id": "0xc2e9f25be6257c210d7adf0d4cd6e3e881ba25f8"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "DAI"},
                    "token1": {"symbol": "WETH"},
                    "price": 2671.2712442099437,
                },
                {
                    "amount0": -0.4428024474548323,
                    "amount1": 0.8605272976553902,
                    "pool": {"id": "0xcf7e21b96a7dae8e1663b5a266fd812cbe973e70"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "gOHM"},
                    "token1": {"symbol": "WETH"},
                    "price": 0.514571064347756,
                },
                {
                    "amount0": -1847.299583,
                    "amount1": 0.6961540545406192,
                    "pool": {"id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "USDC"},
                    "token1": {"symbol": "WETH"},
                    "price": 2653.578717169152,
                },
                {
                    "amount0": 791669.33237876,
                    "amount1": -1.5528013588977019,
                    "pool": {"id": "0x00f59b15dc1fe2e16cde0678d2164fd5ff10e424"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "STC"},
                    "token1": {"symbol": "WETH"},
                    "price": 509832.97241621936,
                },
                {
                    "amount0": -99949.68933919803,
                    "amount1": 100000.0,
                    "pool": {"id": "0x298b7c5e0770d151e4c5cf6cca4dae3a3ffc8e27"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "MIM"},
                    "token1": {"symbol": "USDC"},
                    "price": 0.9994968933919802,
                },
                {
                    "amount0": -106116.660281,
                    "amount1": 39.972066137717945,
                    "pool": {"id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "USDC"},
                    "token1": {"symbol": "WETH"},
                    "price": 2654.7704568332915,
                },
                {
                    "amount0": 29.51901886792453,
                    "amount1": -78304.637188,
                    "pool": {"id": "0x11b815efb8f581194ae79006d24e0d814b7697f6"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "WETH"},
                    "token1": {"symbol": "USDT"},
                    "price": 0.00037697663801254737,
                },
                {
                    "amount0": -74534.218793,
                    "amount1": 28.0,
                    "pool": {"id": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "USDC"},
                    "token1": {"symbol": "WETH"},
                    "price": 2661.9363854642856,
                },
                {
                    "amount0": -15043.573567,
                    "amount1": 5.661734867843959,
                    "pool": {"id": "0x60594a405d53811d3bc4766596efd80fd545a270"},
                    "timestamp": 1643206992,
                    "token0": {"symbol": "DAI"},
                    "token1": {"symbol": "WETH"},
                    "price": 2657.060762848602,
                },
            ]
        }
    ]

    fpaths = [
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pool",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Pool", kind="OBJECT"),
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="String", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token1",
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Swap", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="price",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="OBJECT"),
                    ),
                ),
            ],
        ),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)


def test_df_of_json_6(univ3_subgraph: Subgraph):
    expected = pl.DataFrame(
        data={
            "pools_id": [
                "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
                "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
                "0x6c6bc977e13df9b0de53b251522280bb72383700",
                "0x6c6bc977e13df9b0de53b251522280bb72383700",
            ],
            "pools_token0_symbol": ["UNI", "UNI", "DAI", "DAI"],
            "pools_token1_symbol": ["WETH", "WETH", "USDC", "USDC"],
            "pools_swaps_timestamp": [1643206408, 1643205177, 1643206881, 1643206138],
            "pools_swaps_amountUSD": [
                2261.033938028949777104204086349881,
                35534.09369471071499539246013240419,
                5497.8352603453796463465,
                243989.9166851892808097625,
            ],
        }
    )

    json = [
        {
            "pools": [
                {
                    "id": "0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801",
                    "token0": {"symbol": "UNI"},
                    "token1": {"symbol": "WETH"},
                    "swaps": [
                        {"amountUSD": 2261.0339380289497, "timestamp": 1643206408},
                        {"amountUSD": 35534.09369471072, "timestamp": 1643205177},
                    ],
                },
                {
                    "id": "0x6c6bc977e13df9b0de53b251522280bb72383700",
                    "token0": {"symbol": "DAI"},
                    "token1": {"symbol": "USDC"},
                    "swaps": [
                        {"amountUSD": 5497.8352603453795, "timestamp": 1643206881},
                        {"amountUSD": 243989.9166851893, "timestamp": 1643206138},
                    ],
                },
            ]
        }
    ]

    fpaths = [
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Pool"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pools",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Pool"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pools",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Pool"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pools",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token1",
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Pool"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pools",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Pool"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pools",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="amountUSD",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigDecimal", kind="OBJECT"),
                    ),
                ),
            ],
        ),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)


def test_df_of_json_semiflat_1(univ3_subgraph: Subgraph):
    expected = pd.DataFrame(
        data={
            "token_id": "0xdbdb4d16eda451d0503b854cf79d55697f90c8df",
            "token_name": "Alchemix",
            "token_symbol": "ALCX",
            "token_whitelistPools_id": [
                "0x689b322bf5056487eec7f9b2577cd43a37eb6302",
                "0xb80946cd2b4b68bedd769a21ca2f096ead6e0ee8",
            ],
        }
    )

    json = [
        {
            "token": {
                "id": "0xdbdb4d16eda451d0503b854cf79d55697f90c8df",
                "name": "Alchemix",
                "symbol": "ALCX",
                "whitelistPools": [
                    {"id": "0x689b322bf5056487eec7f9b2577cd43a37eb6302"},
                    {"id": "0xb80946cd2b4b68bedd769a21ca2f096ead6e0ee8"},
                ],
            }
        }
    ]

    fpaths = [
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Token", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token",
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Token", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="name",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="String", kind="SCALAR"),
                    ),
                ),
            ],
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Token", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token",
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Token", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="Token", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="whitelistPools",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
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
        ),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)


def test_df_of_json_semiflat_2(univ3_subgraph: Subgraph):
    expected = pl.DataFrame(
        data={
            "pool_token0_symbol": "USDC",
            "pool_token1_symbol": "WETH",
            "pool_swaps_timestamp": [1620179783, 1620203006, 1620208251, 1620215997],
        }
    )

    json = [
        {
            "pool": {
                "token0": {"symbol": "USDC"},
                "token1": {"symbol": "WETH"},
                "swaps": [
                    {"timestamp": 1620179783},
                    {"timestamp": 1620203006},
                    {"timestamp": 1620208251},
                    {"timestamp": 1620215997},
                ],
            }
        }
    ]

    fpaths = [
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Pool", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pool",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.Named(name="Pool", kind="OBJECT"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pool",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="token1",
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
        ),
        FieldPath(
            univ3_subgraph,
            TypeRef.non_null_list("Pool"),
            TypeRef.Named(name="BigInt", kind="SCALAR"),
            [
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="pool",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Pool"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="swaps",
                        description="",
                        args=[],
                        type=TypeRef.non_null_list("Swap", kind="OBJECT"),
                    ),
                ),
                (
                    None,
                    TypeMeta.FieldMeta(
                        name="timestamp",
                        description="",
                        args=[],
                        type=TypeRef.Named(name="BigInt", kind="SCALAR"),
                    ),
                ),
            ],
        ),
    ]

    assert_frame_equal(df_of_json(json, fpaths), expected)
