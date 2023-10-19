from subgrounds.contrib.polars.utils import (
    force_numeric,
)

overflow_data = [
    {
        "x24e88aa9a8dbf48e": [
            {
                "blockNumber": 18381720,
                "amountIn": 1008310,
                "amountOut": 10082717795683768903291,
                "id": "swap-0xb67a23794697275f14c0b4e3d6d73960d13a6df4e0e1d3cd2269a9bbffe59d3e-49",
                "timestamp": 1697686283,
            },
            {
                "blockNumber": 18381710,
                "amountIn": 2402410386963680919,
                "amountOut": 7683170744157548213,
                "id": "swap-0x37fede110a1267df8833ad2b0db6f85d663e6b30cd1b85757314c67e6235b5e6-69",
                "timestamp": 1697686163,
            },
        ]
    }
]

output = force_numeric(overflow_data[0]["x24e88aa9a8dbf48e"])

print(output)
