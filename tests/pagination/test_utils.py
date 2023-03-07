from typing import Any

import pytest

from subgrounds.pagination.utils import merge


@pytest.mark.parametrize(
    ["data1", "data2", "expected"],
    [
        (
            {},
            {
                "pairs": [
                    {
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ]
                    },
                ]
            },
            {
                "pairs": [
                    {
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ]
                    },
                ]
            },
        ),
        (
            {
                "pairs": [
                    {
                        "id": "p0",
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ],
                    },
                    {
                        "id": "p1",
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ],
                    },
                    {
                        "id": "p2",
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ],
                    },
                ]
            },
            {
                "pairs": [
                    {
                        "id": "p3",
                        "swaps": [
                            {"id": "D1"},
                            {"id": "D2"},
                            {"id": "D3"},
                            {"id": "D4"},
                        ],
                    },
                    {
                        "id": "p4",
                        "swaps": [
                            {"id": "E1"},
                            {"id": "E2"},
                            {"id": "E3"},
                            {"id": "E4"},
                        ],
                    },
                    {
                        "id": "p5",
                        "swaps": [
                            {"id": "F1"},
                            {"id": "F2"},
                            {"id": "F3"},
                            {"id": "F4"},
                            {"id": "F5"},
                        ],
                    },
                ]
            },
            {
                "pairs": [
                    {
                        "id": "p0",
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ],
                    },
                    {
                        "id": "p1",
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ],
                    },
                    {
                        "id": "p2",
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ],
                    },
                    {
                        "id": "p3",
                        "swaps": [
                            {"id": "D1"},
                            {"id": "D2"},
                            {"id": "D3"},
                            {"id": "D4"},
                        ],
                    },
                    {
                        "id": "p4",
                        "swaps": [
                            {"id": "E1"},
                            {"id": "E2"},
                            {"id": "E3"},
                            {"id": "E4"},
                        ],
                    },
                    {
                        "id": "p5",
                        "swaps": [
                            {"id": "F1"},
                            {"id": "F2"},
                            {"id": "F3"},
                            {"id": "F4"},
                            {"id": "F5"},
                        ],
                    },
                ]
            },
        ),
        (
            {
                "pairs": [
                    {
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ]
                    },
                ]
            },
            {
                "mints": [
                    {"id": "M1"},
                    {"id": "M2"},
                    {"id": "M3"},
                    {"id": "M4"},
                    {"id": "M5"},
                ]
            },
            {
                "pairs": [
                    {
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ]
                    },
                ],
                "mints": [
                    {"id": "M1"},
                    {"id": "M2"},
                    {"id": "M3"},
                    {"id": "M4"},
                    {"id": "M5"},
                ],
            },
        ),
        (
            {
                "pairs": [
                    {
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ]
                    },
                ],
                "mints": [
                    {"id": "M1"},
                    {"id": "M2"},
                    {"id": "M3"},
                    {"id": "M4"},
                    {"id": "M5"},
                ],
            },
            {
                "mints": [
                    {"id": "N1"},
                    {"id": "N2"},
                    {"id": "N3"},
                    {"id": "N4"},
                    {"id": "N5"},
                ]
            },
            {
                "pairs": [
                    {
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ]
                    },
                    {
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ]
                    },
                ],
                "mints": [
                    {"id": "M1"},
                    {"id": "M2"},
                    {"id": "M3"},
                    {"id": "M4"},
                    {"id": "M5"},
                    {"id": "N1"},
                    {"id": "N2"},
                    {"id": "N3"},
                    {"id": "N4"},
                    {"id": "N5"},
                ],
            },
        ),
        (
            {
                "token": {"symbol": "USDC"},
                "pairs": [
                    {
                        "id": "p0",
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ],
                    },
                    {
                        "id": "p1",
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ],
                    },
                    {
                        "id": "p2",
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ],
                    },
                ],
            },
            {
                "token": {"symbol": "USDC"},
                "pairs": [
                    {
                        "id": "p3",
                        "swaps": [
                            {"id": "D1"},
                            {"id": "D2"},
                            {"id": "D3"},
                            {"id": "D4"},
                        ],
                    },
                    {
                        "id": "p4",
                        "swaps": [
                            {"id": "E1"},
                            {"id": "E2"},
                            {"id": "E3"},
                            {"id": "E4"},
                        ],
                    },
                    {
                        "id": "p5",
                        "swaps": [
                            {"id": "F1"},
                            {"id": "F2"},
                            {"id": "F3"},
                            {"id": "F4"},
                            {"id": "F5"},
                        ],
                    },
                ],
            },
            {
                "token": {"symbol": "USDC"},
                "pairs": [
                    {
                        "id": "p0",
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ],
                    },
                    {
                        "id": "p1",
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ],
                    },
                    {
                        "id": "p2",
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ],
                    },
                    {
                        "id": "p3",
                        "swaps": [
                            {"id": "D1"},
                            {"id": "D2"},
                            {"id": "D3"},
                            {"id": "D4"},
                        ],
                    },
                    {
                        "id": "p4",
                        "swaps": [
                            {"id": "E1"},
                            {"id": "E2"},
                            {"id": "E3"},
                            {"id": "E4"},
                        ],
                    },
                    {
                        "id": "p5",
                        "swaps": [
                            {"id": "F1"},
                            {"id": "F2"},
                            {"id": "F3"},
                            {"id": "F4"},
                            {"id": "F5"},
                        ],
                    },
                ],
            },
        ),
        (
            {
                "pairs": [
                    {
                        "id": "p0",
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ],
                    },
                    {
                        "id": "p1",
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ],
                    },
                    {
                        "id": "p2",
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ],
                    },
                ]
            },
            {"pairs": []},
            {
                "pairs": [
                    {
                        "id": "p0",
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ],
                    },
                    {
                        "id": "p1",
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ],
                    },
                    {
                        "id": "p2",
                        "swaps": [
                            {"id": "C1"},
                            {"id": "C2"},
                            {"id": "C3"},
                            {"id": "C4"},
                            {"id": "C5"},
                        ],
                    },
                ]
            },
        ),
        (
            {
                "pairs": [
                    {
                        "id": "abc",
                        "mints": [
                            {"id": "M1"},
                            {"id": "M2"},
                            {"id": "M3"},
                            {"id": "M4"},
                        ],
                    },
                    {
                        "id": "xyz",
                        "mints": [
                            {"id": "N1"},
                            {"id": "N2"},
                            {"id": "N3"},
                            {"id": "N4"},
                        ],
                    },
                ]
            },
            {
                "pairs": [
                    {
                        "id": "abc",
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ],
                    },
                    {
                        "id": "xyz",
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ],
                    },
                ]
            },
            {
                "pairs": [
                    {
                        "id": "abc",
                        "swaps": [
                            {"id": "A1"},
                            {"id": "A2"},
                            {"id": "A3"},
                            {"id": "A4"},
                        ],
                        "mints": [
                            {"id": "M1"},
                            {"id": "M2"},
                            {"id": "M3"},
                            {"id": "M4"},
                        ],
                    },
                    {
                        "id": "xyz",
                        "swaps": [
                            {"id": "B1"},
                            {"id": "B2"},
                            {"id": "B3"},
                            {"id": "B4"},
                        ],
                        "mints": [
                            {"id": "N1"},
                            {"id": "N2"},
                            {"id": "N3"},
                            {"id": "N4"},
                        ],
                    },
                ]
            },
        ),
    ],
)
def test_merge(data1: dict[str, Any], data2: dict[str, Any], expected: dict[str, Any]):
    assert merge(data1, data2) == expected
