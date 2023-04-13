from datetime import datetime

import dash
from dash import html

from subgrounds import Subgrounds, SyntheticField
from subgrounds.contrib.dash import Graph
from subgrounds.contrib.plotly import Figure, Indicator, Scatter

sg = Subgrounds()
olympusDAO = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/drondin/olympus-protocol-metrics"
)

# Define useful synthetic fields
olympusDAO.ProtocolMetric.datetime = SyntheticField(
    lambda timestamp: str(datetime.fromtimestamp(timestamp)),
    SyntheticField.STRING,
    olympusDAO.ProtocolMetric.timestamp,
)

# olympusDAO.ProtocolMetric.circ_supply_percent = 100 * olympusDAO.ProtocolMetric.ohmCirculatingSupply / olympusDAO.ProtocolMetric.totalSupply

olympusDAO.ProtocolMetric.staked_supply_percent = (
    100
    * olympusDAO.ProtocolMetric.sOhmCirculatingSupply
    / olympusDAO.ProtocolMetric.totalSupply
)
olympusDAO.ProtocolMetric.unstaked_supply_percent = (
    100 - olympusDAO.ProtocolMetric.staked_supply_percent
)

protocol_metrics_1year = olympusDAO.Query.protocolMetrics(
    orderBy=olympusDAO.ProtocolMetric.timestamp, orderDirection="desc", first=365
)

last_metric = olympusDAO.Query.protocolMetrics(
    orderBy=olympusDAO.ProtocolMetric.timestamp, orderDirection="desc", first=1
)


# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.Div(
            [
                Graph(
                    Figure(
                        subgrounds=sg,
                        traces=[
                            Indicator(
                                value=last_metric.marketCap,
                                title="Market Cap",
                                domain={"row": 0, "column": 0},
                                number={"valueformat": "$,.0f"},
                            ),
                            Indicator(
                                value=last_metric.ohmPrice,
                                title="Price",
                                domain={"row": 0, "column": 1},
                                number={"valueformat": "$,.2f"},
                            ),
                            Indicator(
                                value=0,
                                title="OHM Current Index",
                                domain={"row": 0, "column": 2},
                            ),
                            Indicator(
                                value=0,
                                title="Circulating Supply/Total Supply",
                                domain={"row": 0, "column": 3},
                            ),
                        ],
                        layout={
                            "grid": {"rows": 1, "columns": 4},
                        },
                    )
                )
            ]
        ),
        html.Div(
            [
                html.Div(
                    [
                        Graph(
                            Figure(
                                subgrounds=sg,
                                traces=[
                                    Scatter(
                                        name="OHM Market Cap",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.marketCap,
                                    )
                                ],
                                layout={
                                    "title": {"text": "OHM Market Cap"},
                                    "paper_bgcolor": "rgb(255, 255, 255)",
                                },
                            )
                        )
                    ],
                    style={"width": "48%", "display": "inline-block"},
                ),
                html.Div(
                    [
                        Graph(
                            Figure(
                                subgrounds=sg,
                                traces=[
                                    Scatter(
                                        name="staked_supply_percent",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.staked_supply_percent,
                                        mode="lines",
                                        line={"width": 0.5, "color": "rgb(0, 255, 0)"},
                                        stackgroup="one",
                                    ),
                                    Scatter(
                                        name="unstaked_supply_percent",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.unstaked_supply_percent,
                                        mode="lines",
                                        line={"width": 0.5, "color": "rgb(255, 0, 0)"},
                                        stackgroup="one",
                                    ),
                                ],
                                layout={
                                    "title": {"text": "Circulating Supply (%)"},
                                    "yaxis": {
                                        "type": "linear",
                                        "range": [1, 100],
                                        "ticksuffix": "%",
                                    },
                                },
                            )
                        )
                    ],
                    style={"width": "48%", "display": "inline-block"},
                ),
            ]
        ),
        html.Div(
            [
                Graph(
                    Figure(
                        subgrounds=sg,
                        traces=[
                            Indicator(
                                value=last_metric.treasuryRiskFreeValue,
                                title="Treasury Risk-Free Value",
                                domain={"row": 0, "column": 0},
                            ),
                            Indicator(
                                value=0,
                                title="Olympus Pro Treasury",
                                domain={"row": 0, "column": 1},
                            ),
                            Indicator(
                                value=last_metric.treasuryMarketValue,
                                title="Treasury Market Value",
                                domain={"row": 0, "column": 2},
                            ),
                        ],
                        layout={
                            "grid": {"rows": 1, "columns": 3},
                            "template": {
                                "data": {
                                    "indicator": [{"number": {"valueformat": "$,.0f"}}]
                                }
                            },
                        },
                    )
                )
            ]
        ),
        html.Div(
            [
                html.Div(
                    [
                        Graph(
                            Figure(
                                subgrounds=sg,
                                traces=[
                                    # Risk free value treasury assets
                                    Scatter(
                                        name="lusd_rfv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryLusdRiskFreeValue,
                                        mode="lines",
                                        line={
                                            "width": 0.5,
                                            "color": "rgb(0, 128, 255)",
                                        },
                                        stackgroup="one",
                                    ),
                                    Scatter(
                                        name="frax_rfv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryFraxRiskFreeValue,
                                        mode="lines",
                                        line={"width": 0.5, "color": "rgb(0, 0, 64)"},
                                        stackgroup="one",
                                    ),
                                    Scatter(
                                        name="dai_rfv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryDaiRiskFreeValue,
                                        mode="lines",
                                        line={
                                            "width": 0.5,
                                            "color": "rgb(255, 128, 64)",
                                        },
                                        stackgroup="one",
                                    ),
                                ],
                                layout={
                                    "showlegend": True,
                                    "yaxis": {"type": "linear"},
                                    "title": {
                                        "text": "Risk Free Value of Treasury Assets"
                                    },
                                },
                            )
                        )
                    ],
                    style={"width": "48%", "display": "inline-block"},
                ),
                html.Div(
                    [
                        Graph(
                            Figure(
                                subgrounds=sg,
                                traces=[
                                    # Market value treasury assets
                                    Scatter(
                                        name="xsushi_mv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryXsushiMarketValue,
                                        mode="lines",
                                        line={
                                            "width": 0.5,
                                            "color": "rgb(255, 0, 255)",
                                        },
                                        stackgroup="one",
                                    ),
                                    Scatter(
                                        name="cvx_mv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryCVXMarketValue,
                                        mode="lines",
                                        line={
                                            "width": 0.5,
                                            "color": "rgb(0, 128, 128)",
                                        },
                                        stackgroup="one",
                                    ),
                                    Scatter(
                                        name="weth_mv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryWETHMarketValue,
                                        mode="lines",
                                        line={
                                            "width": 0.5,
                                            "color": "rgb(128, 0, 128)",
                                        },
                                        stackgroup="one",
                                    ),
                                    Scatter(
                                        name="lusd_mv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryLusdMarketValue,
                                        mode="lines",
                                        line={
                                            "width": 0.5,
                                            "color": "rgb(0, 128, 255)",
                                        },
                                        stackgroup="one",
                                    ),
                                    Scatter(
                                        name="frax_mv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryFraxMarketValue,
                                        mode="lines",
                                        line={"width": 0.5, "color": "rgb(0, 0, 64)"},
                                        stackgroup="one",
                                    ),
                                    Scatter(
                                        name="dai_mv",
                                        x=protocol_metrics_1year.datetime,
                                        y=protocol_metrics_1year.treasuryDaiMarketValue,
                                        mode="lines",
                                        line={
                                            "width": 0.5,
                                            "color": "rgb(255, 128, 64)",
                                        },
                                        stackgroup="one",
                                    ),
                                ],
                                layout={
                                    "showlegend": True,
                                    "yaxis": {"type": "linear"},
                                    "title": {
                                        "text": "Market Value of Treasury Assets"
                                    },
                                },
                            )
                        )
                    ],
                    style={"width": "48%", "display": "inline-block"},
                ),
            ]
        ),
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
