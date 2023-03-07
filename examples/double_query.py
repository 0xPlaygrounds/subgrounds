from datetime import datetime

import dash
from dash import html

from subgrounds.dash_wrappers import Graph
from subgrounds.plotly_wrappers import Figure, Scatter
from subgrounds.schema import TypeRef
from subgrounds.subgraph import SyntheticField
from subgrounds.subgrounds import Subgrounds

sg = Subgrounds()
uniswapV2 = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
)

# This is unecessary, but nice for brevity
Query = uniswapV2.Query
PairDayData = uniswapV2.PairDayData

# This is a synthetic field

PairDayData.exchange_rate = PairDayData.reserve0 / PairDayData.reserve1

# This is a synthetic field
PairDayData.datetime = SyntheticField(
    lambda timestamp: str(datetime.fromtimestamp(timestamp)),
    TypeRef.Named(name="String", kind="SCALAR"),
    PairDayData.date,
)

uni_eth = Query.pairDayDatas(
    orderBy=PairDayData.date,
    orderDirection="desc",
    first=100,
    where=[PairDayData.pairAddress == "0xd3d2e2692501a5c9ca623199d38826e513033a17"],
)

toke_eth = Query.pairDayDatas(
    orderBy=PairDayData.date,
    orderDirection="desc",
    first=100,
    where=[PairDayData.pairAddress == "0x5fa464cefe8901d66c09b85d5fcdc55b3738c688"],
)

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
    html.Div(
        [
            html.Div(
                [
                    Graph(
                        Figure(
                            subgrounds=sg,
                            traces=[
                                Scatter(
                                    x=uni_eth.datetime,
                                    y=uni_eth.exchange_rate,
                                    mode="lines",
                                ),
                                Scatter(
                                    x=toke_eth.datetime,
                                    y=toke_eth.exchange_rate,
                                    mode="lines",
                                ),
                            ],
                        )
                    )
                ]
            )
        ]
    )
)

if __name__ == "__main__":
    app.run_server(debug=True)
