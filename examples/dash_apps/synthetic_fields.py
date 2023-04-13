from datetime import datetime

import dash
from dash import html

from subgrounds import Subgrounds, SyntheticField
from subgrounds.contrib.dash import Graph
from subgrounds.contrib.plotly import Figure, Scatter
from subgrounds.schema import TypeRef

sg = Subgrounds()
uniswapV2 = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
)

# This is unecessary, but nice for brevity
Query = uniswapV2.Query
Swap = uniswapV2.Swap

# This is a synthetic field
Swap.price1 = abs(Swap.amount0In - Swap.amount0Out) / abs(
    Swap.amount1In - Swap.amount1Out
)

# This is a synthetic field
Swap.datetime = SyntheticField(
    lambda timestamp: str(datetime.fromtimestamp(timestamp)),
    TypeRef.Named(name="String", kind="SCALAR"),
    Swap.timestamp,
)

swaps = Query.swaps(
    orderBy=Swap.timestamp,
    orderDirection="desc",
    first=500,
    where=[Swap.pair == "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"],
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
                            traces=[Scatter(x=swaps.datetime, y=swaps.price1)],
                        )
                    )
                ]
            )
        ]
    )
)

if __name__ == "__main__":
    app.run_server(debug=True)
