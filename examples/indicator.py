import dash
from dash import html

from subgrounds.dash_wrappers import Graph
from subgrounds.plotly_wrappers import Figure, Indicator
from subgrounds.subgrounds import Subgrounds

sg = Subgrounds()
uniswapV2 = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
)

# This is unecessary, but nice for brevity
Swap = uniswapV2.Swap

# This is a synthetic field
Swap.price1 = abs(Swap.amount0In - Swap.amount0Out) / abs(
    Swap.amount1In - Swap.amount1Out
)

swaps = uniswapV2.Query.swaps(
    orderBy=Swap.timestamp,
    orderDirection="desc",
    first=1,
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
                            traces=[
                                Indicator(value=swaps.price1),
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
