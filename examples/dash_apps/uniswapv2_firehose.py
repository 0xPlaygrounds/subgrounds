import dash
from dash import html

from subgrounds import Subgrounds
from subgrounds.contrib.dash import AutoUpdate, DataTable

sg = Subgrounds()
uniswapV2 = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
)

# Not necessary, but nice for brevity
Query = uniswapV2.Query
Swap = uniswapV2.Swap

swaps = Query.swaps(orderBy=Swap.timestamp, orderDirection="desc", first=10)

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
    html.Div(
        [
            html.H4("Entities"),
            html.Div(
                [
                    html.Div(
                        [
                            html.H4("Last 10 Swaps"),
                            AutoUpdate(
                                app,
                                sec_interval=15,
                                children=[
                                    DataTable(
                                        subgrounds=sg,
                                        data=[
                                            swaps.timestamp,
                                            swaps.transaction.id,
                                            swaps.pair.token0.symbol,
                                            swaps.pair.token1.symbol,
                                            swaps.amount0In,
                                            swaps.amount1In,
                                            swaps.amount0Out,
                                            swaps.amount1Out,
                                        ],
                                    ),
                                ],
                            ),
                        ]
                    ),
                ]
            ),
        ]
    )
)

if __name__ == "__main__":
    app.run_server(debug=True)
