import dash
from dash import html

from subgrounds.dash_wrappers import DataTable
from subgrounds.plotly_wrappers import Bar, Figure
from subgrounds.subgrounds import Subgrounds

sg = Subgrounds()
uniswapV2 = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
)

# Not necessary, but nice for brevity
Query = uniswapV2.Query
Burn = uniswapV2.Burn
Mint = uniswapV2.Mint

mints = Query.mints(
    orderBy=Mint.timestamp,
    orderDirection="desc",
    first=10,
    where=[Mint.pair == "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"],
)

burns = Query.burns(
    orderBy=Burn.timestamp,
    orderDirection="desc",
    first=10,
    where=[Burn.pair == "0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc"],
)

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
                            html.H4("Last 10 Mints of ETH-USDC LP tokens"),
                            DataTable(
                                subgrounds=sg,
                                data=[
                                    mints.timestamp,
                                    mints.transaction.id,
                                    mints.to,
                                    mints.amountUSD,
                                ],
                            ),
                        ],
                        style={
                            "width": "48%",
                            "display": "inline-block",
                            "margin-right": "4%",
                        },
                    ),
                    html.Div(
                        [
                            html.H4("Last 10 Burns of ETH-USDC LP tokens"),
                            DataTable(
                                subgrounds=sg,
                                data=[
                                    burns.timestamp,
                                    burns.transaction.id,
                                    burns.to,
                                    burns.amountUSD,
                                ],
                            ),
                        ],
                        style={"width": "48%", "display": "inline-block"},
                    ),
                ]
            ),
        ]
    )
)

if __name__ == "__main__":
    app.run_server(debug=True)
