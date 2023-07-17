import dash
from dash import html

from subgrounds import Subgrounds
from subgrounds.contrib.dash import AutoUpdate, Graph
from subgrounds.contrib.plotly import Bar, Figure, Indicator

sg = Subgrounds()
uniswapV2 = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
)
aaveV2 = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/aave/protocol-v2")

# Define formatted token amounts on Borrow and Repay entities
aaveV2.Borrow.adjusted_amount = (
    aaveV2.Borrow.amount / 10**aaveV2.Borrow.reserve.decimals
)
aaveV2.Repay.adjusted_amount = aaveV2.Repay.amount / 10**aaveV2.Repay.reserve.decimals

weth_usdc_last_price = uniswapV2.Query.pair(
    id="0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc",
).token0Price

wbtc_usdc_last_price = uniswapV2.Query.pair(
    id="0x004375dff511095cc5a197a54140a24efef3a416",
).token1Price


borrows = aaveV2.Query.borrows(
    orderBy=aaveV2.Borrow.timestamp,
    orderDirection="desc",
    first=100,
)

repays = aaveV2.Query.repays(
    orderBy=aaveV2.Repay.timestamp,
    orderDirection="desc",
    first=100,
)


# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
    [
        html.H3("Price stuff"),
        html.Div(
            [
                html.Div(
                    [
                        html.H4("ETH price (USDC) (UniswapV2-mainnet)"),
                        AutoUpdate(
                            app,
                            sec_interval=20,
                            children=[
                                Graph(
                                    Figure(
                                        subgrounds=sg,
                                        traces=[Indicator(value=weth_usdc_last_price)],
                                    )
                                )
                            ],
                        ),
                    ],
                    style={"width": "25%", "display": "inline-block"},
                ),
                html.Div(
                    [
                        html.H4("WBTC price (USDC) (UniswapV2-mainnet)"),
                        AutoUpdate(
                            app,
                            sec_interval=20,
                            children=[
                                Graph(
                                    Figure(
                                        subgrounds=sg,
                                        traces=[Indicator(value=wbtc_usdc_last_price)],
                                    )
                                )
                            ],
                        ),
                    ],
                    style={"width": "25%", "display": "inline-block"},
                ),
            ]
        ),
        html.H3("Aave V2 stuff"),
        html.Div(
            [
                html.Div(
                    [
                        html.H4("Aave V2 last 100 borrows"),
                        html.Div(
                            [
                                Graph(
                                    Figure(
                                        subgrounds=sg,
                                        traces=[
                                            Bar(
                                                x=borrows.reserve.symbol,
                                                y=borrows.adjusted_amount,
                                            ),
                                            Bar(
                                                x=repays.reserve.symbol,
                                                y=repays.adjusted_amount,
                                            ),
                                        ],
                                    )
                                )
                            ]
                        ),
                    ]
                )
            ]
        ),
    ]
)

if __name__ == "__main__":
    app.run_server(debug=True)
