import dash
from dash import html

from subgrounds.dash_wrappers import AutoUpdate, Graph
from subgrounds.plotly_wrappers import Figure, Indicator
from subgrounds.subgrounds import Subgrounds

sg = Subgrounds()
uniswapV2 = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2"
)

# This is unecessary, but nice for brevity
pair = uniswapV2.Query.pair(id="0xb4e16d0168e52d35cacd2c6185b44281ec28c9dc")

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
    html.Div(
        [
            AutoUpdate(
                app,
                sec_interval=15,
                children=[
                    Graph(
                        Figure(
                            subgrounds=sg,
                            traces=[
                                Indicator(value=pair.token0Price),
                            ],
                        )
                    )
                ],
            )
        ]
    )
)

if __name__ == "__main__":
    app.run_server(debug=True)
