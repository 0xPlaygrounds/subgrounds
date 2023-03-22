import dash
from dash import html

from subgrounds.dash_wrappers import Graph
from subgrounds.plotly_wrappers import Bar, Figure
from subgrounds.subgrounds import Subgrounds

sg = Subgrounds()
aaveV2 = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/aave/protocol-v2")

# Define formatted token amounts on Borrow and Repay entities
aaveV2.Borrow.adjusted_amount = (
    aaveV2.Borrow.amount / 10**aaveV2.Borrow.reserve.decimals
)
aaveV2.Repay.adjusted_amount = aaveV2.Repay.amount / 10**aaveV2.Repay.reserve.decimals

borrows = aaveV2.Query.borrows(
    orderBy=aaveV2.Borrow.timestamp, orderDirection="desc", first=100
)

repays = aaveV2.Query.repays(
    orderBy=aaveV2.Repay.timestamp, orderDirection="desc", first=100
)

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
    html.Div(
        [
            html.H4("Entities"),
            html.Div(
                [
                    Graph(
                        Figure(
                            subgrounds=sg,
                            traces=[
                                Bar(
                                    x=borrows.reserve.symbol, y=borrows.adjusted_amount
                                ),
                                Bar(x=repays.reserve.symbol, y=repays.adjusted_amount),
                            ],
                        )
                    )
                ]
            ),
        ]
    )
)

if __name__ == "__main__":
    app.run_server(debug=True)
