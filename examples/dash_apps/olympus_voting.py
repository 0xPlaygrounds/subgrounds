from datetime import datetime

import dash
from dash import html

from subgrounds import Subgrounds, SyntheticField
from subgrounds.contrib.dash import Graph
from subgrounds.contrib.plotly import Figure, Scatter

sg = Subgrounds()
olympusDAO = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/drondin/olympus-protocol-metrics"
)
snapshot = sg.load_api("https://hub.snapshot.org/graphql")

# Define useful synthetic fields
olympusDAO.ProtocolMetric.datetime = SyntheticField(
    lambda timestamp: str(datetime.fromtimestamp(timestamp)),
    SyntheticField.STRING,
    olympusDAO.ProtocolMetric.timestamp,
)

protocol_metrics_1year = olympusDAO.Query.protocolMetrics(
    orderBy=olympusDAO.ProtocolMetric.timestamp, orderDirection="desc", first=365
)

snapshot.Proposal.datetime = SyntheticField(
    lambda timestamp: str(datetime.fromtimestamp(timestamp)),
    SyntheticField.STRING,
    snapshot.Proposal.end,
)


def fmt_summary(title, choices, scores):
    total_vote = sum(scores)
    vote_string = "<br>".join(
        f"{choice}: {100*score/total_vote:.2f}%"
        for choice, score in zip(choices, scores)
    )
    return f"{title}<br>{vote_string}"


snapshot.Proposal.summary = SyntheticField(
    fmt_summary,
    SyntheticField.STRING,
    [snapshot.Proposal.title, snapshot.Proposal.choices, snapshot.Proposal.scores],
)

proposals = snapshot.Query.proposals(
    orderBy="created",
    orderDirection="desc",
    first=100,
    where=[
        snapshot.Proposal.space == "olympusdao.eth",
        snapshot.Proposal.state == "closed",
    ],
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
                                Scatter(
                                    x=proposals.datetime,
                                    y=proposals.votes,
                                    text=proposals.summary,
                                    name="proposals",
                                    mode="markers",
                                ),
                                Scatter(
                                    x=protocol_metrics_1year.datetime,
                                    y=protocol_metrics_1year.marketCap,
                                    name="treasury_market_value",
                                    yaxis="y2",
                                ),
                            ],
                            layout={
                                "showlegend": True,
                                "hovermode": "x",
                                "spikedistance": -1,
                                "xaxis": {
                                    "showspikes": True,
                                    "spikedash": "solid",
                                    "spikemode": "across+toaxis",
                                    "spikesnap": "cursor",
                                    "spikecolor": "black",
                                    "spikethickness": 1,
                                    "showline": True,
                                    "showgrid": True,
                                },
                                "yaxis": {
                                    "title": "Proposals",
                                    "titlefont": {"color": "#1f77b4"},
                                    "tickfont": {"color": "#ffffff"},
                                },
                                "yaxis2": {
                                    "title": "Treasury Market Value",
                                    "titlefont": {"color": "#ff7f0e"},
                                    "tickfont": {"color": "#ff7f0e"},
                                    "anchor": "free",
                                    "overlaying": "y",
                                    "side": "left",
                                    "position": 0.05,
                                },
                            },
                        )
                    )
                ]
            ),
        ]
    )
)

if __name__ == "__main__":
    app.run_server(debug=True)
