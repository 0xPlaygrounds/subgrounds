# Subgrounds
<!-- [![GitHub Actions](https://github.com/0xPlaygrounds/subgrounds/workflows/CI/badge.svg)](https://github.com/0xPlaygrounds/subgrounds/actions) -->
[![PyPI](https://img.shields.io/pypi/v/subgrounds.svg)](https://pypi.org/project/subgrounds/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/subgrounds.svg)](https://pypi.org/project/subgrounds/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
<br>

[![Discord](https://img.shields.io/discord/896944341598208070?color=7289DA&label=discord&logo=discord&logoColor=fff)](https://discord.gg/gMSSh5bjvk)
[![Twitter Follow](https://img.shields.io/twitter/follow/Playgrounds0x?color=%231fa1f2&label=Playgrounds%20Analytics&logo=Twitter&logoColor=1fa1f2&style=flat-square)](https://twitter.com/Playgrounds0x)


<!-- start elevator-pitch -->
An intuitive python library for interfacing with Subgraphs.

## Features
- **Simple**: Leverage a Pythonic API to easily build queries and transformations without the need for raw GraphQL manipulation.
- **Automated**: Automatically handle pagination and schema introspection for effortless data retrieval.
- **Powerful**: Create sophisticated queries using the `SyntheticFields` transformation system.

<!-- end elevator-pitch -->

## Resources
- [**Subgrounds Docs**](http://docs.playgrounds.network/): User guide and API documentation
- [**Examples**](https://github.com/0xPlaygrounds/subgrounds/tree/main/examples): A list of examples showcasing Subgrounds integration with Dash and Plotly
- [**Community projects**](http://docs.playgrounds.network//examples/): An ever growing list of projects created by our community members
- [**MetricsDAO Subgrounds Workshop**](https://docs.metricsdao.xyz/get-involved/workshops/2022-03-30+-subgrounds-workshop-series): Subgrounds workshop series hosted by MetricsDAO 

## Installation
> Subgrounds **requires** atleast Python 3.10+

Subgrounds is available on PyPi. To install it, run the following:<br>
`pip install subgrounds`.

Subgrounds also comes bundled with some handy `dash` wrappers. To use those wrappers, you can install the extra `dash` dependencies.<br>
`pip install subgrounds[dash]`.

## Simple example
<!-- start simple-example -->
```python
>>> from subgrounds import Subgrounds

>>> sg = Subgrounds()

>>> # Load
>>> aave_v2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/messari/aave-v2-ethereum')

>>> # Construct the query
>>> latest = aave_v2.Query.markets(
  orderBy=aave_v2.Market.totalValueLockedUSD,
  orderDirection='desc',
  first=5,
)

>>> # Return query to a dataframe
>>> sg.query_df([
  latest.name,
  latest.totalValueLockedUSD,
])
                  markets_name  markets_totalValueLockedUSD
0  Aave interest bearing STETH                 1.522178e+09
1   Aave interest bearing WETH                 1.221299e+09
2   Aave interest bearing USDC                 8.140547e+08
3   Aave interest bearing WBTC                 6.615692e+08
4   Aave interest bearing USDT                 3.734017e+08
```
<!-- end simple-example -->


## About Us
Playgrounds Analytics is a data solutions company providing serverless on-chain data infrastructures and services for data teams, analysts, and engineers. Checkout us out [here](https://playgrounds.network/) to learn more!




<!-- TODO: Move this to github pages docs -->
<!-- # Dash and Plotly wrappers
Subgrounds provides wrappers for Plotly objects and Dash components to facilitate visualization of data from The Graph.

Plotly wrappers can be found in the `subgrounds.plotly_wrappers` submodule. The wrappers include a `Figure` wrapper as well as wrappers for most Plotly traces (see https://plotly.com/python/reference/). All Plotly trace wrappers accept the same arguments as their underlying Plotly trace with the notable difference being that Subgrounds `FieldPath` objects can be used as arguments wherever one would usually provide data to the traces.

```python
from subgrounds.plotly_wrappers import Bar, Figure
from subgrounds.dash_wrappers import Graph

borrows = aave_v2.Query.borrows(
  orderBy=aave_v2.Borrow.timestamp,
  orderDirection='desc',
  first=100
)

repays = aave_v2.Query.repays(
  orderBy=aave_v2.Repay.timestamp,
  orderDirection='desc',
  first=100
)

# Dashboard
app = dash.Dash(__name__)

app.layout = html.Div(
  html.Div([
    html.H4('Entities'),
    html.Div([
      # Subgrounds Graph Dash component
      Graph(
        # A Subgrounds Plotly figure 
        Figure(
          subgrounds=sg,
          traces=[
            # Subgrounds Plotly traces
            Bar(x=borrows.reserve.symbol, y=borrows.amount),
            Bar(x=repays.reserve.symbol, y=repays.amount)
          ]
        )
      )
    ])
  ])
)
``` -->

<!-- Generates the following Dash dashboard (at time of writing):
![Alt text](https://raw.githubusercontent.com/Protean-Labs/subgrounds/main/img/bar-chart-example.png) -->

<!-- # Examples and resources
See the `examples/` directory for an evergrowing list of examples. -->


## Acknowledgments
This software project would not be possible without the support of The Graph Foundation. You can learn more about The Graph and its mission [here](https://thegraph.com/).

<!-- TODO: Move this to github pages docs -->
<!-- # Notes
## Non-subgraph GraphQL APIs
Although Subgrounds has been developed with subgraph APIs in mind, most features will also work with any GraphQL API. However, Subgrounds has not been tested with non-subgraph GraphQL APIs and some features will definitely break if the non-subgraph APIs do not follow the same conventions as subgraph APIs (e.g.: automatic pagination relies on each entity having a unique `id` field).

It is nonetheless possible to use Subgrounds with non-subgraph GraphQL APIs by using `load_api` instead of `load_subgraph`. This will bypass Subgrounds automatic pagination feature and pagination will therefore have to be done manually. Below is an example of using Subgrounds with the snapshot GraphQL API.
```python
>>> from datetime import datetime

>>> from subgrounds.subgrounds import Subgrounds
>>> from subgrounds.subgraph import SyntheticField

>>> sg = Subgrounds()
>>> snapshot = sg.load_api('https://hub.snapshot.org/graphql')

>>> snapshot.Proposal.datetime = SyntheticField(
...   lambda timestamp: str(datetime.fromtimestamp(timestamp)),
...   SyntheticField.STRING,
...   snapshot.Proposal.end,
... )

>>> proposals = snapshot.Query.proposals(
...   orderBy='created',
...   orderDirection='desc',
...   first=100,
...   where=[
...     snapshot.Proposal.space == 'olympusdao.eth',
...     snapshot.Proposal.state == 'closed'
...   ]
... )

>>> sg.query_df([
...   proposals.datetime,
...   proposals.title,
...   proposals.votes,
... ])
     proposals_datetime                                    proposals_title  proposals_votes
0   2022-03-25 15:33:00  OIP-87: BeraChain Investment + Strategic Partn...              184
1   2022-03-25 12:00:00                 OIP-86: Uniswap Migration Proposal              146
2   2022-03-25 13:12:00                    TAP 8 - Yearn Finance Whitelist              137
3   2022-03-22 15:10:10                      OIP-85: Emissions Adjustments              167
4   2022-03-13 20:17:26                  TAP 7 - Anchor Protocol Whitelist              141
..                  ...                                                ...              ...
95  2021-11-20 23:00:00                 OlympusDAO Add ETH to the Treasury               52
96  2022-01-31 12:00:00                            Add BTC to the Treasury              232
97  2021-11-29 23:00:00   OlympusDAO OlympusDAO Launch OHM on POLYGON c...              234
98  2021-11-29 23:00:00                 OlympusDAO Launch OHM on BSC chain               92
99  2021-11-20 23:00:00  Suggestions to add more video operation guidan...               53

[100 rows x 3 columns]
```

## GraphQL Aliases
The use of the alias `xf608864358427cfb` in the query string is to prevent conflict when merging fieldpaths that select the same fields with different arguments. For example, in the following code, the `borrows` query field is selected twice with different arguments:
```python
>>> latest_borrows = aave_v2.Query.borrows(
...  orderBy=aave_v2.Borrow.timestamp,
...  orderDirection='desc',
...  first=100
...)

>>> largest_borrows = aave_v2.Query.borrows(
...  orderBy=aave_v2.Borrow.amount,
...  orderDirection='desc',
...  first=100
...)

>>> req = sg.mk_request([
...   latest_borrows.reserve.symbol,
...   latest_borrows.amount,
...   largest_borrows.reserve.symbol,
...   largest_borrows.amount,
... ])
>>> print(req.graphql)
query {
  x8b3edf3dc6501837: borrows(first: 100, orderBy: amount, orderDirection: desc) {
    reserve {
      symbol
    }
    amount
  }
  xf608864358427cfb: borrows(first: 100, orderBy: timestamp, orderDirection: desc) {
    reserve {
      symbol
    }
    amount
  }
}
``` -->
