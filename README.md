# Subgrounds
<!-- [![GitHub Actions](https://github.com/0xPlaygrounds/subgrounds/workflows/CI/badge.svg)](https://github.com/0xPlaygrounds/subgrounds/actions) -->
[![PyPI](https://img.shields.io/pypi/v/subgrounds.svg)](https://pypi.org/project/subgrounds/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/subgrounds.svg)](https://pypi.org/project/subgrounds/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![CI](https://github.com/0xPlaygrounds/subgrounds/actions/workflows/main.yml/badge.svg)](https://github.com/0xPlaygrounds/subgrounds/actions/workflows/main.yml)
<br>

[![Discord](https://img.shields.io/discord/896944341598208070?color=7289DA&label=discord&logo=discord&logoColor=fff)](https://discord.gg/gMSSh5bjvk)
[![Twitter Follow](https://img.shields.io/badge/Playgrounds-Analytics-31fa1f2Playgrounds0x?color=%231fa1f2&logo=Twitter&logoColor=1fa1f2&style=flat)](https://twitter.com/Playgrounds0x)

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/0xPlaygrounds/subgrounds/blob/main/examples/notebook.ipynb)
[![Github Codepsaces](https://img.shields.io/badge/Github-Codespaces-24292f.svg?logo=Github)](https://codespaces.new/0xPlaygrounds/subgrounds-template?quickstart=1)

<!-- start elevator-pitch -->
An intuitive Python library for interfacing with subgraphs and GraphQL.

## Features
- **Simple**: Leverage a Pythonic API to easily build queries and transformations without the need for raw GraphQL manipulation.
- **Automated**: Automatically handle pagination and schema introspection for effortless data retrieval.
- **Powerful**: Create sophisticated queries using the `SyntheticFields` transformation system.
<!-- end elevator-pitch -->

## Resources
- [**Docs**](http://docs.playgrounds.network/): User guide and API documentation
- [**Snippets**](https://github.com/0xPlaygrounds/subgrounds/tree/main/examples): A list of examples showcasing Subgrounds integration with Dash and Plotly
- [**Examples**](http://docs.playgrounds.network/subgrounds/examples/): An ever growing list of projects created by our community members and team
- [**Videos**](https://docs.playgrounds.network/subgrounds/videos/): Video workshops on Subgrounds

## Installation
> Subgrounds **requires** atleast Python 3.10+

Subgrounds is available on PyPi. To install it, run the following:<br>
`pip install subgrounds`.

Subgrounds also comes bundled with extra modules that may require extra libraries. You can get all functionality of `subgrounds` via the following:<br>
`pip install subgrounds[all]`.

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


## Acknowledgments
This software project would not be possible without the support of The Graph Foundation. You can learn more about The Graph and its mission [here](https://thegraph.com/).
