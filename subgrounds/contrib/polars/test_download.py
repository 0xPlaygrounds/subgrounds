from subgrounds.contrib.polars.client import PolarsSubgrounds


sg = PolarsSubgrounds()

subgraph = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/messari/curve-finance-ethereum"
)


# Partial FieldPath selecting the top 4 most traded pools on Curve
curve_swaps = subgraph.Query.swaps(
    orderBy=subgraph.Swap.timestamp,
    orderDirection="desc",
    first=500,
)

df = sg.query_df(
    [
        curve_swaps.timestamp,
        curve_swaps.blockNumber,
        curve_swaps.pool._select("id"),
        curve_swaps.hash,
        curve_swaps.tokenIn._select("id"),
        curve_swaps.tokenOut._select("id"),
    ],
    parquet_name="curve_swaps",
)  # amountIn and amountOut cols will give int overflow errors. How to deal with this? Maybe just filter. If it isn't timestamp or blockNumber column, then convert to float.

print(df)
