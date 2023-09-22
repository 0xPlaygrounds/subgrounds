from polars_client import SubgroundsPolars
from subgrounds.subgraph import FieldPath, Subgraph

from polars_utils import *

import polars as pl

sg = SubgroundsPolars()

snx_endpoint = "https://api.thegraph.com/subgraphs/name/synthetix-perps/perps"

snx = sg.load_subgraph(
    url=snx_endpoint,
)

trades_json = sg.query_json(
    [
        # set the first parameter to a larger size to query more rows.
        snx.Query.futuresTrades(
            first=2500,
            orderBy="timestamp",
            orderDirection="desc",
            # where=[{"timestamp_lte": "1694131200"}],  # 1694131200 = 9/8/23
        )
    ]
)

json_trades_key = list(trades_json[0].keys())[0]
trades_df = pl.from_dicts(
    trades_json[0][list(trades_json[0].keys())[0]], infer_schema_length=None
)

fmted_df = fmt_dict_cols(trades_df)

# # polars calculations to convert big ints. All synthetix big ints are represented with 10**18 decimals.
fmted_df = fmted_df.with_columns(
    [
        (pl.col("margin") / 10**18),
        (pl.col("size") / 10**18),
        (pl.col("price") / 10**18),
        (pl.col("positionSize") / 10**18),
        (pl.col("realizedPnl") / 10**18),
        (pl.col("netFunding") / 10**18),
        (pl.col("feesPaidToSynthetix") / 10**18),
        # convert timestamp to datetime
        pl.from_epoch("timestamp").alias("datetime"),
    ]
)

print(fmted_df.shape)
print(fmted_df.head(5))

print("done")
