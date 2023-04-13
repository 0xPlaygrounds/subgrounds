from subgrounds import Subgrounds

sg = Subgrounds()

curve = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/messari/curve-finance-ethereum"
)

# Partial FieldPath selecting the top 4 most traded pools on Curve
most_traded_pools = curve.Query.liquidityPools(
    orderBy=curve.LiquidityPool.cumulativeVolumeUSD,
    orderDirection="desc",
    first=4,
)

# Partial FieldPath selecting the top 2 pools by daily total revenue of
#  the top 4 most traded tokens.
# Mote that reuse of `most_traded_pools` in the partial FieldPath
most_traded_snapshots = most_traded_pools.dailySnapshots(
    orderBy=curve.LiquidityPoolDailySnapshot.dailyTotalRevenue,
    orderDirection="desc",
    first=3,
)

# Querying:
#  - the name of the top 4 most traded pools, their 2 most liquid
# pools' token symbols and their 2 most liquid pool's TVL in USD
df = sg.query_df(
    [
        most_traded_pools.name,
        most_traded_snapshots.dailyVolumeUSD,
        most_traded_snapshots.dailyTotalRevenueUSD,
    ]
)

print(df)
