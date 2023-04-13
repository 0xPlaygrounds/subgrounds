from subgrounds import Subgrounds

sg = Subgrounds()

# Load
aave_v2 = sg.load_subgraph(
    "https://api.thegraph.com/subgraphs/name/messari/aave-v2-ethereum"
)

# Construct the query
latest = aave_v2.Query.markets(
    orderBy=aave_v2.Market.totalValueLockedUSD,
    orderDirection="desc",
    first=5,
)

# Return query to a dataframe
df = sg.query_df([latest.name, latest.totalValueLockedUSD])

print(df)
