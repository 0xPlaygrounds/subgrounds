from subgrounds.contrib.polars.client import PolarsSubgrounds

sg = PolarsSubgrounds()

snx_endpoint = "https://api.thegraph.com/subgraphs/name/synthetix-perps/perps"

snx = sg.load_subgraph(
    url=snx_endpoint,
)


trades_df = sg.query_df(
    [
        # set the first parameter to a larger size to query more rows.
        snx.Query.futuresTrades(
            first=5000,
            orderBy="timestamp",
            orderDirection="desc",
        )
    ]
)

print(trades_df.head(10))


print("done")
