# Subgrounds Basics
This page was written to provide an overview of the main concepts in Subgrounds and to serve as a quick reference for those concepts.

<!-- 
``{thebe-button}
```
-->

## Subgrounds
The `Subgrounds` class provides the toplevel Subgrounds API and most Subgrounds users will be using this class exclusively. This class is used to load (i.e.: introspect) GraphQL APIs (subgraph APIs or vanilla GraphQL APIs) as well as execute querying operations. Moreover, this class is meant to be used as a singleton, i.e.: initialized once and reused throughout a project.

The code cell below demonstrates how to initialize your `Subgrounds` object and load a GraphQL API.

```{code-block} python
:class: thebe
from subgrounds import Subgrounds

# Initialize Subgrounds
sg = Subgrounds()

# Load a subgraph using its API URL
aave_v2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')

# Load a vanilla GraphQL API using its API URL
snapshot = sg.load_api('https://hub.snapshot.org/graphql')
```

## FieldPaths
`FieldPaths` are the main building blocks used to construct Subgrounds queries. A `FieldPath` represents a selection path through a GraphQL schema starting from the root `Query` entity (see [The Query and Mutation types](https://graphql.org/learn/schema/#the-query-and-mutation-types)) all the way down to a scalar leaf.

`FieldPaths` are created by simply selecting attributes starting from the subgraph object returned by the `Subgrounds.load_subgraph` or `Subgrounds.load_api` methods:

```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

# Create a FieldPath
pools_tvl = uniswap_v3.Query.pools.totalValueLockedUSD
```

In the example above, the `pools_tvl` variable is a `FieldPath` object representing the following GraphQL query:

```graphql
query {
  pools {
    totalValueLockedUSD
  }
}
```

Note that `FieldPaths` don't have to be selected from root to leaf each time and partial `FieldPaths` can be reused:

```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

# Create a FieldPath
pools_tvl = uniswap_v3.Query.pools.totalValueLockedUSD

# Create a partial FieldPath
pools = uniswap_v3.Query.pools

# This FieldPath is equivalent to the `pools_tvl` FieldPath
pools_tvl2 = pools.totalValueLockedUSD
```

### FieldPath arguments
Field arguments can be specified via `FieldPaths` by "calling" the field in question:

```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

# Create a partial FieldPath of `pools` created after block 14720000 
# ordered by their TVL in descending order
new_pools = uniswap_v3.Query.pools(
  orderBy=uniswap_v3.Pool.totalValueLockedUSD,
  orderDirection='desc',
  where=[
    uniswap_v3.Pool.createdAtBlockNumber > 14720000
  ]
)

# Select the `totalValueLockedUSD` of the
pools_tvl = new_pools.totalValueLockedUSD
```

In the example above, the `pools_tvl` `FieldPath` represents the following GraphQL query:

```graphql
query {
  pools(
    orderBy: totalValueLockedUSD,
    orderDirection: desc,
    where: {
      createdAtBlockNumber_gt: 14720000
    }
  ) {
    totalValueLockedUSD
  }
}
```

Notice that the values for the `orderBy` and `where` arguments are `FieldPath` themselves. This allows users to construct complex queries in pure Python by using the `Subgraph` object returned when loading an API. Note however that the `FieldPaths` used as argument values are *relative* `FieldPath`, i.e.: they do not start from the root `Query` entity, but rather start from a user defined entity type (in this case the `Pool` entity). It is important to make sure that the relative `FieldPath` used as values for the `orderBy` and `where` arguments match the entity type of the field on which the arguments are applied (in our example, the `pools` field is of type `Pool`). If this is not respected, a type error exception will be thrown. 

Argument values can also be supplied in their "raw" form, without the use of relative `FieldPaths`:

```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

# This partial FieldPath is equivalant to the `new_pools` FieldPath
# in the previous example
new_pools2 = uniswap_v3.Query.pools(
  orderBy='totalValueLockedUSD',
  orderDirection='desc',
  where={
    'createdAtBlockNumber_gt': 14720000
  }
)
```

## Querying data
The `Subgrounds` class provides three methods for querying data: 
1. `query`
2. `query_df`
3. `query_json` 

All three methods take the same arguments (ignoring optional arguments), namely a list of `FieldPaths` and differ only in the way the returned data is formatted. This section will go over each method individually.

### `Subgrounds.query`
The `query` method returns the data in its simplest form which, depending on the `FieldPaths` given as argument, will be either: 1) a single value; 2) a list of values; or 3) a tuple containing single values or lists of values. It is important to consider the shape of the queried data (e.g.: single entities, list of entities...) as the shape of the returned data will depend on it.

```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

# 1) Single value: Query the quantity of WETH locked on Uniswap V3 
sg.query([
  uniswap_v3.Query.token(id='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2').totalValueLocked
])
```

```{code-block} python
:class: thebe
# Partial FieldPath of top 10 most traded tokens 
most_traded_tokens = uniswap_v3.Query.tokens(
  orderBy=uniswap_v3.Token.volumeUSD,
  orderDirection='desc',
  first=10
)

# 2) List of values: Query symbol of top 10 most traded tokens
sg.query([
  most_traded_tokens.symbol
])
```

```{code-block} python
:class: thebe
# 3) Tuple of values: Query symbol and TVL of top 10 most traded tokens
sg.query([
  most_traded_tokens.symbol,
  most_traded_tokens.totalValueLocked
])
```

### `Subgrounds.query_df`
Subgrounds provides a simple way to query subgraph data directly into a pandas `DataFrame` via the `query_df` method. Just like the `query` method, `query_df` takes as argument a list of `FieldPaths` and returns one or more `DataFrames` depending on the shape of the queried data. `query_df` will attempt to flatten all the data to a single `DataFrame` (effectively mimicking the SQL `JOIN` operation) but when that is not possible, two or more `DataFrames` will be returned.

#### Example with single DataFrame returned
```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

# Select top 10 pools by cummulative volume 
top_10_pools = uniswap_v3.Query.pools(
  orderBy=uniswap_v3.Pool.volumeUSD,
  orderDirection='desc',
  first=10
)

# Query data flattened to a single DataFrame
sg.query_df([
  top_10_pools.id,
  top_10_pools.token0.symbol,
  top_10_pools.token1.symbol,
])
```
#### Example with multiple DataFrames returned
```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

# Select WETH-USDC pool
weth_usdc = uniswap_v3.Query.pool(id='0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640')

# Define FieldPaths for last 10 mints and burns
weth_usdc_last10_mints = weth_usdc.mints(
  orderBy=uniswap_v3.Mint.timestamp,
  orderDirection='desc',
  first=10
)

weth_usdc_last10_burns = weth_usdc.burns(
  orderBy=uniswap_v3.Burn.timestamp,
  orderDirection='desc',
  first=10
)

# Query flattened data. Since we are selecting two unnested list fields (i.e.: one is not nested inside the other) we will get back two DataFrames
[mints, burns] = sg.query_df([
  weth_usdc_last10_mints.timestamp,
  weth_usdc_last10_mints.amount0,
  weth_usdc_last10_mints.amount1,
  weth_usdc_last10_burns.timestamp,
  weth_usdc_last10_burns.amount0,
  weth_usdc_last10_burns.amount1,
])
```

### `Subgrounds.query_json`
Subgrounds allows users to query data in its raw JSON format using the `query_json` method:

```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

top_2_traded_tokens = uniswap_v3.Query.tokens(
  orderBy=uniswap_v3.Token.volumeUSD,
  orderDirection='desc',
  first=2
)

sg.query_json([
  top_2_traded_tokens.symbol
])
```

```{note}
Subgrounds uses GraphQL aliases to differentiate queries selecting the same fields but with different arguments, which is why the JSON response data contains the key `x6e2162a85075a2b3` instead of the "expected" `tokens` key (see Subgrounds query aliases for more information). Subgrounds also selects additional fields automatically due to how automatic pagination is implemented. In the previous example, the `id` and `volumeUSD` fields were automatically added to the query by Subgrounds. However, these particularities are only apparent when using `query_json`, hence why using this method is not recommended unless it is absolutely necessary to get the raw JSON data.
```

### Combining FieldPaths
When passing a list of `FieldPaths` as argument to one of the aforementioned functions, the `FieldPaths` are merged together in a single query **if the `FieldPaths` originate from the same subgraph**:

```{code-block} python
:class: thebe
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

# Partial FieldPath selecting the top 4 most traded tokens on Uniswap V3
most_traded_tokens = uniswap_v3.Query.tokens(
  orderBy=uniswap_v3.Token.volumeUSD,
  orderDirection='desc',
  first=4
)

# Partial FieldPath selecting the top 2 pools by TVL of the top 4 most traded tokens
# (notice the FieldPath starts from most_traded_tokens) 
most_liquid_pairs = most_traded_tokens.whitelistPools(
  orderBy=uniswap_v3.Pool.totalValueLockedUSD,
  orderDirection='desc',
  first=2
)

# Querying the symbol of the top 4 most traded tokens, their 2 most liquid 
# pools' token symbols and their 2 most liquid pool's TVL in USD
sg.query_df([
  most_traded_tokens.symbol,
  most_liquid_pairs.token0.symbol,
  most_liquid_pairs.token1.symbol,
  most_liquid_pairs.totalValueLockedUSD
])
```

Under the hood, when executing the previous code, Subgrounds will combine the queried `FieldPaths` into the following GraphQL query:
```graphql
query {
  tokens(first: 4, orderBy: volumeUSD, orderDirection: desc) {
    symbol
    whitelistPools(first: 2, orderBy: totalValueLockedUSD, orderDirection: desc) {
      token0 {
        symbol
      }
      token1 {
        symbol
      }
      totalValueLockedUSD
    }
  }
}
```

<!-- In the cases where the `FieldPaths` originate from different subgraphs, then multiple queries will be executed concurrently: -->

## SyntheticFields
One of Subgrounds' unique features is the ability to define schema-based (i.e.: pre-querying) transformations using `SyntheticFields`.

`SyntheticFields` can be created using Python arithmetic operators on relative `FieldPaths` (i.e.: `FieldPaths` starting from an entity and not the root `Query` object) and must be added to the entity which they enhance. Once added to an entity, `SyntheticFields` can be queried just like regular GraphQL fields. The example below demonstrates how to create a simple `SyntheticField` to calculate the swap price of `Swap` events stored on the Sushiswap subgraph:

```{code-block} python
:class: thebe
sushiswap = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/sushiswap/exchange')

# Define a synthetic field named price1 (the swap price of token1,
# in terms of token0) on Swap entities
sushiswap.Swap.price1 = abs(sushiswap.Swap.amount0Out - sushiswap.Swap.amount0In) / abs(sushiswap.Swap.amount1Out - sushiswap.Swap.amount1In)

# Build query to get the last 10 swaps of the WETH-USDC pair on Sushiswap 
weth_usdc = sushiswap.Query.pair(id='0x397ff1542f962076d0bfe58ea045ffa2d347aca0')

last_10_swaps = weth_usdc.swaps(
  orderBy=sushiswap.Swap.timestamp,
  orderDirection='desc',
  first=10
)

# Query swap prices using the SyntheticField price1 just like they were regular fields
sg.query_df([
  last_10_swaps.timestamp,
  last_10_swaps.price1
])
```

`SyntheticFields` can also be created using the constructor, allowing for much more complex transformations.

```{code-block} python
:class: thebe
from datetime import datetime
from subgrounds.subgraph import SyntheticField

sushiswap = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/sushiswap/exchange')

# Create a SyntheticField on the Swap entity called `datetime`, which will format 
# the timestamp field into something more human readable
sushiswap.Swap.datetime = SyntheticField(
  lambda timestamp: str(datetime.fromtimestamp(timestamp)),
  SyntheticField.STRING,
  sushiswap.Swap.timestamp
)

last_10_swaps = sushiswap.Query.swaps(
  orderBy=sushiswap.Swap.timestamp,
  orderDirection='desc',
  first=10
)

sg.query_df([
  last_10_swaps.datetime,
  last_10_swaps.to,
  last_10_swaps.pair.token0.symbol,
  last_10_swaps.pair.token1.symbol
])
```
