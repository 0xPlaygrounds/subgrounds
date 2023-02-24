# Advanced Topics

## Pagination
By default, Subgrounds handles GraphQL query pagination automatically. That is, if a query selects more than 1000 entities using the `first` argument (1000 being The Graph's limit to the `first` argument), then Subgrounds will automatically split the query into multiple queries that each query at most 1000 entities.

Pagination is performed by Subgrounds with the use of a pagination strategy: a class that implements the `PaginationStrategy` protocol. Subgrounds provides two pagination strategies out of the box, however, users wishing to implement their own strategy should create a class that implements the aforementioned protocol (see below).

If at some point during the pagination process, an unhandled exception occurs, Subgrounds will raise a `PaginationError` exception containing the initial exception message as well as the `PaginationStrategy` object in the state it was in when the error occured, which, in the case of iterative querying (e.g.: when using `query_df_iter`), could be useful to recover and start pagination from a later stage.

### Available pagination strategies
Subgrounds provides two pagination strategies out of the box:
1. `LegacyStrategy`: A pagination strategy that implements the pagination algorithm that was used by default prior to this update. This pagination strategy supports pagination on nested fields, but is quite slow. Below is an example of a query for which you should use this strategy:
    ```graphql
    query {
      liquidityPools(first: 10) {
        swaps(first: 5000) {
          id
        }
      }
    }
    ```

2. `ShallowStrategy`: A new pagination strategy that is faster than the `LegacyStrategy`, but does not paginate on nested list fields. In other words, this strategy is best when nested list fields select fewer than 1000 entities. Below is an example of a query for which you should use this strategy:
    ```graphql
    query {
      liquidityPools(first: 5000) {
        swaps(first: 10) {
          id
        }
      }
    }
    ```

To use either pagination strategy, set the `pagination_strategy` argument of toplevel querying functions:
```python
from subgrounds import Subgrounds
from subgrounds.pagination import ShallowStrategy

sg = Subgrounds()
subgraph = sg.load_subgraph("https://api.thegraph.com/subgraphs/name/messari/compound-ethereum")

mkt_daily_snapshots = subgraph.Query.marketDailySnapshots(
    orderBy='timestamp',
    orderDirection='desc',
    first=1000
)

field_paths = [
    mkt_daily_snapshots.timestamp,
    mkt_daily_snapshots.market.inputToken.symbol,
    mkt_daily_snapshots.rates.rate,
    mkt_daily_snapshots.rates.side,
]

df = sg.query_df(field_paths, pagination_strategy=ShallowStrategy) 
```

Note that pagination can be explicitely disabled by setting `pagination_strategy` to `None`, in which case the query will be executed as-is:
```python
df = sg.query_df(field_paths, pagination_strategy=ShallowStrategy) 
```

### Custom pagination strategy
Subgrounds allows developers to create their own pagination strategy by creating a class that implements the `PaginationStrategy` protocol:
```python
class PaginationStrategy(Protocol):
    def __init__(
        self,
        schema: SchemaMeta,
        document: Document
    ) -> None: ...

    def step(
        self,
        page_data: Optional[dict[str, Any]] = None
    ) -> Tuple[Document, dict[str, Any]]: ...
```

The class's constructor should accept a `SchemaMeta` argument which represents the schema of the subgraph API that the query is directed to and a `Document` argument which represents the query to be paginated on. If no pagination is required for the given document, then the constructor should raise a `SkipPagination` exception.

The class's `step` method is where the main logic of the pagination strategy is located. The method accepts a single argument, `page_data` which is a dictionary containing the response data of the previous query (i.e.: the previous page of data). The `step` method should return a tuple `(doc, vars)`, where `doc` is a `Document` representing the query to be made to fetch the next page of data. When pagination is over (e.g.: when all pages of data have been fetched), the `step` method should raise a `StopPagination` exception.

Below is the algorithm used by Subgrounds to paginate over a query document given a pagination strategy:
```python
def paginate(
    schema: SchemaMeta,
    doc: Document,
    pagination_strategy: Type[PaginationStrategy]
) -> dict[str, Any]:
    try:
        # Initialize the strategy
        strategy = pagination_strategy(schema, doc)

        data: dict[str, Any] = {}

        # Compute the query document and variables to get the first page of data
        next_page_doc, variables = strategy.step(page_data=None)

        while True:
            try:
                # Fetch a data page
                page_data = client.query(
                    url=next_page_doc.url,
                    query_str=next_page_doc.graphql,
                    variables=next_page_doc.variables | variables
                )

                # Merge the page with the data blob
                data = merge(data, page_data)

                # Compute the query document and variables to get the next page of data
                next_page_doc, variables = strategy.step(page_data=page_data)
            
            except StopPagination:
                break
            
            except Exception as exn:
                raise PaginationError(exn.args[0], strategy)

        return data

    except SkipPagination:
        # Excecute the query document as is if `SkipPagination` is raised
        return client.query(doc.url, doc.graphql, variables=doc.variables)
```
