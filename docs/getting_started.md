# Getting Started

## Installation

Subgrounds can be installed via `pip` with the following command:
`pip install subgrounds`

```{important}
Subgrounds requires `python>=3.10`
```


```{note}
If you run into problems during installation, see {ref}`Set up an isolated environment <isolated_environment_setup>`.
```

## Simple example
```{repl}
#repl:hide-output
from subgrounds import Subgrounds
#repl:show-output

sg = Subgrounds()
aave_v2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/aave/protocol-v2')

aave_v2.Borrow.adjusted_amount = aave_v2.Borrow.amount / 10 ** aave_v2.Borrow.reserve.decimals

last10_borrows = aave_v2.Query.borrows(
  orderBy=aave_v2.Borrow.timestamp,
  orderDirection='desc',
  first=10,
)

sg.query_df([
  last10_borrows.reserve.symbol, 
  last10_borrows.timestamp,
  last10_borrows.adjusted_amount,
])
```
