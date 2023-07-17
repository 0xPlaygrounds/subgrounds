"""This module contains all code related to automatic pagination.

The `pagination` module contains the pagination algorithms (both regular and iterative)
  that make use of `PaginationStrategies`.

The `preprocess` and `strategties` modules implement the currently supported
  `PaginationStrategies`: `LegacyStrategy` and `ShallowStrategy`.

The `utils` module contains some generic functions that are useful in the context of
  pagination.
"""

from subgrounds.pagination.pagination import (
    PaginationError,
    PaginationStrategy,
    paginate,
)
from subgrounds.pagination.preprocess import (
    PaginationNode,
    generate_pagination_nodes,
    prune_doc,
)
from subgrounds.pagination.strategies import (
    LegacyStrategy,
    ShallowStrategy,
    SkipStrategy,
    normalize_strategy,
)

__all__ = [
    "generate_pagination_nodes",
    "LegacyStrategy",
    "normalize_strategy",
    "paginate",
    "paginate",
    "PaginationError",
    "PaginationNode",
    "PaginationStrategy",
    "prune_doc",
    "ShallowStrategy",
    "SkipStrategy",
]
