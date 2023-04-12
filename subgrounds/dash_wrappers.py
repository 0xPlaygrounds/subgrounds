"""
DEPRECIATED: Use `subgrounds.contrib.dash` instead
"""

import warnings

from subgrounds.contrib.dash import AutoUpdate, DataTable, Graph, Refreshable

__all__ = [
    "Refreshable",
    "Graph",
    "DataTable",
    "AutoUpdate",
]

warnings.warn(
    "Importing from `subgrounds.plotly_wrappers` is deprecated."
    " Use `subgrounds.contrib.plotly` instead.\n"
    "Will be removed in a future version.",
    DeprecationWarning,
)
