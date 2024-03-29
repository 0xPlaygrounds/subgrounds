"""
DEPRECIATED: Use `subgrounds.contrib.plotly` instead
"""

import warnings

from subgrounds.contrib.plotly import (
    Bar,
    Barpolar,
    Box,
    Candlestick,
    Carpet,
    Choropleth,
    Choroplethmapbox,
    Contour,
    Contourcarpet,
    Densitymapbox,
    Figure,
    Funnel,
    Heatmap,
    Histogram,
    Histogram2d,
    Histogram2dContour,
    Icicle,
    Indicator,
    Ohlc,
    Parcats,
    Parcoords,
    Pie,
    Sankey,
    Scatter,
    Scatter3d,
    Scattercarpet,
    Scattergeo,
    Scattermapbox,
    Scatterpolar,
    Sunburst,
    Surface,
    Table,
    TraceWrapper,
    Treemap,
    Violin,
    Waterfall,
)

__all__ = [
    "Figure",
    "TraceWrapper",
    "Scatter",
    "Pie",
    "Bar",
    "Heatmap",
    "Contour",
    "Table",
    "Box",
    "Violin",
    "Histogram",
    "Histogram2d",
    "Histogram2dContour",
    "Ohlc",
    "Candlestick",
    "Waterfall",
    "Funnel",
    "Indicator",
    "Scatter3d",
    "Surface",
    "Scattergeo",
    "Choropleth",
    "Scattermapbox",
    "Choroplethmapbox",
    "Densitymapbox",
    "Scatterpolar",
    "Barpolar",
    "Sunburst",
    "Treemap",
    "Icicle",
    "Sankey",
    "Parcoords",
    "Parcats",
    "Carpet",
    "Scattercarpet",
    "Contourcarpet",
]


warnings.warn(
    "Importing from `subgrounds.plotly_wrappers` is deprecated."
    " Use `subgrounds.contrib.plotly` instead.\n"
    "Will be removed in a future version.",
    DeprecationWarning,
)
