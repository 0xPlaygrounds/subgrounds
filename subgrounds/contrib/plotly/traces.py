from abc import ABC
from typing import Any

import plotly.graph_objects as go
from plotly.basedatatypes import BaseTraceType

from subgrounds import FieldPath


class TraceWrapper(ABC):
    graph_object: BaseTraceType

    fpaths: dict[str, FieldPath]
    args: dict[str, Any]

    def __init__(self, **kwargs) -> None:
        self.fpaths = {}
        self.args = {}

        for key, arg in kwargs.items():
            match arg:
                case FieldPath():
                    self.fpaths[key] = arg
                case _:
                    self.args[key] = arg

    def mk_trace(self, data: list[dict[str, Any]] | dict[str, Any]) -> BaseTraceType:
        fpath_data = {}
        for key, fpath in self.fpaths.items():
            item = fpath._extract_data(data)

            if type(item) == list and len(item) == 1:
                fpath_data[key] = item[0]
            else:
                fpath_data[key] = item

        return self.graph_object(**(fpath_data | self.args))  # type: ignore

    @property
    def field_paths(self) -> list[FieldPath]:
        return [fpath for _, fpath in self.fpaths.items()]


# Simple
class Scatter(TraceWrapper):
    """See https://plotly.com/python/line-and-scatter/"""

    graph_object = go.Scatter  # type: ignore


class Pie(TraceWrapper):
    """See https://plotly.com/python/pie-charts/"""

    graph_object = go.Pie  # type: ignore


class Bar(TraceWrapper):
    """See https://plotly.com/python/bar-charts/"""

    graph_object = go.Bar  # type: ignore


class Heatmap(TraceWrapper):
    """See https://plotly.com/python/heatmaps/"""

    graph_object = go.Heatmap  # type: ignore


class Contour(TraceWrapper):
    """See https://plotly.com/python/contour-plots/"""

    graph_object = go.Contour  # type: ignore


class Table(TraceWrapper):
    """See https://plotly.com/python/contour-plots/"""

    graph_object = go.Table  # type: ignore


# Distributions
class Box(TraceWrapper):
    """See https://plotly.com/python/box-plots/"""

    graph_object = go.Box  # type: ignore


class Violin(TraceWrapper):
    """See https://plotly.com/python/violin/"""

    graph_object = go.Violin  # type: ignore


class Histogram(TraceWrapper):
    """See https://plotly.com/python/histograms/"""

    graph_object = go.Histogram  # type: ignore


class Histogram2d(TraceWrapper):
    """See https://plotly.com/python/2D-Histogram/"""

    graph_object = go.Histogram2d  # type: ignore


class Histogram2dContour(TraceWrapper):
    """See https://plotly.com/python/2d-histogram-contour/"""

    graph_object = go.Histogram2dContour  # type: ignore


# Finance
class Ohlc(TraceWrapper):
    """See https://plotly.com/python/ohlc-charts/"""

    graph_object = go.Ohlc  # type: ignore


class Candlestick(TraceWrapper):
    """See https://plotly.com/python/candlestick-charts/"""

    graph_object = go.Candlestick  # type: ignore


class Waterfall(TraceWrapper):
    """See https://plotly.com/python/waterfall-charts/"""

    graph_object = go.Waterfall  # type: ignore


class Funnel(TraceWrapper):
    """See https://plotly.com/python/funnel-charts/"""

    graph_object = go.Funnel  # type: ignore


class Indicator(TraceWrapper):
    """See https://plotly.com/python/indicator/"""

    graph_object = go.Indicator  # type: ignore


# 3d
class Scatter3d(TraceWrapper):
    """See https://plotly.com/python/3d-scatter-plots/"""

    graph_object = go.Scatter3d  # type: ignore


class Surface(TraceWrapper):
    """See https://plotly.com/python/3d-surface-plots/"""

    graph_object = go.Surface  # type: ignore


# Maps
class Scattergeo(TraceWrapper):
    """See https://plotly.com/python/scatter-plots-on-maps/"""

    graph_object = go.Scattergeo  # type: ignore


class Choropleth(TraceWrapper):
    """See https://plotly.com/python/choropleth-maps/"""

    graph_object = go.Choropleth  # type: ignore


class Scattermapbox(TraceWrapper):
    """See https://plotly.com/python/scattermapbox/"""

    graph_object = go.Scattermapbox  # type: ignore


class Choroplethmapbox(TraceWrapper):
    """See https://plotly.com/python/mapbox-county-choropleth/"""

    graph_object = go.Choroplethmapbox  # type: ignore


class Densitymapbox(TraceWrapper):
    """See https://plotly.com/python/mapbox-density-heatmaps/"""

    graph_object = go.Densitymapbox  # type: ignore


# Specialized
class Scatterpolar(TraceWrapper):
    """See https://plotly.com/python/polar-chart/"""

    graph_object = go.Scatterpolar  # type: ignore


class Barpolar(TraceWrapper):
    """See https://plotly.com/python/wind-rose-charts/"""

    graph_object = go.Barpolar  # type: ignore


class Sunburst(TraceWrapper):
    """See https://plotly.com/python/sunburst-charts/"""

    graph_object = go.Sunburst  # type: ignore


class Treemap(TraceWrapper):
    """See https://plotly.com/python/treemaps/"""

    graph_object = go.Treemap  # type: ignore


class Icicle(TraceWrapper):
    """See https://plotly.com/python/icicle-charts/"""

    graph_object = go.Icicle  # type: ignore


class Sankey(TraceWrapper):
    """See https://plotly.com/python/sankey-diagram/"""

    graph_object = go.Sankey  # type: ignore


class Parcoords(TraceWrapper):
    """See https://plotly.com/python/parallel-coordinates-plot/"""

    graph_object = go.Parcoords  # type: ignore


class Parcats(TraceWrapper):
    """See https://plotly.com/python/parallel-categories-diagram/"""

    graph_object = go.Parcats  # type: ignore


class Carpet(TraceWrapper):
    """See https://plotly.com/python/carpet-plot/"""

    graph_object = go.Carpet  # type: ignore


class Scattercarpet(TraceWrapper):
    """See https://plotly.com/python/carpet-scatter/"""

    graph_object = go.Scattercarpet  # type: ignore


class Contourcarpet(TraceWrapper):
    """See https://plotly.com/python/carpet-contour/"""

    graph_object = go.Contourcarpet  # type: ignore
