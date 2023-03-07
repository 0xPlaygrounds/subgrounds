from abc import ABC
from typing import Any

import plotly.graph_objects as go
from pipe import map, traverse
from plotly.basedatatypes import BaseTraceType

from subgrounds.query import DataRequest
from subgrounds.subgraph import FieldPath
from subgrounds.subgrounds import Subgrounds


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

        # print(f'mk_trace: {fpath_data}')
        # for key, item in fpath_data.items():
        #   print(f'{key}: {len(item)} datapoints')

        return self.graph_object(**(fpath_data | self.args))

    @property
    def field_paths(self) -> list[FieldPath]:
        return [fpath for _, fpath in self.fpaths.items()]


# Simple
class Scatter(TraceWrapper):
    """See https://plotly.com/python/line-and-scatter/"""

    graph_object = go.Scatter


class Pie(TraceWrapper):
    """See https://plotly.com/python/pie-charts/"""

    graph_object = go.Pie


class Bar(TraceWrapper):
    """See https://plotly.com/python/bar-charts/"""

    graph_object = go.Bar


class Heatmap(TraceWrapper):
    """See https://plotly.com/python/heatmaps/"""

    graph_object = go.Heatmap


class Contour(TraceWrapper):
    """See https://plotly.com/python/contour-plots/"""

    graph_object = go.Contour


class Table(TraceWrapper):
    """See https://plotly.com/python/contour-plots/"""

    graph_object = go.Table


# Distributions
class Box(TraceWrapper):
    """See https://plotly.com/python/box-plots/"""

    graph_object = go.Box


class Violin(TraceWrapper):
    """See https://plotly.com/python/violin/"""

    graph_object = go.Violin


class Histogram(TraceWrapper):
    """See https://plotly.com/python/histograms/"""

    graph_object = go.Histogram


class Histogram2d(TraceWrapper):
    """See https://plotly.com/python/2D-Histogram/"""

    graph_object = go.Histogram2d


class Histogram2dContour(TraceWrapper):
    """See https://plotly.com/python/2d-histogram-contour/"""

    graph_object = go.Histogram2dContour


# Finance
class Ohlc(TraceWrapper):
    """See https://plotly.com/python/ohlc-charts/"""

    graph_object = go.Ohlc


class Candlestick(TraceWrapper):
    """See https://plotly.com/python/candlestick-charts/"""

    graph_object = go.Candlestick


class Waterfall(TraceWrapper):
    """See https://plotly.com/python/waterfall-charts/"""

    graph_object = go.Waterfall


class Funnel(TraceWrapper):
    """See https://plotly.com/python/funnel-charts/"""

    graph_object = go.Funnel


class Indicator(TraceWrapper):
    """See https://plotly.com/python/indicator/"""

    graph_object = go.Indicator


# 3d
class Scatter3d(TraceWrapper):
    """See https://plotly.com/python/3d-scatter-plots/"""

    graph_object = go.Scatter3d


class Surface(TraceWrapper):
    """See https://plotly.com/python/3d-surface-plots/"""

    graph_object = go.Surface


# Maps
class Scattergeo(TraceWrapper):
    """See https://plotly.com/python/scatter-plots-on-maps/"""

    graph_object = go.Scattergeo


class Choropleth(TraceWrapper):
    """See https://plotly.com/python/choropleth-maps/"""

    graph_object = go.Choropleth


class Scattermapbox(TraceWrapper):
    """See https://plotly.com/python/scattermapbox/"""

    graph_object = go.Scattermapbox


class Choroplethmapbox(TraceWrapper):
    """See https://plotly.com/python/mapbox-county-choropleth/"""

    graph_object = go.Choroplethmapbox


class Densitymapbox(TraceWrapper):
    """See https://plotly.com/python/mapbox-density-heatmaps/"""

    graph_object = go.Densitymapbox


# Specialized
class Scatterpolar(TraceWrapper):
    """See https://plotly.com/python/polar-chart/"""

    graph_object = go.Scatterpolar


class Barpolar(TraceWrapper):
    """See https://plotly.com/python/wind-rose-charts/"""

    graph_object = go.Barpolar


class Sunburst(TraceWrapper):
    """See https://plotly.com/python/sunburst-charts/"""

    graph_object = go.Sunburst


class Treemap(TraceWrapper):
    """See https://plotly.com/python/treemaps/"""

    graph_object = go.Treemap


class Icicle(TraceWrapper):
    """See https://plotly.com/python/icicle-charts/"""

    graph_object = go.Icicle


class Sankey(TraceWrapper):
    """See https://plotly.com/python/sankey-diagram/"""

    graph_object = go.Sankey


class Parcoords(TraceWrapper):
    """See https://plotly.com/python/parallel-coordinates-plot/"""

    graph_object = go.Parcoords


class Parcats(TraceWrapper):
    """See https://plotly.com/python/parallel-categories-diagram/"""

    graph_object = go.Parcats


class Carpet(TraceWrapper):
    """See https://plotly.com/python/carpet-plot/"""

    graph_object = go.Carpet


class Scattercarpet(TraceWrapper):
    """See https://plotly.com/python/carpet-scatter/"""

    graph_object = go.Scattercarpet


class Contourcarpet(TraceWrapper):
    """See https://plotly.com/python/carpet-contour/"""

    graph_object = go.Contourcarpet


class Figure:
    subgrounds: Subgrounds
    traces: list[TraceWrapper]
    req: DataRequest
    data: list[dict[str, Any]]
    figure: go.Figure

    args: dict[str, Any]

    def __init__(
        self,
        subgrounds: Subgrounds,
        traces: TraceWrapper | list[TraceWrapper],
        **kwargs
    ) -> None:
        self.subgrounds = subgrounds
        self.traces = list([traces] | traverse)

        traces = list(self.traces | map(lambda trace: trace.field_paths) | traverse)
        if len(traces) > 0:
            self.req = self.subgrounds.mk_request(traces)
            self.data = self.subgrounds.execute(self.req)
        else:
            self.req = None
            self.data = None

        self.args = kwargs
        self.refresh()

    def refresh(self) -> None:
        # TODO: Modify this to support x/y in different documents
        self.figure = go.Figure(**self.args)

        if self.req is not None:
            self.data = self.subgrounds.execute(self.req)

            for trace in self.traces:
                self.figure.add_trace(trace.mk_trace(self.data))

    # @staticmethod
    # def mk_subplots(rows, cols, **kwargs):
    #   return make_subplots(rows, cols, **kwargs)
