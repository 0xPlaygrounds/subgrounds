from typing import Any

import plotly.graph_objects as go
from pipe import map, traverse

from subgrounds import Subgrounds
from subgrounds.query import DataRequest

from .traces import TraceWrapper


class Figure:
    subgrounds: Subgrounds
    traces: list[TraceWrapper]
    req: DataRequest | None
    data: list[dict[str, Any]] | None
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

        if self.req is None:
            return

        self.data = self.subgrounds.execute(self.req)

        for trace in self.traces:
            self.figure.add_trace(trace.mk_trace(self.data))
