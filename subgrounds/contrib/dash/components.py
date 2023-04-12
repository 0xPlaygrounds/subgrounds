from __future__ import annotations

from typing import Any, ClassVar

import pandas as pd
from dash import dash_table, dcc, html
from dash.dependencies import Input, Output
from pipe import map, where

from subgrounds import FieldPath, Subgrounds
from subgrounds.contrib.plotly import Figure

from .abcs import Refreshable


class Graph(dcc.Graph, Refreshable):
    counter: ClassVar[int] = 0
    wrapped_figure: Figure

    def __init__(self, fig: Figure, **kwargs) -> None:
        super().__init__(id=f"graph-{Graph.counter}", figure=fig.figure, **kwargs)
        Graph.counter += 1
        self.wrapped_figure = fig

    @property
    def dash_dependencies(self) -> list[Output]:
        return [Output(self.id, "figure")]

    @property
    def dash_dependencies_outputs(self) -> list[Any]:
        self.wrapped_figure.refresh()
        return [self.wrapped_figure.figure]


class DataTable(dash_table.DataTable, Refreshable):
    counter: ClassVar[int] = 0

    subgrounds: Subgrounds
    data: list[FieldPath]
    columns: list[str] | None
    concat: bool
    append: bool
    df: pd.DataFrame | list[pd.DataFrame] | None

    def __init__(
        self,
        subgrounds: Subgrounds,
        data: FieldPath | list[FieldPath],
        columns: list[str] | None = None,
        concat: bool = False,
        append: bool = False,
        **kwargs,
    ):
        self.subgrounds = subgrounds
        self.fpaths = data if type(data) == list else [data]
        self.column_names = columns
        self.concat = concat
        self.append = append
        self.df = None

        super().__init__(id=f"datatable-{DataTable.counter}", **kwargs)
        DataTable.counter += 1

        self.refresh()

    def refresh(self) -> None:
        match (self.df, self.append):
            case (None, _) | (_, False):
                self.df = self.subgrounds.query_df(
                    self.fpaths, columns=self.column_names, concat=self.concat
                )
            case (_, True):
                self.df = pd.concat(
                    [
                        self.df,
                        self.subgrounds.query_df(
                            self.fpaths, columns=self.column_names, concat=self.concat
                        ),
                    ],
                    ignore_index=True,
                )
                self.df = self.df.drop_duplicates()

        self.columns = [{"name": i, "id": i} for i in self.df.columns]
        self.data = self.df.to_dict("records")

    @property
    def dash_dependencies(self) -> list[Output]:
        return [Output(self.id, "data")]

    @property
    def dash_dependencies_outputs(self) -> list[Output]:
        self.refresh()
        return [self.df.to_dict("records")]


class AutoUpdate(html.Div):
    counter: ClassVar[int] = 0

    def __init__(self, app, sec_interval: int = 1, children=[], **kwargs):
        id = f"interval-{AutoUpdate.counter}"

        super().__init__(
            children=[
                dcc.Interval(id=id, interval=sec_interval * 1000, n_intervals=0),
                *children,
            ],
            **kwargs,
        )

        AutoUpdate.counter += 1

        def flatten(input: list[Any]):
            return [item for sublist in input for item in sublist]

        subgrounds_children = list(
            children | where(lambda child: isinstance(child, Refreshable))
        )
        deps = flatten(
            list(subgrounds_children | map(lambda child: child.dash_dependencies))
        )

        def update(n):
            outputs = flatten(
                list(
                    subgrounds_children
                    | map(lambda child: child.dash_dependencies_outputs)
                )
            )

            return outputs[0] if len(outputs) == 1 else outputs

        # Register callback
        app.callback(*deps, Input(id, "n_intervals"))(update)
