""" Pandas DataFrame utility module containing functions related to the
formatting of GraphQL JSON data into DataFrames.
"""

from __future__ import annotations

from dataclasses import dataclass
from functools import partial
from itertools import cycle
from typing import Any

import pandas as pd
from pipe import dedup, groupby, map, traverse, where
from typing_extensions import Self

from subgrounds.query import Selection
from subgrounds.subgraph import FieldPath
from subgrounds.utils import union


def gen_columns(data: list | dict, prefix: str = "") -> list[str]:
    match data:
        case dict():
            return list(
                list(data.keys())
                | map(
                    lambda key: gen_columns(
                        data[key], f"{prefix}_{key}" if prefix != "" else key
                    )
                )
                | traverse
            )
        case list():
            return gen_columns(data[0], prefix)
        case _:
            return prefix


def fmt_cols(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    df = df.rename(col_map, axis="columns")
    cols = list(col_map.values() | dedup | where(lambda name: name in df.columns))
    return df[cols]


@dataclass
class DataFrameColumns:
    """Helper class that holds data related to the shape of a DataFrame"""

    key: str
    fpaths: list[str]

    def combine(self, other: Self) -> Self:
        """Returns new DataFrameColumns containing the union of :attr:`self` and
        :attr:`other`'s columns

        Args:
          other: Columns to be combined to :attr:`self`

        Returns:
          New :class:`Self` containing the union of :attr:`self` and :attr:`other`
        """
        return DataFrameColumns(self.key, union(self.fpaths, other.fpaths))

    def mk_df(
        self, data: list[dict[str, Any]], path_map: dict[str, FieldPath]
    ) -> pd.DataFrame:
        """Formats the JSON data :attr:`data` into a DataFrame containing the columns
        defined in :attr:`self`.

        Args:
          data: The JSON data to be formatted into a dataframe
          path_map: A dictionary of :attr:`(key-FieldPath)` pairs

        Returns:
          The JSON data formatted into a DataFrame
        """
        cols_data = {
            col: path_map[col]._extract_data(data)
            for col in self.fpaths
            if col in path_map
        }

        rows_data = []

        def mk_rows(data: dict, row: dict = {}):
            if all([type(d) != list for d in list(data.values())]):
                rows_data.append(data | row)
            else:
                non_list_items = {
                    key: value for key, value in data.items() if type(value) != list
                }
                list_items = {
                    key: value for key, value in data.items() if type(value) == list
                }
                length = len(list(list_items.values())[0])
                for i in range(length):
                    mk_rows(
                        data={key: value[i] for key, value in list_items.items()},
                        row=row | non_list_items,
                    )

        mk_rows(cols_data, row={})
        return pd.DataFrame(data=rows_data)


def columns_of_selections(selections: list[Selection]) -> list[DataFrameColumns]:
    """Generates a list of DataFrame columns specifications based on a list of
    :class:`Selection` trees.

    Args:
      selections: The selection trees

    Returns:
      The list of DataFrame columns specifications
    """

    def columns_of_selections(
        selections: list[Selection], keys: list[str] = [], fpaths: list[str] = []
    ) -> list[DataFrameColumns]:
        if len(selections) > 0:
            non_list_selections = [
                select for select in selections if not select.contains_list()
            ]
            non_list_fpaths = list(
                non_list_selections
                | map(
                    lambda select: [
                        "_".join([*keys, *path]) for path in select.data_paths
                    ]
                )
                | traverse
            )
        else:
            non_list_fpaths = ["_".join(keys)]

        list_selections = [select for select in selections if select.contains_list()]

        if list_selections == []:
            return [DataFrameColumns("_".join(keys), fpaths + non_list_fpaths)]
        else:
            return list(
                list_selections
                | map(
                    lambda select: columns_of_selections(
                        select.selection,
                        keys=[*keys, select.key],
                        fpaths=fpaths + non_list_fpaths,
                    )
                )
            )

    return list(columns_of_selections(selections) | traverse)


def df_of_json(
    json_data: list[dict[str, Any]],
    fpaths: list[FieldPath],
    columns: list[str] | None = None,
    concat: bool = False,
) -> pd.DataFrame | list[pd.DataFrame]:
    """Formats the JSON data :attr:`json_data` into Pandas DataFrames,
    flattening the data in the process.

    Depending on the request's fieldpaths, either one or multiple dataframes will
    be returned based on how flattenable the response data is.

    :attr:`fpaths` is a list of :class:`FieldPath` objects corresponding to the
    set of fieldpaths that were used to get the response data :attr:`json_data`.

    :attr:`columns` is an optional argument used to rename the dataframes(s)
    columns. The length of :attr:`columns` must be the same as the number of columns
    of all returned dataframes.

    :attr:`concat` indicates whether or not the resulting dataframes should be
    concatenated together. The dataframes must have the same number of columns,
    as well as the same column names (which can be set using the :attr:`columns`
    argument).

    Args:
      json_data: Response data
      fpaths: Fieldpaths that yielded the response data
      columns: Column names. Defaults to None.
      concat: Flag indicating whether or not to concatenate the resulting dataframes,
        if there are more than one. Defaults to False.

    Returns:
      The resulting dataframe(s)
    """

    if columns is None:
        columns = list(fpaths | map(lambda fpath: fpath._name()))

    col_fpaths = zip(fpaths, cycle(columns))
    col_map = {fpath._name(use_aliases=True): colname for fpath, colname in col_fpaths}

    path_map = {fpath._name(use_aliases=True): fpath for fpath in fpaths}

    dfs = list(
        fpaths
        | groupby(lambda fpath: fpath._subgraph._url)
        | map(lambda group: FieldPath._merge(group[1]))
        | map(columns_of_selections)
        | traverse
        | map(partial(DataFrameColumns.mk_df, data=json_data, path_map=path_map))
    )

    if len(dfs) == 0:
        return pd.DataFrame(columns=columns, data=[])

    if len(dfs) == 1:
        return fmt_cols(dfs[0], col_map)

    if concat:
        return pd.concat(
            list(dfs | map(lambda df: fmt_cols(df, col_map))),
            ignore_index=True,
        )

    return list(dfs | map(lambda df: fmt_cols(df, col_map)))
