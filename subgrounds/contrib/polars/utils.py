from typing import Any

import polars as pl


def df_of_json(data: dict[str, Any]) -> pl.DataFrame:
    """Creates a ``pl.DataFrame`` from a JSON `data`.

    Args:
        json_data: Response data
    
    Returns:
        A ``pl.DataFrame`` formatted from the response data.
    """

    # TODO: refactor
    # Get the first key of the first JSON object.
    # This is the key that contains the data.
    json_data_key = list(data.keys())[0]
    numeric_data = force_numeric(data[json_data_key])

    graphql_df = pl.from_dicts(numeric_data, infer_schema_length=None)

    # Apply the formatting to the Polars DataFrame
    graphql_df = format_array_columns(graphql_df)
    return format_dictionary_columns(graphql_df)


def format_dictionary_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Unnest dictionary values into their own columns, renaming them appropriately.

    Args:
        df (pl.DataFrame): Input DataFrame containing dictionary columns.

    Returns:
        pl.DataFrame: DataFrame with dictionary values unnested into separate columns.

    Example:
        >>> data = {
        ...     "dict_col": [{"A": 1, "B": 2}, {"A": 3, "B": 4}],
        ...     "arr_col": [[1, 2, 3], [4, 5, 6]],
        ... }
        >>> df = pl.DataFrame(data)
        >>> result = fmt_dict_cols(df)
        >>> print(result)
        after test: shape: (2, 3)
        ...
        (output example here)

    """

    for column in df.columns:
        if len(df[column]) > 0 and isinstance(df[column][0], dict):
            col_names = df[column][0].keys()
            # Rename struct columns
            struct_df = df.select(
                pl.col(column).struct.rename_fields(
                    [f"{column}_{c}" for c in col_names]
                )
            )
            struct_df = struct_df.unnest(column)
            # Add struct_df columns to df and drop the original column
            df = df.with_columns(struct_df).drop(column)

    return df


def format_array_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Unnest array values into their own columns, renaming them appropriately.

    Args:
        df (pl.DataFrame): Input DataFrame containing array columns.

    Returns:
        pl.DataFrame: DataFrame with array values unnested into separate columns.

    Example:
        >>> data = {
        ...     "dict_col": [{"A": 1, "B": 2}, {"A": 3, "B": 4}],
        ...     "arr_col": [[1, 2, 3], [4, 5, 6]],
        ... }
        >>> df = pl.DataFrame(data)
        >>> result = fmt_arr_cols(df)
        >>> print(result)
        after test: shape: (2, 4)
        ...
        (output example here)

    """

    # use this logic if column is a list (rows show up as pl.Series)
    for column in df.columns:
        if len(df[column]) > 0 and isinstance(df[column][0], pl.Series):
            # convert struct to array
            struct_df = df.select([pl.col(column).list.to_struct()])
            # rename struct fields
            struct_df = struct_df.select(
                pl.col(column).struct.rename_fields(
                    [f"{column}_{i}" for i in range(len(struct_df.shape))]
                )
            )
            # unnest struct fields into their own columns
            struct_df = struct_df.unnest(column)
            # add struct_df columns to df and
            df = df.with_columns(struct_df).drop(column)

    return df


def force_numeric(json_data: list[str]) -> list[str]:
    # scan all keys. If one of the keys is timestamp or blockNumber, then leave alone. For any other key that has int values, convert to float
    # print(json_data)

    for entry in json_data:
        for key, value in entry.items():
            if key != "timestamp" and key != "blockNumber" and isinstance(value, int):
                entry[key] = float(value)

    return json_data
