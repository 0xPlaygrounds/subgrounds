import polars as pl


def format_dictionary_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Unnest dictionary values into their own columns, renaming them appropriately.

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
        if isinstance(df[column][0], dict):
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
    """
    Unnest array values into their own columns, renaming them appropriately.

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
        if isinstance(df[column][0], pl.Series):
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
