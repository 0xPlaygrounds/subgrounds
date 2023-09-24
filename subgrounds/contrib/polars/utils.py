import polars as pl


############################
# Polars Support Functions - Convert GraphQL Response to Polars DataFrame
############################


def fmt_dict_cols(df: pl.DataFrame) -> pl.DataFrame:
    """
    formats dictionary cols, which are 'structs' in a polars df, into separate columns and renames accordingly.
    """
    for column in df.columns:
        if isinstance(df[column][0], dict):
            col_names = df[column][0].keys()
            # rename struct columns
            struct_df = df.select(
                pl.col(column).struct.rename_fields(
                    [f"{column}_{c}" for c in col_names]
                )
            )
            struct_df = struct_df.unnest(column)
            # add struct_df columns to df and
            df = df.with_columns(struct_df)
            # drop the df column
            df = df.drop(column)

    return df


def fmt_arr_cols(df: pl.DataFrame) -> pl.DataFrame:
    """
    formats lists, which are arrays in a polars df, into separate columns and renames accordingly.
    Since there isn't a direct way to convert array -> new columns, we convert the array to a struct and then
    unnest the struct into new columns.
    """
    # use this logic if column is a list (rows show up as pl.Series)
    for column in df.columns:
        if isinstance(df[column][0], pl.Series):
            # convert struct to array
            struct_df = df.select([pl.col(column).arr.to_struct()])
            # rename struct fields
            struct_df = struct_df.select(
                pl.col(column).struct.rename_fields(
                    [f"{column}_{i}" for i in range(len(struct_df.shape))]
                )
            )
            # unnest struct fields into their own columns
            struct_df = struct_df.unnest(column)
            # add struct_df columns to df and
            df = df.with_columns(struct_df)
            # drop the df column
            df = df.drop(column)

    return df
