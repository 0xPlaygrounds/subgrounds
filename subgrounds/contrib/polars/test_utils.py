from subgrounds.contrib.polars import utils
import polars as pl


# prepare test data
test_data = {
    "dict_col": [{"A": 1, "B": 2}, {"A": 3, "B": 4}],
    "arr_col": [[1, 2, 3], [4, 5, 6]],
}
test_data_df = pl.DataFrame(test_data)

print(f"before test: {test_data_df}")

# test fmt_dict_cols()
test_output_df = utils.format_dictionary_columns(test_data_df)
print(f"after test: {test_output_df}")

# test fmt_arr_cols()
fmt_arr_test_df = utils.format_array_columns(test_data_df)
print(f"after fmt arr test: {fmt_arr_test_df}")
