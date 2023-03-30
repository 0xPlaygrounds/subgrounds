from typing import overload

from subgrounds.query import InputValue

from .pagination import PaginationStrategy
from .strategies import LegacyStrategy, SkipStrategy

DEFAULT_NUM_ENTITIES = 100
PAGE_SIZE = 900


@overload
def merge_input_value_object_metas(
    data1: InputValue.Object, data2: InputValue.Object
) -> InputValue.Object:
    ...


@overload
def merge_input_value_object_metas(
    data1: dict[str, InputValue.Object], data2: dict[str, InputValue.Object]
) -> dict[str, InputValue.Object]:
    ...


def merge_input_value_object_metas(
    data1: InputValue.Object | dict[str, InputValue.Object],
    data2: InputValue.Object | dict[str, InputValue.Object],
) -> InputValue.Object | dict[str, InputValue.Object]:
    """Merges ``data1`` and ``data2`` and returns the combined result.

    ``data1`` and ``data2`` must be of the same type. Either both are
    ``dict``, ``InputValue.Object``
    """

    match data1, data2:
        case dict(d1), dict(d2):
            data = {}
            for key in d1:
                if key in d2:
                    data[key] = merge_input_value_object_metas(d1[key], d2[key])
                else:
                    data[key] = d1[key]

            for key in d2:
                if key not in data:
                    data[key] = d2[key]

            return data

        case InputValue.Object(value=data1), InputValue.Object(value=data2):
            return InputValue.Object(merge_input_value_object_metas(data1, data2))

        case val1, _:
            return val1


def normalize_strategy(
    strategy: type[PaginationStrategy] | None = LegacyStrategy, is_subgraph: bool = True
):
    if strategy is not None and is_subgraph:
        return strategy
    return SkipStrategy
