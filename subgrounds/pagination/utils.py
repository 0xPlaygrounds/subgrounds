from typing import Any, overload

from subgrounds.query import InputValue
from subgrounds.utils import union

DEFAULT_NUM_ENTITIES = 100
PAGE_SIZE = 900


def merge(
    data1: list[Any] | dict[str, Any] | Any, data2: list[Any] | dict[str, Any] | Any
) -> list[Any] | dict[str, Any] | Any:
    """Merges ``data1`` and ``data2`` and returns the combined result.

    ``data1`` and ``data2`` must be of the same type. Either both are
    ``dict``, ``list`` or anything else.

    Args:
      data1 (list[Any] | dict[str, Any] | Any): First data blob
      data2 (list[Any] | dict[str, Any] | Any): Second data blob

    Returns:
        list[Any] | dict[str, Any] | Any: Combined data blob
    """
    match (data1, data2):
        case (list() as l1, list() as l2):
            return union(l1, l2, lambda data: data["id"], combine=merge)
            # return data1 + data2

        case (dict() as d1, dict() as d2):
            data = {}
            for key in d1:
                if key in d2:
                    data[key] = merge(d1[key], d2[key])
                else:
                    data[key] = d1[key]

            for key in d2:
                if key not in data:
                    data[key] = d2[key]

            return data

        case (dict(), _) | (_, dict()) | (list(), _) | (_, list()):
            raise TypeError(
                f"merge: incompatible data types! type(data1): {type(data1)} != type(data2): {type(data2)}"
            )

        case (val1, _):
            return val1

    assert False  # Suppress mypy missing return statement warning


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
