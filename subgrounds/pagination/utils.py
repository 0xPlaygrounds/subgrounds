from operator import itemgetter
from typing import TypeVar, overload

from subgrounds.query import InputValue
from subgrounds.utils import union

DEFAULT_NUM_ENTITIES = 100
PAGE_SIZE = 900

T = TypeVar("T")
Container = list[T] | dict[str, T]


@overload
def merge(data1: list[T], data2: list[T]) -> list[T]:
    ...


@overload
def merge(data1: dict[str, T], data2: dict[str, T]) -> dict[str, T]:
    ...


def merge(data1: Container, data2: Container) -> Container:
    """Merges ``data1`` and ``data2`` and returns the combined result.

    ``data1`` and ``data2`` must be of the same type. Either both are
    ``dict`` or ``list``.

    Args:
      data1 (list[T] | dict[str, T]): First data blob
      data2 (list[T] | dict[str, T]): Second data blob

    Returns:
        list[T] | dict[str, T]: Combined data blob
    """

    match data1, data2:
        case list(l1), list(l2):
            return union(l1, l2, itemgetter("id"), combine=merge)

        case dict(d1), dict(d2):
            data = d1.copy()

            for key in d1:
                if key in d2:
                    data[key] = merge(d1[key], d2[key])

            for key in d2:
                if key not in data:
                    data[key] = d2[key]

            return data

        case (dict(), _) | (_, dict()) | (list(), _) | (_, list()):
            raise TypeError(
                f"merge: incompatible data types!"
                f"type(data1): {type(data1)} != type(data2): {type(data2)}"
            )

        case val1, _:
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
