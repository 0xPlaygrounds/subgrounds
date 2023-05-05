"""
Utility module for Subgrounds
"""

import platform
from functools import cache
from itertools import accumulate as _accumulate
from itertools import filterfalse
from typing import Any, Callable, Iterator, Optional, Tuple, TypeVar

from pipe import Pipe, map

accumulate = Pipe(_accumulate)


def flatten(t):
    return [item for sublist in t for item in sublist]


def identity(x):
    return x


T = TypeVar("T")
U = TypeVar("U")


def fst(tup: Tuple[T, U]) -> T:
    return tup[0]


def snd(tup: Tuple[T, U]) -> U:
    return tup[1]


# ================================================================
# Set utility functions
# ================================================================
def intersection(
    l1: list[T],
    l2: list[T],
    key: Callable[[T], Any] = identity,
    combine: Callable[[T, T], T] = lambda x, _: x,
) -> list[T]:
    l1_keys, l2_keys = list(l1 | map(key)), list(l2 | map(key))
    l2_in_l1 = list(filter(lambda x: key(x) in l1_keys, l2))
    l1_in_l2 = list(filter(lambda x: key(x) in l2_keys, l1))
    l2_in_l1.sort(key=key)
    l1_in_l2.sort(key=key)

    return list(zip(l1_in_l2, l2_in_l1) | map(lambda tup: combine(tup[0], tup[1])))


def rel_complement(
    l1: list[T], l2: list[T], key: Callable[[T], Any] = identity
) -> list[T]:
    l2_keys = list(l2 | map(key))
    return list(filterfalse(lambda x: key(x) in l2_keys, l1))


def sym_diff(l1: list[T], l2: list[T], key: Callable[[T], Any] = identity) -> list[T]:
    return rel_complement(l1, l2, key) + rel_complement(l2, l1, key)


def union(
    l1: list[T],
    l2: list[T],
    key: Callable[[T], Any] = identity,
    combine: Callable[[T, T], T] = lambda x, _: x,
) -> list[T]:
    return (
        rel_complement(l1, l2, key)
        + intersection(l1, l2, key, combine)
        + rel_complement(l2, l1, key)
    )


# ================================================================
# Misc
# ================================================================
def filter_none(items: list[Optional[T]]) -> list[T]:
    return list(filter(None, items))


@Pipe
def filter_map(items: Iterator[T], mapping: Callable[[T], Optional[T]]) -> Iterator[T]:
    for item in items:
        new_item = mapping(item)
        if new_item is not None:
            yield new_item


def loop_generator(items: list):
    while True:
        for item in items:
            yield item


def repeat(value: T, n: int) -> list[T]:
    return [value for _ in range(n)]


# ================================================================
# Dictionary related utility functions
# ================================================================
def extract_data(
    keys: list[str], data: dict[str, Any] | list[dict[str, Any]]
) -> list[Any] | Any:
    def f(keys: list[str], data: dict | list | Any):
        match keys:
            case []:
                return data
            case [name, *rest]:
                match data:
                    case dict():
                        return f(rest, data[name])
                    case list():
                        return list(data | map(lambda row: f(rest, row[name])))
                    case None:
                        return None
                    case _:
                        raise Exception(
                            f"extract_data: unexpected state! path = {keys}, data = {data}"
                        )

    match data:
        case dict():
            return f(keys, data)
        case list():
            for doc_data in data:
                try:
                    return f(keys, doc_data)
                except KeyError:
                    continue
            raise Exception(f"extract_data: not found! path = {keys}, data = {data}")
        case _:
            raise Exception("extract_data: data is not dict or list")


def flatten_dict(data: dict[str, Any], keys: list[str] = []) -> dict:
    """Takes a dictionary containing key-value pairs where all values are of type
    other than `list` and flattens it such that all key-value pairs in nested dictionaries
    are now at depth 1.

    Args:
      data (dict): Dictionary containing non-list values
      keys (list[str], optional): Keys of `data` if `data` is a nested `dict` (`len(keys)` == depth of `data`). Defaults to [].

    Returns:
      dict: Flat dictionary containing all key-value pairs in `data` and its nested dictionaries
    """
    flat_dict: dict[str, Any] = {}
    for key, value in data.items():
        match value:
            case dict():
                flat_dict = flat_dict | flatten_dict(value, [*keys, key])
            case value:
                flat_dict["_".join([*keys, key])] = value

    return flat_dict


def contains_list(data: dict | list | str | int | float | bool) -> bool:
    """Returns `True` if `data` contains a value of type `list` in its nested data
    and `False` otherwise

    Args:
      data (dict | list | str | int | float | bool): Data

    Returns:
      bool: `True` if `data` contains a list, `False` otherwise
    """

    match data:
        case list():
            return True
        case dict():
            return any(data.values() | map(contains_list))
        case set():
            return any(data | map(contains_list))
        case str() | int() | float() | bool():
            return False


# def columns_of_json(data: dict, keys: list[str] = []) -> list[str]:
#   columns: list[str] = []
#   for key, value in data.items():
#     match value:
#       case dict():
#         columns.append(columns_of_json(value, [*keys, key]))
#       case list():
#         columns.append(reduce(union, value | map(partial(columns_of_json, keys=[*keys, key])) | map(lambda x: list(x | traverse)), []))
#       case value:
#         columns.append('_'.join([*keys, key]))

#   return columns

# ================================================================
# User Agent / Headers
# ================================================================


@cache
def default_header():
    """Contains the default header information for requests made by subgrounds"""

    return {"Content-Type": "application/json", "User-Agent": user_agent()}


@cache
def user_agent():
    """A basic user agent describing the following details:

    - "Subgrounds" (and version)
    - Platform/OS (and architecture)
    - Python Type (and version)

    Examples:
    - Subgrounds/1.1.2 (Darwin; arm64) CPython/3.11.2
    - Subgrounds/1.1.2 (Emscripten; wasm32) CPython/3.10.2

    ⚠️ To override this, construct your :class:`~subgrounds.Subgrounds` with a headers
      parameter with a dictionary containing an empty "User-Agent" key-value pairing.
    """

    from subgrounds import __version__  # avoid import loops

    system = platform.system()
    python = platform.python_implementation()
    python_version = platform.python_version()
    machine = platform.machine()

    return (
        f"Subgrounds/{__version__}"
        f" ({system}; {machine})"
        f" {python}/{python_version}"
    )
