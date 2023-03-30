"""
Utility module for Subgrounds
"""

import os
import platform
import warnings
from functools import cache
from itertools import accumulate as _accumulate
from itertools import filterfalse
from operator import itemgetter
from typing import Any, Callable, Iterator, Optional, Tuple, TypeVar, overload

from pipe import Pipe, map

PLAYGROUNDS_APP_URL = "https://app.playgrounds.network/"
PLAYGROUNDS_API_URL = "https://api.playgrounds.network/"
PLAYGROUNDS_ENV_VAR = "PLAYGROUNDS_API_KEY"

accumulate = Pipe(_accumulate)


def flatten(t):
    return [item for sublist in t for item in sublist]


def identity(x):
    return x


T = TypeVar("T")
Container = list[T] | dict[str, T]


def fst(tup: Tuple[T, Any]) -> T:
    return tup[0]


def snd(tup: Tuple[Any, T]) -> T:
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
                            f"extract_data: unexpected state!"
                            f" path = {keys}, data = {data}"
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
def default_header(url: str):
    """Contains the default header information for requests made by subgrounds

    Inserts the Playgrounds API Key iff:
      a) targetting the Playgrounds API
      AND
      b) if the `PLAYGROUNDS_API_KEY` environment variable is set
    """

    base = {"Content-Type": "application/json", "User-Agent": user_agent()}

    if url.startswith(PLAYGROUNDS_API_URL) and PLAYGROUNDS_ENV_VAR in os.environ:
        key = os.environ[PLAYGROUNDS_ENV_VAR]
        if key.startswith("pg-"):
            return base | {"Playgrounds-Api-Key": key}

        warnings.warn(
            RuntimeWarning(
                "Invalid Playgrounds Api Key at $PLAYGROUNDS_API_KEY:"
                " Playgrounds api keys should start with 'pg-'.\n\n"
                f"Go to {PLAYGROUNDS_APP_URL} to double check your API Key!"
            )
        )

    return base


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
