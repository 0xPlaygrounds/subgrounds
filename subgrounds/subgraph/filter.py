from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from enum import Enum, auto
from functools import reduce
from typing import TYPE_CHECKING, Any

from subgrounds.schema import TypeMeta
from subgrounds.utils import merge

if TYPE_CHECKING:
    from subgrounds.subgraph import FieldPath

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")


@dataclass
class Filter:
    fields: list[TypeMeta.FieldMeta]
    op: Filter.Operator
    value: Any

    class Operator(Enum):
        EQ = auto()
        NEQ = auto()
        LT = auto()
        LTE = auto()
        GT = auto()
        GTE = auto()

    @classmethod
    def mk_filter(cls, fpath: FieldPath, op: Filter.Operator, value: Any) -> Filter:
        match fpath._path:
            case [(_, TypeMeta.FieldMeta()), *_] as path:  # assert atleast len of 1
                return cls([tup[1] for tup in path], op, value)
            case _:
                raise TypeError(
                    f"Cannot create filter on FieldPath {fpath}: not a native field!"
                )

    @property
    def leaf_name(self):
        leaf = self.fields[-1].name
        match self.op:
            case Filter.Operator.EQ:
                return leaf
            case Filter.Operator.NEQ:
                return f"{leaf}_not"
            case Filter.Operator.LT:
                return f"{leaf}_lt"
            case Filter.Operator.GT:
                return f"{leaf}_gt"
            case Filter.Operator.LTE:
                return f"{leaf}_lte"
            case Filter.Operator.GTE:
                return f"{leaf}_gte"

    @classmethod
    def to_dict(cls, filters: list[Filter]) -> dict[str, Any]:
        """Converts a series of filters into a single dict.

        Each filter represents a single key, value pair where the key is generated based
          on the operation (via :func:`~subgrounds.subgraph.filter.Filter.leaf_name`).
          For nested filters, this base dict is further stacked under sub-objects:

        ```py
        {"outer_": {"outer2_": {"inner_op": "value"}}}
        ```
        """

        return reduce(
            merge,
            (
                reduce(
                    lambda x, y: {f"{y.name}_": x},
                    filter.fields[:-1],
                    {filter.leaf_name: filter.value},
                )
                for filter in filters
            ),
            {},
        )
