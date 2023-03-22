from __future__ import annotations

import logging
import warnings
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

from subgrounds.schema import TypeMeta

if TYPE_CHECKING:
    from subgrounds.subgraph.fieldpath import FieldPath

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")


@dataclass
class Filter:
    field: TypeMeta.FieldMeta
    op: Filter.Operator
    value: Any

    class Operator(Enum):
        EQ = auto()
        NEQ = auto()
        LT = auto()
        LTE = auto()
        GT = auto()
        GTE = auto()

    @staticmethod
    def mk_filter(fpath: FieldPath, op: Filter.Operator, value: Any) -> Filter:
        match fpath._leaf:
            case TypeMeta.FieldMeta() as fmeta:
                return Filter(fmeta, op, value)
            case _:
                raise TypeError(
                    f"Cannot create filter on FieldPath {fpath}: not a native field!"
                )

    @property
    def name(self):
        match self.op:
            case Filter.Operator.EQ:
                return self.field.name
            case Filter.Operator.NEQ:
                return f"{self.field.name}_not"
            case Filter.Operator.LT:
                return f"{self.field.name}_lt"
            case Filter.Operator.GT:
                return f"{self.field.name}_gt"
            case Filter.Operator.LTE:
                return f"{self.field.name}_lte"
            case Filter.Operator.GTE:
                return f"{self.field.name}_gte"

    @staticmethod
    def to_dict(filters: list[Filter]) -> dict[str, Any]:
        return {f.name: f.value for f in filters}
