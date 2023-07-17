""" Subgrounds request/response transformation layers module

This module defines interfaces (abstract classes) for transformation layers.
Transformation layers, or transforms, can be applied to entire
requests (see :class:`RequestTransform`) or on a per-document basis (see
:class:`DocumentTransform`). Classes that implement either type of transforms
can be used to perform modifications to queries and their response data.

For example, the :class:`TypeTransform` class is used to tranform the response
data of ``BigInt`` and ``BigDecimal`` fields (which are represented as
strings in the response JSON data) to python ``int`` and ``float``
respectively (see the actual transforms in ``DEFAULT_SUBGRAPH_TRANSFORMS``).

Transforms are also used to apply :class:`SyntheticField` to queries and the
response data (see :class:`LocalSyntheticField` transform class). Each
:class:`SyntheticField` defined on a subgraph creates a new transformation layer
by instantiating a new :class:`LocalSyntheticField` object.
"""

import logging
from functools import partial
from typing import Any

from pipe import map, traverse

from subgrounds.query import Selection
from subgrounds.schema import TypeMeta

logger = logging.getLogger("subgrounds")


def select_data(select: Selection, data: dict) -> list[Any]:
    match (select, data):
        case (
            Selection(TypeMeta.FieldMeta(name=name), None, _, [] | None)
            | Selection(TypeMeta.FieldMeta(), name, _, [] | None),
            dict() as data,
        ) if name in data:
            return [data[name]]

        case (
            Selection(TypeMeta.FieldMeta(name=name), None, _, inner_select)
            | Selection(TypeMeta.FieldMeta(), name, _, inner_select),
            dict() as data,
        ) if name in data:
            return list(
                inner_select | map(partial(select_data, data=data[name])) | traverse
            )

        case (select, data):
            raise Exception(f"select_data: invalid selection {select} for data {data}")

    assert False  # Suppress mypy missing return statement warning
