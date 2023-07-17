from subgrounds.schema import TypeRef

from .base import DocumentTransform, RequestTransform
from .transforms import TypeTransform

DEFAULT_GLOBAL_TRANSFORMS: list[RequestTransform] = []

DEFAULT_SUBGRAPH_TRANSFORMS: list[DocumentTransform] = [
    TypeTransform(
        TypeRef.Named(name="BigDecimal", kind="SCALAR"),
        lambda bigdecimal: float(bigdecimal),
    ),
    TypeTransform(
        TypeRef.Named(name="BigInt", kind="SCALAR"),
        lambda bigint: int(bigint),
    ),
]
