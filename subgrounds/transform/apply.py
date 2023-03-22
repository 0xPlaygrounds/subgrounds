from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from subgrounds.query import DataRequest, Document

from .abcs import DocumentExecutor, RequestTransform
from .transforms import DocumentTransform

if TYPE_CHECKING:
    from subgrounds.subgraph import Subgraph

logger = logging.getLogger("subgrounds")


def apply_document_transform(
    transforms: list[DocumentTransform],
    doc: Document,
    executor: DocumentExecutor,
) -> Iterator[dict[str, Any]]:
    logger.debug(f"execute_iter.transform_doc: doc = \n{doc.graphql}")
    match transforms:
        case []:
            yield from executor(doc)

        case [transform, *rest]:
            new_doc = transform.transform_document(doc)
            for data in apply_document_transform(rest, new_doc, executor):
                yield transform.transform_response(doc, data)


def apply_request_transform(
    subgraphs: dict[str, Subgraph],
    transforms: list[RequestTransform],
    req: DataRequest,
    executor: DocumentExecutor,
) -> Iterator[dict[str, Any]]:
    match transforms:
        case []:
            for doc in req.documents:
                yield from apply_document_transform(
                    subgraphs[doc.url]._transforms,
                    doc,
                    executor,
                )

        case [transform, *rest]:
            new_req = transform.transform_request(req)
            for data in apply_request_transform(subgraphs, rest, new_req, executor):
                yield transform.transform_response(req, data)
