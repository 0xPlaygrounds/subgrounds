from __future__ import annotations

import logging
from typing import Generator

from pipe import chain_with, map

from subgrounds.query import DataRequest, DataResponse

from .abcs import DocumentTransform, RequestTransform
from .transforms import DocumentRequestTransform

logger = logging.getLogger("subgrounds")
TransformGen = Generator[DataRequest | DataResponse, DataRequest | DataResponse, None]


def handle_transform(transform: RequestTransform) -> TransformGen:
    req: DataRequest = yield
    resp: DataResponse = yield transform.transform_request(req)
    yield transform.transform_response(req, resp)


def apply_transforms(
    request_transforms: list[RequestTransform],
    document_transforms: dict[str, list[DocumentTransform]],
    req: DataRequest,
) -> Generator[DataRequest | DataResponse, DataResponse, None]:
    """Apply all `RequestTransforms` and `DocumentTransforms` to a `DataRequest` and a
     corresponding `DataResponse`.

    This function abstractly applies a series of transforms onto a request and response.
    The execution of the request is handled outside this function (ala. sans-io), which
     allows this function to only work with the abstracted components.

    Note: For simplification, all `DocumentTransforms` stored at the subgraph are
     converted to specific `RequestTransforms` when applied.
    """

    unique_urls = {doc.url for doc in req.documents}

    # Iterating through all unique document urls, get each document's transforms from
    #  the subgraph converting all `DocumentTransforms` into `DocumentRequestTransforms`
    # We do this to make it easier for us to only work with one type of transform.
    converted_transforms = (
        DocumentRequestTransform(transform, url)
        for url in unique_urls
        for transform in document_transforms[url]
    )

    # Construct our list of generators from our two sets of transforms.
    # The request transforms are before the (converted) document transforms.
    stack: list[TransformGen] = list(
        request_transforms | chain_with(converted_transforms) | map(handle_transform)
    )

    for gen in stack:
        next(gen)  # advance generator
        req: DataRequest = gen.send(req)

    # yield the final transformed request (valid "graphql")
    # retrieve the response (from the `executor` governing this generator)
    resp = yield req

    # Finally, using the response, iterate through the transforms in reverse order,
    #  transforming the raw response up back through the transforms
    for transform in reversed(stack):
        resp: DataResponse = transform.send(resp)

    # Take the final transformed response and send it back to the `executor`
    yield resp
