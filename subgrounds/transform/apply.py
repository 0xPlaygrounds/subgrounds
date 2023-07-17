from __future__ import annotations

import logging
from typing import Generator, cast

from pipe import chain_with, map

from subgrounds.query import DataRequest, DataResponse

from .base import DocumentTransform, RequestTransform
from .transforms import DocumentRequestTransform

logger = logging.getLogger("subgrounds")
TransformGen = Generator[DataRequest | DataResponse, DataRequest | DataResponse, None]


def handle_transform(
    transform: RequestTransform,
) -> Generator[None | DataRequest | DataResponse, DataRequest | DataResponse, None]:
    """This function bundles the transform request and response as a generator.

    The follow is as follows:
    -> :class:`subgrounds.query.DataRequest` is sent in
    -> It is transformed, and then sent out
    -> Then, in an infinite loop:
      -> :class:`subgrounds.query.DataResponse` is sent in
      -> It is transformed, then sent out

    The infinite loop allows us to continously transform responses,
      - Needed by `execute_iter`
    """

    req = cast(DataRequest, (yield))
    yield transform.transform_request(req)

    while True:
        resp = cast(DataResponse, (yield))
        yield transform.transform_response(req, resp)


def apply_transforms(
    request_transforms: list[RequestTransform],
    document_transforms: dict[str, list[DocumentTransform]],
    req: DataRequest,
) -> Generator[None | DataRequest | DataResponse, DataResponse, None]:
    """Apply all `RequestTransforms` and `DocumentTransforms` to a `DataRequest` and a
     corresponding `DataResponse`.

    This function abstractly applies a series of transforms onto a request and response.
    The execution of the request is handled outside this function (ala. sans-io), which
     allows this function to only work with the abstracted components.

    Note: For simplification, all `DocumentTransforms` stored at the subgraph are
     converted to specific `RequestTransforms` when applied.
    """

    unique_doc_urls = {doc.url for doc in req.documents}

    # Iterating through all unique document urls, get each document's transforms from
    #  the subgraph converting all `DocumentTransforms` into `DocumentRequestTransforms`
    # We do this to make it easier for us to only work with one type of transform.
    converted_transforms = (
        DocumentRequestTransform(transform, url)
        for url in unique_doc_urls
        for transform in document_transforms[url]
    )

    # Construct our list of generators from our two sets of transforms.
    # The request transforms are before the (converted) document transforms.
    stack: list[TransformGen] = list(
        request_transforms | chain_with(converted_transforms) | map(handle_transform)
    )

    # Go through every transform in the stack and transform the request
    for transform in stack:
        next(transform)  # ditch `None`
        req = cast(DataRequest, transform.send(req))

    # yield the final transformed request (valid "graphql")
    yield req

    # We enter an infinite loop here to allow multiple documents to be transformed
    #  back up the stack since `execute_iter` produces them page-by-page to be streamed.
    while True:
        # retrieve the response (from the `executor` governing this generator)
        resp = yield

        # Finally, using the response, iterate through the transforms in reverse order,
        #  transforming the raw response up back through the transforms
        for transform in reversed(stack):
            next(transform)  # ditch `None`
            resp = cast(DataResponse, transform.send(resp))

        # Take the final transformed response and send it back to the `executor`
        yield resp
