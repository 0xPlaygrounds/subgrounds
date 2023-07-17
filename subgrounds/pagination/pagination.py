""" This module contains the pagination algorithms (both regular and 
iterative) that make use of pagination strategies.
"""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import replace
from typing import Any

from subgrounds.query import Document, DocumentResponse
from subgrounds.schema import SchemaMeta

from .strategies import PaginationStrategy, SkipPagination, StopPagination

PaginateGenerator = Generator[Document, DocumentResponse, None]


class PaginationError(RuntimeError):
    def __init__(self, message: Any, strategy: PaginationStrategy):
        super().__init__(message)
        self.strategy = strategy


def paginate(
    schema: SchemaMeta, doc: Document, pagination_strategy: type[PaginationStrategy]
) -> PaginateGenerator:
    """Executes the request document `doc` based on the GraphQL schema `schema` and
    returns the response as a JSON dictionary.

    Args:
      schema (SchemaMeta): The GraphQL schema on which the request document is based
      doc (Document): The request document

    Returns:
      dict[str, Any]: The response data as a JSON dictionary
    """

    try:
        strategy = pagination_strategy(schema, doc)
        doc, args = strategy.step()

        while True:
            page = yield replace(doc, variables=doc.variables | args)

            try:
                doc, args = strategy.step(page.data)
            except StopPagination:
                break
            except Exception as exn:
                raise PaginationError(exn.args[0], strategy)

    except SkipPagination:
        _ = yield doc  # consistency
