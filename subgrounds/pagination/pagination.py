""" This module contains the pagination algorithms (both regular and 
iterative) that make use of pagination strategies.
"""

from __future__ import annotations

from functools import reduce
from typing import Any, Iterator, Type

import subgrounds.client as client
from subgrounds.query import Document
from subgrounds.schema import SchemaMeta
from subgrounds.utils import merge

from .strategies import PaginationStrategy, SkipPagination, StopPagination


class PaginationError(RuntimeError):
    def __init__(self, message: Any, strategy: PaginationStrategy):
        super().__init__(message)
        self.strategy = strategy


def paginate(
    schema: SchemaMeta,
    doc: Document,
    pagination_strategy: Type[PaginationStrategy],
    headers: dict[str, Any],
) -> dict[str, Any]:
    """Executes the request document `doc` based on the GraphQL schema `schema` and
    returns the response as a JSON dictionary.

    Args:
      schema (SchemaMeta): The GraphQL schema on which the request document is based
      doc (Document): The request document

    Returns:
      dict[str, Any]: The response data as a JSON dictionary
    """

    gen = paginate_iter(schema, doc, pagination_strategy, headers)

    return reduce(merge, gen, next(gen))


def paginate_iter(
    schema: SchemaMeta,
    doc: Document,
    pagination_strategy: Type[PaginationStrategy],
    headers: dict[str, Any],
) -> Iterator[dict[str, Any]]:
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
            try:
                page_data = client.query(
                    url=doc.url,
                    query_str=doc.graphql,
                    variables=doc.variables | args,
                    headers=headers,
                )
                yield page_data
                doc, args = strategy.step(page_data)
            except StopPagination:
                break
            except Exception as exn:
                raise PaginationError(exn.args[0], strategy)

    except SkipPagination:
        return client.query(
            doc.url, doc.graphql, variables=doc.variables, headers=headers
        )
