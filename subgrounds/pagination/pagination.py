""" This module contains the pagination algorithms (both regular and 
iterative) that make use of pagination strategies.
"""

from __future__ import annotations

from typing import Any, Iterator, Protocol, Type

import subgrounds.client as client
from subgrounds.query import Document
from subgrounds.schema import SchemaMeta
from subgrounds.utils import merge

from .strategies import SkipPagination, StopPagination


class PaginationError(RuntimeError):
    def __init__(self, message: Any, strategy: PaginationStrategy):
        super().__init__(message)
        self.strategy = strategy


class PaginationStrategy(Protocol):
    def __init__(self, schema: SchemaMeta, document: Document) -> None:
        """Initializes the pagination strategy. If there is no need for pagination given
        the provided :class:`Document` ``document``, then the constructor should raise a
        :class:`SkipPagination` exception.

        Args:
            schema (SchemaMeta): The schema of the API against which ``document`` will
              be executed
            document (Document): The query document
        """

    def step(
        self, page_data: dict[str, Any] | None = None
    ) -> tuple[Document, dict[str, Any]]:
        """Returns the new query document and its variables which will be executed to
        get the next page of data.

        If this is the first query made as part of the pagination strategy, then
          ``page_data`` will be ``None``.

        If pagination should be interupted (e.g.: if enough entities have been queried),
        then this method should raise a :class:`StopPagination` exception.

        Args:
            page_data (dict[str, Any] | None, optional): The previous query's response
             data. If this is the first query (i.e.: the first page of data), then it
             will be `None`. Defaults to None.

        Returns:
            tuple[Document, dict[str, Any]]: A tuple `(doc, vars)` where `doc` is the
             query document that will be executed to fetch the next page of data and
             `vars` are the variables for that document.
        """
        ...


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

    data = {}
    for page in paginate_iter(schema, doc, pagination_strategy):
        data = merge(data, page)

        data: dict[str, Any] = {}
        doc, args = strategy.step()

        while True:
            try:
                page_data = client.query(
                    url=doc.url,
                    query_str=doc.graphql,
                    variables=doc.variables | args,
                    headers=headers,
                )
                data = merge(data, page_data)
                doc, args = strategy.step(page_data)
            except StopPagination:
                break
            except Exception as exn:
                raise PaginationError(exn.args[0], strategy)

        return data

    except SkipPagination:
        return client.query(
            doc.url, doc.graphql, variables=doc.variables, headers=headers
        )


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
