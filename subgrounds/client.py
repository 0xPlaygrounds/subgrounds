""" Small module containing low level functions related to sending
GraphQL http requests.
"""

import logging
from importlib import resources
from json import JSONDecodeError
from typing import Any

import httpx

from subgrounds.query import Document, DocumentResponse

from .errors import GraphQLError, ServerError
from .utils import default_header

logger = logging.getLogger("subgrounds")


INTROSPECTION_QUERY = (
    resources.files("subgrounds") / "resources" / "introspection.graphql"
)


def get_schema(
    url: str, client: httpx.Client, headers: dict[str, Any] = {}
) -> dict[str, Any]:
    """Runs the introspection query on the GraphQL API served localed at
    :attr:`url` and returns the result. In case of errors, an exception containing
    the error message is thrown.

    Args:
      url (str): The url of the GraphQL API

    Raises:
      HttpError: If the request response resulted in an error
      ServerError: If server responds back non-json content
      GraphQLError: If the GraphQL query failed or other grapql server errors

    Returns:
      dict[str, Any]: The GraphQL API's schema in JSON
    """

    resp = client.post(
        url,
        json={"query": INTROSPECTION_QUERY.read_text()},
        headers=default_header(url) | headers,
    )

    resp.raise_for_status()

    try:
        raw_data = resp.json()

    except JSONDecodeError:
        raise ServerError(
            f"Server ({url}) did not respond with proper JSON"
            f"\nDid you query a proper GraphQL endpoint?"
            f"\n\n{resp.content}"
        )

    if (data := raw_data.get("data")) is None:
        raise GraphQLError(raw_data.get("errors", "Unknown Error(s) Found"))

    return data


async def async_get_schema(
    url: str, client: httpx.AsyncClient, headers: dict[str, Any] = {}
) -> dict[str, Any]:
    """Runs the introspection query on the GraphQL API served localed at
    :attr:`url` and returns the result. In case of errors, an exception containing
    the error message is thrown.

    Args:
      url (str): The url of the GraphQL API

    Raises:
      HttpError: If the request response resulted in an error
      ServerError: If server responds back non-json content
      GraphQLError: If the GraphQL query failed or other grapql server errors

    Returns:
      dict[str, Any]: The GraphQL API's schema in JSON
    """

    resp = await client.post(
        url,
        json={"query": INTROSPECTION_QUERY.read_text()},
        headers=default_header(url) | headers,
    )

    resp.raise_for_status()

    try:
        raw_data = resp.json()

    except JSONDecodeError:
        raise ServerError(
            f"Server ({url}) did not respond with proper JSON"
            f"\nDid you query a proper GraphQL endpoint?"
            f"\n\n{resp.content}"
        )

    if (data := raw_data.get("data")) is None:
        raise GraphQLError(raw_data.get("errors", "Unknown Error(s) Found"))

    return data


def query(
    doc: Document, client: httpx.Client, headers: dict[str, Any] = {}
) -> DocumentResponse:
    """Executes the GraphQL query :attr:`query_str` with variables
    :attr:`variables` against the API served at :attr:`url` and returns the
    response data. In case of errors, an exception containing the error message is
    thrown.

    Args:
      url: The URL of the GraphQL API
      query_str: The GraphQL query string
      variables: Variables for the GraphQL query. Defaults to {}.

    Raises:
      HttpError: If the request response resulted in an error
      ServerError: If server responds back non-json content
      GraphQLError: If the GraphQL query failed or other grapql server errors

    Returns:
      Response data
    """

    logger.info(
        f"client.query: url = {doc.url}, variables = {doc.variables}\n{doc.graphql}"
    )

    resp = client.post(
        doc.url,
        json=(
            {"query": doc.graphql}
            | ({"variables": doc.variables} if doc.variables else {})
        ),
        headers=default_header(doc.url) | headers,
    )

    resp.raise_for_status()

    try:
        raw_data = resp.json()

    except JSONDecodeError:
        raise ServerError(
            f"Server ({doc.url}) did not respond with proper JSON"
            f"\nDid you query a proper GraphQL endpoint?"
            f"\n\n{resp.content}"
        )

    if (data := raw_data.get("data")) is None:
        raise GraphQLError(raw_data.get("errors", "Unknown Error(s) Found"))

    return DocumentResponse(data=data, url=doc.url)


async def async_query(
    doc: Document, client: httpx.AsyncClient, headers: dict[str, Any] = {}
) -> DocumentResponse:
    """Executes the GraphQL query :attr:`query_str` with variables
    :attr:`variables` against the API served at :attr:`url` and returns the
    response data. In case of errors, an exception containing the error message is
    thrown.

    Args:
      url: The URL of the GraphQL API
      query_str: The GraphQL query string
      variables: Variables for the GraphQL query. Defaults to {}.

    Raises:
      HttpError: If the request response resulted in an error
      ServerError: If server responds back non-json content
      GraphQLError: If the GraphQL query failed or other grapql server errors

    Returns:
      Response data
    """

    logger.info(
        f"client.query: url = {doc.url}, variables = {doc.variables}\n{doc.graphql}"
    )

    resp = await client.post(
        doc.url,
        json=(
            {"query": doc.graphql}
            | ({"variables": doc.variables} if doc.variables else {})
        ),
        headers=default_header(doc.url) | headers,
    )

    resp.raise_for_status()

    try:
        raw_data = resp.json()

    except JSONDecodeError:
        raise ServerError(
            f"Server ({doc.url}) did not respond with proper JSON"
            f"\nDid you query a proper GraphQL endpoint?"
            f"\n\n{resp.content}"
        )

    if (data := raw_data.get("data")) is None:
        raise GraphQLError(raw_data.get("errors", "Unknown Error(s) Found"))

    return DocumentResponse(data=data, url=doc.url)
