""" Small module containing low level functions related to sending
GraphQL http requests.
"""

import logging
from functools import cache
from importlib import resources
from typing import Any

from gql import Client, gql
from gql.transport import AsyncTransport, Transport
from gql.transport.exceptions import TransportQueryError
from gql.transport.requests import RequestsHTTPTransport
from gql.transport.aiohttp import AIOHTTPTransport

from subgrounds.query import Document, DocumentResponse

from .errors import GraphQLError, ServerError
from .utils import default_header

logger = logging.getLogger("subgrounds")


INTROSPECTION_QUERY = (
    resources.files("subgrounds") / "resources" / "introspection.graphgql"
)


@cache
def get_client(transport: AsyncTransport | Transport | None = None):
    # Create a GraphQL client using the defined transport
    return Client(
        transport=transport, fetch_schema_from_transport=False  # type: ignore
    )


def get_schema(url: str, headers: dict[str, Any]) -> dict[str, Any]:
    """Runs the introspection query on the GraphQL API served located at
    :attr:`url` and returns the result. In case of errors, an exception containing
    the error message is thrown.

    Args:
      url: The url of the GraphQL API

    Raises:
      HttpError: If the request response resulted in an error
      ServerError: If server responds back non-json content
      GraphQLError: If the GraphQL query failed or other grapql server errors

    Returns:
      The GraphQL API's schema in JSON
    """

    introspection = gql(INTROSPECTION_QUERY.read_text())
    client = get_client(RequestsHTTPTransport(url, headers))

    try:
        resp = client.execute(introspection)

    except TransportQueryError:
        raise ServerError(
            f"Server ({url}) did not respond with proper JSON"
            f"\nDid you query a proper GraphQL endpoint?"
            f"\n\n{resp.content}"
        )

    return resp["data"]


def query(doc: Document, headers: dict[str, Any] = {}) -> DocumentResponse:
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
    client = get_client()
    resp = requests.post(
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

    except requests.JSONDecodeError:
        raise ServerError(
            f"Server ({doc.url}) did not respond with proper JSON"
            f"\nDid you query a proper GraphQL endpoint?"
            f"\n\n{resp.content}"
        )

    if (data := raw_data.get("data")) is None:
        raise GraphQLError(raw_data.get("errors", "Unknown Error(s) Found"))

    return DocumentResponse(data=data, url=doc.url)
