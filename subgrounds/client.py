""" Small module containing low level functions related to sending
GraphQL http requests.
"""

import logging
from typing import Any

import requests

from .errors import GraphQLError, ServerError
from .utils import default_header

logger = logging.getLogger("subgrounds")


INTROSPECTION_QUERY: str = """
  query IntrospectionQuery {
    __schema {
      queryType { name }
      mutationType { name }
      types {
        ...FullType
      }
      directives {
        name
        description
        locations
        args {
          ...InputValue
        }
      }
    }
  }
  fragment FullType on __Type {
    kind
    name
    description
    fields(includeDeprecated: true) {
      name
      description
      args {
        ...InputValue
      }
      type {
        ...TypeRef
      }
      isDeprecated
      deprecationReason
    }
    inputFields {
      ...InputValue
    }
    interfaces {
      ...TypeRef
    }
    enumValues(includeDeprecated: true) {
      name
      description
      isDeprecated
      deprecationReason
    }
    possibleTypes {
      ...TypeRef
    }
  }
  fragment InputValue on __InputValue {
    name
    description
    type { ...TypeRef }
    defaultValue
  }
  fragment TypeRef on __Type {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
      }
    }
  }
"""


def get_schema(url: str, headers: dict[str, Any]) -> dict[str, Any]:
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

    resp = requests.post(
        url,
        json={"query": INTROSPECTION_QUERY},
        headers=default_header() | headers,
    )

    resp.raise_for_status()

    try:
        raw_data = resp.json()

    except requests.JSONDecodeError:
        raise ServerError(
            f"Server ({url}) did not respond with proper JSON"
            f"\nDid you query a proper GraphQL endpoint?"
            f"\n\n{resp.content}"
        )

    if (data := raw_data.get("data")) is None:
        raise GraphQLError(raw_data.get("errors", "Unknown Error(s) Found"))

    return data


def query(
    url: str,
    query_str: str,
    variables: dict[str, Any] = {},
    headers: dict[str, Any] = {},
) -> dict[str, Any]:
    """Executes the GraphQL query :attr:`query_str` with variables
    :attr:`variables` against the API served at :attr:`url` and returns the
    response data. In case of errors, an exception containing the error message is
    thrown.

    Args:
      url (str): The URL of the GraphQL API
      query_str (str): The GraphQL query string
      variables (dict[str, Any], optional): Variables for the GraphQL query.
        Defaults to {}.

    Raises:
      HttpError: If the request response resulted in an error
      ServerError: If server responds back non-json content
      GraphQLError: If the GraphQL query failed or other grapql server errors

    Returns:
      dict[str, Any]: Response data
    """

    logger.info(f"client.query: url = {url}, variables = {variables}\n{query_str}")
    resp = requests.post(
        url,
        json=(
            {"query": query_str}
            if variables == {}
            else {"query": query_str, "variables": variables}
        ),
        headers=default_header() | headers,
    )

    resp.raise_for_status()

    try:
        raw_data = resp.json()

    except requests.JSONDecodeError:
        raise ServerError(
            f"Server ({url}) did not respond with proper JSON"
            f"\nDid you query a proper GraphQL endpoint?"
            f"\n\n{resp.content}"
        )

    if (data := raw_data.get("data")) is None:
        raise GraphQLError(raw_data.get("errors", "Unknown Error(s) Found"))

    return data
