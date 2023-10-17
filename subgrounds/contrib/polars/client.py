import polars as pl
import warnings
from functools import cached_property
from json import JSONDecodeError
from typing import Any, Type

import httpx
from pipe import map, traverse

from subgrounds.client import SubgroundsBase
from subgrounds.contrib.polars import utils
from subgrounds.errors import GraphQLError, ServerError
from subgrounds.pagination import LegacyStrategy, PaginationStrategy
from subgrounds.query import DataRequest, DataResponse, DocumentResponse
from subgrounds.subgraph import FieldPath, Subgraph
from subgrounds.utils import default_header

HTTP2_SUPPORT = True


class PolarsSubgrounds(SubgroundsBase):
    """TODO: Write comment"""

    @cached_property
    def _client(self):
        """Cached client"""

        return httpx.Client(http2=HTTP2_SUPPORT, timeout=self.timeout)

    def load(
        self,
        url: str,
        save_schema: bool = False,
        cache_dir: str | None = None,
        is_subgraph: bool = True,
    ) -> Subgraph:
        if cache_dir is not None:
            warnings.warn("This will be depreciated", DeprecationWarning)

        try:
            loader = self._load(url, save_schema, is_subgraph)
            url, query = next(loader)  # if this fails, schema is loaded from cache
            data = self._fetch(url, {"query": query})
            loader.send(data)

        except StopIteration as e:
            return e.value

        assert False

    def load_subgraph(
        self, url: str, save_schema: bool = False, cache_dir: str | None = None
    ) -> Subgraph:
        """Performs introspection on the provided GraphQL API ``url`` to get the
        schema, stores the schema if ``save_schema`` is ``True`` and returns a
        generated class representing the subgraph with all its entities.

        Args:
          url The url of the API.
          save_schema: Flag indicating whether or not the schema should be cached to
            disk.

        Returns:
          Subgraph: A generated class representing the subgraph and its entities
        """

        return self.load(url, save_schema, cache_dir, True)

    def _fetch(self, url: str, blob: dict[str, Any]) -> dict[str, Any]:
        resp = self._client.post(
            url, json=blob, headers=default_header(url) | self.headers
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

    def execute(
        self,
        req: DataRequest,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> DataResponse:
        """Executes a :class:`DataRequest` and returns a :class:`DataResponse`.

        Args:
          req: The :class:`DataRequest` object to be executed.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          A :class:`DataResponse` object representing the response
        """

        try:
            executor = self._execute(req, pagination_strategy)

            doc = next(executor)
            while True:
                data = self._fetch(
                    doc.url, {"query": doc.graphql, "variables": doc.variables}
                )
                doc = executor.send(DocumentResponse(url=doc.url, data=data))

        except StopIteration as e:
            return e.value

    def query_json(
        self,
        fpaths: FieldPath | list[FieldPath],
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> list[dict[str, Any]]:
        """Equivalent to
         ``Subgrounds.execute(Subgrounds.mk_request(fpaths), pagination_strategy)``.

        Args:
          fpaths: One or more :class:`FieldPath` objects
            that should be included in the request.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          The reponse data
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        req = self.mk_request(fpaths)
        data = self.execute(req, pagination_strategy)
        return [doc.data for doc in data.responses]

    def query_df(
        self,
        fpaths: FieldPath | list[FieldPath],
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> pl.DataFrame:
        """
        Queries and converts raw GraphQL data to a Polars DataFrame.

        Args:
            fpaths (FieldPath or list[FieldPath]): One or more FieldPath objects that
            should be included in the request.
            pagination_strategy (Type[PaginationStrategy] or None, optional):
            A class implementing the PaginationStrategy Protocol. If None, then automatic
            pagination is disabled. Defaults to LegacyStrategy.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the queried data.
        """

        # Query raw GraphQL data
        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        graphql_data = self.query_json(fpaths, pagination_strategy=pagination_strategy)

        # Get the first key of the first JSON object. This is the key that contains the data.
        json_data_key = list(graphql_data[0].keys())[0]

        # Convert the JSON data to a Polars DataFrame
        graphql_df = pl.from_dicts(
            graphql_data[0][json_data_key], infer_schema_length=None
        )

        # Apply the formatting to the Polars DataFrame if necessary
        # graphql_df = utils.format_dictionary_columns(graphql_df)

        return graphql_df
