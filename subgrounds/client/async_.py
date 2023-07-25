""" Toplevel Subgrounds module

This module implements the toplevel API that most developers will be using when
querying The Graph with Subgrounds.
"""

import logging
import warnings
from functools import cached_property
from json import JSONDecodeError
from typing import Any, Type

import httpx
import pandas as pd
from pipe import map, traverse

from ..dataframe_utils import df_of_json
from ..errors import GraphQLError, ServerError
from ..pagination import LegacyStrategy, PaginationStrategy
from ..query import DataRequest, DataResponse, DocumentResponse
from ..subgraph import FieldPath, Subgraph
from ..utils import default_header
from .base import SubgroundsBase

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")

HTTP2_SUPPORT = True


class AsyncSubgrounds(SubgroundsBase):
    @cached_property
    def _client(self):
        """Cached client"""

        return httpx.AsyncClient(http2=HTTP2_SUPPORT, timeout=self.timeout)

    async def load(self, url: str, save_schema: bool = False, is_subgraph: bool = True):
        """Performs introspection on the provided GraphQL API ``url`` to get the
        schema, stores the schema if ``save_schema`` is ``True`` and returns a
        generated class representing the GraphQL endpoint with all its entities.
        """

        try:
            loader = self._load(url, save_schema, is_subgraph)
            url, query = next(loader)  # if this fails, schema is loaded from cache
            data = await self._fetch(url, {"query": query})
            loader.send(data)

        except StopIteration as e:
            return e.value

        assert False

    async def load_subgraph(self, url: str, save_schema: bool = False) -> Subgraph:
        """Performs introspection on the provided GraphQL API ``url`` to get the
        schema, stores the schema if ``save_schema`` is ``True`` and returns a
        generated class representing the subgraph with all its entities.

        Args:
          url: The url of the API
          save_schema: Flag indicating whether or not the schema should be cached to
            disk. Defaults to False.
          cache_dir: If ``save_schema == True``, then subgraph schemas will be stored
            under ``cache_dir``. Defaults to ``schemas/``

        Returns:
          Subgraph: A generated class representing the subgraph and its entities
        """

        return await self.load(url, save_schema, True)

    async def load_api(self, url: str, save_schema: bool = False) -> Subgraph:
        """Performs introspection on the provided GraphQL API ``url`` to get the
         schema, stores the schema if ``save_schema`` is ``True`` and returns a
         generated class representing the GraphQL endpoint with all its entities.

        Args:
          url: The url of the API
          save_schema: Flag indicating whether or not the schema should be saved
           to disk. Defaults to False.

        Returns:
          A generated class representing the subgraph and its entities
        """

        return await self.load(url, save_schema, False)

    async def execute(
        self,
        req: DataRequest,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> DataResponse:
        """Executes a :class:`DataRequest` and returns a :class:`DataResponse`.

        Args:
          req: The :class:`DataRequest` object to be executed
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
                data = await self._fetch(
                    doc.url, {"query": doc.graphql, "variables": doc.variables}
                )
                doc = executor.send(DocumentResponse(url=doc.url, data=data))

        except StopIteration as e:
            return e.value

    async def query_json(
        self,
        fpaths: FieldPath | list[FieldPath],
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> list[dict[str, Any]]:
        """See :func:`~subgrounds.Subgrounds.query_json`.

        Args:
          fpaths: One or more :class:`FieldPath` objects that should be included in
            the request.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          The reponse data
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        req = self.mk_request(fpaths)
        data = await self.execute(req, pagination_strategy)

        return [doc.data for doc in data.responses]

    async def query_df(
        self,
        fpaths: FieldPath | list[FieldPath],
        columns: list[str] | None = None,
        concat: bool = False,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> pd.DataFrame | list[pd.DataFrame]:
        """See :func:`~subgrounds.Subgrounds.query_df`.

        Args:
          fpaths: One or more `FieldPath` objects that should be included in the request
          columns: The column labels.
          merge: Whether or not to merge resulting dataframes.
          pagination_strategy: A class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.

        Returns:
          A :class:`pandas.DataFrame` containing the reponse data.
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        json_data = await self.query_json(fpaths, pagination_strategy)

        return df_of_json(json_data, fpaths, columns, concat)

    async def query(
        self,
        fpaths: FieldPath | list[FieldPath],
        unwrap: bool = True,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> str | int | float | bool | list | tuple | None:
        """See :func:`~subgrounds.Subgrounds.query`.

        Args:
          fpaths: One or more ``FieldPath`` object(s) to query.
          unwrap: Flag indicating whether or not, in the case where the returned data
            is a list of one element, the element itself should be returned instead of
            the list. Defaults to ``True``.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          The ``FieldPath`` object(s) data
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        blob = await self.query_json(fpaths, pagination_strategy=pagination_strategy)

        def f(fpath: FieldPath) -> dict[str, Any]:
            data = fpath._extract_data(blob)

            if type(data) == list and len(data) == 1 and unwrap:
                return data[0]

            return data

        data = tuple(fpaths | map(f))

        if len(data) == 1:
            return data[0]

        return data

    async def _fetch(self, url: str, blob: dict[str, Any]) -> dict[str, Any]:
        resp = await self._client.post(
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

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *args):
        await self._client.__aexit__(*args)
        return self
