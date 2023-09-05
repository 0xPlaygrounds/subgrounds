""" Toplevel Subgrounds module

This module implements the toplevel API that most developers will be using when
querying The Graph with Subgrounds.
"""

import functools
import logging
import warnings
from collections.abc import Iterator
from contextlib import suppress
from functools import cached_property
from json import JSONDecodeError
from typing import Any, Type, cast

import httpx
import pandas as pd
from pipe import map, traverse

from ..dataframe_utils import df_of_json
from ..errors import GraphQLError, ServerError
from ..pagination import LegacyStrategy, PaginationStrategy
from ..query import DataRequest, DataResponse, Document, DocumentResponse
from ..subgraph import FieldPath, Subgraph
from ..utils import default_header
from .base import SubgroundsBase

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")

HTTP2_SUPPORT = True


class Subgrounds(SubgroundsBase):
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

    def load_api(
        self, url: str, save_schema: bool = False, cache_dir: str | None = None
    ) -> Subgraph:
        """Performs introspection on the provided GraphQL API ``url`` to get the
         schema, stores the schema if ``save_schema`` is ``True`` and returns a
         generated class representing the GraphQL endpoint with all its entities.

        Args:
          url: The url of the API.
          save_schema: Flag indicating whether or not the schema should be saved
           to disk.

        Returns:
          A generated class representing the subgraph and its entities
        """

        return self.load(url, save_schema, cache_dir, False)

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

    def execute_iter(
        self,
        req: DataRequest,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Iterator[DocumentResponse]:
        """Same as `execute`, except that an iterator is returned which will iterate
        the data pages.

        Args:
          req: The :class:`DataRequest` object to be executed
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
           ``Protocol``. If ``None``, then automatic pagination is disabled.
           Defaults to :class:`LegacyStrategy`.

        Returns:
          An iterator over the :class:`DocumentResponse` pages.

        ⚠️ DOES NOT apply global transforms across multiple documents or their pages.
         Since we yield each page as we get it, it's not possible to accurately perform
         the transforms since we don't collect the pages. This means transforms
         expecting multiple documents or pages of documents will be inaccurate.
        """

        with suppress(StopIteration):
            executor = self._execute_iter(req, pagination_strategy)

            while True:
                doc = cast(Document, next(executor))
                data = self._fetch(
                    doc.url, {"query": doc.graphql, "variables": doc.variables}
                )

                yield cast(
                    DocumentResponse,
                    executor.send(DocumentResponse(url=doc.url, data=data)),
                )

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

    def query_json_iter(
        self,
        fpaths: FieldPath | list[FieldPath],
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Iterator[dict[str, Any]]:
        """Same as `query_json` returns an iterator over the response data pages.

        Args:
          fpaths: One or more :class:`FieldPath` objects that should be included
            in the request.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          list[dict[str, Any]]: The reponse data
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        req = self.mk_request(fpaths)
        responses = self.execute_iter(req, pagination_strategy)

        for resp in responses:
            yield resp.data

    def query_df(
        self,
        fpaths: FieldPath | list[FieldPath],
        columns: list[str] | None = None,
        concat: bool = False,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> pd.DataFrame | list[pd.DataFrame]:
        """Same as :func:`Subgrounds.query` but formats the response data into a
        Pandas DataFrame. If the response data cannot be flattened to a single query
        (e.g.: when querying multiple list fields that return different entities),
        then multiple dataframes are returned

        ``fpaths`` is a list of :class:`FieldPath` objects that indicate which
        data must be queried.

        ``columns`` is an optional argument used to rename the dataframes(s)
        columns. The length of ``columns`` must be the same as the number of columns
        of *all* returned dataframes.

        ``concat`` indicates whether or not the resulting dataframes should be
        concatenated together. The dataframes must have the same number of columns,
        as well as the same column names and types (the names can be set using the
        ``columns`` argument).

        Args:
          fpaths: One or more `FieldPath` objects that should be included in the request
          columns: The column labels. Defaults to None.
          merge: Whether or not to merge resulting dataframes.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          A :class:`pandas.DataFrame` containing the reponse data.

        Example:

        .. code-block:: python

            >>> from subgrounds import Subgrounds
            >>> sg = Subgrounds()
            >>> univ3 = sg.load_subgraph(
            ...    'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

            # Define price SyntheticField
            >>> univ3.Swap.price = abs(univ3.Swap.amount0) / abs(univ3.Swap.amount1)

            # Query last 10 swaps from the ETH/USDC pool
            >>> eth_usdc = univ3.Query.swaps(
            ...     orderBy=univ3.Swap.timestamp,
            ...     orderDirection='desc',
            ...     first=10,
            ...     where=[
            ...         univ3.Swap.pool == '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
            ...     ]
            ... )
            >>> sg.query_df([
            ...     eth_usdc.timestamp,
            ...     eth_usdc.price
            ... ])
              swaps_timestamp  swaps_price
            0       1643213811  2618.886394
            1       1643213792  2618.814281
            2       1643213792  2617.500494
            3       1643213763  2615.458495
            4       1643213763  2615.876574
            5       1643213739  2615.352390
            6       1643213678  2615.205713
            7       1643213370  2614.115746
            8       1643213210  2613.077301
            9       1643213196  2610.686563
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        json_data = self.query_json(fpaths, pagination_strategy=pagination_strategy)

        return df_of_json(json_data, fpaths, columns, concat)

    def query_df_iter(
        self,
        fpaths: FieldPath | list[FieldPath],
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Iterator[pd.DataFrame | list[pd.DataFrame]]:
        """Same as `query_df` except returns an iterator over the response data pages

        Args:
          fpaths: One or more `FieldPath` objects that should be included in the request
          columns: The column labels. Defaults to None.
          merge: Whether or not to merge resulting dataframes.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          An iterator over the response data pages, each as a :class:`pandas.DataFrame`.
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        for page in self.query_json_iter(fpaths, pagination_strategy):
            yield df_of_json(page, fpaths, None, False)

    def query(
        self,
        fpaths: FieldPath | list[FieldPath],
        unwrap: bool = True,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> str | int | float | bool | list | tuple | None:
        """Executes one or multiple ``FieldPath`` objects immediately and returns the
        data (as a tuple if multiple ``FieldPath`` objects are provided).

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

        Example:

        .. code-block:: python

          >>> from subgrounds import Subgrounds
          >>> sg = Subgrounds()
          >>> univ3 = sg.load_subgraph(
          ...  'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')

          # Define price SyntheticField
          >>> univ3.Swap.price = abs(univ3.Swap.amount0) / abs(univ3.Swap.amount1)

          # Construct FieldPath to get price of last swap on ETH/USDC pool
          >>> eth_usdc_last = univ3.Query.swaps(
          ...     orderBy=univ3.Swap.timestamp,
          ...     orderDirection='desc',
          ...     first=1,
          ...     where=[
          ...         univ3.Swap.pool == '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
          ...     ]
          ... ).price

          # Query last price FieldPath
          >>> sg.query(eth_usdc_last)
          2628.975030015892
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        blob = self.query_json(fpaths, pagination_strategy=pagination_strategy)

        def f(fpath: FieldPath) -> dict[str, Any]:
            data = fpath._extract_data(blob)

            if type(data) == list and len(data) == 1 and unwrap:
                return data[0]

            return data

        data = tuple(fpaths | map(f))

        if len(data) == 1:
            return data[0]

        return data

    def query_iter(
        self,
        fpaths: FieldPath | list[FieldPath],
        unwrap: bool = True,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Iterator[str | int | float | bool | list[Any] | tuple | None]:
        """Same as `query` except an iterator over the resonse data pages is returned.

        Args:
          fpath: One or more ``FieldPath`` object(s) to query.
          unwrap: Flag indicating whether or not, in the case where
            the returned data is a list of one element, the element itself should be
            returned instead of the list. Defaults to ``True``.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          An iterator over the ``FieldPath`` object(s)' data pages
        """

        def f(fpath: FieldPath, blob: dict[str, Any]) -> dict[str, Any]:
            data = fpath._extract_data(blob)

            if type(data) == list and len(data) == 1 and unwrap:
                return data[0]

            return data

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        for page in self.query_json_iter(fpaths, pagination_strategy):
            data = tuple(fpaths | map(functools.partial(f, blob=page)))

            if len(data) == 1:
                yield data[0]
            else:
                yield data
            data = tuple(fpaths | map(functools.partial(f, blob=page)))

            if len(data) == 1:
                yield data[0]
            else:
                yield data

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

    def __enter__(self):
        self._client.__enter__()
        return self

    def __exit__(self, *args):
        self._client.__exit__(*args)
        return self
