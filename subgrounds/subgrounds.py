""" Toplevel Subgrounds module

This module implements the toplevel API that most developers will be using when
querying The Graph with Subgrounds.
"""

import functools
import json
import logging
import warnings
from dataclasses import dataclass, field
from functools import reduce
from pathlib import Path
from typing import Any, Iterator, Type

import pandas as pd
from pipe import groupby, map, traverse

import subgrounds.client as client

from .dataframe_utils import df_of_json
from .errors import SubgroundsError
from .pagination import LegacyStrategy, PaginationStrategy, normalize_strategy, paginate
from .query import DataRequest, DataResponse, Document, DocumentResponse, Query
from .schema import SchemaMeta
from .subgraph import FieldPath, Subgraph
from .transform import (
    DEFAULT_GLOBAL_TRANSFORMS,
    DEFAULT_SUBGRAPH_TRANSFORMS,
    RequestTransform,
    apply_transforms,
)
from .utils import PLAYGROUNDS_APP_URL

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")


def store_schema(schema: dict[str, Any], path: Path):
    with path.open("w") as f:
        json.dump(schema, f)


def load_schema(path: Path) -> dict[str, Any]:
    with path.open() as f:
        return json.load(f)


def subgraph_slug(url: str) -> str:
    author = url.split("/")[-2]
    name = url.split("/")[-1]
    return f"{author}_{name}"


@dataclass
class Subgrounds:
    headers: dict[str, Any] = field(default_factory=dict)
    global_transforms: list[RequestTransform] = field(
        default_factory=lambda: DEFAULT_GLOBAL_TRANSFORMS
    )
    subgraphs: dict[str, Subgraph] = field(default_factory=dict)

    @classmethod
    def from_pg_key(cls, key: str):
        if not key.startswith("pg-"):
            raise SubgroundsError(
                "Invalid Playgrounds Key: key should start with 'pg-'.\n\n"
                f"Go to {PLAYGROUNDS_APP_URL} to double check your API Key!"
            )

        return cls(headers={"Playgrounds-Api-Key": key})

    def load(
        self,
        url: str,
        save_schema: bool = False,
        cache_dir: str = "schemas/",
        is_subgraph: bool = True,
    ):
        if save_schema:
            cache_path = Path(cache_dir)
            if not cache_path.exists():
                cache_path.mkdir(parents=True)

            schema_path = cache_path / (subgraph_slug(url) + ".json")

            if schema_path.exists():
                schema = load_schema(schema_path)
            else:
                schema = client.get_schema(url, headers=self.headers)
                store_schema(schema, schema_path)

        else:
            schema = client.get_schema(url, headers=self.headers)

        subgraph = Subgraph(
            url,
            SchemaMeta(**schema["__schema"]),
            DEFAULT_SUBGRAPH_TRANSFORMS,
            is_subgraph,
        )
        self.subgraphs[url] = subgraph
        return subgraph

    def load_subgraph(
        self, url: str, save_schema: bool = False, cache_dir: str = "schemas/"
    ) -> Subgraph:
        """Performs introspection on the provided GraphQL API ``url`` to get the
        schema, stores the schema if ``save_schema`` is ``True`` and returns a
        generated class representing the subgraph with all its entities.

        Args:
          url (str): The url of the API
          save_schema (bool, optional): Flag indicating whether or not the schema
            should be cached to disk. Defaults to False.
          cache_dir (str, optional): If ``save_schema == True``, then subgraph schemas
            will be stored under ``cache_dir``. Defaults to ``schemas/``

        Returns:
          Subgraph: A generated class representing the subgraph and its entities
        """

        return self.load(url, save_schema, cache_dir, True)

    def load_api(
        self, url: str, save_schema: bool = False, cache_dir: str = "schemas/"
    ) -> Subgraph:
        """Performs introspection on the provided GraphQL API ``url`` to get the
        schema, stores the schema if ``save_schema`` is ``True`` and returns a
        generated class representing the GraphQL endpoint with all its entities.

        Args:
          url (str): The url of the API
          save_schema (bool, optional): Flag indicating whether or not the schema
            should be saved to disk. Defaults to False.

        Returns:
          Subgraph: A generated class representing the subgraph and its entities
        """

        return self.load(url, save_schema, cache_dir, False)

    def mk_request(self, fpaths: FieldPath | list[FieldPath]) -> DataRequest:
        """Creates a :class:`DataRequest` object by combining one or more
        :class:`FieldPath` objects.

        Args:
          fpaths (FieldPath | list[FieldPath]): One or more :class:`FieldPath`
            objects that should be included in the request

        Returns:
          DataRequest: A new :class:`DataRequest` object
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)

        return DataRequest(
            documents=list(
                fpaths
                | groupby(lambda fpath: fpath._subgraph._url)
                | map(
                    lambda group: Document(
                        url=group[0],
                        query=reduce(
                            Query.add, group[1] | map(FieldPath._selection), Query()
                        ),
                    )
                )
            )
        )

    def execute(
        self,
        req: DataRequest,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> DataResponse:
        """Same as `execute`, except that an iterator is returned which will iterate
        the data pages.

        Args:
          req (DataRequest): The :class:`DataRequest` object to be executed
          pagination_strategy (Type[PaginationStrategy] | None, optional): A Class
            implementing the :class:`PaginationStrategy` ``Protocol``. If ``None``, then
            automatic pagination is disabled. Defaults to :class:`LegacyStrategy`.

        Returns:
          list[dict[str, Any]]: A list over the reponse data pages
        """

        transformer = apply_transforms(
            self.global_transforms,
            {url: subgraph._transforms for url, subgraph in self.subgraphs.items()},
            req,
        )
        strategy = normalize_strategy(pagination_strategy)

        resps = []
        req = next(transformer)
        for doc in req.documents:
            paginator = paginate(self.subgraphs[doc.url]._schema, doc, strategy)
            resp = None
            while True:
                try:
                    paginated_doc = paginator.send(resp)
                except StopIteration:
                    break
                resp = client.query(paginated_doc, headers=self.headers)
                resps.append(resp)

        return transformer.send(DataResponse(responses=resps))

    def execute_iter(
        self,
        req: DataRequest,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Iterator[DocumentResponse]:
        """Same as `execute`, except that an iterator is returned which will iterate
        the data pages.

        Args:
          req (DataRequest): The :class:`DataRequest` object to be executed
          pagination_strategy (Type[PaginationStrategy] | None, optional): A Class
            implementing the :class:`PaginationStrategy` ``Protocol``. If ``None``, then
            automatic pagination is disabled. Defaults to :class:`LegacyStrategy`.

        Returns:
          Iterator[dict]: An iterator over the reponse data pages
        """

        generator = apply_transforms(
            self.global_transforms,
            {url: subgraph._transforms for url, subgraph in self.subgraphs.items()},
            req,
        )
        strategy = normalize_strategy(pagination_strategy)

        for doc in next(generator).documents:
            paginator = paginate(self.subgraphs[doc.url]._schema, doc, strategy)
            resp = None
            while True:
                try:
                    paginated_doc = paginator.send(resp)
                except StopIteration:
                    break
                resp = client.query(paginated_doc, headers=self.headers)
                data: DataResponse = generator.send(DataResponse(responses=[resp]))
                yield data.responses[0]

    def query_json(
        self,
        fpaths: FieldPath | list[FieldPath],
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> list[dict[str, Any]]:
        """Equivalent to
         ``Subgrounds.execute(Subgrounds.mk_request(fpaths), pagination_strategy)``.

        Args:
          fpaths (FieldPath | list[FieldPath]): One or more :class:`FieldPath` objects
            that should be included in the request.
          pagination_strategy (Type[PaginationStrategy] | None, optional): A Class
            implementing the :class:`PaginationStrategy` ``Protocol``. If ``None``, then
            automatic pagination is disabled. Defaults to :class:`LegacyStrategy`.

        Returns:
          list[dict[str, Any]]: The reponse data
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        req = self.mk_request(fpaths)
        return [doc.data for doc in self.execute(req, pagination_strategy).responses]

    def query_json_iter(
        self,
        fpaths: FieldPath | list[FieldPath],
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Iterator[dict[str, Any]]:
        """Same as `query_json` returns an iterator over the response data pages.

        Args:
          fpaths (FieldPath | list[FieldPath]): One or more :class:`FieldPath` objects
            that should be included in the request.
          pagination_strategy (Type[PaginationStrategy] | None, optional): A Class
            implementing the :class:`PaginationStrategy` ``Protocol``. If ``None``, then
            automatic pagination is disabled. Defaults to :class:`LegacyStrategy`.

        Returns:
          list[dict[str, Any]]: The reponse data
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        req = self.mk_request(fpaths)
        yield from (resp.data for resp in self.execute_iter(req, pagination_strategy))

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
          fpaths (FieldPath | list[FieldPath]): One or more `FieldPath` objects that
            should be included in the request.
          columns (list[str] | None, optional): The column labels. Defaults to None.
          merge (bool, optional): Whether or not to merge resulting dataframes.
          pagination_strategy (Type[PaginationStrategy] | None, optional): A Class
            implementing the :class:`PaginationStrategy` ``Protocol``. If ``None``, then
            automatic pagination is disabled. Defaults to :class:`LegacyStrategy`.

        Returns:
          pd.DataFrame | list[pd.DataFrame]: A DataFrame containing the reponse data

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
          fpaths (FieldPath | list[FieldPath]): One or more `FieldPath` objects that
            should be included in the request
          columns (Optional[list[str]], optional): The column labels. Defaults to None.
          merge (bool, optional): Whether or not to merge resulting dataframes.
          pagination_strategy (Optional[Type[PaginationStrategy]], optional): A Class
            implementing the :class:`PaginationStrategy` ``Protocol``. If ``None``, then
            automatic pagination is disabled. Defaults to :class:`LegacyStrategy`.

        Returns:
          Iterator[pd.DataFrame]: An iterator over the response data pages,
            each as a DataFrame
        """

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        for page in self.query_json_iter(
            fpaths, pagination_strategy=pagination_strategy
        ):
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
          fpaths (FieldPath | list[FieldPath]): One or more ``FieldPath`` object(s)
            to query.
          unwrap (bool, optional): Flag indicating whether or not, in the case where
            the returned data is a list of one element, the element itself should be
            returned instead of the list. Defaults to ``True``.
          pagination_strategy (Type[PaginationStrategy] | None, optional): A Class
            implementing the :class:`PaginationStrategy` ``Protocol``. If ``None``, then
            automatic pagination is disabled. Defaults to :class:`LegacyStrategy`.

        Returns:
          [type]: The ``FieldPath`` object(s) data

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
            else:
                return data

        data = tuple(fpaths | map(f))

        if len(data) == 1:
            return data[0]
        else:
            return data

    def query_iter(
        self,
        fpaths: FieldPath | list[FieldPath],
        unwrap: bool = True,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Iterator[str | int | float | bool | list | tuple | None]:
        """Same as `query` except an iterator over the resonse data pages is returned.

        Args:
          fpath (FieldPath | list[FieldPath]): One or more ``FieldPath`` object(s)
            to query.
          unwrap (bool, optional): Flag indicating whether or not, in the case where
            the returned data is a list of one element, the element itself should be
            returned instead of the list. Defaults to ``True``.
          pagination_strategy (Type[PaginationStrategy] | None, optional): A Class
            implementing the :class:`PaginationStrategy` ``Protocol``. If ``None``, then
            automatic pagination is disabled. Defaults to :class:`LegacyStrategy`.

        Returns:
          Iterator[type]: An iterator over the ``FieldPath`` object(s)' data pages
        """

        def f(fpath: FieldPath, blob: dict[str, Any]) -> dict[str, Any]:
            data = fpath._extract_data(blob)
            if type(data) == list and len(data) == 1 and unwrap:
                return data[0]
            else:
                return data

        fpaths = list([fpaths] | traverse | map(FieldPath._auto_select) | traverse)
        for page in self.query_json_iter(
            fpaths, pagination_strategy=pagination_strategy
        ):
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
