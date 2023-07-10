""" Toplevel Subgrounds module

This module implements the toplevel API that most developers will be using when
querying The Graph with Subgrounds.
"""

import json
import logging
import warnings
from abc import ABC
from collections.abc import Generator
from dataclasses import dataclass, field
from functools import reduce
from importlib import resources
from pathlib import Path
from typing import Any, Type, cast

from pipe import groupby, map, traverse

from ..errors import SubgroundsError
from ..pagination import (
    LegacyStrategy,
    PaginationStrategy,
    normalize_strategy,
    paginate,
)
from ..query import DataRequest, DataResponse, Document, DocumentResponse, Query
from ..schema import SchemaMeta
from ..subgraph import FieldPath, Subgraph
from ..transform import (
    DEFAULT_GLOBAL_TRANSFORMS,
    DEFAULT_SUBGRAPH_TRANSFORMS,
    RequestTransform,
    apply_transforms,
)
from ..utils import PLAYGROUNDS_APP_URL

logger = logging.getLogger("subgrounds")
warnings.simplefilter("default")


HTTP2_SUPPORT = True

INTROSPECTION_QUERY = (
    resources.files("subgrounds") / "resources" / "introspection.graphql"
)


@dataclass
class SubgroundsBase(ABC):
    """A base instance for all `Subgrounds` (should not be used directly)"""

    headers: dict[str, Any] = field(default_factory=dict)
    global_transforms: list[RequestTransform] = field(
        default_factory=lambda: DEFAULT_GLOBAL_TRANSFORMS.copy()
    )
    subgraphs: dict[str, Subgraph] = field(default_factory=dict)
    schema_cache: Path = Path("schemas/")

    def __post_init__(self):
        self.schema_cache = Path(self.schema_cache)

    @classmethod
    def from_pg_key(cls, key: str, **kwargs: Any):
        """Create a Subgrounds* instance using a playgrounds key directly.

        This sets the `headers` field internally to be used with all queries made out.

        Args:
          key: The aforementioned Playgrounds API Key
          **kwargs: Anything else to construct the Subgrounds* instance

        Returns:
          An instance Subgrounds* with Playgrounds API support baked in
        """

        if not key.startswith("pg-"):
            raise SubgroundsError(
                "Invalid Playgrounds Key: key should start with 'pg-'.\n\n"
                f"Go to {PLAYGROUNDS_APP_URL} to double check your API Key!"
            )

        headers: dict[str, Any] = {"headers": {"Playgrounds-Api-Key": key}}

        if (headers_arg := kwargs.get("headers")) is not None:
            if "Playgrounds-Api-Key" in headers_arg:
                raise TypeError(
                    f"{cls.__name__}.from_pg_key cannot take `headers`"
                    "as a keyword argument."
                )

            headers |= headers_arg

        return cls(**(kwargs | headers))

    @classmethod
    def _subgraph_slug(cls, url: str) -> str:
        *_, author, name = url.split("/")

        return f"{author}_{name}"

    def make_request(self, fpaths: FieldPath | list[FieldPath]) -> DataRequest:
        """Creates a :class:`DataRequest` object by combining one or more
        :class:`FieldPath` objects.

        Args:
          fpaths: One or more :class:`FieldPath` objects that should be included
           in the request

        Returns:
          Brand new request
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

    mk_request = make_request

    def fetch_schema(self, url: str):
        """Grabs schema from filesystem based the subgraph_slug of the url"""

        self.schema_cache.mkdir(parents=True, exist_ok=True)
        schema_path = self.schema_cache / self._subgraph_slug(url)

        if (schema := schema_path.with_suffix(".json")).exists():
            return json.loads(schema.read_text())

    def cache_schema(self, url: str, data: dict[str, Any]):
        """Dumps schema into filesystem based the subgraph_slug of the url"""

        self.schema_cache.mkdir(parents=True, exist_ok=True)
        schema_path = self.schema_cache / self._subgraph_slug(url)

        if (schema := schema_path.with_suffix(".json")).exists():
            schema.write_text(json.dumps(data))
        else:
            raise ValueError(f"Schema at {schema} doesn't exist.")

    def _load(
        self, url: str, save_schema: bool = False, is_subgraph: bool = True
    ) -> Generator[tuple[str, str], dict[str, Any], Subgraph]:
        """Loads thingy into thingy"""

        if not save_schema or (schema := self.fetch_schema(url)) is None:
            # TODO Yield `Document` once we have graphql -> AST converter
            schema = yield url, INTROSPECTION_QUERY.read_text()

            if save_schema:
                self.cache_schema(url, schema)

        self.subgraphs[url] = Subgraph(
            url,
            SchemaMeta.parse_obj(schema["__schema"]),
            DEFAULT_SUBGRAPH_TRANSFORMS,
            is_subgraph,
        )
        return self.subgraphs[url]

    def _execute(
        self,
        req: DataRequest,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Generator[Document, DocumentResponse, DataResponse]:
        """Executes a :class:`DataRequest` and returns a :class:`DataResponse`.

        Args:
          req: The :class:`DataRequest` object to be executed.
          pagination_strategy: A Class implementing the :class:`PaginationStrategy`
            ``Protocol``. If ``None``, then automatic pagination is disabled.
            Defaults to :class:`LegacyStrategy`.

        Returns:
          A :class:`DataResponse` object representing the response
        """

        document_transforms = {
            url: subgraph._transforms for url, subgraph in self.subgraphs.items()
        }
        transformer = apply_transforms(self.global_transforms, document_transforms, req)
        strategy = normalize_strategy(pagination_strategy)

        data_resp = DataResponse(responses=[])
        data_req = cast(DataRequest, next(transformer))

        for doc in data_req.documents:
            paginator = paginate(self.subgraphs[doc.url]._schema, doc, strategy)
            paginated_doc = next(paginator)
            doc_resp = DocumentResponse(url=doc.url, data={})

            while True:
                resp = yield paginated_doc
                doc_resp = doc_resp.combine(resp)

                try:
                    paginated_doc = paginator.send(resp)
                except StopIteration:
                    break

            data_resp = data_resp.add_responses(doc_resp)

        next(transformer)  # toss empty None
        return cast(DataResponse, transformer.send(data_resp))

    def _execute_iter(
        self,
        req: DataRequest,
        pagination_strategy: Type[PaginationStrategy] | None = LegacyStrategy,
    ) -> Generator[Document | DocumentResponse, DocumentResponse, None]:
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

        document_transforms = {
            url: subgraph._transforms for url, subgraph in self.subgraphs.items()
        }
        transformer = apply_transforms(self.global_transforms, document_transforms, req)
        strategy = normalize_strategy(pagination_strategy)

        data_req = cast(DataRequest, next(transformer))
        for doc in data_req.documents:
            paginator = paginate(self.subgraphs[doc.url]._schema, doc, strategy)
            paginated_doc = next(paginator)
            while True:
                resp = yield paginated_doc

                next(transformer)  # toss empty None
                data_resp = cast(
                    DataResponse, transformer.send(DataResponse(responses=[resp]))
                )
                yield data_resp.responses[0]  # will only be one

                try:
                    paginated_doc = paginator.send(resp)
                except StopIteration:
                    break
