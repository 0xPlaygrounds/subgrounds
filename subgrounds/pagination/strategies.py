""" Subgrounds pagination strategies

This module implements functions and data structures used to implement the
two ``PaginationStrategies`` that Subgrounds offers.

For both strategies, pagination is done in four steps:

#. Generate one or many :class:`PaginationNode` objects per query document.
   These are tree-like data structures that extract all information about pagination
   fields (i.e.: fields that return lists of entities) while maintaining the nestedness
   relationship between each pagination field (i.e.: which is nested in which).

#. The query document is normalized such that every pagination field in the query has:
   
    #. An ordering (i.e.: ``orderBy`` and ``orderDirection`` are specified)
    #. A ``first`` argument set to the ``firstN`` variable
    #. A ``skip`` argument set to the ``skipN`` variable
    #. A ``where`` filter with the filter name derived from the ordering and the
       value being a variable named ``lastOrderingValueN``
 
   In other words, the query will be transformed in a form which allows Subgrounds
   to paginate automatically by simply setting the set of pagination variables
   (i.e.: ``firstN``, ``skipN`` and ``lastOrderingValueN``) to different
   values. Each field that requires pagination (i.e.: each field that yields a list)
   will have its own set of variables, hence the ``N`` post-fix.
 
   Example:
   The initial query
 
   .. code-block:: none
 
     query {
       items(
         orderBy: timestamp,
         orderDirection: desc,
         first: 10000
       ) {
         foo
       }
     }
 
   will be transformed to
 
   .. code-block:: none
 
     query($first0: Int, $skip0: Int, $lastOrderingValue0: BigInt) {
       items(
         orderBy: timestamp,
         orderDirection: desc,
         first: $first0,
         skip: $skip0,
         where: {
           timestamp_lt: $lastOrderingValue0
         }
       ) {
         foo
       }
     }

#. For each data page, generate the values for the pagination variables (i.e.:
   ``firstN``, ``skipN`` and ```lastOrderingValueN```)

#. If some variables are undefined (i.e.: they are present in the query document,
   but not given a value as part of step 3), then the document is pruned and all
   selections (and sub-selections) containing undefined variables are removed.

Depending on the strategy, the variable values computed at step 3 will change.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from itertools import count
from typing import Any, Callable, Iterator, Literal, Optional, Protocol

from pipe import map, traverse

from subgrounds.query import Document
from subgrounds.schema import SchemaMeta
from subgrounds.utils import extract_data

from .preprocess import (
    PaginationNode,
    generate_pagination_nodes,
    normalize,
    prune_doc,
)

PAGE_SIZE = 900


class StopPagination(Exception):
    """Raise within :class:`PaginationStategy` when pagination is completed, usually
    within :func:`~subgrounds.pagination.pagination.PaginationStrategy.step`.

    Causes :func:`~subgrounds.pagination.pagination.paginate` to be stopped.
    """


class SkipPagination(Exception):
    """Raise within :class:`PaginationStategy` should be skipped, usually within
    :func:`~subgrounds.pagination.pagination.PaginationStrategy.__init___`.

    Causes :func:`~subgrounds.pagination.pagination.paginate` to be skipped.
    """


class PaginationStrategy(Protocol):
    def __init__(self, schema: SchemaMeta, document: Document) -> None:
        """Initializes the pagination strategy. If there is no need for pagination given
        the provided :class:`Document` ``document``, then the constructor should raise a
        :class:`SkipPagination` exception.

        Args:
            schema: The schema of the API against which ``document`` will be executed
            document: The query document
        """
        ...

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
            page_data: The previous query's response data. If this is the first query
              (i.e.: the first page of data), then it will be `None`. Defaults to None.

        Returns:
            tuple: A tuple `(doc, vars)` where `doc` is the query document that will be
              executed to fetch the next page of data and `vars` are the variables for
              that document.
        """
        ...


@dataclass
class LegacyStrategyArgGenerator:
    cursor: list[Cursor]
    active_idx: int = 0

    @dataclass
    class Cursor:
        page_node: PaginationNode

        inner: list[LegacyStrategyArgGenerator.Cursor]
        inner_idx: int = 0

        filter_value: Any = None
        queried_entities: int = 0
        stop: bool = False
        page_count: int = 0
        keys: set[str] = field(default_factory=set)

        def __init__(self, page_node: PaginationNode) -> None:
            self.page_node = page_node
            self.inner = list(page_node.inner | map(LegacyStrategyArgGenerator.Cursor))
            self.reset()

        @property
        def is_leaf(self):
            return len(self.inner) == 0

        def update(self, data: dict[str, Any]) -> None:
            """Moves `self` cursor forward according to previous response data `data`

            Args:
              data: Previous response data

            Raises:
              StopIteration: _description_
            """

            # Current node step
            index_field_data = list(
                extract_data(
                    [
                        *self.page_node.key_path,
                        *self.page_node.filter_field.split("__"),
                    ],
                    data,
                )
                | traverse
            )
            num_entities = len(index_field_data)
            filter_value = index_field_data[-1] if len(index_field_data) > 0 else None

            id_data = list(
                extract_data([*self.page_node.key_path, "id"], data) | traverse
            )
            for key in id_data:
                if key not in self.keys:
                    self.keys.add(key)

            self.page_count = self.page_count + 1
            self.queried_entities = len(self.keys)

            if filter_value:
                self.filter_value = filter_value

            if (
                (self.is_leaf and num_entities < PAGE_SIZE)
                or (not self.is_leaf and num_entities == 0)
                or (self.queried_entities == self.page_node.first_value)
            ):
                raise StopPagination

        def step(self, data: dict[str, Any]) -> None:
            """Updates either ``self`` cursor or inner state machine depending on
            whether the inner state machine has reached its limit

            Args:
              data: _description_
            """
            if self.is_leaf:
                self.update(data)
            else:
                try:
                    self.inner[self.inner_idx].step(data)
                except StopPagination:
                    if self.inner_idx < len(self.inner) - 1:
                        self.inner_idx = self.inner_idx + 1
                    else:
                        self.update(data)
                        self.inner_idx = 0

                    self.inner[self.inner_idx].reset()

        def first_arg_value(self) -> int:
            if self.is_leaf:
                return (
                    self.page_node.first_value - self.queried_entities
                    if self.page_node.first_value - self.queried_entities < PAGE_SIZE
                    else PAGE_SIZE
                )
            else:
                return 1

        def args(self) -> dict[str, Any]:
            """Returns the pagination arguments for the current state of the
             state machine

            Returns:
              _description_
            """

            args = {}
            args[f"first{self.page_node.node_idx}"] = self.first_arg_value()

            args[f"skip{self.page_node.node_idx}"] = (
                self.page_node.skip_value if self.page_count == 0 else 0
            )

            if self.filter_value is not None:
                args[f"lastOrderingValue{self.page_node.node_idx}"] = self.filter_value

            if self.is_leaf:
                return args
            else:
                inner_args = self.inner[self.inner_idx].args()
                return args | inner_args

        def reset(self):
            """Reset state machine"""

            self.inner_idx = 0
            self.filter_value = self.page_node.filter_value
            self.queried_entities = 0
            self.stop = False
            self.page_count = 0
            self.keys = set()

    def __init__(self, pagination_nodes: list[PaginationNode]) -> None:
        self.cursor = list(pagination_nodes | map(LegacyStrategyArgGenerator.Cursor))

    def step(self, page_data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        try:
            if page_data is not None:
                self.cursor[self.active_idx].step(page_data)

            return self.cursor[self.active_idx].args()
        except StopPagination:
            if self.active_idx < len(self.cursor) - 1:
                self.active_idx = self.active_idx + 1
                return self.cursor[self.active_idx].args()
            else:
                raise StopPagination


class LegacyStrategy:
    schema: SchemaMeta
    arg_generator: LegacyStrategyArgGenerator
    normalized_doc: Document

    def __init__(self, schema: SchemaMeta, document: Document) -> None:
        self.schema = schema

        pagination_nodes = generate_pagination_nodes(schema, document)

        if len(pagination_nodes) == 0:
            raise SkipPagination

        self.arg_generator = LegacyStrategyArgGenerator(pagination_nodes)
        self.normalized_doc = normalize(schema, document, pagination_nodes)

    def step(
        self, page_data: dict[str, Any] | None = None
    ) -> tuple[Document, dict[str, Any]]:
        args = self.arg_generator.step(page_data)
        trimmed_doc = prune_doc(self.normalized_doc, args)
        return trimmed_doc, args


@dataclass
class ShallowStrategyArgGenerator:
    @dataclass
    class Cursor:
        """Class used to generate the pagination variables for a given tree of
        ``PaginationNode`` objects.

        Attributes:
          page_node: The ``PaginationNode`` object which this cursor is iterating
            through
          inner: The cursors for nested ``PaginationNodes``, if any
          inner_idx: The index of the inner ``PaginationNode`` through which this cursor
            iterating
          filter_value: The previous page's index value used to query the next data page
            Depends on ``page_node.filter_field``, e.g.: if ``page_node.filter_field``
            is ``timestamp_gt``, then ``filter_value`` will be the highest timestamp
            the entities returned in the previous data page.
          queried_entities: Counter for the total number of queried entities
          stop: Flag indicating whether or not to stop the cursor
          page_count: Counter keeping track of the total number data pages queried
          keys: Set of keys of all queried entities to avoid duplicates

        # TODO: Add GreedyStrategy doc
        """

        page_node: PaginationNode

        inner: list[ShallowStrategyArgGenerator.Cursor]
        inner_idx: int = 0

        filter_value: Any = None
        queried_entities: int = 0
        page_count: int = 0

        @staticmethod
        def from_pagination_node(
            page_node: PaginationNode,
        ) -> ShallowStrategyArgGenerator.Cursor:
            return ShallowStrategyArgGenerator.Cursor(
                page_node=page_node,
                filter_value=page_node.filter_value,
                inner=list(
                    page_node.inner
                    | map(ShallowStrategyArgGenerator.Cursor.from_pagination_node)
                ),
            )

        @property
        def is_leaf(self) -> bool:
            return len(self.inner) == 0

        @property
        def active_cursor(self) -> Optional[ShallowStrategyArgGenerator.Cursor]:
            if len(self.inner) > 0:
                return self.inner[self.inner_idx]
            else:
                return None

        def iter(
            self, only_active: bool = False
        ) -> Iterator[ShallowStrategyArgGenerator.Cursor]:
            yield self
            if only_active and len(self.inner) > 0:
                yield from self.inner[self.inner_idx].iter(only_active=True)
            else:
                for cursor in self.inner:
                    yield from cursor.iter()

        def mapi(
            self,
            map_f: Callable[
                [int, ShallowStrategyArgGenerator.Cursor],
                ShallowStrategyArgGenerator.Cursor,
            ],
            priority: Literal["self"] | Literal["children"] = "self",
            counter: count[int] | None = None,
        ) -> ShallowStrategyArgGenerator.Cursor:
            if counter is None:
                counter = count()

            match priority:
                case "self":
                    new_cursor = map_f(next(counter), self)

                    return ShallowStrategyArgGenerator.Cursor(
                        page_node=new_cursor.page_node,
                        inner=list(
                            new_cursor.inner
                            | map(
                                partial(
                                    ShallowStrategyArgGenerator.Cursor.mapi,
                                    map_f=map_f,
                                    counter=counter,
                                )
                            )
                        ),
                        inner_idx=new_cursor.inner_idx,
                        filter_value=new_cursor.filter_value,
                        queried_entities=new_cursor.queried_entities,
                        page_count=new_cursor.page_count,
                    )

                case "children":
                    i = next(counter)

                    new_cursor = ShallowStrategyArgGenerator.Cursor(
                        page_node=self.page_node,
                        inner=list(
                            self.inner
                            | map(
                                partial(
                                    ShallowStrategyArgGenerator.Cursor.mapi,
                                    map_f=map_f,
                                    counter=counter,
                                )
                            )
                        ),
                        inner_idx=self.inner_idx,
                        filter_value=self.filter_value,
                        queried_entities=self.queried_entities,
                        page_count=self.page_count,
                    )

                    return map_f(i, new_cursor)

                case _:
                    raise Exception(f"Cursor.mapi: invalid priority {priority}")

    cursor: list[Cursor]

    def __init__(self, pagination_nodes: list[PaginationNode]) -> None:
        self.cursor = list(
            pagination_nodes
            | map(ShallowStrategyArgGenerator.Cursor.from_pagination_node)
        )

    def iter_cursors(self) -> Iterator[Cursor]:
        for cur in self.cursor:
            yield from cur.iter()

    @staticmethod
    def update_cursor(cursor: Cursor, data: dict[str, Any]) -> Cursor:
        index_field_data = list(
            extract_data(
                [*cursor.page_node.key_path, cursor.page_node.filter_field], data
            )
            | traverse
        )
        num_entities = len(index_field_data)
        filter_value = index_field_data[-1] if len(index_field_data) > 0 else None

        new_cursor = ShallowStrategyArgGenerator.Cursor(
            page_node=cursor.page_node,
            inner=cursor.inner,
            inner_idx=0,
            filter_value=filter_value,
            queried_entities=cursor.queried_entities + num_entities,
            page_count=cursor.page_count + 1,
        )

        if (
            new_cursor.queried_entities >= new_cursor.page_node.first_value
            or num_entities < PAGE_SIZE
        ):
            raise StopPagination

        return new_cursor

    def step(self, page_data: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        if page_data is not None:
            self.cursor = list(
                self.cursor
                | map(
                    partial(
                        ShallowStrategyArgGenerator.Cursor.mapi,
                        map_f=lambda i, cursor: (
                            ShallowStrategyArgGenerator.update_cursor(cursor, page_data)
                            if i == 0
                            else cursor
                        ),
                    )
                )
            )

        args = {}
        for cur in self.iter_cursors():
            args[f"first{cur.page_node.node_idx}"] = (
                cur.page_node.first_value - cur.queried_entities
                if cur.page_node.first_value - cur.queried_entities < PAGE_SIZE
                else PAGE_SIZE
            )

            args[f"skip{cur.page_node.node_idx}"] = (
                cur.page_node.skip_value if cur.page_count == 0 else 0
            )

            if cur.filter_value is not None:
                args[f"lastOrderingValue{cur.page_node.node_idx}"] = cur.filter_value

        return args


class ShallowStrategy:
    schema: SchemaMeta
    arg_generator: ShallowStrategyArgGenerator
    normalized_doc: Document

    def __init__(self, schema: SchemaMeta, document: Document) -> None:
        self.schema = schema

        pagination_nodes = generate_pagination_nodes(schema, document)

        if len(pagination_nodes) == 0:
            raise SkipPagination

        self.arg_generator = ShallowStrategyArgGenerator(pagination_nodes)
        self.normalized_doc = normalize(schema, document, pagination_nodes)

    def step(
        self, page_data: dict[str, Any] | None = None
    ) -> tuple[Document, dict[str, Any]]:
        args = self.arg_generator.step(page_data)
        trimmed_doc = prune_doc(self.normalized_doc, args)
        return (trimmed_doc, args)


class SkipStrategy:
    """This strategy always raises a :class:`~subgrounds.pagination.SkipPagination`
      when constructed or if *somehow* `step` is called.

    This is the default if `None` is passed to the strategy (default for non-subgraphs).
    """

    def __init__(self, schema: SchemaMeta, document: Document) -> None:
        raise SkipPagination

    def step(
        self, page_data: Optional[dict[str, Any]] = None
    ) -> tuple[Document, dict[str, Any]]:
        raise StopPagination


def normalize_strategy(
    strategy: type[PaginationStrategy] | None = LegacyStrategy,
    is_subgraph: bool = True,
):
    if strategy is not None and is_subgraph:
        return strategy

    return SkipStrategy
