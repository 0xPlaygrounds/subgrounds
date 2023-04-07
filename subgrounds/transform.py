""" Subgrounds request/response transformation layers module

This module defines interfaces (abstract classes) for transformation layers.
Transformation layers, or transforms, can be applied to entire
requests (see :class:`RequestTransform`) or on a per-document basis (see
:class:`DocumentTransform`). Classes that implement either type of transforms
can be used to perform modifications to queries and their response data.

For example, the :class:`TypeTransform` class is used to tranform the response
data of ``BigInt`` and ``BigDecimal`` fields (which are represented as
strings in the response JSON data) to python ``int`` and ``float``
respectively (see the actual transforms in ``DEFAULT_SUBGRAPH_TRANSFORMS``).

Transforms are also used to apply :class:`SyntheticField` to queries and the
response data (see :class:`LocalSyntheticField` transform class). Each
:class:`SyntheticField` defined on a subgraph creates a new transformation layer
by instantiating a new :class:`LocalSyntheticField` object.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from functools import partial
from typing import TYPE_CHECKING, Any, Callable

from pipe import map, traverse

from .errors import TransformError
from .query import DataRequest, Document, Query, Selection
from .schema import TypeMeta, TypeRef
from .utils import flatten, union

if TYPE_CHECKING:
    from .subgraph import Subgraph

logger = logging.getLogger("subgrounds")


def select_data(select: Selection, data: dict) -> list[Any]:
    match (select, data):
        case (
            Selection(TypeMeta.FieldMeta(name=name), None, _, [] | None)
            | Selection(TypeMeta.FieldMeta(), name, _, [] | None),
            dict() as data,
        ) if name in data:
            return [data[name]]

        case (
            Selection(TypeMeta.FieldMeta(name=name), None, _, inner_select)
            | Selection(TypeMeta.FieldMeta(), name, _, inner_select),
            dict() as data,
        ) if name in data:
            return list(
                inner_select | map(partial(select_data, data=data[name])) | traverse
            )

        case (select, data):
            raise TransformError(
                f"select_data: invalid selection {select} for data {data}"
            )

    assert False  # Suppress mypy missing return statement warning


class RequestTransform(ABC):
    """Abstract class representing a transformation layer to be applied to entire
    :class:`DataRequest` objects.
    """

    @abstractmethod
    def transform_request(self, req: DataRequest) -> DataRequest:
        """Method that will be applied to all :class:`DataRequest` objects that
        pass through the transformation layer.

        Args:
          req (DataRequest): The initial request object

        Returns:
          DataRequest: The transformed request object
        """

        return req

    @abstractmethod
    def transform_response(
        self, req: DataRequest, data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Method to be applied to all response data ``data`` of requests that pass
        through the transformation layer.

        ``req`` is the initial :class:`DataRequest` object that yielded the
        resulting JSON data ``data``.

        Args:
          req (DataRequest): Initial data request object
          data (list[dict[str, Any]]): JSON data blob resulting from the execution
            of the transformed data request.

        Returns:
          list[dict[str, Any]]: The transformed response data
        """

        return data


class DocumentTransform(ABC):
    """Abstract class representing a transformation layer to be applied to
    :class:`Document` objects.
    """

    @abstractmethod
    def transform_document(self, doc: Document) -> Document:
        """Method that will be applied to all :class:`Document` objects that pass
        through the transformation layer.

        Args:
          doc (Document): The initial document

        Returns:
          Document: The transformed document
        """

        return doc

    @abstractmethod
    def transform_response(self, req: Document, data: dict[str, Any]) -> dict[str, Any]:
        """Method to be applied to all response data ``data`` of requests that pass
        through the transformation layer.

        ``doc`` is the initial :class:`Document` object that yielded the
        resulting JSON data ``data``.

        Args:
          doc (Document): Initial document
          data (dict[str, Any]): JSON data blob resulting from the execution of the
            transformed document.

        Returns:
          dict[str, Any]: The transformed response data
        """

        return data


class TypeTransform(DocumentTransform):
    """Transform to be applied to scalar fields on a per-type basis.

    Attributes:
      type_ (TypeRef.T): Type indicating which scalar values (i.e.: values of that
        type) should be transformed using the function ``f``
      f (Callable[[Any], Any]): Function to be applied to scalar values of type
        ``type_`` in the response data.
    """

    type_: TypeRef.T
    f: Callable[[Any], Any]

    def __init__(self, type_: TypeRef.T, f: Callable[[Any], Any]) -> None:
        self.type_ = type_
        self.f = f
        super().__init__()

    def transform_document(self: TypeTransform, doc: Document) -> Document:
        return doc

    def transform_response(self, doc: Document, data: dict[str, Any]) -> dict[str, Any]:
        def transform(select: Selection, data: dict[str, Any]) -> None:
            # TODO: Handle NonNull and List more graciously
            #  (i.e.: without using TypeRef.root_type_name)
            match (select, data):
                # Type matches
                case (
                    Selection(
                        TypeMeta.FieldMeta(name=name, type_=ftype), None, _, [] | None
                    )
                    | Selection(
                        TypeMeta.FieldMeta(type_=ftype), str() as name, _, [] | None
                    ),
                    dict() as data,
                ) if TypeRef.root_type_name(self.type_) == TypeRef.root_type_name(
                    ftype
                ):
                    match data[name]:
                        case list() as values:
                            data[name] = list(
                                values
                                | map(
                                    lambda value: self.f(value)
                                    if value is not None
                                    else None
                                )
                            )
                        case None:
                            data[name] = None
                        case _ as value:
                            data[name] = self.f(value)

                case (Selection(_, _, _, [] | None), dict()):
                    pass

                case (
                    Selection(TypeMeta.FieldMeta(name=name), None, _, inner_select)
                    | Selection(TypeMeta.FieldMeta(), str() as name, _, inner_select),
                    dict() as data,
                ):
                    match data[name]:
                        case list() as elts:
                            for elt in elts:
                                for select in inner_select:
                                    transform(select, elt)
                        case dict() as elt:
                            for select in inner_select:
                                transform(select, elt)
                        case None:
                            return None
                        case _:
                            raise TransformError(
                                f"transform_data_type: data for selection {select} is"
                                f" neither list or dict {data[name]}"
                            )

                case (select, data):
                    raise TransformError(
                        f"transform_data_type: invalid selection {select}"
                        f" for data {data}"
                    )

        for select in doc.query.selection:
            transform(select, data)

        return data


class LocalSyntheticField(DocumentTransform):
    """Transform used to implement synthetic fields on GraphQL objects that only
    depend on values accessible from that object.

    :meth:`transform_document` replaces the field :attr:`fmeta` by the argument
    fields selections :attr:`args` if the synthetic field :attr:`fmeta` is present
    in the document.

    :meth:`transform_response` applied :attr:`f` to the fields corresponding to
    the argument selections :attr:`args` and places the result in the response.

    Attributes:
      subgraph (Subgraph): The subgraph to which the synthetic field's object
        belongs.
      fmeta (TypeMeta.FieldMeta): The synthetic field
      type_ (TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta): The object for which
        the synthetic field is defined
      f (Callable): The function to be applied to the argument fields
      default (Any): The default value of the synthetic field used in case of
        exceptions (e.g.: division by zero)
      args (list[Selection]): The selections of the fields used as arguments to
        compute the synthetic field
    """

    subgraph: Subgraph
    fmeta: TypeMeta.FieldMeta
    type_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta
    f: Callable
    default: Any
    args: list[Selection]

    def __init__(
        self,
        subgraph: Subgraph,
        fmeta: TypeMeta.FieldMeta,
        type_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta,
        f: Callable,
        default: Any,
        args: list[Selection],
    ) -> None:
        self.subgraph = subgraph
        self.fmeta = fmeta
        self.type_ = type_
        self.f = f
        self.default = default
        self.args = args

    def transform_document(self, doc: Document) -> Document:
        def transform(select: Selection) -> Selection | list[Selection]:
            match select:
                # case Selection(TypeMeta.FieldMeta(name) as fmeta, _, _, [] | None) if name == self.fmeta.name and fmeta.type_.name == self.type_.name:
                case Selection(
                    TypeMeta.FieldMeta(name=name), _, _, [] | None
                ) if name == self.fmeta.name:
                    return Selection.merge(self.args)
                case Selection(_, _, _, [] | None):
                    return [select]
                case Selection(
                    TypeMeta.FieldMeta(name=name) as select_fmeta,
                    alias,
                    args,
                    inner_select,
                ):
                    new_inner_select = list(inner_select | map(transform) | traverse)
                    return Selection(select_fmeta, alias, args, new_inner_select)
                case _:
                    raise TransformError(
                        f"transform_document: unhandled selection {select}"
                    )

            assert False  # Suppress mypy missing return statement warning

        def transform_on_type(select: Selection) -> Selection:
            match select:
                case Selection(
                    TypeMeta.FieldMeta(type_=type_) as select_fmeta,
                    alias,
                    args,
                    inner_select,
                ) if type_.name == self.type_.name:
                    new_inner_select = Selection.merge(
                        list(inner_select | map(transform) | traverse)
                    )
                    return Selection(select_fmeta, alias, args, new_inner_select)

                case Selection(fmeta, alias, args, inner_select):
                    return Selection(
                        fmeta, alias, args, list(inner_select | map(transform_on_type))
                    )

            assert False  # Suppress mypy missing return statement warning

        if self.subgraph._url == doc.url:
            return Document.transform(
                doc,
                query_f=lambda query: Query.transform(
                    query, selection_f=transform_on_type
                ),
            )
        else:
            return doc

    def transform_response(self, doc: Document, data: dict[str, Any]) -> dict[str, Any]:
        def transform(select: Selection, data: dict) -> None:
            match (select, data):
                case (
                    Selection(TypeMeta.FieldMeta(name=name), None, _, [] | None)
                    | Selection(TypeMeta.FieldMeta(), name, _, [] | None),
                    dict() as data,
                ) if name == self.fmeta.name and name not in data:
                    # Case where the selection selects a the syntheticfield of the
                    #  current transform that is not in the data blob and there are
                    #  no inner selections

                    # Try to grab the arguments to the synthetic field transform in
                    #  the data blob
                    arg_values = flatten(
                        list(self.args | map(partial(select_data, data=data)))
                    )

                    try:
                        data[name] = self.f(*arg_values)
                    except Exception:
                        data[name] = self.default

                case (
                    Selection(TypeMeta.FieldMeta(name=name), None, _, [] | None)
                    | Selection(TypeMeta.FieldMeta(), name, _, [] | None),
                    dict() as data,
                ) if name not in data:
                    # Case where the selection selects a regular field but it is not in
                    #  the data blob (caused by None value at higher selection)
                    data[name] = None

                case (
                    Selection(TypeMeta.FieldMeta(name=name), None, _, [] | None)
                    | Selection(TypeMeta.FieldMeta(), name, _, [] | None),
                    dict() as data,
                ):
                    # Case where the selection selects a regular field and there are
                    #  no inner selections (nothing to do)
                    pass

                case (
                    Selection(TypeMeta.FieldMeta(name=name), None, _, inner_select)
                    | Selection(TypeMeta.FieldMeta(), name, _, inner_select),
                    dict() as data,
                ) if name in data:
                    match data[name]:
                        case list() as elts:
                            for elt in elts:
                                for select in inner_select:
                                    transform(select, elt)

                        case dict() as elt:
                            for select in inner_select:
                                transform(select, elt)

                        case None:
                            data[name] = {}
                            for select in inner_select:
                                transform(select, data[name])

                        case _:
                            raise TransformError(
                                f"transform_response: data for selection {select} is"
                                f" neither list or dict {data[name]}"
                            )

                case (select, data):
                    raise TransformError(
                        f"transform_response: invalid selection {select}"
                        f" for data {data}"
                    )

        def transform_on_type(select: Selection, data: dict) -> None:
            match select:
                case Selection(TypeMeta.FieldMeta(type_=type_), None, _, _) | Selection(
                    TypeMeta.FieldMeta(type_=type_), _, _, _
                ) if type_.name == self.type_.name:
                    # for select in inner_select:
                    #   transform(select, data[name])
                    match data:
                        case list():
                            for d in data:
                                transform(select, d)
                        case dict():
                            transform(select, data)

                case (
                    Selection(TypeMeta.FieldMeta(name=name), None, _, inner_select)
                    | Selection(_, name, _, inner_select)
                ):
                    match data:
                        case list():
                            for d in data:
                                list(
                                    inner_select
                                    | map(partial(transform_on_type, data=d[name]))
                                )
                        case dict():
                            list(
                                inner_select
                                | map(partial(transform_on_type, data=data[name]))
                            )

        if self.subgraph._url == doc.url:
            for select in doc.query.selection:
                transform_on_type(select, data)

        return data


# TODO: Decide if necessary
class SplitTransform(RequestTransform):
    def __init__(self, query: Query) -> None:
        self.query = query

    def transform_request(self, req: DataRequest) -> DataRequest:
        def split(doc: Document) -> list[Document]:
            if Query.contains(doc.query, self.query):
                return [
                    Document(
                        doc.url, Query.remove(doc.query, self.query), doc.fragments
                    ),
                    Document(
                        doc.url, Query.select(doc.query, self.query), doc.fragments
                    ),
                ]
            else:
                return [doc]

        return DataRequest(documents=list(req.documents | map(split) | traverse))

    # TODO: Fix transform_response
    def transform_response(
        self, req: DataRequest, data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        def merge_data(
            data1: dict | list | Any, data2: dict | list | Any
        ) -> dict | list | Any:
            match (data1, data2):
                case (dict() as data1, dict() as data2):
                    return dict(
                        union(
                            list(data1.items()),
                            list(data2.items()),
                            key=lambda item: item[0],
                            combine=lambda item1, item2: (
                                item1[0],
                                merge_data(item1[1], item2[1]),
                            ),
                        )
                    )

                case (list(), list()):
                    return list(
                        zip(data1, data2) | map(lambda tup: merge_data(tup[0], tup[1]))
                    )

                case (value, _):
                    return value

            assert False  # Suppress mypy missing return statement warning

        def transform(
            docs: list[Document], data: list[dict[str, Any]], acc: list[dict[str, Any]]
        ) -> list[dict[str, Any]]:
            match (docs, data):
                case ([doc, *docs_rest], [d1, d2, *data_rest]) if Query.contains(
                    doc.query, self.query
                ):
                    return transform(docs_rest, data_rest, [*acc, merge_data(d1, d2)])

                case ([], []):
                    return acc

            assert False  # Suppress mypy missing return statement warning

        return transform(req.documents, data, [])


DEFAULT_GLOBAL_TRANSFORMS: list[RequestTransform] = []

DEFAULT_SUBGRAPH_TRANSFORMS: list[DocumentTransform] = [
    TypeTransform(
        TypeRef.Named(name="BigDecimal", kind="SCALAR"),
        lambda bigdecimal: float(bigdecimal),
    ),
    TypeTransform(
        TypeRef.Named(name="BigInt", kind="SCALAR"), lambda bigint: int(bigint)
    ),
]
