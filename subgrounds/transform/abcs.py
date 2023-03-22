from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any, Protocol

from subgrounds.query import DataRequest, Document


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


class DocumentExecutor(Protocol):
    def __call__(self, doc: Document) -> Iterator[dict[str, Any]]:
        ...
