from subgrounds.query import DataRequest, DataResponse, Document, DocumentResponse


class DocumentTransform:
    """Abstract class representing a transformation layer to be applied to
    :class:`Document` objects.
    """

    def transform_document(self, doc: Document) -> Document:
        """Method that will be applied to all :class:`Document` objects that pass
        through the transformation layer.

        Args:
          doc: The initial document

        Returns:
          The transformed document
        """

        return doc

    def transform_response(
        self, req: Document, resp: DocumentResponse
    ) -> DocumentResponse:
        """Method to be applied to all response data ``data`` of requests that pass
        through the transformation layer.

        ``doc`` is the initial :class:`Document` object that yielded the
        resulting JSON data ``data``.

        Args:
          doc: Initial document
          data: Response resulting from the execution of the transformed document.

        Returns:
          The transformed response data
        """

        return resp


class RequestTransform:
    """Abstract class representing a transformation layer to be applied to entire
    :class:`DataRequest` objects.
    """

    def transform_request(self, req: DataRequest) -> DataRequest:
        """Method that will be applied to all :class:`DataRequest` objects that
        pass through the transformation layer.

        Args:
          req: The initial request object

        Returns:
          The transformed request object
        """

        return req

    def transform_response(self, req: DataRequest, resp: DataResponse) -> DataResponse:
        """Method to be applied to all response data ``data`` of requests that pass
        through the transformation layer.

        ``req`` is the initial :class:`DataRequest` object that yielded the
        resulting JSON data ``data``.

        Args:
          req: Initial data request object
          data: Response resulting from the execution of the transformed data request.

        Returns:
          The transformed response data
        """

        return resp
