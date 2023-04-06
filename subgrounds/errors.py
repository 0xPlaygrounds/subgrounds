class SubgroundsError(Exception):
    """The base error for all subgrounds errors"""


class SchemaError(SubgroundsError):
    """Errors related to schema"""


class TransformError(SubgroundsError):
    """Errors related to transforms"""


class ServerError(SubgroundsError):
    """Errors returned by a server"""


class GraphQLError(ServerError):
    """Errors returned by a GraphQL server"""
