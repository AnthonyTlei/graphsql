import os
import hashlib

from graphsql.introspection.introspection import GraphQLIntrospection
from graphsql.introspection.schema_parser import SchemaParser

from graphsql.dbapi.cursor import GraphSQLCursor 

from urllib.parse import urlparse

class Error(Exception):
    """Generic DBAPI Error."""
    pass

paramstyle = "named"
class GraphSQLConnection:
    """DBAPI-compliant connection for GraphSQL."""

    def __init__(self, endpoint: str, headers: dict = None):
        """
        Initialize a connection to the GraphQL endpoint.
        On initialization, it:
        - Fetches the introspection schema
        - Parses the schema into mappings and relations
        """
        self.endpoint = endpoint
        self.headers = headers or {}
        self._closed = False

        introspection = GraphQLIntrospection(endpoint)
        self.schema_path = introspection.load_schema()

        parsed_url = urlparse(self.endpoint)
        if parsed_url.scheme in ["http", "https", "graphsql"]:
            cleaned_endpoint = parsed_url.netloc + parsed_url.path
        else:
            cleaned_endpoint = self.endpoint
        
        endpoint_hash = hashlib.md5(cleaned_endpoint.encode()).hexdigest()[:10]
        mappings_path = f"schemas/mappings_{endpoint_hash}.json"
        relations_path = f"schemas/relations_{endpoint_hash}.json"
        if not os.path.exists(mappings_path) or not os.path.exists(relations_path):
            schema_parser = SchemaParser(self.schema_path)
            schema_parser.parse()

    def cursor(self):
        """Returns a new cursor object for executing queries."""
        if self._closed:
            raise Exception("Connection is closed.")
        return GraphSQLCursor(self.endpoint, self.headers)

    def close(self):
        """Closes the connection."""
        self._closed = True

    def commit(self):
        """GraphQL is stateless, so commit does nothing."""
        pass

    def rollback(self):
        """GraphQL is stateless, so rollback does nothing."""
        pass

    def __enter__(self):
        """Enable use of 'with' statements."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Automatically close the connection on exit."""
        self.close()
        
def connect(endpoint: str, headers: dict = None):
    """
    SQLAlchemy expects a DBAPI `connect()` function.
    This function returns a `GraphSQLConnection` instance.
    """
    return GraphSQLConnection(endpoint, headers)