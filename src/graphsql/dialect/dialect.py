from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.dialects import registry
from graphsql.dbapi.connection import GraphSQLConnection

class GraphSQLDialect(DefaultDialect):
    """Custom SQLAlchemy dialect for GraphSQL."""

    name = "graphsql"
    driver = "graphsql_dbapi"
    supports_alter = False
    supports_sequences = False
    supports_native_enum = False
    supports_native_decimal = False
    preexecute_autoincrement_sequences = False
    postfetch_lastrowid = False
    supports_multivalues_insert = True
    
    @classmethod
    def import_dbapi(cls):
        """Return the DBAPI implementation."""
        from graphsql.dbapi import connection
        return connection

    @classmethod
    def dbapi(cls):
        """Return the DBAPI implementation."""
        from graphsql.dbapi import connection
        return connection

    def create_connect_args(self, url):
        """
        Parse the SQLAlchemy connection URL and return args for GraphSQLConnection.
        Example URL: graphsql://graphql-endpoint
        If ?is_http=1 is present in the URL's query string, then "http" will be used
        instead of "https".
        """
        is_http = url.query.get("is_http", "0")
        scheme = "http" if is_http == "1" else "https"
        
        endpoint = f"{scheme}://{url.host}"
        if url.port:
            endpoint += f":{url.port}"
        
        headers = {}
        if "auth" in url.query:
            headers["Authorization"] = url.query["auth"]
            
        return (endpoint,), {"headers": headers}

    def do_execute(self, cursor, statement, parameters, context=None):
        """
        Execute a statement using GraphSQLCursor.
        """
        cursor.execute(statement, parameters)
        
    def do_ping(self, dbapi_connection):
        """Handles Superset's test connection process."""
        return True

registry.register("graphsql", "graphsql.dialect", "GraphSQLDialect")