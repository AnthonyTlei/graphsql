import hashlib
import json
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.dialects import registry
from graphsql.dbapi.connection import GraphSQLConnection

from sqlalchemy.engine import reflection
from sqlalchemy.types import Integer, String, Boolean, Float, JSON

from urllib.parse import urlparse

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
    
    def _load_json(self, path):
        with open(path, "r") as f:
            return json.load(f)
    
    @reflection.cache
    def get_schema_names(self, connection, **kw):
        """
        Return a list of "schemas" available. 
        For many databases, there's a system query here.
        For GraphQL, you can either return a single dummy schema or multiple if you want.
        """
        print("Getting Schemas")
        # If your GraphQL doesn't have a concept of schemas, just return one
        return ["main"]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        """
        Return a list of "tables". Superset uses this to populate the table dropdown in SQL Lab.
        For your case, you might load them from your introspection or from the `mappings.json`.
        """
        endpoint_url = str(connection.engine.url)
        parsed_url = urlparse(endpoint_url)
        if parsed_url.scheme in ["http", "https", "graphsql"]:
            cleaned_endpoint = parsed_url.netloc + parsed_url.path
        else:
            cleaned_endpoint = endpoint_url
        endpoint_hash = hashlib.md5(cleaned_endpoint.encode()).hexdigest()[:10]
        mappings_path = f"schemas/mappings_{endpoint_hash}.json"
        relations_path = f"schemas/relations_{endpoint_hash}.json"
        mappings = self._load_json(mappings_path)
        relations = self._load_json(relations_path)
        
        # Suppose you have something like mappings = {"Page": {...}, "Media": {...}}
        # This'll return the keys as "table" names.
        print(mappings)
        return list(mappings.keys())

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        """
        If you want to treat certain GraphQL objects as 'views', return them here.
        Otherwise, return an empty list.
        """
        print("Getting Views")
        return []
    
    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Return a list of indexes for the given table.
        Each index is a dict:
        {
            "name": <index name>,
            "column_names": [list of columns],
            "unique": <boolean if unique>
        }
        If you have no concept of indexes, return [].
        """
        return []
    
    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Return information about the primary key constraint on `table_name`.
        If there's no real PK in GraphQL, just return an empty constraint dict.
        """
        return {
            "constrained_columns": [],
            "name": None
        }
        
    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Return information about foreign_keys in a table.
        If you have no concept of FKs, just return an empty list.
        """
        return []

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        print("Getting Columns for:", table_name)
        endpoint_url = str(connection.engine.url)
        parsed_url = urlparse(endpoint_url)
        if parsed_url.scheme in ["http", "https", "graphsql"]:
            cleaned_endpoint = parsed_url.netloc + parsed_url.path
        else:
            cleaned_endpoint = endpoint_url
        endpoint_hash = hashlib.md5(cleaned_endpoint.encode()).hexdigest()[:10]
        mappings_path = f"schemas/mappings_{endpoint_hash}.json"
        relations_path = f"schemas/relations_{endpoint_hash}.json"

        mappings = self._load_json(mappings_path)
        relations = self._load_json(relations_path)

        table_mappings = mappings.get(table_name, {})
        table_relations = relations.get(table_name, [])

        columns = []

        # A) Scalar fields
        for col_name, col_gql_type in table_mappings.items():
            sa_type = self._map_graphql_to_sa_type(col_gql_type)
            columns.append({
                "name": col_name,
                "type": sa_type,
                "nullable": True,
                "default": None,
            })

        # B) Relationship fields
        for rel in table_relations:
            field_name = rel.get("field")
            relation_type = rel.get("relation")
            if not field_name:
                continue

            # "one-to-many", "many-to-many", etc. as JSON
            if relation_type in ["one-to-many", "many-to-many"]:
                sa_type = JSON()
            else:
                sa_type = JSON()

            columns.append({
                "name": field_name,
                "type": sa_type,
                "nullable": True,
                "default": None,
            })

        print(f"Columns for {table_name}: {columns}")
        return columns

    def _map_graphql_to_sa_type(self, gql_type):
        """Map a GraphQL scalar type to a SQLAlchemy type instance."""
        gql_lower = gql_type.lower()
        if gql_lower in ["int", "id", "number"]:
            return Integer()
        elif gql_lower in ["float", "double", "decimal"]:
            return Float()
        elif gql_lower in ["boolean", "bool"]:
            return Boolean()
        else:
            return String()

    def _load_json(self, path):
        """Load JSON from a file, handle errors gracefully."""
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading JSON from {path}: {e}")
            return {}
    
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