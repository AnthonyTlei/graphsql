from graphsql.translators.sql_parser import SQLParser
from graphsql.datafetch.data_fetch import DataFetch
from graphsql.translators.json_to_tabular import JSONToTabular
from graphsql.dbapi.duckdb import DuckDBSingleton

import hashlib
from urllib.parse import urlparse

class GraphSQLCursor:
    """DBAPI-compliant cursor for executing SQL queries via GraphQL."""

    def __init__(self, endpoint: str, headers: dict = None, output_format="duckdb"):
        """
        Initializes the cursor.

        Args:
            endpoint (str): The GraphQL API URL.
            headers (dict, optional): Authentication headers.
            output_format (str): Format for tabular data (csv, parquet, jsonl).
        """
        self.endpoint = endpoint
        self.headers = headers or {}
        self.output_format = output_format
        self._closed = False
        self._results = None
        self._description = None

    def execute(self, statement, parameters=None, context=None):
        """
        Executes an SQL query by translating it to GraphQL and fetching results.

        Args:
            sql (str): The SQL query to execute.
        """
        if self._closed:
            raise Exception("Cursor is closed.")
        
        parsed_url = urlparse(self.endpoint)
        if parsed_url.scheme in ["http", "https", "graphsql"]:
            cleaned_endpoint = parsed_url.netloc + parsed_url.path
        else:
            cleaned_endpoint = self.endpoint
        endpoint_hash = hashlib.md5(cleaned_endpoint.encode()).hexdigest()[:10]
        mappings_path = f"schemas/mappings_{endpoint_hash}.json"
        relations_path = f"schemas/relations_{endpoint_hash}.json"
        parsed_data = SQLParser(mappings_path=mappings_path, relations_path=relations_path).convert_to_graphql(statement)
        queries = parsed_data.get("queries", [])
        filters = parsed_data.get("filters", {})

        if self.headers and self.headers["Authorization"]:
            json_files_path = DataFetch(self.endpoint, auth_token=self.headers["Authorization"]).fetch_data(queries)
        else :
            json_files_path = DataFetch(self.endpoint).fetch_data(queries)

        table_name = JSONToTabular(output_format=self.output_format,depth_cutoff=5).convert(json_paths=json_files_path)
        
        print("Remaining filters: ", filters)

        self._load_results(table_name)
        
    def _load_results(self, table_name):
        """Loads the tabular data from DuckDB instead of reading from a file."""

        con = DuckDBSingleton.get_connection()
        df = con.execute(f"SELECT * FROM {table_name} LIMIT 5").fetchdf()
        self._results = con.execute(f"SELECT * FROM {table_name}").fetchall()
        self._description = [(col, None) for col in df.columns]

        print("\n✅ Loaded Results (Columns):", df.columns)
        print(df.head())

    def fetchall(self):
        """Returns all rows of the last executed query."""
        if self._results is None:
            raise Exception("No query executed.")
        return list(self._results)

    def fetchone(self):
        """Returns a single row from the last executed query."""
        if self._results is None:
            raise Exception("No query executed.")
        return self._results[0] if self._results else None

    def close(self):
        """Closes the cursor."""
        self._closed = True

    @property
    def description(self):
        """Returns column names like a real DB cursor."""
        return self._description

    def __iter__(self):
        """Allow iteration over results like a real cursor."""
        return iter(self.fetchall())