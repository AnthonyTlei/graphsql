from graphsql.translators.sql_parser import SQLParser
from graphsql.datafetch.data_fetch import DataFetch
from graphsql.translators.json_to_tabular import JSONToTabular

import hashlib

class GraphSQLCursor:
    """DBAPI-compliant cursor for executing SQL queries via GraphQL."""

    def __init__(self, endpoint: str, headers: dict = None, output_format="parquet"):
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
        
        endpoint_hash = hashlib.md5(self.endpoint.encode()).hexdigest()[:10]
        mappings_path = f"schemas/mappings_{endpoint_hash}.json"
        relations_path = f"schemas/relations_{endpoint_hash}.json"
        graphql_query = SQLParser(mappings_path=mappings_path, relations_path=relations_path).convert_to_graphql(statement)

        if self.headers and self.headers["Authorization"]:
            json_file_path = DataFetch(self.endpoint, auth_token=self.headers["Authorization"], auth_type="Basic").fetch_data(graphql_query)
        else :
            json_file_path = DataFetch(self.endpoint).fetch_data(graphql_query)

        tabular_file_path = JSONToTabular(output_format=self.output_format,depth_cutoff=5).convert(json_path=json_file_path)

        self._load_results(tabular_file_path)

    def _load_results(self, file_path):
        """Loads the tabular data from file into memory."""
        import pandas as pd

        if self.output_format == "csv":
            df = pd.read_csv(file_path)
        elif self.output_format == "parquet":
            df = pd.read_parquet(file_path)
        elif self.output_format == "jsonl":
            df = pd.read_json(file_path, lines=True)
        else:
            raise ValueError(f"Unsupported format: {self.output_format}")

        self._results = df.to_records(index=False)
        self._description = [(col, None) for col in df.columns]

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