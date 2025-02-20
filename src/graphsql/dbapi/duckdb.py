import duckdb

class DuckDBSingleton:
    _instance = None

    @staticmethod
    def get_connection():
        """Returns a single shared DuckDB connection."""
        if DuckDBSingleton._instance is None:
            DuckDBSingleton._instance = duckdb.connect(database=":memory:")
        return DuckDBSingleton._instance