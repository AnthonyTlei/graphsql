import os
import pandas as pd

from graphsql.introspection.introspection import GraphQLIntrospection
from graphsql.introspection.schema_parser import SchemaParser
from graphsql.translators.sql_parser import SQLParser
from graphsql.datafetch.data_fetch import DataFetch
from graphsql.translators.json_to_tabular import JSONToTabular

class GraphSQLFlatConnector:
    """
    Connects all components together:
    - Ensures introspection schema, mappings, and relations exist.
    - Translates SQL queries to GraphQL.
    - Fetches GraphQL data.
    - Converts the data into a tabular format for visualization.
    """

    def __init__(self, endpoint, depth_cutoff=2, output_format="parquet"):
        self.endpoint = endpoint.rstrip('/')
        self.depth_cutoff = depth_cutoff
        self.output_format = output_format.lower()
        self.schema_dir = "schemas"
        self.data_dir = "data"
        self.schema_path = ""
        self.schema_filename = ""
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.introspector = GraphQLIntrospection(self.endpoint)
        self._ensure_schema()
        self.schema_hash = self._get_schema_hash()
        self.mappings_path = f"{self.schema_dir}/mappings_{self.schema_hash}.json"
        self.relations_path = f"{self.schema_dir}/relations_{self.schema_hash}.json"
        self._ensure_mappings()
        
        self.sql_parser = SQLParser(mappings_path=self.mappings_path, relations_path=self.relations_path)
        self.data_fetcher = DataFetch(self.endpoint)
        
        self.json_to_tabular = JSONToTabular(depth_cutoff=self.depth_cutoff, output_format=self.output_format)
        
    def _get_schema_hash(self):
        """Retrieves schema hash from introspection filename."""
        schema_filename = self.schema_filename
        return schema_filename.replace("schema_", "").replace(".json", "")

    def _ensure_schema(self):
        """Ensures schema, mappings, and relations exist."""
        schema_path = self.introspector.load_schema()
        self.schema_path = schema_path
        self.schema_filename = os.path.basename(schema_path)
        if not os.path.exists(schema_path):
            raise FileNotFoundError(f"Schema file {schema_path} was not created properly.")

    def _ensure_mappings(self):
        if not os.path.exists(self.mappings_path) or not os.path.exists(self.relations_path):
            schema_parser = SchemaParser(self.schema_path)
            schema_parser.parse()
        if not os.path.exists(self.mappings_path) or not os.path.exists(self.relations_path):
            raise FileNotFoundError("Mappings or relations JSON not found. Run SchemaParser first.")
        print("✅ Schema, mappings, and relations confirmed.")

    def execute_sql(self, sql_query):
        """Executes SQL query by translating and fetching data."""
        print(f"🔍 Processing SQL Query: {sql_query}")
        graphql_query = self.sql_parser.convert_to_graphql(sql_query)
        print(" Query: ", graphql_query)
        file_path = self.data_fetcher.fetch_data(graphql_query)
        
        if not file_path:
            raise RuntimeError("❌ Failed to fetch data from GraphQL endpoint.")
        
        tabular_data_path = self.json_to_tabular.convert(file_path)
        
        return tabular_data_path

if __name__ == "__main__":
    endpoint = "https://spacex-production.up.railway.app/"
    connector = GraphSQLFlatConnector(endpoint, depth_cutoff=2, output_format="parquet")
    
    # test_sql = "SELECT name FROM rockets;"
    test_sql = "SELECT * FROM rockets;"
    # test_sql = "SELECT id, name FROM rockets;"
    # test_sql = "SELECT id, name FROM rocket WHERE id = '5e9d0d95eda69955f709d1eb'"
    # test_sql = "SELECT roles FROM ships"
    # test_sql = "SELECT url, roles FROM ships"
    
    import pandas as pd
    output_path = connector.execute_sql(test_sql)
    df = pd.read_parquet(output_path, engine='pyarrow')
    print(df.head())
