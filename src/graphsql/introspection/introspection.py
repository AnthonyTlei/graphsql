import requests
import json
import os
import hashlib
import importlib.resources as pkg_resources

class GraphQLIntrospection:
    """
    Handles fetching and caching of GraphQL schemas via introspection.
    """
    def __init__(self, endpoint, schema_dir="schemas"):
        """
        Initializes the introspection handler.
        :param endpoint: GraphQL endpoint URL.
        :param schema_dir: Directory where schemas are stored.
        """
        self.endpoint = endpoint.rstrip('/')
        self.schema_dir = schema_dir
        os.makedirs(self.schema_dir, exist_ok=True)
        self.schema_filename = self._generate_schema_filename()
        self.schema_path = os.path.join(self.schema_dir, self.schema_filename)

    def _generate_schema_filename(self):
        """
        Generates a unique filename based on the endpoint URL.
        :return: Hashed filename for schema storage.
        """
        endpoint_hash = hashlib.md5(self.endpoint.encode()).hexdigest()[:10]
        return f"schema_{endpoint_hash}.json"

    def fetch_schema(self):
        """
        Fetches the GraphQL schema via introspection and saves it.
        :return: Parsed schema types.
        """
        
        schema_file = pkg_resources.files("graphsql.introspection").joinpath("introspection_query.graphql")

        with schema_file.open("r") as file:
            introspection_query_str = file.read()
          
        response = requests.post(self.endpoint, json={"query": introspection_query_str})
        response.raise_for_status()
        schema = response.json()

        if "data" not in schema or "__schema" not in schema["data"]:
            raise ValueError("Invalid schema response from GraphQL endpoint.")
        
        with open(self.schema_path, "w") as file:
            json.dump(schema, file, indent=2)
        
        print(f"✅ GraphQL schema saved to {self.schema_path}")
        return schema

    def load_schema(self):
        """
        Loads the cached schema from file if available, otherwise fetches a new one.
        :return: Parsed schema types.
        """
        if os.path.exists(self.schema_path):
            self.schema_path
            return self.schema_path
            
        print("⚠️ No cached schema found. Fetching...")
        self.fetch_schema()
        return self.schema_path
