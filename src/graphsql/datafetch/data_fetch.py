import json
import hashlib
import os
import requests

class DataFetch:
    def __init__(self, endpoint, output_dir="data/"):
        self.endpoint = endpoint
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def _generate_filename(self, query, operation):
        """Generate a unique filename based on query hash and operation."""
        query_hash = hashlib.md5(query.encode()).hexdigest()
        return os.path.join(self.output_dir, f"response_{query_hash}_{operation.lower()}.json")

    def _save_json(self, filepath, data, operation="DISPLAY", limit=None, order_by_col=None, order_by_dir=None):
        """Save JSON response to file, adding 'operation' field if necessary."""
        if operation != "DISPLAY":
            data["operation"] = operation
        if limit != None:
            data["limit"] = limit
        if order_by_col and order_by_dir:
            data["order_by_col"] = order_by_col
            data["order_by_dir"] = order_by_dir
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    def fetch_data(self, data):
        """
        Executes a list of GraphQL queries sequentially.

        Args:
            queries_with_operations (list of tuples): [(query, operation)]

        Returns:
            list: Filepaths of the stored JSON responses.
        """
        filepaths = []
        queries_with_operations = data["queries"]
        limit = data.get("limit", None)
        order_by_column = data.get("order_by_col", None)
        order_by_dir = data.get("order_by_dir", None)
        
        for query_tuple in queries_with_operations:
            if isinstance(query_tuple, tuple) and len(query_tuple) == 2:
                query, operation = query_tuple
            else:
                print(f"Invalid query format: {query_tuple}. Skipping.")
                filepaths.append(None)
                continue
            
            operation = operation.upper() if operation else "DISPLAY"
            
            response = requests.post(self.endpoint, json={"query": query})
            if response.status_code == 200:
                result = response.json()
                filepath = self._generate_filename(query, operation)
                self._save_json(filepath, result, operation, limit=limit, order_by_col=order_by_column, order_by_dir=order_by_dir)
                filepaths.append(filepath)
            else:
                print(f"Query failed ({operation}): {response.status_code}\n{response.text}")
                filepaths.append(None)

        print("Fetched Data Files: ", filepaths)
        return filepaths