import json
import hashlib
import os
import requests

class DataFetch:
    def __init__(self, endpoint, output_dir="data/", auth_token=None):
        self.endpoint = endpoint
        self.output_dir = output_dir
        self.auth_token = auth_token
        os.makedirs(self.output_dir, exist_ok=True)

    def _generate_filename(self, query):
        """Generate a unique filename based on query hash."""
        query_hash = hashlib.md5(query.encode()).hexdigest()
        return os.path.join(self.output_dir, f"response_{query_hash}.json")

    def _save_json(self, filepath, data):
        """Save JSON response to file."""
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)

    def fetch_data(self, data):
        """
        Executes a list of GraphQL queries sequentially.

        Args:
            data

        Returns:
            list: Filepaths of the stored JSON responses.
        """
        filepaths = []
        queries = data["queries"]
        
        for query in queries:
            payload = {"query": query}
            headers = {"Content-Type": "application/json"}
            if self.auth_token:
                headers["Authorization"] = f"{self.auth_token}"
            response = requests.post(
                    self.endpoint,
                    json=payload,
                    headers=headers,
                )
            if response.status_code == 200:
                result = response.json()
                filepath = self._generate_filename(query)
                self._save_json(filepath, result)
                filepaths.append(filepath)
            else:
                print(f"Query failed: {response.status_code}\n{response.text}")
                filepaths.append(None)

        print("Fetched Data Files: ", filepaths)
        return filepaths