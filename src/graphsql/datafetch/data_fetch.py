import os
import json
import requests
import hashlib
import time

class DataFetch:
    """
    Fetches data from a GraphQL endpoint and saves the JSON response to a file.
    """
    def __init__(self, endpoint_url, data_dir="./data", retries=3, timeout=10, auth_token=None):
        """
        Initialize DataFetch with endpoint details and configurations.
        :param endpoint_url: URL of the GraphQL endpoint.
        :param data_dir: Directory where JSON responses are stored.
        :param retries: Number of retries on failure.
        :param timeout: Timeout for each request.
        :param auth_token: Optional authentication token for the request.
        """
        self.endpoint_url = endpoint_url
        self.data_dir = data_dir
        self.retries = retries
        self.timeout = timeout
        self.auth_token = auth_token

        os.makedirs(self.data_dir, exist_ok=True)

    def _generate_filename(self, query):
        """
        Generates a unique filename based on the GraphQL query.
        :param query: GraphQL query string.
        :return: Filename for storing the JSON response.
        """
        query_hash = hashlib.md5(query.encode()).hexdigest()[:10]
        timestamp = int(time.time())
        return f"query_{query_hash}_{timestamp}.json"

    def fetch_data(self, query, variables=None):
        """
        Sends a GraphQL query to the endpoint and saves the response to a file.
        :param query: The GraphQL query string.
        :param variables: Optional variables for the query.
        :return: Path to the saved JSON file.
        """
        payload = {"query": query, "variables": variables or {}}
        headers = {"Content-Type": "application/json"}
        
        if self.auth_token:
            headers["Authorization"] = f"{self.auth_token}"
            
        if not query:
            return ""
        
        for attempt in range(1, self.retries + 1):
            try:
                response = requests.post(
                    self.endpoint_url,
                    json=payload,
                    headers=headers,
                    timeout=self.timeout
                )
                response.raise_for_status()
                json_data = response.json()
                
                if "errors" in json_data:
                    raise ValueError(f"GraphQL Error: {json_data['errors']}")
                
                filename = self._generate_filename(query)
                file_path = os.path.join(self.data_dir, filename)
                with open(file_path, "w") as file:
                    json.dump(json_data, file, indent=2)
                
                print(f"✅ Data saved to {file_path}")
                return file_path
            
            except (requests.RequestException, ValueError) as e:
                print(f"⚠️ Attempt {attempt}/{self.retries} failed: {e}")
                if attempt == self.retries:
                    print("❌ All retry attempts failed.")
                    return None
                time.sleep(2 ** attempt)

        return None