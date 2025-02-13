import requests
import json

# endpoint = "https://spacex-production.up.railway.app/"
endpoint = "https://graphql.anilist.co"

# QUERY = """
# {
#     rockets {
#         active
#         diameter {
#             feet
#             meters
#         }
#   }
# }
# """

QUERY = """
{
    Page {
        media {
            id
            title {
                english
            }
        }
  }
}
"""

def test_spacex_graphql():
    """Tests the endpoint by sending a query and validating the response."""

    print("Sending query to endpoint...")
    response = requests.post(endpoint, json={"query": QUERY})

    if response.status_code != 200:
        print(f"API request failed with status {response.status_code}: {response.text}")
        return
    
    result = response.json()

    if "data" not in result:
        print(f"API response does not contain 'data': {json.dumps(result, indent=2)}")
        return
    
    data = result["data"]
    
    if data is None:
        print("No latest data found.")
        return

    print("Data:")
    print(json.dumps(data, indent=2))

if __name__ == "__main__":
    test_spacex_graphql()