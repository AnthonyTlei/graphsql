import requests
import json

SPACEX_GRAPHQL_URL = "https://spacex-production.up.railway.app/"

ROOT_SCHEMA_QUERY = """
{
  __schema {
    queryType { name }
    mutationType { name }
    types {
      name
      kind
    }
  }
}
"""

def fetch_root_schema():
    """Fetches the minimal GraphQL schema structure."""
    response = requests.post(SPACEX_GRAPHQL_URL, json={"query": ROOT_SCHEMA_QUERY})
    schema = response.json()

    with open("output/graphql_root_schema.json", "w") as f:
        json.dump(schema, f, indent=2)

    print("Root schema saved.")
    return schema["data"]["__schema"]["types"]

types_list = fetch_root_schema()

TYPE_INTROSPECTION_QUERY = """
query TypeIntrospection($typeName: String!) {
  __type(name: $typeName) {
    name
    kind
    fields {
      name
      type {
        name
        kind
        ofType {
          name
          kind
        }
      }
    }
  }
}
"""

def fetch_type_details(type_name):
    """Fetches details for a specific GraphQL type."""
    response = requests.post(SPACEX_GRAPHQL_URL, json={
        "query": TYPE_INTROSPECTION_QUERY,
        "variables": {"typeName": type_name}
    })
    type_details = response.json()

    with open(f"output/graphql_type_{type_name}.json", "w") as f:
        json.dump(type_details, f, indent=2)

    print(f"Type schema saved for {type_name}")
    return type_details["data"]["__type"]

# Example: Fetch details for 'Rocket'
rocket_schema = fetch_type_details("Rocket")