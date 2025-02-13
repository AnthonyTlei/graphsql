import os

from graphsql.dbapi.connection import GraphSQLConnection

GRAPHQL_ENDPOINT = "https://graphql.anilist.co"

def test_connection():
    """Test establishing a connection and checking schema files."""
    
    conn = GraphSQLConnection(GRAPHQL_ENDPOINT)

    assert os.path.exists(conn.schema_path), "Schema file was not created!"

    schema_hash = os.path.basename(conn.schema_path).split("_")[1].split(".")[0]
    mappings_path = f"schemas/mappings_{schema_hash}.json"
    relations_path = f"schemas/relations_{schema_hash}.json"

    assert os.path.exists(mappings_path), "Mappings file was not created!"
    assert os.path.exists(relations_path), "Relations file was not created!"

    print("✅ Connection test passed!")

if __name__ == "__main__":
    test_connection()