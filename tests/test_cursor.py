import os

from graphsql.dbapi.connection import GraphSQLConnection

endpoint = "https://graphql.anilist.co"
sql_query = "SELECT media.id, media.title.english FROM Page"
hash = "c0ca28bd42"

def test_graphsql_cursor():
    """Test if GraphSQLCursor can execute queries and fetch results."""
    
    schema_path = f"schemas/schema_{hash}.json"
    mappings_path = f"schemas/mappings_{hash}.json"
    relations_path = f"schemas/relations_{hash}.json"

    assert os.path.exists(schema_path), f"Schema file missing: {schema_path}"
    assert os.path.exists(mappings_path), f"Mappings file missing: {mappings_path}"
    assert os.path.exists(relations_path), f"Relations file missing: {relations_path}"

    print("✅ Schema files exist!")

    conn = GraphSQLConnection(endpoint)

    cursor = conn.cursor()
    cursor.execute(sql_query)

    results = cursor.fetchall()
    assert results is not None, "Query execution returned no results!"
    
    print("✅ Query executed successfully!")
    print("Results:", results)

    cursor.close()
    conn.close()
    print("✅ Cursor and connection closed.")

if __name__ == "__main__":
    test_graphsql_cursor()
    
    
    
