import graphsql.dialect.dialect

from sqlalchemy.dialects import registry
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

def test_graphsql_dialect():
    """Test if GraphSQLDialect is registered and usable."""

    assert "graphsql" in registry.load("graphsql").name, "GraphSQL dialect is not registered!"

    print("✅ GraphSQL dialect is registered!")

    # url = make_url("graphsql://graphql.anilist.co?is_http=1&auth=Basic%20AbCdEfGh123")
    url = make_url("graphsql://graphql.anilist.co?is_http=0")
    engine = create_engine(url)

    assert engine.dialect.name == "graphsql", "Engine did not use the GraphSQL dialect!"

    print("✅ Engine created successfully with GraphSQL dialect!")
    
    with engine.connect() as conn:
        sql_query = text("SELECT media.id, media.title.english FROM Page")
        result = conn.execute(sql_query)
        rows = result.fetchall()

        print("✅ Query executed successfully!")
        print("Results:", rows)

if __name__ == "__main__":
    test_graphsql_dialect()