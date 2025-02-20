# GraphSQL

GraphSQL is a lightweight SQL-to-GraphQL connector designed to enable seamless querying of GraphQL endpoints using familiar SQL syntax. It aims to allow users to fetch, and analyze data from GraphQL APIs as if they were working with a relational database.

## 🚀 Features

- **SQL-Compatible Queries**: Write SQL queries that translate into GraphQL.
- **GraphQL Data Fetching**: Retrieve data from any GraphQL endpoint.
- **Live JSON-to-Tabular Conversion**: Convert GraphQL JSON responses into structured tabular formats (CSV, Parquet, JSONL).
- **Seamless Integration with SQLAlchemy**: Use GraphSQL as a SQLAlchemy dialect.
- **Superset Compatibility**: Connect GraphQL endpoints to Apache Superset for visualization.

## 📦 Installation

For the experimental release, install directly from GitHub:

```sh
pip install git+https://github.com/AnthonyTlei/graphsql.git
```

## 🔧 Usage

### Uuse with python and sqlalchemy

```python
import graphsql.dialect.dialect
from sqlalchemy import create_engine, text
from sqlalchemy.dialects import registry
from sqlalchemy.engine.url import make_url

def test_graphsql_dialect():
    assert "graphsql" in registry.load("graphsql").name, "GraphSQL dialect is not registered!"
    print("✅ GraphSQL dialect is registered!")
    url = make_url("graphsql://graphql.anilist.co")
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
```

### 🌐 Handling HTTP Endpoints & Authentication

If your GraphQL endpoint uses HTTP instead of HTTPS, you need to set `is_http=1` when initializing the connection:

```python
url = make_url(endpoint="graphsql://your-graphql-endpoint.com?is_http=1")
```

To pass authentication arguments, use the `auth` parameter:

```python
url = make_url(endpoint="graphsql://your-graphql-endpoint.com?auth=<>")
```

## 🎯 Roadmap

- ✅ SQLAlchemy dialect support
- ✅ Superset compatibility
- ✅ Advanced introspection for schema validation
- ✅ Support for multi-line SQL
- ⏳ Support for more SQL features
- ⏳ Argument validation, filter, and conditional validation
- ⏳ Support for more Superset features
