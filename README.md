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

### 🌐 Handling HTTP Endpoints & Authentication

If your GraphQL endpoint uses HTTP instead of HTTPS, you need to set `is_http=1` when initializing the connection:

```python
conn = GraphSQLConnection(endpoint="http://your-graphql-endpoint.com?is_http=1")
```

To pass authentication arguments, use the `auth` parameter:

```python
conn = GraphSQLConnection(endpoint="https://your-graphql-endpoint.com?auth=<>"
```

### 1️⃣ Connect to a GraphQL Endpoint

```python
from graphsql.dbapi.connection import GraphSQLConnection

conn = GraphSQLConnection(endpoint="https://your-graphql-endpoint.com")
```

### 2️⃣ Execute SQL Queries

```python
cursor = conn.cursor()
cursor.execute("SELECT name, age FROM users WHERE age > 25")
result = cursor.fetchall()
print(result)
```

### 3️⃣ Use with SQLAlchemy

```python
from sqlalchemy import create_engine

dialect = "graphsql://your-graphql-endpoint.com"
engine = create_engine(dialect)
result = engine.execute("SELECT id, title FROM posts")
print(result.fetchall())
```

## 🎯 Roadmap

- ✅ SQLAlchemy dialect support
- ✅ Superset compatibility
- ⏳ Advanced introspection for schema validation
- ⏳ Incremental data filling
- ⏳ Support for multi-line SQL
- ⏳ Support for more SQL syntax
- ⏳ Argument validation, filter, and conditional validation
- ⏳ Add support for more Superset features with DBAPI
