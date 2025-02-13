import pandas as pd

from graphsql.connectors.graphsql_flat_connector import GraphSQLFlatConnector

endpoint_1 = "https://spacex-production.up.railway.app/"
endpoint_2 = "https://graphql.anilist.co"
connector_1 = GraphSQLFlatConnector(endpoint_1, depth_cutoff=5, output_format="parquet")
connector_2 = GraphSQLFlatConnector(endpoint_2, depth_cutoff=5, output_format="parquet")

test_queries_1 = [
    "SELECT name FROM rockets;",
    "SELECT * FROM rockets;",
    "SELECT id, name FROM rockets;",
    "SELECT id, name FROM rocket WHERE id = '5e9d0d95eda69955f709d1eb';",
    "SELECT roles FROM ships;",
    "SELECT url, roles FROM ships;",
    "SELECT description FROM dragons",
    "SELECT active, crew_capacity, description, dry_mass_kg, dry_mass_lb, first_flight, type, wikipedia FROM dragons",
    "SELECT * FROM dragons",
]

test_queries_2 = [
    "SELECT id, trending, isAdult FROM Media WHERE id = 68;",
    "SELECT media.title.english FROM Page",
    "SELECT * FROM Media WHERE id = 68;",
    "SELECT media.id, media.title.english, pageInfo.total FROM Page"
]

# for test_sql in test_queries_1:
#     print(f"\nExecuting: {test_sql}")
#     output_path = connector_1.execute_sql(test_sql)
#     df = pd.read_parquet(output_path, engine='pyarrow')
#     print(df.head())
    
for test_sql in test_queries_2:
    print(f"\nExecuting: {test_sql}")
    output_path = connector_2.execute_sql(test_sql)
    df = pd.read_parquet(output_path, engine='pyarrow')
    print(df.head())