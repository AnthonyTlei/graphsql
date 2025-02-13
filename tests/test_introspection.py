from graphsql.introspection.introspection import GraphQLIntrospection

endpoint = "https://spacex-production.up.railway.app/"
introspector = GraphQLIntrospection(endpoint)
schema = introspector.load_schema()

