from graphsql.introspection.schema_parser import SchemaParser

parser = SchemaParser("schemas/schema_be75f17d59.json")
parser.parse()

print("✅ Parsing Complete!")