import json

class SQLGenerator:
    def __init__(self, mappings_path="schemas/mappings.json", relations_path="schemas/relations.json"):
        """Initialize SQLGenerator with mappings & relations JSON files."""
        self.mappings_path = mappings_path
        self.relations_path = relations_path

        self.mappings = self._load_json(self.mappings_path)
        self.relations = self._load_json(self.relations_path)

    def _load_json(self, path):
        """Loads JSON data from a file."""
        with open(path, "r") as file:
            return json.load(file)

    def generate_sql(self):
        """Generates SQL tables from mappings and relations."""
        sql_lines = []
        join_tables = []
        alter_statements = []

        for table, fields in self.mappings.items():
            sql_lines.append(f"CREATE TABLE {table} (")
            columns = []
            has_primary_key = False

            for field, field_type in fields.items():
                if isinstance(field_type, dict):
                    referenced_table = list(field_type.keys())[0]
                    columns.append(f"{field}_id UUID REFERENCES {referenced_table}(graphsql_id)")

                elif isinstance(field_type, str) and "PRIMARY KEY" in field_type:
                    has_primary_key = True
                    columns.append(f"{field} {field_type}")

                elif isinstance(field_type, str) and field_type.startswith("LIST<"):  
                    related_table = field_type.replace("LIST<", "").replace(">", "")
                    join_table = f"{table}_{related_table}"
                    join_tables.append(join_table)

                else:
                    columns.append(f"{field} {field_type}")

            if not has_primary_key:
                columns.insert(0, "graphsql_id UUID PRIMARY KEY")

            sql_lines.append(", ".join(columns))
            sql_lines.append(");\n")

        for parent_table, relations in self.relations.items():
            for relation in relations:
                field_name = relation["field"]
                relation_type = relation["relation"]
                target_table = relation["target"]

                if relation_type == "one-to-many":
                    join_table = f"{parent_table}_{target_table}"
                    join_tables.append(join_table)

                elif relation_type == "many-to-many":
                    join_table = f"{parent_table}_{target_table}"
                    join_tables.append(join_table)

                elif relation_type in ["one-to-one", "many-to-one"]:
                    alter_statements.append(
                        f"ALTER TABLE {parent_table} ADD COLUMN {field_name}_id UUID REFERENCES {target_table}(graphsql_id);"
                    )

        for join_table in set(join_tables):
            if "_" not in join_table:
                continue

            parent_table, related_table = join_table.rsplit("_", 1)

            sql_lines.append(f"CREATE TABLE {join_table} ("
                            f"{parent_table}_id UUID REFERENCES {parent_table}(graphsql_id),"
                            f"{related_table}_id UUID REFERENCES {related_table}(graphsql_id),"
                            "PRIMARY KEY (" + f"{parent_table}_id, {related_table}_id" + "));\n")

        sql_lines.extend(alter_statements)

        with open("schemas/schema.sql", "w") as file:
            file.write("\n".join(sql_lines))
