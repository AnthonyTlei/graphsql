import json
import sqlparse
import re

class SQLParser:
    def __init__(self, mappings_path="schemas/mappings.json", relations_path="schemas/relations.json"):
        """Initialize SQL to GraphQL converter with mappings & relations JSON files."""
        self.mappings = self._load_json(mappings_path)
        self.relations = self._load_json(relations_path)

    def _load_json(self, path):
        """Loads JSON data from a file."""
        with open(path, "r") as file:
            return json.load(file)

    def parse_sql(self, sql_query):
        """Parses SQL query into structured components."""
        parsed = sqlparse.parse(sql_query)[0]
        tokens = [token for token in parsed.tokens if not token.is_whitespace]

        sql_structure = {
            "operation": None,
            "fields": [],
            "table": None,
            "conditions": [],
            "order_by": None,
            "limit": None
        }
        
        if tokens[0].ttype is sqlparse.tokens.DML:
            sql_structure["operation"] = tokens[0].value.upper()

        from_seen = False
        select_seen = False
        order_by_seen = False
        limit_seen = False

        for token in tokens:
            value = token.value.upper()

            if select_seen and value != "FROM" and not from_seen and "WHERE" not in value:
                if token.ttype is sqlparse.tokens.Wildcard:
                    sql_structure["fields"].append("*")
                else:
                    field_names = [f.strip() for f in token.value.split(",")]
                    sql_structure["fields"].extend(field_names)

            if value == "SELECT":
                select_seen = True

            if from_seen and sql_structure["table"] is None and token.ttype is None:
                sql_structure["table"] = token.get_real_name()

            if value == "FROM":
                from_seen = True

            if "WHERE" in value:
                where_clause = token.value.replace("WHERE", "").strip()
                conditions = where_clause.split("AND")
                for condition in conditions:
                    match = re.match(r"(\w+)\s*=\s*['\"]?(\w+)['\"]?", condition.strip())
                    if match:
                        sql_structure["conditions"].append({match.group(1): match.group(2)})

            if order_by_seen:
                sql_structure["order_by"] = f"{token.get_real_name()}: DESC"
                order_by_seen = False

            if value == "ORDER BY":
                order_by_seen = True

            if limit_seen:
                sql_structure["limit"] = str(token).strip()
                limit_seen = False

            if value == "LIMIT":
                limit_seen = True

        return sql_structure

    def _resolve_table_mapping(self, table):
        """Resolves table mapping from SQL to GraphQL."""
        if table == "virtual_table":
            raise ValueError("Handling for virtual table is not supported yet")
        if table in self.mappings:
            return table, table
        if "Query" in self.relations:
            for relation in self.relations["Query"]:
                if relation["field"] == table:
                    return relation["field"], relation["target"]
        for parent, relations in self.relations.items():
            for relation in relations:
                if relation["field"] == table:
                    return relation["field"], relation["target"]
        raise ValueError(f"Unknown table: {table}")

    def _parse_fields_with_nesting(self, fields, table):
        """Parses nested fields based on mappings."""
        parsed_fields = {}
        
        if "*" in fields:
            for field, field_type in self.mappings[table].items():
                parsed_fields[field] = True
            return parsed_fields
        
        for field in fields:
            parts = field.split(".")
            parent = parts[0]
            
            if len(parts) == 1:
                parsed_fields[parent] = True
            else:
                current_level = parsed_fields
                for i, part in enumerate(parts):
                    if part not in current_level:
                        current_level[part] = {} if i < len(parts) - 1 else True
                    current_level = current_level[part]
        
        return parsed_fields

    def _generate_conditions(self, conditions, singular_table):
        """Generates GraphQL conditions."""
        graphql_conditions = []
        for condition in conditions:
            for key, value in condition.items():
                if key in self.mappings[singular_table]:
                    formatted_value = f'"{value}"' if not value.isdigit() else value
                    graphql_conditions.append(f"{key}: {formatted_value}")
        return f'({", ".join(graphql_conditions)})' if graphql_conditions else ""

    def _resolve_graphql_structure(self, graphql_table, graphql_fields, conditions_str):
        """Builds the GraphQL query structure dynamically."""
        def build_graphql_fields(fields):
            if isinstance(fields, dict):
                return "{ " + " ".join(f"{key} {build_graphql_fields(value)}" for key, value in fields.items()) + " }"
            return ""
        return f'query {graphql_table} {{ {graphql_table}{conditions_str} {build_graphql_fields(graphql_fields)} }}'.strip()

    def convert_to_graphql(self, sql_query):
        """Converts SQL query to GraphQL query dynamically with nesting support."""
        sql_data = self.parse_sql(sql_query)
        table = sql_data["table"]
        fields = sql_data["fields"]
        conditions = sql_data["conditions"]

        graphql_table, singular_table = self._resolve_table_mapping(table)
        graphql_fields = self._parse_fields_with_nesting(fields, singular_table)
        conditions_str = self._generate_conditions(conditions, singular_table)

        return self._resolve_graphql_structure(graphql_table, graphql_fields, conditions_str)