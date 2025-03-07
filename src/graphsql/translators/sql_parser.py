import json
import re
import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Parenthesis, Token, Function
from sqlparse.tokens import DML, Keyword, Wildcard, Whitespace, Punctuation

AGGREGATION_FUNCTIONS = {"COUNT", "SUM", "AVG", "MIN", "MAX"}
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
        """
        Parses a single SQL statement into structured components,
        including potential subqueries.
        """
        # Parse the entire statement (Only 1 statement is supported at a time for now)
        statement = sqlparse.parse(sql_query)[0]

        # Build result structure
        sql_structure = {
            "operation": None,
            "fields": [],
            "table": None,
            "alias": None,
            "subquery": None,
            "conditions": [],
            "order_by": None,
            "limit": None,
            "aggregations": [],
        }

        # 1) Identify operation (e.g. SELECT, UPDATE, etc.)
        statement_type = statement.get_type()
        if statement_type.upper() == "SELECT":
            sql_structure["operation"] = "SELECT"
        else:
            # Or check tokens[0].ttype is DML
            first_token = statement.tokens[0] if statement.tokens else None
            if first_token and first_token.ttype is DML:
                sql_structure["operation"] = first_token.value.upper()

        # 2) Extract the SELECT-ed fields
        self._extract_fields(statement, sql_structure)

        # 3) Extract the FROM part (table or subquery)
        self._extract_from_part(statement, sql_structure)

        # 4) Extract WHERE, ORDER BY, LIMIT, GROUP BY
        self._extract_keywords(statement, sql_structure)
        
        # 5) Extract aggregations
        self._extract_aggregations(sql_structure)

        return sql_structure

    def _extract_fields(self, statement, sql_structure):
        """
        Look for tokens between SELECT and FROM (or next Keyword) that
        indicate the fields being selected. We'll also handle basic aliasing,
        e.g. "field AS alias".
        """
        select_seen = False
        
        for token in statement.tokens:
            if token.ttype is DML and token.value.upper() == "SELECT":
                select_seen = True
                continue
            
            if select_seen:
                if (token.ttype is Keyword and token.value.upper() in ["FROM","WHERE","ORDER BY","LIMIT", "GROUP BY"]) \
                   or token.is_group and isinstance(token, Parenthesis):
                    break
                
                # If aggregation function
                if isinstance(token, Function):
                    func_name = token.get_real_name()
                    if func_name and func_name.upper() in AGGREGATION_FUNCTIONS:
                        match = re.match(r'(\w+)\(([\w\d\.\*]+)\)', token.value.strip())
                        if match:
                            agg_func, agg_field = match.groups()
                            sql_structure["aggregations"].append((agg_func.upper(), agg_field))
                        else:
                            sql_structure["aggregations"].append((func_name.upper(), token.value.strip()))
                        continue
                
                # If it's a comma-separated list of fields
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        self._handle_single_field(identifier.value.strip(), sql_structure)
                        
                # If it's a single field (Identifier)
                elif isinstance(token, Identifier):
                    self._handle_single_field(token.value.strip(), sql_structure)
                    
                # If it's a wildcard
                elif token.ttype is Wildcard:
                    sql_structure["fields"].append("*")
                
                # Fallback
                else:
                    raw_val = token.value.strip()
                    if raw_val:
                        for f in raw_val.split(","):
                            self._handle_single_field(f.strip(), sql_structure)

    def _handle_single_field(self, field_string, sql_structure):
        """
        Helper to handle a single field string, possibly containing
        alias syntax like:  "mytable.id AS myalias" or quotes.
        """
        aggregation_pattern = r'\b(' + '|'.join(AGGREGATION_FUNCTIONS) + r')\s*\(\s*([\w\d\.\*]+)\s*\)'
        match = re.search(aggregation_pattern, field_string, re.IGNORECASE)
        if match:
            function_name = match.group(1)
            field_inside_function = match.group(2)
            sql_structure["aggregations"].append((function_name.upper(), field_inside_function))
        else:
            upper_field = field_string.upper()
            if "AS" in upper_field:
                return
            else:
                field_clean = field_string.strip('"').strip()
                sql_structure["fields"].append(field_clean)

    def _extract_from_part(self, statement, sql_structure):
        """
        Detect the FROM clause. If it's just a table, store table name.
        If it's a subquery (Parenthesis), parse subquery.
        Also detect an optional alias after the table or subquery.
        """
        from_seen = False
        tokens = statement.tokens
        print("Extracting from statement: ", str(statement))

        for i, token in enumerate(tokens):

            # If we see "FROM", the next token(s) describe the table or subquery
            if token.ttype is Keyword and token.value.upper() == "FROM":
                from_seen = True
                continue

            if from_seen:
                # If it's a Parenthesis, it's a direct subquery
                if isinstance(token, Parenthesis):
                    inner_sql = token.value.strip("()")
                    sql_structure["subquery"] = self._parse_subquery(inner_sql)
                    self._maybe_extract_alias(tokens, i+1, sql_structure)
                    break

                # Handle case where subquery is inside an Identifier
                elif isinstance(token, Identifier):
                    val = token.value.strip()
                    # subquery Identifier
                    if "(" in val and ")" in val:
                        self._handle_subquery_in_identifier(val, sql_structure)
                    else:
                        # Table name
                        sql_structure["table"] = token.get_real_name()
                        if token.get_alias():
                            sql_structure["alias"] = token.get_alias()
                    break

                # Multiple tables (IdentifierList)
                elif isinstance(token, IdentifierList):
                    first_id = list(token.get_identifiers())[0]
                    sql_structure["table"] = first_id.get_real_name()
                    if first_id.get_alias():
                        sql_structure["alias"] = first_id.get_alias()
                    break

                # Single Table
                elif token.ttype is None:
                    raw_val = token.value.strip()
                    if "(" in raw_val and ")" in raw_val:
                        self._handle_subquery_in_identifier(raw_val, sql_structure)
                    else:
                        sql_structure["table"] = raw_val
                    self._maybe_extract_alias(tokens, i+1, sql_structure)
                    break

    def _handle_subquery_in_identifier(self, identifier_value, sql_structure):
        """
        Handles subqueries appearing inside an Identifier with parentheses.

        Example:
            "(SELECT media.id FROM Page ) AS virtual_table"
        Extracts:
            - The subquery inside `()`
            - The alias `virtual_table` (if present)
        """
        match = re.match(r"^\(\s*(SELECT\s+.*)\)\s*(?:AS\s+([\w\d_]+))?$", identifier_value, flags=re.IGNORECASE | re.DOTALL)
        if match:
            subquery_sql = match.group(1).strip()
            possible_alias = match.group(2)
            if subquery_sql:
                parsed_statements = sqlparse.parse(subquery_sql)
                if parsed_statements:
                    subquery_structure = {
                        "operation": None,
                        "fields": [],
                        "table": None,
                        "conditions": [],
                        "order_by": None,
                        "limit": None,
                        "aggregations": [],
                    }
                    statement = parsed_statements[0]
                    statement_type = statement.get_type()
                    if statement_type.upper() == "SELECT":
                        subquery_structure["operation"] = "SELECT"
                    else:
                        first_token = statement.tokens[0] if statement.tokens else None
                        if first_token and first_token.ttype is DML:
                            subquery_structure["operation"] = first_token.value.upper()
                    self._extract_fields(statement, subquery_structure)
                    self._extract_from_part(statement, subquery_structure)
                    self._extract_keywords(statement, subquery_structure)
                    sql_structure["subquery"] = subquery_structure
            if possible_alias:
                sql_structure["alias"] = possible_alias.strip()
                sql_structure["table"] = possible_alias.strip()
        else:
            print("     Subquery is table")
            sql_structure["table"] = identifier_value.strip()

    def _maybe_extract_alias(self, tokens, start_index, sql_structure):
        """
        If we have something like 'FROM (...) AS alias' or 'FROM (...) alias',
        parse out the alias.
        """
        if start_index < len(tokens):
            next_token = tokens[start_index]
            if next_token.value.upper() == "AS":
                alias_idx = start_index + 1
                if alias_idx < len(tokens):
                    alias_token = tokens[alias_idx]
                    sql_structure["alias"] = alias_token.value.strip()
                    if not sql_structure["table"]:
                        sql_structure["table"] = alias_token.get_real_name()
            else:
                if next_token.ttype is None or isinstance(next_token, Identifier):
                    alias_str = next_token.value.strip()
                    sql_structure["alias"] = alias_str
                    if not sql_structure["table"]:
                        sql_structure["table"] = next_token.get_real_name()

    def _extract_keywords(self, statement, sql_structure):
        """
        Look for WHERE, ORDER BY, and LIMIT tokens in the statement.
        We'll do a simpler version that basically checks each token's value.
        """
        tokens = statement.tokens
        i = 0
        while i < len(tokens):
            t = tokens[i]
            upper_val = t.value.upper()

            # WHERE
            if t.ttype is Keyword and upper_val == "WHERE":
                if (i+1) < len(tokens):
                    where_token = tokens[i+1]
                    where_clause = where_token.value.strip()
                    conditions = where_clause.split("AND")
                    for cond in conditions:
                        match = re.match(r"(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?", cond.strip())
                        if match:
                            sql_structure["conditions"].append({match.group(1): match.group(2)})
                i += 2
                continue

            # ORDER BY
            if t.ttype is Keyword and upper_val == "ORDER BY":
                j = i + 1

                while j < len(tokens) and tokens[j].ttype in Whitespace:
                    j += 1

                if j < len(tokens):
                    token = tokens[j]
                    order_by_clause = token.value.strip()
                    j += 1

                    parts = order_by_clause.split()
                    
                    if len(parts) == 2:
                        column, direction = parts
                    else:
                        column = parts[0]
                        while j < len(tokens) and tokens[j].ttype in Whitespace:
                            j += 1
                        if j < len(tokens):
                            next_token = tokens[j].value.strip().upper()
                            direction = next_token if next_token in {"ASC", "DESC"} else "ASC"
                        else:
                            direction = "ASC"

                    if (column.startswith("'") and column.endswith("'")) or \
                    (column.startswith('"') and column.endswith('"')):
                        column = column[1:-1]

                    sql_structure["order_by"] = column
                    sql_structure["order_by_direction"] = direction

                i = j + 1
                continue
            
            # GROUP BY
            if t.ttype is Keyword and upper_val == "GROUP BY":
                j = i + 1
                while j < len(tokens) and tokens[j].ttype in Whitespace:
                    j += 1
                if j < len(tokens):
                    group_by_token = tokens[j]
                    group_by_val = group_by_token.value.strip()

                    if (group_by_val.startswith("'") and group_by_val.endswith("'")) or \
                    (group_by_val.startswith('"') and group_by_val.endswith('"')):
                        group_by_val = group_by_val[1:-1]

                    sql_structure["group_by"] = group_by_val

                i = j + 1
                continue

            # LIMIT
            if t.ttype is Keyword and upper_val == "LIMIT":
                j = i + 1
                while j < len(tokens) and tokens[j].ttype in Whitespace:
                    j += 1
                if j < len(tokens):
                    limit_token = tokens[j]
                    sql_structure["limit"] = limit_token.value.strip()

                i = j + 1
                continue

            i += 1

    def _parse_fields_with_nesting(self, fields, table, aggregations=None):
        """Parses nested fields based on mappings, supporting aggregations."""
        parsed_fields = {}

        if not table:
            return parsed_fields

        if aggregations:
            for agg in aggregations:
                if isinstance(agg, tuple) and len(agg) == 2:
                    agg_func, agg_field = agg
                elif isinstance(agg, str):
                    match = re.match(r'(\w+)\(([\w\d\.\*]+)\)', agg)
                    if match:
                        agg_func, agg_field = match.groups()
                    else:
                        agg_func, agg_field = None, agg
                if agg_field:
                    fields.append(agg_field)

        if "*" in fields:
            if table in self.mappings:
                for field, _type in self.mappings[table].items():
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
        """Generates GraphQL conditions, e.g. (id: 123, name: "x")."""
        if not singular_table or singular_table not in self.mappings:
            return ""
        graphql_conditions = []
        for condition in conditions:
            for key, value in condition.items():
                if key in self.mappings[singular_table]:
                    if value.isdigit():
                        graphql_conditions.append(f"{key}: {value}")
                    else:
                        graphql_conditions.append(f'{key}: "{value}"')
        if graphql_conditions:
            return "(" + ", ".join(graphql_conditions) + ")"
        else:
            return ""

    def _generate_aggregation_queries(self, sql_structure):
        aggregations = sql_structure["aggregations"]
        aggregation_queries = []

        def _construct_graphql_fields(fields):
            """
            Recursively constructs nested GraphQL fields.
            Example: ['media', 'id'] -> '{ media { id } }'
            """
            if not fields:
                return ""

            field = fields[0]
            if len(fields) == 1:
                return f"{{ {field} }}"

            return f"{{ {field} {_construct_graphql_fields(fields[1:])} }}"

        for agg in aggregations:
            function, column = agg
            column = column.strip('"')
            parts = column.split(".")

            if len(parts) > 1:
                if sql_structure["subquery"] and sql_structure["subquery"]["table"]:
                    table = sql_structure["subquery"]["table"]
                    nested_fields = parts if table not in parts else parts[1:]
                elif sql_structure["table"]:
                    table = sql_structure["table"]
                    nested_fields = parts if table not in parts else parts[1:]
                else:
                    table, nested_fields = parts[0], parts[1:]

            if table and table in self.mappings:
                graphql_table, _ = self._resolve_table_mapping(table)
                nested_field_str = _construct_graphql_fields(nested_fields)
                query = f'query {graphql_table} {{ {graphql_table} {nested_field_str} }}'
                aggregation_queries.append((query, function.upper()))

        return aggregation_queries

    def _validate_order_by(self, sql_structure):
        """Validates the ORDER BY clause, ensuring the column exists in selected fields or aggregations."""
        
        order_col = sql_structure.get("order_by", None)
        order_dir = sql_structure.get("order_by_direction", None)
        
        if not order_col:
            return (None, None)

        # TODO: Implement validation, order can be aggregation of selected field or a selected field

        return (order_col, order_dir)

    def _resolve_graphql_structure(self, graphql_table, graphql_fields, conditions_str):
        """Build the final GraphQL query string."""
        
        def build_graphql_fields(fields):
            if not fields:
                return ""
            if isinstance(fields, dict):
                parts = []
                for key, val in fields.items():
                    if val is True:
                        parts.append(key)
                    else:
                        parts.append(f"{key} {build_graphql_fields(val)}")
                return "{ " + " ".join(parts) + " }" if parts else ""
            return ""

        graphql_fields_str = build_graphql_fields(graphql_fields)
        
        if not graphql_fields_str:
            # If reached it means the table has no scalar fields (with *) or something went wrong
            return ""

        return f'query {graphql_table} {{ {graphql_table}{conditions_str} {graphql_fields_str} }}'.strip()

    def _resolve_table_mapping(self, table):
        """Resolves table mapping from SQL name to GraphQL field/type name."""
        if not table:
            return "Unknown", "Unknown"

        if table == "virtual_table":
            return "virtual_table", "virtual_table"

        # Normal logic
        if table in self.mappings:
            return table, table

        # Check "Query" in relations
        if "Query" in self.relations:
            for relation in self.relations["Query"]:
                if relation["field"] == table:
                    return relation["field"], relation["target"]

        # Or check other parents
        for parent, rels in self.relations.items():
            for r in rels:
                if r["field"] == table:
                    return r["field"], r["target"]

        # If we get here, table not found
        raise ValueError(f"Unknown table: {table}")

    def _extract_aggregations(self, sql_structure):
        """Extracts aggregation functions like COUNT, AVG, MIN, MAX and returns structured aggregation queries."""
        aggregation_pattern = re.compile(r"(COUNT|AVG|SUM|MIN|MAX)\s*\(\s*(.*?)\s*\)", re.IGNORECASE)
        fields = sql_structure["fields"]
        for field in fields:
            match = aggregation_pattern.match(field)
            if match:
                function, column = match.groups()
                column = column.strip('"')
                sql_structure["aggregations"].append((function, column))
        
    def convert_to_graphql(self, sql_query):
        """
        Main method: parse the SQL, then convert to GraphQL.
        If the parsed structure has a subquery, we'll use the subquery's
        table/fields as the real data source.
        """
        sql_data = self.parse_sql(sql_query)
        result_queries = []
        
        print("Parsed SQL: ", sql_data)
        table = sql_data["table"]
        alias = sql_data.get("alias", None)
        fields = sql_data["fields"]
        conditions = sql_data["conditions"]
        limit = sql_data["limit"]
        aggregations = sql_data.get("aggregations", [])
        order_by = sql_data.get("order_by", None)
        group_by = sql_data.get("group_by", None)
        order_by_column, order_by_direction = self._validate_order_by(sql_data)
        filters_data = {
            "limit": limit,
            "order_by": order_by_column,
            "order_by_direction": order_by_direction,
            "group_by": group_by,
            "aggregations": aggregations
        }
        
        subquery_filters_data = {}
        if sql_data["subquery"]:  
            subquery_data = sql_data["subquery"]
            subquery_filters_data = {
                "limit": subquery_data.get("limit", None),
                "order_by": subquery_data.get("order_by",None),
                "order_by_direction": subquery_data.get("order_by_direction", None),
                "group_by": subquery_data.get("group_by", None),
                "aggregations": subquery_data.get("aggregations", None)
            }
            table = subquery_data["table"]
            fields = subquery_data["fields"]
            conditions = subquery_data["conditions"]
            

        graphql_table, singular_table = self._resolve_table_mapping(table)
        graphql_fields = self._parse_fields_with_nesting(fields, singular_table, aggregations=aggregations)
        conditions_str = self._generate_conditions(conditions, singular_table)

        graphql_query = self._resolve_graphql_structure(
            graphql_table, graphql_fields, conditions_str
        )
        
        if graphql_query:
            query_tuple = (graphql_query)
            result_queries.append(query_tuple)
        else:
            aggregation_queries = self._generate_aggregation_queries(sql_data)
            result_queries.extend(aggregation_queries)
        
        
        data = {
            "graphql_queries": result_queries,
            "filters": filters_data,
            "fields": sql_data['fields'],
            "subquery_filters": subquery_filters_data,
            "subquery_alias": alias
        }
        print("\nData: ", data, "\n")
        
        return data