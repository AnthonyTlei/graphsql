from graphsql.dbapi.duckdb import DuckDBSingleton
import re

class SQLPostProcessor:
    """
    Applies remaining SQL filters (ORDER BY, GROUP BY, aggregations) on the DuckDB virtual_table.
    """

    def __init__(self, parsed_data):
        """
        Initializes the post-processor with filters extracted from SQLParser.

        Args:
            filters (dict): Remaining SQL filters (group_by, aggregations, order_by, limit, etc.).
        """
        self.parsed_data = parsed_data
        self.filters = parsed_data.get("filters", {})
        self.table_name = (
            parsed_data.get("subquery_alias")
            or parsed_data.get("table")
            or "virtual_table"
        )
        self.con = DuckDBSingleton.get_connection()

    def construct_query(self):
        """Constructs the final SQL query dynamically based on the filters."""
        
        selected_fields = self.parsed_data.get("fields", [])
        if "*" in selected_fields:
            select_clauses = ["*"]
        else:
            select_clauses = []
            for field in selected_fields:
                if field in select_clauses:
                    continue
                if "." in field:
                    select_clauses.append(f'"{field}"')
                else:
                    select_clauses.append(field)
                    
        group_by_clause = ""
        order_by_clause = ""
        limit_clause = ""

        if self.filters.get("aggregations"):
            aggregations = self.filters.get("aggregations")
            agg_clauses = []
            for agg, field in aggregations:
                if field in select_clauses:
                    select_clauses.remove(field)
                if f'"{field}"' in select_clauses:
                    select_clauses.remove(f'"{field}"')
                if agg == "COUNT_DISTINCT":
                    agg = "COUNT"
                    if "." in field:
                        field = f'DISTINCT "{field}"'
                    else:
                        field = f"DISTINCT {field}"
                else:
                    if "." in field:
                        field = f'"{field}"'
                    else:
                        field = f"{field}"
                
                field_alias = field.replace(".", "_").replace(" ", "_").replace("\"", "")
                agg_clauses.append(f'{agg}({field}) AS {agg.lower()}_{field_alias}')
                
            select_clauses.extend(agg_clauses)

        if not select_clauses:
            select_clauses = ["*"]  

        if self.filters.get("group_by"):
            group_by_raw = self.filters["group_by"].strip()
            group_by_columns = [col.strip() for col in group_by_raw.split(",")] if "," in group_by_raw else [group_by_raw]
            parsed_group_by_columns = []

            for col in group_by_columns:
                match = re.match(r'(\w+)\(([\w\d\.\*]+)\)', col)
                if match:
                    agg_func, field_name = match.groups()
                    parsed_group_by_columns.append(f'{agg_func}("{field_name}")')
                else:
                    if not (col.startswith('"') and col.endswith('"')):
                        parsed_group_by_columns.append(f'"{col}"')
                    else:
                        parsed_group_by_columns.append(f'{col}')

            group_by_clause = f"GROUP BY {', '.join(parsed_group_by_columns)}"

        if self.filters.get("order_by"):
            order_col = self.filters["order_by"]
            order_dir = self.filters.get("order_by_direction", "ASC")
            # Match if the col is an aggregation
            match = re.match(r'(\w+)\(([\w\d\.\*]+)\)', order_col)
            if match:
                agg, field = match.groups()
                if agg == "COUNT_DISTINCT":
                    agg = "COUNT"
                    if "." in field:
                        field = f'DISTINCT "{field}"'
                    else:
                        field = f"DISTINCT {field}"
                    order_col = f'{agg}({field})'
                else:
                    if "." in field:
                        field = f'"{field}"'
                    else:
                        field = f"{field}"
                    order_col = f'{agg}({field})'
            else:
                order_col = f'"{order_col}"'
            order_by_clause = f"ORDER BY {order_col} {order_dir}"

        if self.filters.get("limit"):
            limit_clause = f"LIMIT {self.filters['limit']}"

        final_query = f"SELECT {', '.join(select_clauses)} FROM {self.table_name} {group_by_clause} {order_by_clause} {limit_clause}"

        print("\nPost Processing Query: ", final_query.strip())
        return final_query.strip()

    def execute(self):
        """Executes the constructed SQL query on DuckDB and returns results."""
        
        print("\nPost Processing Data: ", self.parsed_data)
        
        final_query = self.construct_query()
        df = self.con.execute(final_query).fetchdf()
        
        return df