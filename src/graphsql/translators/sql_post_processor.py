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
        self.con = DuckDBSingleton.get_connection()

    def construct_query(self):
        """Constructs the final SQL query dynamically based on the filters."""
        
        selected_fields = self.parsed_data.get("fields", [])
        select_clauses = [f'"{field}"' for field in selected_fields] if selected_fields else []

        group_by_clause = ""
        order_by_clause = ""
        limit_clause = ""

        if self.filters.get("aggregations"):
            agg_clauses = [f'{agg}("{col}") AS {agg.lower()}_{col.replace(".", "_")}' for agg, col in self.filters["aggregations"]]
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
                    parsed_group_by_columns.append(f'"{col}"')

            group_by_clause = f"GROUP BY {', '.join(parsed_group_by_columns)}"

            for col in parsed_group_by_columns:
                if col not in select_clauses:
                    select_clauses.insert(0, col)

        if self.filters.get("order_by"):
            order_col = self.filters["order_by"]
            order_dir = self.filters.get("order_by_direction", "ASC")
            match = re.match(r'(\w+)\(([\w\d\.\*]+)\)', order_col)
            if match:
                agg_func, field_name = match.groups()
                order_col = f'{agg_func}("{field_name}")'
                if field_name not in {col for _, col in self.filters.get("aggregations", [])}:
                    select_clauses.append(f'"{field_name}"')
            else:
                order_col = f'"{order_col}"'
            order_by_clause = f"ORDER BY {order_col} {order_dir}"

        if self.filters.get("limit"):
            limit_clause = f"LIMIT {self.filters['limit']}"

        final_query = f"SELECT {', '.join(select_clauses)} FROM virtual_table {group_by_clause} {order_by_clause} {limit_clause}"

        print("Filters Query: ", final_query.strip())
        return final_query.strip()

    def execute(self):
        """Executes the constructed SQL query on DuckDB and returns results."""
        final_query = self.construct_query()
        df = self.con.execute(final_query).fetchdf()
        return df