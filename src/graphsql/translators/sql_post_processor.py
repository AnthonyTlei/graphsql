from graphsql.dbapi.duckdb import DuckDBSingleton

import re

class SQLPostProcessor:
    """
    Applies remaining SQL filters (ORDER BY, GROUP BY, aggregations) on the DuckDB virtual_table.
    """

    def __init__(self, filters):
        """
        Initializes the post-processor with filters extracted from SQLParser.

        Args:
            filters (dict): Remaining SQL filters (group_by, aggregations, order_by, limit, etc.).
        """
        self.filters = filters
        self.con = DuckDBSingleton.get_connection()

    def construct_query(self):
        """Constructs the final SQL query dynamically based on the filters."""
        base_query = "SELECT * FROM virtual_table"

        select_clauses = []
        group_by_clause = ""
        order_by_clause = ""
        limit_clause = ""

        if self.filters.get("aggregations"):
            select_clauses = [f'{agg}("{col}") AS {agg.lower()}_{col.replace(".", "_")}' for agg, col in self.filters["aggregations"]]

        if self.filters.get("group_by"):
            group_by_col = self.filters["group_by"]
            group_by_col = f'"{group_by_col}"'
            select_clauses.insert(0, group_by_col)
            group_by_clause = f"GROUP BY {group_by_col}"
            
        # if self.filters.get("order_by"):
        #     order_col = self.filters["order_by"]
        #     order_dir = self.filters.get("order_by_direction", "ASC")

        #     match = re.match(r'(\w+)\(([\w\d\.\*]+)\)', order_col)
        #     if match:
        #         agg_func, field_name = match.groups()
        #         order_col = f'{agg_func}("{field_name}")'
        #     else:
        #         order_col = f'"{order_col}"'

        #     order_by_clause = f"ORDER BY {order_col} {order_dir}"
            
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

        if select_clauses:
            final_query = f"SELECT {', '.join(select_clauses)} FROM virtual_table {group_by_clause} {order_by_clause} {limit_clause}"
        else:
            final_query = f"{base_query} {order_by_clause} {limit_clause}"


        print("Filters Query: ", final_query.strip())
        return final_query.strip()

    def execute(self):
        """Executes the constructed SQL query on DuckDB and returns results."""
        final_query = self.construct_query()

        df = self.con.execute(final_query).fetchdf()
        return df