from sqlalchemy.sql.compiler import SQLCompiler

class GraphSQLCompiler(SQLCompiler):
    """Custom SQLAlchemy compiler for GraphSQL."""

    def visit_select(self, select, **kwargs):
        """Convert a SQLAlchemy SELECT statement into a SQL string."""
        return super().visit_select(select, **kwargs)

    def visit_insert(self, insert_stmt, **kwargs):
        """Convert an INSERT statement."""
        return super().visit_insert(insert_stmt, **kwargs)

    def visit_update(self, update_stmt, **kwargs):
        """Convert an UPDATE statement."""
        return super().visit_update(update_stmt, **kwargs)

    def visit_delete(self, delete_stmt, **kwargs):
        """Convert a DELETE statement."""
        return super().visit_delete(delete_stmt, **kwargs)