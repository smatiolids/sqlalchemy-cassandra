from sqlalchemy.sql import compiler
from sqlalchemy import exc
from sqlalchemy import util

class CassandraCompiler(compiler.SQLCompiler):
    # Define operators directly instead of copying from parent
    operators = {
        # Standard SQL operators
        'and': ' AND ',
        'or': ' OR ',
        'in': ' IN ',
        'is': ' IS ',
        'is not': ' IS NOT ',
        'equals': ' = ',
        'notequals': ' != ',
        'gt': ' > ',
        'lt': ' < ',
        'ge': ' >= ',
        'le': ' <= ',
        # Add any Cassandra-specific operators here
    }

    operators.update({
        # Add Cassandra-specific operator translations
    })
    
    def visit_select(self, select, **kw):
        # Handle SELECT queries
        text = "SELECT "
        if select._distinct:
            text += "DISTINCT "

        text += self._generate_columns(select)
        text += " FROM " + self._generate_from(select)

        if select._whereclause is not None:
            text += " WHERE " + self.process(select._whereclause)

        # Add ALLOW FILTERING if needed
        text += " ALLOW FILTERING"
        return text

    def visit_insert(self, insert_stmt, **kw):
        # Handle INSERT queries
        table = insert_stmt.table.name
        columns = [c.name for c in insert_stmt.columns]

        text = f"INSERT INTO {table} ({', '.join(columns)}) "
        text += "VALUES (" + ", ".join(["%s" for _ in columns]) + ")"
        return text

    def visit_update(self, update_stmt, **kw):
        # Handle UPDATE queries
        table = update_stmt.table.name

        text = f"UPDATE {table} SET "
        text += ", ".join(
            f"{c.name} = %s"
            for c in update_stmt.parameters
        )

        if update_stmt._whereclause is not None:
            text += " WHERE " + self.process(update_stmt._whereclause)
        return text

    def visit_delete(self, delete_stmt, **kw):
        # Handle DELETE queries
        text = f"DELETE FROM {delete_stmt.table.name}"

        if delete_stmt._whereclause is not None:
            text += " WHERE " + self.process(delete_stmt._whereclause)
        return text

    def limit_clause(self, select, **kw):
        # Handle LIMIT clause
        if select._limit_clause is not None:
            return " LIMIT " + self.process(select._limit_clause)
        return ""

    def _generate_columns(self, select):
        # Helper method to generate column list
        if select._columns_clause is None:
            return "*"
        return ", ".join(self.process(c) for c in select._columns_clause)

    def _generate_from(self, select):
        # Helper method to generate FROM clause
        return ", ".join(
            self.process(t, asfrom=True)
            for t in select._froms
        )


class CassandraDDLCompiler(compiler.DDLCompiler):
    def visit_create_table(self, create):
        # Handle CREATE TABLE statements
        table = create.element

        text = f"CREATE TABLE {table.name} ("
        text += ", ".join(
            self.get_column_specification(c)
            for c in table.columns
        )
        text += ")"
        return text

    def get_column_specification(self, column):
        # Generate column specifications
        text = column.name
        text += " " + self.dialect.type_compiler.process(column.type)

        if column.primary_key:
            text += " PRIMARY KEY"

        return text


class CassandraTypeCompiler(compiler.GenericTypeCompiler):
    def visit_UUID(self, type_, **kw):
        return 'uuid'

    def visit_TIMESTAMP(self, type_, **kw):
        return 'timestamp'
