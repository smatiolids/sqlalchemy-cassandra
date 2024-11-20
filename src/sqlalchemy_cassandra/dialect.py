from sqlalchemy.engine import default
from sqlalchemy import exc, pool, util
from sqlalchemy.sql import compiler
from cassandra.cluster import (
    Cluster,
)
from cassandra.auth import PlainTextAuthProvider
from .base import CassandraDialect
from .compiler import CassandraCompiler, CassandraDDLCompiler
from .types import (
    UUID,
    Timestamp,
    Text,
    Integer,
    Float,
    Boolean,
    Map,
    List,
    Set
)
from .resultproxy import CassandraResultProxy

class CassandraExecutionContext(default.DefaultExecutionContext):
    def get_result_proxy(self):
        # Handle result sets from Cassandra
        return CassandraResultProxy(self)
    
    def create_cursor(self):
        # Create and return a cursor
        return self._dbapi_connection.cursor()

class CassandraIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = set([
        'add', 'allow', 'alter', 'and', 'any', 'apply',
        'asc', 'authorize', 'batch', 'begin', 'by',
        # ... more Cassandra reserved words ...
    ])

class CassandraDialect_cassandra(CassandraDialect):
    name = 'cassandra'
    driver = 'cassandra'

    statement_compiler = CassandraCompiler
    ddl_compiler = CassandraDDLCompiler
    preparer = CassandraIdentifierPreparer
    execution_ctx_cls = CassandraExecutionContext
    default_paramstyle = 'named'  # or 'named' depending on how you want to handle parameters
  

    def _get_server_version_info(self, connection):
        # Get Cassandra version info
        return tuple(
            int(x) for x in connection.server_version.split('.')[:3]
        )

    def get_columns(self, connection, table_name, schema=None, **kw):
        # Return column information for a table
        q = "SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = %s AND table_name = %s"
        result = connection.execute(q, [schema or self.default_schema_name, table_name])
        return [self._map_column(row) for row in result]

    def get_table_names(self, connection, schema=None, **kw):
        # Return list of table names
        print("Getting table names dialect")
        q = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
        result = connection.execute(q, [schema or self.default_schema_name])
        return [row[0] for row in result]

    def has_table(self, connection, table_name, schema=None, **kw):
        # Check if table exists
        q = """
        SELECT table_name FROM system_schema.tables 
        WHERE keyspace_name = %s AND table_name = %s
        """
        result = connection.execute(q, [schema or self.default_schema_name, table_name])
        return bool(result.first())

    def do_execute(self, cursor, statement, parameters, context=None):
        # Execute a statement
        cursor.execute(statement, parameters)

    def do_execute_no_params(self, cursor, statement, context=None):
        # Execute a statement without parameters
        cursor.execute(statement)

    def _map_column(self, row):
        # Helper to map column metadata
        return {
            'name': row.column_name,
            'type': self._get_column_type(row.type),
            'nullable': True,  # Cassandra columns are always nullable
            'default': None
        }

    def _get_column_type(self, cassandra_type):
        # Map Cassandra types to SQLAlchemy types
        type_map = {
            'uuid': UUID,
            'timestamp': Timestamp,
            'text': Text,
            'varchar': Text,
            'int': Integer,
            'float': Float,
            'boolean': Boolean,
            'map': Map,
            'list': List,
            'set': Set,
        }
        return type_map.get(cassandra_type.lower(), Text)
