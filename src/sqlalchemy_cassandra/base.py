from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import types as sqltypes
from .compiler import CassandraTypeCompiler
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class CassandraDialect(default.DefaultDialect):
        
    name = 'cassandra'
    driver = 'cassandra'
    
    # Supported data types
    type_compiler = CassandraTypeCompiler
    supports_native_boolean = True
    supports_native_decimal = True
    default_paramstyle = 'named'
    default_schema_name = 'demo'
    
    # Dialect features
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    
    @classmethod
    def import_dbapi(cls):
        from cassandra.protocol import ErrorMessage, ServerError, RequestExecutionException, RequestValidationException

        # Add required DBAPI attributes to Cluster
        if not hasattr(Cluster, 'paramstyle'):
            Cluster.paramstyle = cls.default_paramstyle
            
        # Add Error classes that SQLAlchemy expects
        if not hasattr(Cluster, 'Error'):
            Cluster.Error = ErrorMessage
            Cluster.InterfaceError = ServerError
            Cluster.DatabaseError = RequestExecutionException
            Cluster.DataError = RequestValidationException
            
        return Cluster
    
    def create_connect_args_(self, connect_args):
        if connect_args.get("secure_connect_bundle") != None:
            opts = {
                'cloud': {
                    'secure_connect_bundle': connect_args.get("secure_connect_bundle")
                },
                'auth_provider': PlainTextAuthProvider(
                    connect_args.get("username"),
                    connect_args.get("password")
                )
            }
        else:
            opts = {
                'host': connect_args.get("host") or 'localhost',
                'port': connect_args.get("port") or 9042,
                'username': connect_args.get("username"),
                'password': connect_args.get("password"),
                'database': connect_args.get("database")
            }
        return ([], opts)
    
    def connect(self, *args, **kwargs):
        # Create a Cassandra session using cassandra-driver
        print("Connecting...")
        _,opts = self.create_connect_args_(kwargs)
        cluster = Cluster(**opts)
        session = cluster.connect()
        print("Connected to cluster")
        if kwargs.get("keyspace") != None:
            session.set_keyspace(kwargs.get("keyspace"))
            print("Keyspace set")
            
        session.rollback = lambda: None
        return session    

    def get_table_names(self, connection, schema=None, **kw):
        # Return list of table names
        print("Getting table names base")
        q = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = :keyspace"
        result = connection. execute(q, {"keyspace": schema or self.default_schema_name})
        res = [row[0] for row in result]
        print("Table names:")
        print(res)
        return res 

    def do_execute(self, cursor, statement, parameters, context=None):
        # Execute a statement
        cursor.execute(statement, parameters)