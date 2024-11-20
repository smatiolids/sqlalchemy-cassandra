from sqlalchemy import types as sqltypes
from sqlalchemy.types import TypeEngine
from sqlalchemy import util

class CassandraTypeEngine(TypeEngine):
    """Base class for Cassandra-specific types"""
    pass


class UUID(CassandraTypeEngine):
    """Represents a Cassandra UUID type"""
    __visit_name__ = 'UUID'
    
    def __init__(self, as_uuid=True):
        self.as_uuid = as_uuid
    
    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return str(value) if not self.as_uuid else value
        return process
    
    def result_processor(self, dialect, coltype):
        import uuid
        def process(value):
            if value is None:
                return None
            return uuid.UUID(value) if self.as_uuid else value
        return process

class Timestamp(sqltypes.DateTime):
    """Represents a Cassandra timestamp type"""
    __visit_name__ = 'TIMESTAMP'

class Text(sqltypes.Text):
    """Represents a Cassandra text type"""
    __visit_name__ = 'TEXT'

class Integer(sqltypes.Integer):
    """Represents a Cassandra int type"""
    __visit_name__ = 'INT'

class Float(sqltypes.Float):
    """Represents a Cassandra float type"""
    __visit_name__ = 'FLOAT'

class Boolean(sqltypes.Boolean):
    """Represents a Cassandra boolean type"""
    __visit_name__ = 'BOOLEAN'

class Counter(sqltypes.BigInteger):
    """Represents a Cassandra counter type"""
    __visit_name__ = 'COUNTER'

class Map(CassandraTypeEngine):
    """Represents a Cassandra map type"""
    __visit_name__ = 'MAP'
    
    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type
    
    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return dict(value)
        return process

class List(CassandraTypeEngine):
    """Represents a Cassandra list type"""
    __visit_name__ = 'LIST'
    
    def __init__(self, item_type):
        self.item_type = item_type
    
    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return list(value)
        return process

class Set(CassandraTypeEngine):
    """Represents a Cassandra set type"""
    __visit_name__ = 'SET'
    
    def __init__(self, item_type):
        self.item_type = item_type
    
    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            return set(value)
        return process

# Type mapping dictionary
ischema_names = {
    'uuid': UUID,
    'timeuuid': UUID,
    'timestamp': Timestamp,
    'text': Text,
    'varchar': Text,
    'ascii': Text,
    'int': Integer,
    'bigint': sqltypes.BigInteger,
    'float': Float,
    'double': sqltypes.Float,
    'boolean': Boolean,
    'counter': Counter,
    'decimal': sqltypes.Numeric,
    'varint': sqltypes.BigInteger
}
