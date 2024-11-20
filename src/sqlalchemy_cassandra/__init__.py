from sqlalchemy.dialects import registry

# Version info
__version__ = '0.1.0'

# Register the dialect
registry.register('cassandra', 'sqlalchemy_cassandra.dialect', 'CassandraDialect')

# Make key components available at package level
from .dialect import CassandraDialect
from .resultproxy import CassandraResultProxy

__all__ = [
    'CassandraDialect',
    'CassandraResultProxy'
]
