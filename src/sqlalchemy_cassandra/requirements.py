from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions

class Requirements(SuiteRequirements):
    # Supported Features
    @property
    def primary_key_constraint(self):
        """Cassandra supports primary keys"""
        return exclusions.open()

    @property
    def foreign_key_constraint(self):
        """Cassandra doesn't support foreign keys"""
        return exclusions.closed()

    @property
    def on_update_cascade(self):
        """No cascading updates in Cassandra"""
        return exclusions.closed()

    @property
    def self_referential_foreign_keys(self):
        """No self-referential foreign keys"""
        return exclusions.closed()

    @property
    def returning(self):
        """No RETURNING clause support"""
        return exclusions.closed()

    @property
    def empty_inserts(self):
        """No empty INSERT statements"""
        return exclusions.closed()

    @property
    def boolean_col_expressions(self):
        """Support for boolean expressions"""
        return exclusions.open()

    # Transaction Support
    @property
    def two_phase_transactions(self):
        """No two-phase commit support"""
        return exclusions.closed()

    @property
    def savepoints(self):
        """No savepoint support"""
        return exclusions.closed()

    # Query Features
    @property
    def order_by_col_from_union(self):
        """No complex ORDER BY support in UNIONs"""
        return exclusions.closed()

    @property
    def group_by_complex_expression(self):
        """Limited GROUP BY support"""
        return exclusions.closed()

    # Schema Support
    @property
    def schemas(self):
        """Keyspaces are Cassandra's version of schemas"""
        return exclusions.open()

    @property
    def alter_table(self):
        """Limited ALTER TABLE support"""
        return exclusions.closed()

    # Data Types
    @property
    def json_type(self):
        """JSON type support"""
        return exclusions.open()

    @property
    def uuid_data_type(self):
        """UUID type support"""
        return exclusions.open()

    @property
    def datetime_microseconds(self):
        """Microsecond precision in timestamps"""
        return exclusions.open()

    @property
    def datetime_historic(self):
        """Historic datetime support"""
        return exclusions.open()

    # Constraints
    @property
    def unique_constraint_reflection(self):
        """No unique constraint reflection"""
        return exclusions.closed()

    @property
    def check_constraint_reflection(self):
        """No check constraint support"""
        return exclusions.closed()

    # Other Features
    @property
    def views(self):
        """Materialized view support"""
        return exclusions.open()

    @property
    def autoincrement_insert(self):
        """No autoincrement support"""
        return exclusions.closed()

    @property
    def computed_columns(self):
        """No computed columns"""
        return exclusions.closed()

    @property
    def sequences(self):
        """No sequence support"""
        return exclusions.closed()
