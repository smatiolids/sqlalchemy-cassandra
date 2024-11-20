import pytest
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import DBAPIError


class TestCassandraDialect:
    @pytest.fixture(autouse=True)
    def setup_connection(self):
        # Load environment variables
        from dotenv import load_dotenv
        import os
        load_dotenv(override=True)
        # Get connection parameters from environment variables
        self.username = os.getenv('ASTRA_DB_CLIENT_ID')
        self.password = os.getenv('ASTRA_DB_CLIENT_SECRET')
        self.secure_connect_bundle = os.getenv('ASTRA_DB_SECURE_BUNDLE_PATH')
        
        # Create connection URL and engine
        connection_url = f'cassandra://{self.username}:{self.password}@localhost:9042'
        self.engine = create_engine(connection_url, connect_args={
            'secure_connect_bundle': self.secure_connect_bundle,
            'username': self.username,
            'password': self.password
        })
        self.connection = self.engine.connect()
        
        yield  # This allows the tests to run
        
        # Cleanup after tests
        self.connection.close()

    def test_cassandra_connection(self):
        assert self.connection is not None

    def test_get_table_names(self):
        """Test retrieving table names from the database."""
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        if tables:
            print("Tables in keyspace:")
            for table in tables:
                print(f"- {table}")
        assert isinstance(tables, list)
