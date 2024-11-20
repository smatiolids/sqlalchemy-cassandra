from setuptools import setup, find_packages

setup(
    name="sqlalchemy-cassandra",
    version="0.1",
    description="SQLAlchemy dialect for Cassandra",
    packages=find_packages(),
    install_requires=[
        "sqlalchemy",
        "cassandra-driver"
    ],
    entry_points={
        "sqlalchemy.dialects": [
            "cassandra = sqlalchemy_cassandra.dialect:CassandraDialect"
        ]
    },
)
