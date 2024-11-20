# SQLAlchemy-Cassandra

A SQLAlchemy dialect for Apache Cassandra.

## Installation 

```
bash
poetry install
```

## Usage

```python
from sqlalchemy import create_engine
engine = create_engine('cassandra://username:password@hostname:port/keyspace')
```

## Development

1. Clone the repository
2. Install dependencies: `poetry install`
3. Run tests: `poetry run pytest`