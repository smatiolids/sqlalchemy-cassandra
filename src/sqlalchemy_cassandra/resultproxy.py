from sqlalchemy.engine.cursor import CursorResult

class CassandraResultProxy(CursorResult):
    def __init__(self, context):
        super(CassandraResultProxy, self).__init__(context)
        self._cursor = context.cursor
        self._rows = None

    def _fetch_row(self):
        try:
            return next(self._cursor)
        except StopIteration:
            return None

    def fetchone(self):
        row = self._fetch_row()
        if row is None:
            return None
        return self._process_row(row)

    def fetchall(self):
        rows = list(self._cursor)
        return [self._process_row(row) for row in rows]

    def _process_row(self, row):
        # Convert Cassandra row to tuple format expected by SQLAlchemy
        if hasattr(row, '_fields'):  # Named tuple
            return tuple(getattr(row, field) for field in row._fields)
        return tuple(row) 