import csv
import datetime
import gzip
import time

from psycopg2.extensions import cursor as pgcursor

from growser.app import log

BATCH_SIZE = 50000


class BulkInsertList(object):
    """Bulk insert into Postgres/MySQL using multi-row INSERT support."""

    def __init__(self, table, data, columns: list, batch_size: int=BATCH_SIZE):
        """
        :param table: A :class:`sqlalchemy.sql.schema.Table` object.
        :param data: Iterable containing the data to insert.
        :param columns: List of column names mapping the values of `data` to
                        their respective database columns.
        :param batch_size: Number of rows to insert per SQL statement.
        """
        if hasattr(table, '__table__'):
            table = table.__table__
        self.table = table
        self.columns = columns or []
        self.batch_size = batch_size
        self.data = data

    def execute(self, engine):
        """Insert the data in batches of `batch_size`.

        :param engine: A :class:`sqlalchemy.engine.base.Engine` instance.
        """
        if not self.columns:
            raise ValueError("Columns cannot be empty")

        def batches(data, batch_size) -> list:
            """Return batches of length `batch_size` from any object that
            supports iteration without knowing length a priori."""
            rv = []
            for idx, line in enumerate(data):
                if idx != 0 and idx % batch_size == 0:
                    yield rv
                    rv = []
                rv.append(line)
            yield rv

        total = 0
        first_start = time.time()
        conn = engine.raw_connection()
        query = BulkInsertQuery(self.table, self.columns)

        for batch in batches(self.data, self.batch_size):
            start = time.time()
            total += query.execute(conn, batch)
            log.debug("%s rows inserted in %s seconds (%s total)",
                      len(batch), round(time.time() - start, 4), total)

        log.info("Copied %s rows in %s seconds",
                 total, round(time.time() - first_start, 2))


class BulkInsertCSV(BulkInsertList):
    """Bulk Insert from a CSV file into a database table.

    :param table: A :class:`sqlalchemy.sql.schema.Table` object.
    :param filename: Path of the CSV file to insert.
    :param columns: Ordered list of column names mapping the CSV field to a
                    database column.
    :param header: If true, the CSV file has a header row that will be used
                   in place of `columns`.
    :param batch_size: Number of rows to insert per batch statement.
    """
    def __init__(self, table, filename: str, columns: list=None,
                 header: bool=True, batch_size: int=BATCH_SIZE):
        super().__init__(table, [], batch_size=batch_size, columns=columns)
        self.filename = filename
        self.header = header

    def open_csv(self):
        """Return an open stream for the CSV file."""
        if self.filename.endswith('gz'):
            return gzip.open(self.filename, 'rt')
        return open(self.filename, 'rt')

    def execute(self, engine):
        log.info("Copying %s into %s", self.filename, self.table.name)
        with self.open_csv() as data:
            data = csv.reader(data)
            if self.header:
                self.validate_csv_header(next(data))
            self.data = data
            super().execute(engine)

    def validate_csv_header(self, header: list):
        """Extract the CSV header row and verify the columns exist."""
        for column in header:
            if self.table.columns.get(column) is None:
                raise ValueError("%s not found (%s)", column, self.table.name)
        if not self.columns:
            self.columns = header

    def __repr__(self):
        return "{}(table={}, filename={})".format(
                self.__class__, self.table, self.filename)


class BulkInsertQuery(object):
    """Execute a multi-row INSERT statement."""
    def __init__(self, table, columns: list):
        self.table = table
        self.columns = columns
        self.query = "INSERT INTO {} ({}) VALUES ".format(
                table.name, ", ".join(columns))
        self.converters = None

    def execute(self, conn, rows: list) -> int:
        """Execute a single multi-row INSERT against `values`.

        :param conn: A :class:`psycopg2.extensions.connection` connection.
        :param rows: List of tuples in the same order as :attr:`columns`.
        """
        if not len(rows):
            return

        cursor = conn.cursor()
        if not isinstance(cursor, pgcursor):
            raise NotImplementedError("Only psycopg2 is currently supported")

        rows = self.escape_rows(cursor.mogrify, rows)
        try:
            cursor.execute(self.query + ", ".join(rows))
            conn.commit()
        finally:
            cursor.close()
        return len(rows)

    def escape_rows(self, escape_func, rows: list):
        """Cast values to their expected python types and escape strings for
        use in non-parameterized queries."""
        if not self.converters:
            def escape(value):
                return escape_func('%s', (value,)).decode('UTF-8')

            self.converters = self.to_python_types(escape)

        def to_tuple(values):
            rv = []
            for column, value in zip(self.columns, values):
                rv.append(self.converters[column](value))
            return tuple(rv)

        for idx, row in enumerate(rows):
            rows[idx] = "({})".format(", ".join(map(str, to_tuple(row))))

        return rows

    def to_python_types(self, escape_string) -> dict:
        """Return a dict (name -> func) for casting to SQL-equivalent types."""
        rv = {}

        def to_str(val):
            return escape_string(str(val))
        for column in self.table.columns:
            func = column.type.python_type
            if issubclass(func, (datetime.datetime, datetime.date, str)):
                func = to_str
            rv[column.name] = func
        return rv
