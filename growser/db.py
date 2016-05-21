from collections import OrderedDict
import csv
import datetime
import gzip
from typing import Iterator

from psycopg2.extensions import QuotedString


#: Number of rows to insert per batch transaction
BATCH_SIZE = 50000


class Column:
    """Converts a python value for use in ad-hoc SQL execution."""
    def __init__(self, name, python_type):
        self.name = name
        self.python_type = python_type

    def cast(self, value):
        def to_str(val):
            if isinstance(val, bytes):
                val = val.decode('utf-8')
            return QuotedString(val).getquoted().decode('utf-8')
        func = self.python_type
        if isinstance(value, (datetime.datetime, datetime.date)):
            value = str(value)
            func = to_str
        if issubclass(self.python_type, str):
            func = to_str
        return func(value)

    def __repr__(self):
        return '{}<name={}, type={}>'.format(
            self.__class__.__name__, self.name, self.python_type.__name__)


class ColumnCollection(OrderedDict):
    def __init__(self, columns: list):
        super().__init__([(c.name, c) for c in columns])


class BulkInsertFromIterator:
    def __init__(self, table, data: Iterator, columns,
                 batch_size: int=BATCH_SIZE, header: bool=False):
        """
        Bulk insert into Postgres/MySQL using multi-row INSERT support.

        :param table: Name of the table.
        :param data: Iterable containing the data to insert.
        :param columns: List of :class:`Column` objects.
        :param batch_size: Rows to insert per batch.
        :param header: True if the first row is a header
        """
        self.table = table
        self.data = data
        self.columns = columns
        self.batch_size = batch_size
        self.header = header

        if isinstance(self.data, list):
            self.data = iter(self.data)

        if not isinstance(self.data, Iterator):
            raise TypeError('Expected Iterator, got {}'.format(
                self.data.__class__))

        if not self.columns:
            raise ValueError('Columns cannot be empty')

        if isinstance(self.columns[0], tuple):
            self.columns = [Column(*c) for c in self.columns]

    def batch_execute(self, conn):
        """Insert data in batches of `batch_size`.

        :param conn: A DB API 2.0 connection object
        """
        def batches(data, batch_size) -> list:
            """Return batches of length `batch_size` from any object that
            supports iteration without knowing length."""
            rv = []
            for idx, line in enumerate(data):
                if idx != 0 and idx % batch_size == 0:
                    yield rv
                    rv = []
                rv.append(line)
            yield rv

        columns = ColumnCollection(self.columns)
        if self.header:
            self.columns = [columns.get(h) for h in next(self.data)]
            columns = ColumnCollection(self.columns)

        total = 0
        query = BulkInsertQuery(self.table, columns)
        for batch in batches(self.data, self.batch_size):
            total += query.execute(conn, batch) or 0
            yield total

    def execute(self, conn):
        return max(list(self.batch_execute(conn)))


class BulkInsertQuery:
    def __init__(self, table: str, columns: ColumnCollection):
        """Execute a multi-row INSERT statement.

        :param table: Name of the table being inserted into.
        :param columns: Columns required for type coercion.
        """
        self.table = table
        self.columns = columns
        self.query = 'INSERT INTO {} ({}) VALUES '.format(
                table, ', '.join([c for c in columns]))

    def execute(self, conn, rows: list) -> int:
        """Execute a single multi-row INSERT for `rows`.

        :param conn: Function that returns a database connection
        :param rows: List of tuples in the same order as :attr:`columns`.
        """
        if not len(rows):
            raise ValueError('No data provided')
        if len(self.columns) != len(rows[0]):
            raise ValueError('Expecting {} columns, found {}'.format(
                len(self.columns), len(rows[0])))

        conn = conn()
        cursor = conn.cursor()
        try:
            cursor.execute(self.query + ', '.join(self.escape_rows(rows)))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        return len(rows)

    def escape_rows(self, rows: list):
        """Escape values for use in non-parameterized SQL queries.

        :param rows: List of values to escape.
        """
        def to_tuple(values):
            rv = []
            for column, value in zip(self.columns, values):
                rv.append(self.columns.get(column).cast(value))
            return tuple(rv)

        for idx, row in enumerate(rows):
            rows[idx] = '({})'.format(', '.join(map(str, to_tuple(row))))

        return rows


def from_sqlalchemy_table(table, data, columns, batch_size: int=BATCH_SIZE):
    import os
    from sqlalchemy import Table

    if not isinstance(table, Table):
        raise TypeError('Expected sqlalchemy.Table, got {}'.format(table))

    wrapped = []
    for name in columns:
        column = table.columns.get(name)
        wrapped.append(Column(str(column.name), column.type.python_type))

    if isinstance(data, str) and os.path.exists(data):
        data = from_csv(data)

    return BulkInsertFromIterator(table, data, wrapped, batch_size)


def from_csv(path):  # pragma: no cover
    file = gzip.open(path, 'rt') if path.endswith('gz') else open(path, 'rt')
    return csv.reader(file)
