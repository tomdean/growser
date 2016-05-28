from collections import OrderedDict
import datetime
from typing import Iterator, List

from flask_sqlalchemy import SQLAlchemy
from psycopg2.extensions import QuotedString
from sqlalchemy import Table


#: Number of rows to insert per batch transaction
BATCH_SIZE = 50000


class Column:
    def __init__(self, name: str, python_type: type):
        """Wrapper to cast Python values for use in ad-hoc SQL.

        Example::

            columns = [Column('id', int), Column('amount', float)]

        :param name: Name of the column.
        :param python_type: Python type e.g. int, str, float.
        """
        self.name = name
        self.python_type = python_type

    def escape(self, value) -> str:
        """Escape a value for use in a Postgres ad-hoc SQL statement."""
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

    def __eq__(self, b):
        return self.name == b.name and self.python_type == b.python_type

    def __repr__(self):
        return '{}<name={}, type={}>'.format(
            self.__class__.__name__, self.name, self.python_type.__name__)


class ColumnCollection(OrderedDict):
    def __init__(self, columns: list):
        super().__init__([(c.name, c) for c in columns])


class BulkInsertFromIterator:
    def __init__(self, table, data: Iterator, columns: list,
                 batch_size: int=BATCH_SIZE, header: bool=False):
        """Bulk insert into Postgres from an iterator in fixed-size batches.

        Example::

            bulk = BulkInsertFromIterator(
                'table.name',
                iter([[1, 'Python'], [2, 'PyPy', 3]]),
                [Column('id', int), Column('name', str)]
            )
            bulk.execute(db.engine.raw_connection)

        :param table: Name of the table.
        :param data: Iterable containing the data to insert.
        :param columns: List of :class:`Column` objects.
        :param batch_size: Rows to insert per batch.
        :param header: True if the first row is a header.
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
        """Execute all batches."""
        return max(list(self.batch_execute(conn)))


class BulkInsertQuery:
    def __init__(self, table: str, columns: ColumnCollection):
        """Execute a multi-row INSERT statement.

        This does not take advantage of parameterized queries, but escapes
        string values manually in :class:`Column`.

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
                rv.append(self.columns.get(column).escape(value))
            return tuple(rv)

        for idx, row in enumerate(rows):
            rows[idx] = '({})'.format(', '.join(map(str, to_tuple(row))))

        return rows


def from_sqlalchemy_table(table: Table, data: Iterator, columns: list,
                          batch_size: int=BATCH_SIZE, header: bool=False):
    """Return a :class:`BulkInsertFromIterator` based on the metadata
    of a SQLAlchemy table.

    Example::

        batch = from_sqlalchemy_table(
            Rating.__table__,
            data,
            ['rating_id', 'repo_id', 'login_id', 'rating']
        )

    :param table: A :class:`sqlalchemy.Table` instance.
    :param data: An iterator.
    :param columns: List of column names to use.
    """
    if not isinstance(table, Table):
        raise TypeError('Expected sqlalchemy.Table, got {}'.format(table))

    wrapped = []
    for name in columns:
        column = table.columns.get(name)
        wrapped.append(Column(str(column.name), column.type.python_type))

    return BulkInsertFromIterator(table, data, wrapped, batch_size, header)


def as_columns(columns) -> List[Column]:
    rv = []
    for column in columns:
        if isinstance(column, Column):
            rv.append(column)
        if isinstance(column, tuple):
            rv.append(Column(*column))
        if isinstance(column, str):
            rv.append(Column(column, str))
    return rv


def to_dict_model(self) -> dict:
    """Returns a single SQLAlchemy model instance as a dictionary."""
    return dict((key, getattr(self, key)) for key in self.__mapper__.c.keys())


def to_dict_query(self) -> list:
    """Returns all SQLAlchemy records in a query as dictionaries."""
    return [row.to_dict() for row in self.all()]


class SQLAlchemyAutoCommit(SQLAlchemy):
    """By default ``psycopg2`` will wrap SELECT statements in a transaction.

    This can be avoided using AUTOCOMMIT to rely on Postgres' default
    implicit transaction mode (see this `blog post <http://bit.ly/1N0a7Lj>`_
    for more details).
    """
    def apply_driver_hacks(self, app, info, options):
        super().apply_driver_hacks(app, info, options)
        options['isolation_level'] = 'AUTOCOMMIT'
