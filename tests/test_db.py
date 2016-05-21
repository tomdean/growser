import csv
from datetime import datetime
import math
import random
import unittest
from unittest.mock import MagicMock

from psycopg2.extensions import connection, cursor
from sqlalchemy import Table, Column as SAColumn, Integer, String, MetaData

from growser.db import (
    Column,
    BulkInsertFromIterator,
    BulkInsertQuery,
    ColumnCollection,
    from_sqlalchemy_table,
    from_csv
)

#: Columns for our take table
columns = ['id', 'name', 'description']
columns_t = [Column(n, t) for n, t in zip(columns, [int, str, str])]
columns_c = ColumnCollection(columns_t)

#: SQLAlchemy boilerplate
metadata = MetaData()
TestTable = Table(
    'TestTable',
    metadata,
    SAColumn('id', Integer, primary_key=True),
    SAColumn('name', String(32)),
    SAColumn('description', String(256))
)


#: Mock connection & cursor object
def get_mock_connection():
    mock_cursor = MagicMock(spec=cursor)
    mock_cursor.mogrify = lambda x: x
    mock_connection = MagicMock(spec=connection)
    mock_connection.cursor = lambda: mock_cursor
    return mock_connection


def get_fake_rows(num_rows) -> list:
    idx = [0]

    def fake_row():
        idx[0] += 1
        return [idx[0], "name '{}'".format(idx[0]), 'description {}'.format(idx[0])]
    return [fake_row() for _ in range(num_rows)]


class ColumnTests(unittest.TestCase):
    def test_casts_datetime(self):
        value = datetime(2016, 1, 1, 0, 0, 0)
        column = Column('name', datetime)
        assert column.cast(value) == "'{}'".format(str(value))

    def test_int(self):
        column = Column('id', int)
        assert column.cast('22') == 22

    def test_escaped_values(self):
        column = Column('id', str)
        value = "test 'escaped' string"
        assert column.cast(value) == "'test ''escaped'' string'"

        value = 'test "double quotes"'
        assert column.cast(value) == "'{}'".format(value)

    def test_bytes(self):
        column = Column('id', str)
        value = b'bytes string'
        assert column.cast(value) == "'bytes string'"


class BulkInsertQueryTests(unittest.TestCase):
    def test_execute(self):
        query = BulkInsertQuery(TestTable.name, columns_c)
        mock = get_mock_connection()
        rows = get_fake_rows(random.randrange(5, 10))
        res = query.execute(lambda: mock, rows)

        assert res == len(rows)
        mock.commit.assert_called_once_with()

    def test_error_no_data(self):
        query = BulkInsertQuery(TestTable.name, columns_c)
        with self.assertRaises(ValueError):
            query.execute(get_mock_connection, [])

    def test_error_invalid_data_width(self):
        q = BulkInsertQuery(TestTable.name, ColumnCollection(columns_t[:-1]))
        with self.assertRaises(ValueError):
            q.execute(get_mock_connection, get_fake_rows(5))


class SQLAlchemyTests(unittest.TestCase):
    def test_sqlalchemy_table(self):

        bulk = from_sqlalchemy_table(TestTable, get_fake_rows(5), columns)
        assert columns == [c.name for c in bulk.columns]

    def test_errors_invalid_type(self):
        with self.assertRaises(TypeError):
            from_sqlalchemy_table(None, get_fake_rows(5), columns)


class BulkInsertFromIteratorTests(unittest.TestCase):
    def test_fails_invalid_type(self):
        with self.assertRaises(TypeError):
            BulkInsertFromIterator(TestTable, {}, columns_t)

    def test_basic_type_inference(self):
        columns_r = [(c.name, c.python_type) for c in columns_t]
        bulk = BulkInsertFromIterator(TestTable, [], columns_r)
        assert isinstance(bulk.columns[0], Column)
        assert issubclass(bulk.columns[0].python_type, int)
        assert issubclass(bulk.columns[1].python_type, str)

    def test_fails_no_columns(self):
        with self.assertRaises(ValueError):
            BulkInsertFromIterator(TestTable, [], None)

    def test_header(self):
        rows = [columns] + get_fake_rows(5)

        bulk = BulkInsertFromIterator(TestTable, rows, columns_t, header=True)
        res = bulk.execute(get_mock_connection)

        assert res == len(rows) - 1

    def test_header_missing_column(self):
        rows = [columns[:2]] + [(r[0], r[1]) for r in get_fake_rows(5)]

        bulk = BulkInsertFromIterator(TestTable, rows, columns_t, header=True)
        res = bulk.execute(get_mock_connection)

        assert res == len(rows) - 1
        assert len(bulk.columns) == len(rows[0])

    def test_batch(self):
        rows = get_fake_rows(100)
        batch_size = 5
        batches_expected = math.ceil(len(rows) / batch_size)

        bulk = BulkInsertFromIterator(TestTable, rows, columns_t, batch_size)
        batches = []
        for batch in bulk.batch_execute(get_mock_connection):
            batches.append(batch)

        assert len(batches) == batches_expected
