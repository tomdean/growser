import csv
import datetime
import glob
import gzip
import json
from time import time
import os

import numpy as np
import pandas as pd

from growser.app import app
from growser.models import db, Login, Rating, Repository, Recommendation, \
    RecommendationModel


def initialize_database_schema():
    """Drop & recreate the database schema for SQLAlchemy-bound tables."""
    app.logger.info("Drop and recreate all model tables")

    db.drop_all()
    db.create_all()

    jobs = [
        BatchInsertCSV(Login.__table__, "data/csv/logins.csv"),
        BatchInsertCSV(Repository.__table__, "data/csv/repositories.csv"),
        BatchInsertCSV(Rating.__table__, "data/csv/ratings.datetime.csv"),
        BatchInsertCSV(RecommendationModel.__table__,
                       "data/recommendations/models.csv")
    ]

    columns = ['model_id', 'repo_id', 'recommended_repo_id', 'score']
    for recs in glob.glob('data/recommendations/*/*.gz'):
        jobs.append(BatchInsertCSV(Recommendation.__table__,
                                   recs, header=False, columns=columns))

    app.logger.info("Bulk inserting CSV files into tables")
    for job in jobs:
        job.execute(db.engine)


def merge_repository_data_sources():
    """Merge GitHub API repository data with processed events data."""
    # Data generated during event processing
    df1 = pd.read_csv('data/csv/repos.csv')
    df1['created_at'] = pd.to_datetime(df1['created_at'], unit='s')
    df1['owner'] = df1['name'].str.split("/").str.get(0)
    df1 = df1[df1['owner'] != ''].copy()

    # GitHub API data
    df2 = __json_files_to_df("data/github-api/*.json.gz")
    df2['created_at_alt'] = pd.to_datetime(df2['created_at_alt'])
    df2['updated_at'] = pd.to_datetime(df2['updated_at'], unit='s')

    app.logger.info("Merging data")
    df = pd.merge(df1, df2, on="name", how="left")
    df['created_at'] = df['created_at_alt'].fillna(df['created_at'])
    df['updated_at'] = df['updated_at'].fillna(df['created_at'])
    df['num_stars'] = df['num_stars'].fillna(0).astype(np.int)
    df['num_forks'] = df['num_forks'].fillna(0).astype(np.int)
    df['num_watchers'] = df['num_watchers'].fillna(0).astype(np.int)

    # Pre-sort so that most popular repositories come first
    df.sort_values("num_events", ascending=False, inplace=True)

    # Retain these fields and in this order
    fields = ['repo_id', 'name', 'owner', 'organization', 'language',
              'description', 'num_events', 'num_unique', 'num_stars',
              'num_forks', 'num_watchers', 'updated_at', 'created_at']

    app.logger.info("Saving...")
    df[fields].to_csv("data/csv/repositories.csv", index=False)


def __json_files_to_df(path: str) -> pd.DataFrame:
    exists = {}
    files = glob.glob(path)

    def to_org(r):
        return r['organization']['login'] if 'organization' in r else ''

    rv = []
    app.logger.info("Processing {} JSON files".format(len(files)))
    for filename in files:
        content = json.loads(gzip.open(filename).read().decode("UTF-8"))
        if 'owner' not in content or content['full_name'] in exists:
            continue
        exists[content['full_name']] = True
        rv.append({
            'name': content['full_name'],
            'organization': to_org(content),
            'language': content['language'],
            'description': (content['description'] or "").replace("\n", ""),
            'num_stars': int(content['watchers']),
            'num_forks': int(content['forks']),
            'num_watchers': int(content['subscribers_count']),
            'updated_at': content['updated_at'],
            'created_at_alt': content['created_at']
        })
    return pd.DataFrame(rv)


class BatchInsertCSV(object):
    """Bulk insert a CSV file into a table without SQLAlchemy overhead,
    sacrificing bound parameters for efficiency from multi-valued inserts.

    This introduces the ever-so-unlikely possibility of SQL injection based on
    any vulnerabilities in the underlying DBAPI functions.
    """
    def __init__(self, table, filename: str, batch_size: int=20000,
                 header: bool=True, columns: list=None):
        """
        :param table: SQLAlchemy db.Model
        :param filename: Path to the CSV file to import
        :param batch_size: Number of rows to insert per SQL statement
        :param header: True if the file has a header. If `columns` is empty,
                       the header will be used.
        :param columns: List of columns to insert from CSV into table
        """
        if not os.path.exists(filename):
            raise FileNotFoundError(filename)
        self.table = table
        self.filename = filename
        self.batch_size = batch_size
        self.header = header
        self.columns = columns

    def execute(self, engine):
        app.logger.info("Populating {} from {}".format(
                self.table.name, self.filename))
        with self.open_csv() as data:
            data = csv.reader(data)
            if self.header:
                header = self.validate_csv_header(next(data))
                if not self.columns:
                    self.columns = header

            # Hack for escaping strings without parameter binding
            conn = engine.raw_connection().connection
            func = conn.cursor().mogrify

            def sanitize(val):
                return func('%s', (val,)).decode('UTF-8')

            query = BatchInsert(self.table.name, self.columns)
            for batch in self.next_batch(data, sanitize):
                start = time()
                query.execute(conn, batch)
                app.logger.info("%s inserted (%s)", len(batch), time() - start)

    def conversion_map(self, sanitize) -> dict:
        """Map each column to a function for casting data prior to inserting."""
        rv = {}

        def sanitize_str(val):
            return sanitize(str(val))

        for column in self.table.columns:
            func = column.type.python_type
            if func is datetime.datetime or func is str:
                func = sanitize_str
            rv[column.name] = func
        return rv

    def next_batch(self, data, sanitize=None) -> list:
        """Yield batches of rows from the CSV file as a list of tuples"""
        convert_to = self.conversion_map(sanitize)

        def to_tuple(rv):
            return tuple([convert_to[f](v) for f, v in zip(self.columns, rv)])

        batch = []
        for idx, line in enumerate(data):
            batch.append(to_tuple(line))
            if idx > 0 and idx % self.batch_size == 0:
                yield batch
                batch = []
        yield batch

    def open_csv(self):
        if self.filename.endswith('gz'):
            return gzip.open(self.filename, 'rt')
        return open(self.filename, 'rt')

    def validate_csv_header(self, header: list) -> list:
        """Extract the header from CSV and verify the columns exist."""
        for column in header:
            if self.table.columns.get(column) is None:
                raise ValueError("%s is not a valid column", column)
        return header


class BatchInsert(object):
    def __init__(self, table: str, columns: list):
        self.table = table
        self.columns = columns
        self.query = "INSERT INTO {} ({}) VALUES ".format(
                table, ", ".join(columns))

    def execute(self, conn, values: list):
        values = ["({})".format(",".join(map(str, value))) for value in values]
        cursor = conn.cursor()
        cursor.execute(self.query + ", ".join(values))
        conn.commit()
        cursor.close()
