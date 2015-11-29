import csv
import datetime
import glob
import gzip
import json
import time
import os

import numpy as np
import pandas as pd
from sqlalchemy.schema import Index

from growser.app import app, db
from growser.models import Login, Rating, Repository, Recommendation, \
    RecommendationModel


def initialize_database_schema():
    """Drop, recreate, and repopulate the SQL database."""
    app.logger.info("Drop and recreate all model tables")
    db.drop_all()
    db.create_all()

    # Data from GitHub Archive & static data
    jobs = [BatchInsertCSV(Login.__table__, "data/csv/logins.csv"),
            BatchInsertCSV(Repository.__table__, "data/csv/repositories.csv"),
            BatchInsertCSV(Rating.__table__, "data/csv/ratings.datetime.csv"),
            BatchInsertCSV(RecommendationModel.__table__,
                           "data/recommendations/models.csv")]

    # Recommendations
    columns = ['model_id', 'repo_id', 'recommended_repo_id', 'score']
    for recs in glob.glob('data/recommendations/*/*.gz'):
        jobs.append(BatchInsertCSV(Recommendation.__table__,
                                   recs, header=False, columns=columns))

    app.logger.info("Bulk inserting CSV files into tables")
    for job in jobs:
        job.execute(db.engine)

    app.logger.info("Creating secondary indexes")
    Index("IX_recommendation_model_repo", Recommendation.model_id,
          Recommendation.repo_id, quote=False).create(db.engine)
    Index("IX_rating_repo", Rating.repo_id, Rating.created_at, quote=False) \
        .create(db.engine)


def merge_repository_data_sources():
    """Merge GitHub API repository data with processed events data."""
    # Data generated during event processing
    df1 = pd.read_csv('data/csv/repos.csv')
    df1['created_at'] = pd.to_datetime(df1['created_at'], unit='s')
    df1['owner'] = df1['name'].str.split("/").str.get(0)
    df1 = df1[df1['owner'] != ''].copy()

    # GitHub API data
    df2 = get_github_api_results("data/github-api/*.json.gz")
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


def get_github_api_results(path: str) -> pd.DataFrame:
    """Parse the JSON fetched from the GitHub API so that we have more data to
    work with locally."""
    exists = {}
    files = glob.glob(path)

    def to_org(r):
        return r['organization']['login'] if 'organization' in r else ''

    rv = []
    app.logger.info("Processing {} JSON files".format(len(files)))
    for filename in files:
        content = json.loads(gzip.open(filename).read().decode("UTF-8"))
        # Not found or rate limit error edge case
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

            # Hack for escaping strings without parameter binding (psycopg2)
            conn = engine.raw_connection().connection
            func = conn.cursor().mogrify
            self.escape_string = lambda val: func('%s', (val,)).decode('UTF-8')

            query = BatchInsert(self.table.name, self.columns)
            for batch in self.next_batch(data):
                start = time.time()
                query.execute(conn, batch)
                app.logger.info("%s rows inserted in %s seconds",
                                len(batch), time.time() - start)

    def escape_string(self, value: str) -> str:
        raise NotImplementedError

    def next_batch(self, data) -> list:
        """Yield batches of rows from the CSV file as a list of tuples"""
        convert_to = self.to_python_types()

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
                raise ValueError("%s not found (%s)", column, self.table.name)
        return header

    def to_python_types(self) -> dict:
        """Return a dictionary of (column -> func) for casting values before
        being used in a SQL statement."""
        rv = {}

        def to_str(val):
            return self.escape_string(str(val))
        for column in self.table.columns:
            func = column.type.python_type
            if func is datetime.datetime or func is str:
                func = to_str
            rv[column.name] = func
        return rv


class BatchInsert(object):
    """Build a multi-valued INSERT statement."""
    def __init__(self, table: str, columns: list):
        self.table = table
        self.columns = columns
        self.query = "INSERT INTO {} ({}) VALUES ".format(
                table, ", ".join(columns))

    def execute(self, conn, values: list):
        start = time.time()
        values = ["({})".format(",".join(map(str, value))) for value in values]
        cursor = conn.cursor()
        cursor.execute(self.query + ", ".join(values))
        conn.commit()
        cursor.close()
