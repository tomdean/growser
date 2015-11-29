import csv
import datetime
import glob
import gzip
import json
import time

import numpy as np
import pandas as pd
from sqlalchemy.schema import Index

from growser.app import app, db
from growser.models import Login, Rating, Repository, Recommendation, \
    RecommendationModel

# Default number of rows to insert in a single multi-valued INSERT statement
BATCH_SIZE = 25000


def initialize_database_schema():
    """Drop, recreate, and repopulate the SQL database."""
    app.logger.info("Drop and recreate all model tables")
    db.drop_all()
    db.create_all()

    # Data from GitHub Archive & static data
    jobs = [BulkInsertCSV(Login.__table__, "data/csv/logins.csv"),
            BulkInsertCSV(Repository.__table__, "data/csv/repositories.csv"),
            BulkInsertCSV(Rating.__table__, "data/csv/ratings.datetime.csv"),
            BulkInsertCSV(RecommendationModel.__table__,
                          "data/recommendations/models.csv")]

    # Recommendations
    columns = ['model_id', 'repo_id', 'recommended_repo_id', 'score']
    for recs in glob.glob('data/recommendations/*/*.gz'):
        jobs.append(BulkInsertCSV(Recommendation.__table__,
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
    """Merge GitHub API repository data with events data."""
    # Data from processing events gives us local ID
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

    # Keep these fields and in this order
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


class BulkInsertList(object):
    def __init__(self, table, data, columns: list, batch_size: int=BATCH_SIZE):
        self.table = table
        self.columns = columns or []
        self.batch_size = batch_size
        self.data = data

    @staticmethod
    def escape_string(value: str) -> str:
        return value

    def execute(self, engine):
        # Hack for escaping strings without parameter binding (psycopg2)
        conn = engine.raw_connection().connection
        func = conn.cursor().mogrify
        BulkInsertList.escape_string = \
            lambda val: func('%s', (val,)).decode('UTF-8')

        query = BatchInsert(self.table.name, self.columns)
        for batch in self.next_batch(self.data):
            start = time.time()
            query.execute(conn, batch)
            app.logger.info("%s rows inserted in %s seconds",
                            len(batch), time.time() - start)

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

    def to_python_types(self) -> dict:
        """Return a dictionary of (column -> func) for casting values before
        being used in a SQL statement."""
        rv = {}

        def to_str(val):
            return self.escape_string(str(val))
        for column in self.table.columns:
            func = column.type.python_type
            if issubclass(func, (datetime.datetime, datetime.date, str)):
                func = to_str
            rv[column.name] = func
        return rv


class BulkInsertCSV(BulkInsertList):
    def __init__(self, table, filename: str, columns: list=None,
                 header: bool=False, batch_size: int=BATCH_SIZE):
        super().__init__(table, self.open_csv(filename, header),
                         batch_size=batch_size, columns=columns)

    def open_csv(self, filename, header):
        if filename.endswith('gz'):
            fh = gzip.open(filename, 'rt')
        else:
            fh = open(filename, 'rt')
        rv = csv.reader(fh)
        if header:
            self.validate_csv_header(next(rv))
            if not self.columns:
                self.columns = header
        return rv

    def validate_csv_header(self, header: list):
        """Extract the header from CSV and verify the columns exist."""
        for column in header:
            if self.table.columns.get(column) is None:
                raise ValueError("%s not found (%s)", column, self.table.name)


class BatchInsert(object):
    """Build a multi-valued INSERT statement."""
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
