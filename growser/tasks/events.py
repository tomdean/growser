import datetime

import numpy as np
import pandas as pd

from growser.app import app, log
from growser.tasks import BlockingJobPipeline
from growser.services.bigquery import (
    DeleteTable,
    ExportTableToCSV,
    PersistQueryToTable
)


class BatchManager:
    """Process new events from Github Archive into a local database.

    It also maintains separate CSV files for logins & repositories."""

    def __init__(self, engine, repos, logins):
        self.engine = engine
        self.repos = Source(repos, ['repo_id', 'name', 'created_at'])
        self.logins = Source(logins, ['login_id', 'login', 'created_at'])

    def process_batch(self, filename: str):
        log.info("Processing %s", filename)
        df = self._process_events_csv(filename).rename(columns={'repo': 'name'})

        # Find new repositories and logins, assigning them new IDs
        log.info("Finding new logins & repositories")
        new_repos = self.find_delta(df, self.repos.data, 'repo_id', 'name')
        new_logins = self.find_delta(df, self.logins.data, 'login_id', 'login')

        log.info("Adding new logins and repositories")
        self.repos.update(new_repos)
        self.logins.update(new_logins)

        # Eliminate dupes by grouping by type
        log.info("Group per user/repo/type")
        df = df.groupby(['login', 'name', 'type']) \
            .agg({'created_at': 'min'}) \
            .reset_index()

        # Group & sum on rating will give us a 3 if a user starred & forked
        log.info("Group per user/repo")
        df = df.groupby(['login', 'name']) \
            .agg({'type': 'sum', 'created_at': 'min'}) \
            .reset_index() \
            .rename(columns={'type': 'rating'})

        # Replace the string login & repo values with our new integer ID's
        log.info("Adding repo_id to events")
        events = pd.merge(df, self.repos.data[['repo_id', 'name']], on='name')
        events = events[['repo_id', 'login', 'rating', 'created_at']]

        log.info("Adding login_id to events")
        events = pd.merge(events, self.logins.data[['login_id', 'login']], on='login')
        events = events[['login_id', 'repo_id', 'rating', 'created_at']]

        # Using Postgres timestamp (datetime), convert from epoch.
        log.info('Converting dates')
        for ds in (events, new_repos, new_logins):
            ds['created_at'] = pd.to_datetime(ds['created_at'], unit='s')

        # Explicitly set types
        events = events.sort_values(['created_at', 'repo_id'])
        events['login_id'] = events['login_id'].astype(np.int)
        events['repo_id'] = events['repo_id'].astype(np.int)
        events['rating'] = events['rating'].astype(np.int)

        # Add derived columns
        df['owner'] = df['name'].str.split('/').str.get(0)

        # Save to CSV for bulk inserting into database
        log.info("Saving batch to CSV")
        new_repos.to_csv('data/batch/repos.csv', index=False)
        new_logins.to_csv('data/batch/logins.csv', index=False)
        events.to_csv('data/batch/events.csv', index=False)

        log.info("Processing CSV files in Postgres")
        sql = open("deploy/etl/sql/process_events_batch.sql").read()
        self.engine.execute(sql)
        self.repos.append_delta()
        self.logins.append_delta()

    @staticmethod
    def _process_events_csv(filename: str) -> pd.DataFrame:
        fields = ['type', 'repo', 'login', 'created_at']
        dtypes = {'type': np.int, 'repo': np.object,
                  'login': np.object, 'created_at': np.long}

        df = pd.read_csv(filename, engine='c', usecols=fields, dtype=dtypes)
        df['created_at'] /= 1000000
        df['created_at'] = df['created_at'].astype(np.int)

        return df.sort_values('created_at')

    @staticmethod
    def find_delta(batch: pd.DataFrame, source: pd.DataFrame,
                   id_field: str, name_field: str) -> pd.DataFrame:
        """Find all items that exist in `batch` but not in `source`."""
        df = batch.groupby(name_field) \
            .agg({'created_at': [np.amin]}) \
            .reset_index()

        df.columns = [name_field, 'created_at']
        df = df.sort_values(['created_at', name_field]) \
            .reset_index().drop('index', 1)

        delta = df[~df[name_field].isin(source[name_field])].copy()
        if len(delta):
            max_id = int(source[id_field].iloc[-1]) + 1
            delta[id_field] = list(range(max_id, max_id + len(delta)))
            delta = delta[[id_field, name_field, 'created_at']]

        return delta


class Source:
    """Simple wrapper around repos & logins"""

    def __init__(self, filename: str, fields: list):
        self.filename = filename
        self.data = pd.read_csv(filename)[fields]
        self.delta = None

    def update(self, delta):
        self.delta = delta
        self.data = pd.concat([self.data, delta], ignore_index=True)

    def append_delta(self):
        with open(self.filename, 'a') as fh:
            self.delta.to_csv(fh, header=False, index=False)


def export_daily_events_to_csv(bigquery, year: int, month: int, day: int):
    _export_query_to_csv(bigquery, year, month, day)


def export_monthly_events_to_csv(bigquery, year: int, month: int):
    _export_query_to_csv(bigquery, year, month)


def export_yearly_events_to_csv(bigquery, year: int):
    if datetime.datetime.now().year <= year:
        raise Exception('Archive not available for current year')
    _export_query_to_csv(bigquery, year)


def _export_query_to_csv(bigquery, year: int, month: int=None, day: int=None):
    """Export [watch, fork] events from Google BigQuery to Cloud Storage."""
    month = str(month).zfill(2) if month else ''
    day = str(day).zfill(2) if day else ''
    for_date = '{}{}{}'.format(year, month, day)
    log.debug('Events for {}'.format(for_date))

    if day:
        table = "day.events_{}".format(for_date)
    elif month:
        table = "month.{}".format(for_date)
    else:
        table = "year.{}".format(year)

    if year >= 2015:
        if day is not None:
            query = """
                SELECT
                    CASE WHEN type = 'WatchEvent' THEN 1 ELSE 2 END AS type,
                    org.id AS org_id,
                    repo.id AS repo_id,
                    repo.name AS repo,
                    actor.login AS login,
                    actor.id AS actor_id,
                    TIMESTAMP_TO_USEC(created_at) AS created_at
                FROM [githubarchive:{table}]
                WHERE type IN ('WatchEvent', 'ForkEvent')
            """
        else:
            query = """
                SELECT
                    CASE WHEN type = 'WatchEvent' THEN 1 ELSE 2 END AS type,
                    org_id,
                    repo_id,
                    repo_name AS repo,
                    actor_login AS login,
                    actor_id,
                    TIMESTAMP_TO_USEC(created_at) AS created_at
                FROM [githubarchive:{table}]
                WHERE type IN ('WatchEvent', 'ForkEvent')
            """
    else:
        query = """
            SELECT
                CASE WHEN type = 'WatchEvent' THEN 1 ELSE 2 END AS type,
                repository_organization AS org,
                (CASE WHEN repository_owner IS NULL
                  THEN repository_name
                   ELSE (repository_owner + '/' + repository_name)
                END) AS repo,
                actor AS login,
                PARSE_UTC_USEC(created_at) AS created_at
            FROM [githubarchive:{table}]
            WHERE type IN ('WatchEvent', 'ForkEvent')
              AND repository_name IS NOT NULL
        """

    export_table = app.config.get('BIG_QUERY_EXPORT_TABLE')
    export_path = app.config.get('BIG_QUERY_EXPORT_PATH').format(date=for_date)
    query = query.format(table=table)

    pipeline = BlockingJobPipeline(bigquery)
    pipeline.add(PersistQueryToTable, query, export_table)
    pipeline.add(ExportTableToCSV, export_table, export_path)
    pipeline.add(DeleteTable, export_table)
    pipeline.run()
