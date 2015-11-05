from datetime import datetime
import time

from growser.app import app, bigquery
from growser.services.bigquery import PersistQueryToTable, ExportTableToCSV, \
    DeleteTable
from growser.services.storage import DownloadFile, DeleteFile, \
    FindFilesMatchingPrefix


class BlockingJobPipeline(object):
    """Run two or more jobs in serial order."""
    def __init__(self, service, block_duration: int=5):
        self.service = service
        self.block_duration = block_duration
        self.jobs = []

    def add(self, job, *args, **kwargs):
        self.jobs.append((job, args, kwargs))

    def run(self):
        for job_class, args, kwargs in self.jobs:
            app.logger.debug("Running job " + job_class.__name__)
            job = job_class(self.service)
            job.run(*args, **kwargs)
            while not job.is_complete:
                app.logger.info("Job not complete, waiting...")
                time.sleep(self.block_duration)


def export_monthly_events_to_csv(year: int, month: int):
    _export_query_to_csv("month", year, month)


def export_yearly_events_to_csv(year: int):
    if datetime.now().year == year:
        raise Exception('Archive not available for current year')
    _export_query_to_csv("year", year)


def _export_query_to_csv(table: str, year: int, month: int=None):
    """Export [watch, fork] events from Google BigQuery to Cloud Storage."""
    month = str(month).zfill(2) if month else ''
    for_date = '{}{}'.format(year, month)
    app.logger.debug('Events for {}'.format(for_date))
    export_table = app.config.get('EVENTS_EXPORT_TABLE')
    export_path = app.config.get('EVENTS_EXPORT_PATH').format(date=for_date)

    if year >= 2015:
        query = """
            SELECT
                CASE WHEN type = 'WatchEvent' THEN 1 ELSE 2 END AS type,
                org_id,
                repo_id,
                repo_name AS repo,
                actor_login AS login,
                actor_id,
                TIMESTAMP_TO_USEC(created_at) AS created_at
            FROM [githubarchive:{table}.{date}]
            WHERE type IN ('WatchEvent', 'ForkEvent')
        """
    else:
        query = """
            SELECT
                CASE WHEN type = 'WatchEvent' THEN 1 ELSE 2 END AS type,
                repository_organization AS org,
                (repository_owner + '/' + repository_name) AS repo,
                actor AS login,
                PARSE_UTC_USEC(created_at) AS created_at
            FROM [githubarchive:{table}.{date}]
            WHERE type IN ('WatchEvent', 'ForkEvent')
              AND repository_owner IS NOT NULL
              AND repository_name IS NOT NULL
        """

    query = query.format(table=table, date=for_date)

    pipeline = BlockingJobPipeline(bigquery)
    pipeline.add(PersistQueryToTable, query, export_table)
    pipeline.add(ExportTableToCSV, export_table, export_path)
    pipeline.add(DeleteTable, export_table)
    pipeline.run()


def download_csv_files(service):
    bucket = app.config.get('GOOGLE_STORAGE_BUCKET')
    archives = FindFilesMatchingPrefix(service).run(bucket, 'events/')
    app.logger.debug("Found {} archives".format(len(archives)))
    for file in archives:
        size = round(int(file['size'])/(1024*1024), 2)
        app.logger.debug("Downloading {} ({}MB)".format(file['name'], size))
        DownloadFile(service).run(bucket, file['name'], "data/events")
        DeleteFile(service).run(bucket, file['name'])
