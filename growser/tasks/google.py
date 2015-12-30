from datetime import datetime
import time
import os

from growser.app import app, log
from growser.services.bigquery import BigQueryService, PersistQueryToTable, \
    ExportTableToCSV, DeleteTable
from growser.services.storage import CloudStorageService, DownloadFile, \
    DeleteFile, FindFilesMatchingPrefix


class BlockingJobPipeline(object):
    """Run a list of jobs in order."""
    def __init__(self, service, block_duration: int=5):
        self.service = service
        self.block_duration = block_duration
        self.jobs = []

    def add(self, job, *args, **kwargs):
        self.jobs.append((job, args, kwargs))

    def run(self):
        for job_class, args, kwargs in self.jobs:
            log.debug("Running job " + job_class.__name__)
            job = job_class(self.service)
            job.run(*args, **kwargs)
            while not job.is_complete:
                log.info("Job not complete, waiting...")
                time.sleep(self.block_duration)


def export_monthly_events_to_csv(service: BigQueryService,
                                 year: int, month: int):
    _export_query_to_csv(service, "month", year, month)


def export_yearly_events_to_csv(service: BigQueryService, year: int):
    if datetime.now().year == year:
        raise Exception('Archive not available for current year')
    _export_query_to_csv(service, "year", year)


def _export_query_to_csv(service: BigQueryService, table: str,
                         year: int, month: int=None):
    """Export [watch, fork] events from Google BigQuery to Cloud Storage."""
    month = str(month).zfill(2) if month else ''  # Pad with leading 0
    for_date = '{}{}'.format(year, month)
    log.debug('Events for {}'.format(for_date))
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

    export_table = app.config.get('BIG_QUERY_EXPORT_TABLE')
    export_path = app.config.get('BIG_QUERY_EXPORT_PATH').format(date=for_date)
    query = query.format(table=table, date=for_date)

    pipeline = BlockingJobPipeline(service)
    pipeline.add(PersistQueryToTable, query, export_table)
    pipeline.add(ExportTableToCSV, export_table, export_path)
    pipeline.add(DeleteTable, export_table)
    pipeline.run()


def download_csv_files(service: CloudStorageService):
    """Download all files in the export folder and delete them."""
    export_path = app.config.get('BIG_QUERY_EXPORT_PATH').replace("gs://", "")
    bucket, path = os.path.dirname(export_path).split("/", 1)

    archives = FindFilesMatchingPrefix(service).run(bucket, path)
    log.debug("Found {} archives".format(len(archives)))
    for file in archives:
        size = round(int(file['size'])/(1024*1024), 2)
        log.debug("Downloading {} ({}MB)".format(file['name'], size))
        DownloadFile(service).run(bucket, file['name'],
                                  app.config.get('LOCAL_IMPORT_PATH'))
        DeleteFile(service).run(bucket, file['name'])
