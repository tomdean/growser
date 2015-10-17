from datetime import datetime
import time

from growser.app import app, bigquery
from growser.services.bigquery import PersistQueryToTable, ExportTableToCSV, \
    DeleteTable
from growser.services.storage import DownloadFile, DeleteFile, \
    FindFilesMatchingPrefix


class BlockingJobPipeline(object):
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


def export_monthly_events_to_csv(date: datetime):
    """Export [watch, fork] events from Google BigQuery to Cloud Storage."""
    date = date.strftime('%Y%m')
    app.logger.debug('Monthly events for {}'.format(date))

    export_table = app.config.get('EVENTS_EXPORT_TABLE')
    export_path = app.config.get('EVENTS_EXPORT_PATH').format(date)
    query = """
        SELECT
            type,
            org_id,
            repo_id,
            repo_name,
            actor_login,
            actor_id,
            created_at
        FROM [githubarchive:month.{}]
        WHERE type IN ('WatchEvent', 'ForkEvent')
    """

    pipeline = BlockingJobPipeline(bigquery)
    pipeline.add(PersistQueryToTable, query.format(date), export_table)
    pipeline.add(ExportTableToCSV, export_table, export_path)
    pipeline.add(DeleteTable, export_table)
    pipeline.run()


def download_csv_files(service):
    bucket = app.config.get('GOOGLE_STORAGE_BUCKET')
    archives = FindFilesMatchingPrefix(service).run(bucket, 'events/')
    app.logger.debug("Found {} archives".format(len(archives)))
    for archive in archives:
        filesize = round(int(archive['size'])/(1024*1024), 2)
        app.logger.debug("Downloading {} ({}MB)".format(
            archive['name'], filesize))
        DownloadFile(service).run(bucket, archive['name'], "data/events")
        DeleteFile(service).run(bucket, archive['name'])

