from datetime import datetime
import time

from growser.app import app, bigquery
from growser.services.bigquery import PersistQueryToTable, ExportTableToCSV,\
    DeleteTable


class BlockingJobPipeline(object):
    def __init__(self, service, block_duration: int=5):
        self.service = service
        self.block_duration = block_duration
        self.jobs = []

    def add(self, job, *args, **kwargs):
        self.jobs.append((job, args, kwargs))

    def execute(self):
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
    SELECT type, org_id, repo_id, repo_name, actor_login, actor_id, created_at
    FROM [githubarchive:month.{}]
    WHERE type IN ('WatchEvent', 'ForkEvent')
    """

    pipeline = BlockingJobPipeline(bigquery)
    pipeline.add(PersistQueryToTable, query.format(date), export_table)
    pipeline.add(ExportTableToCSV, export_table, export_path)
    pipeline.add(DeleteTable, export_table)
    pipeline.execute()


def download_events():
    """Download event archives from Google Cloud Storage to local storage."""
