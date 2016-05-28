from datetime import date, timedelta
import os

from dateutil import parser

from growser.app import app, bigquery, db, log, storage
from growser.cmdr import Handles, Command
from growser.google import DownloadBucketPath
from growser.tasks_old.events import BatchManager, export_daily_events_to_csv


class ProcessGithubArchive(Command):
    def __init__(self, date):
        self.date = date


class ProcessGithubArchiveHandler(Handles[ProcessGithubArchive]):
    def handle(self, cmd: ProcessGithubArchive):
        """Download the prior days event data from Github Archive"""
        cmd.date = parser.parse(cmd.date).date()

        today = date.today()
        if not cmd.date:
            cmd.date = today - timedelta(days=1)

        if cmd.date >= today:
            raise ValueError('Date must occur prior to current date')

        log.info("Update Github Archive for {}".format(cmd.date))

        bucket_path = "events"
        bucket = app.config.get('BIGQUERY_EXPORT_BUCKET')
        import_path = app.config.get('BIGQUERY_IMPORT_PATH')
        local_path = os.path.join(import_path, bucket_path)

        # Export the events from BigQuery to Google Cloud Storage
        export_daily_events_to_csv(
            bigquery, cmd.date.year, cmd.date.month, cmd.date.day)

        # Download
        filenames = DownloadBucketPath(storage) \
            .run(bucket, bucket_path, local_path)

        batch = BatchManager(db.engine, 'data/csv/repos.csv', 'data/csv/logins.csv')
        for filename in filenames:
            batch.process_batch(filename)
