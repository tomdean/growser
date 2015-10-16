from datetime import datetime
import time

from growser.app import app, bigquery
from growser.services.bigquery import PersistQueryToTable, ExportTableToCSV,\
    DeleteTable


class block_until_complete:
    def __init__(self, job):
        self.job = job

    def __enter__(self):
        return self.job

    def __exit__(self, *exc):
        while not self.job.is_complete:
            app.logger.info('Job not complete, waiting...')
            time.sleep(5)


def monthly_events(date: datetime):
    date = date.strftime('%Y%m')

    app.logger.debug('Monthly events for {}'.format(date))

    export_table = app.config.get('EVENTS_EXPORT_TABLE')
    export_path = app.config.get('EVENTS_EXPORT_PATH').format(date)
    query = """
    SELECT type, org_id, repo_id, repo_name, actor_login, actor_id, created_at
    FROM [githubarchive:month.{}]
    WHERE type IN ('WatchEvent', 'ForkEvent')
    """

    with block_until_complete(PersistQueryToTable(bigquery)) as job:
        app.logger.debug('Running PersistQueryToTable')
        job.run(query.format(date), export_table)

    with block_until_complete(ExportTableToCSV(bigquery)) as job:
        app.logger.debug('Running ExportTableToCSV')
        job.run(export_table, export_path)

    app.logger.debug("Running DeleteTable")
    DeleteTable(bigquery).run(export_table)
