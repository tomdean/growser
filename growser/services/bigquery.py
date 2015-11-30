from collections import Sized
from itertools import chain

from growser.services.google import BaseJob, HttpError


class BigQueryJob(BaseJob):
    num_retries = 5

    @property
    def info(self):
        if not self.id:
            raise JobNotCompleteException('Job not complete')
        return self._call('get', jobId=self.id)

    @property
    def is_complete(self):
        return self.info['status']['state'] == 'DONE'

    def query(self, body: dict):
        return self._call('query', body=body)

    def insert(self, body: dict):
        return self._call('insert', body=body)

    def _call(self, api: str, **kwargs):
        method = getattr(self.api.jobs, api)
        rv = method(projectId=self.project_id, **kwargs) \
            .execute(num_retries=self.num_retries)
        if 'jobReference' in rv:
            self.id = rv['jobReference']['jobId']
        if 'errors' in rv['status']:
            raise JobFailedException(rv['status']['errors'])
        return rv


class ExecuteQuery(BigQueryJob):
    """Execute a query and immediately return the results."""
    def run(self, query: str):
        results = self.query({'query': query})
        return QueryResult(iter([results]))


class ExecuteAsyncQuery(BigQueryJob):
    def run(self, query: str):
        body = {
            'configuration': {
                'query': {
                    'query': query,
                    'priority': 'BATCH'
                }
            }
        }
        return self.insert(body)

    def results(self):
        return FetchQueryResults(self.api).run(self.id)


class FetchQueryResults(BigQueryJob):
    def run(self, job_id: str):
        self.id = job_id
        if not self.is_complete:
            raise JobNotCompleteException('Job is not complete')
        return QueryResult(self.pages())

    def pages(self):
        kwargs = {'jobId': self.id}
        has_token = True
        while has_token:
            rv = self._call('getQueryResults', **kwargs)
            has_token = 'pageToken' in rv
            if has_token:
                kwargs['pageToken'] = rv['pageToken']
            yield rv


class DeleteTable(BigQueryJob):
    def run(self, table: str):
        try:
            self.api.tables.delete(
                **_table(self.project_id, table)).execute()
        except HttpError:
            return False
        return True

    @property
    def is_complete(self):
        """API has empty response, assume true."""
        return True


class PersistQueryToTable(BigQueryJob):
    """Execute a query and save the results to a table."""
    def run(self, query: str, destination: str):
        DeleteTable(self.api).run(destination)
        body = {
            'configuration': {
                'query': {
                    'query': query,
                    'allowLargeResults': True,
                    'destinationTable': _table(self.project_id, destination)
                }
            }
        }
        return self.insert(body)


class ExportTableToCSV(BigQueryJob):
    """Export a table to Google Cloud Storage as compressed CSV files."""
    def run(self, source: str, destination: str):
        body = {
            'configuration': {
                'extract': {
                    'sourceTable': _table(self.project_id, source),
                    'destinationUris': [destination],
                    'destinationFormat': 'CSV',
                    'compression': 'GZIP'
                }
            }
        }
        return self.insert(body)


class QueryResult(Sized):
    def __init__(self, pages):
        self._pages = pages
        self._first = next(self._pages)
        self.fields = [f['name'] for f in self._first['schema']['fields']]
        self.total_rows = int(self._first['totalRows'])

    def rows(self, as_dict: bool=False):
        def to_tuple(row):
            return list(map(lambda x: x['v'], row['f']))

        def to_dict(row):
            return dict(zip(self.fields, to_tuple(row)))

        for response in chain([self._first], self._pages):
            transform = to_dict if as_dict else to_tuple
            yield from (transform(row) for row in response['rows'])

    def __len__(self):
        return self.total_rows


class JobNotCompleteException(Exception):
    pass


class JobFailedException(Exception):
    pass


def _table(project_id, table):
    id1, id2 = table.split('.')
    return {'projectId': project_id, 'datasetId': id1, 'tableId': id2}
