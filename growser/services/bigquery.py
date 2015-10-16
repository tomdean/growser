from collections import Sized
from itertools import chain

from apiclient.discovery import build
from apiclient.errors import HttpError
from httplib2 import Http
from oauth2client.client import SignedJwtAssertionCredentials


class BigQueryService(object):
    """Wrapper over google-api-client"""
    def __init__(self, email: str, private_key: str, project_id: str):
        self.project_id = project_id
        auth = SignedJwtAssertionCredentials(
            email, private_key, 'https://www.googleapis.com/auth/bigquery')
        self.api = build('bigquery', 'v2', http=auth.authorize(Http()))

    @property
    def jobs(self):
        return self.api.jobs()

    @property
    def tables(self):
        return self.api.tables()


class BigQueryJob(object):
    num_retries = 5

    def __init__(self, service: BigQueryService):
        self.service = service
        self.project_id = self.service.project_id
        self.id = ''

    @property
    def info(self):
        return self._call('get', jobId=self.id)

    @property
    def is_complete(self):
        return self.info['status']['state'] == 'DONE'

    def query(self, body: dict):
        return self._call('query', body=body)

    def insert(self, body: dict):
        return self._call('insert', body=body)

    def _call(self, api: str, **kwargs):
        method = getattr(self.service.jobs, api)
        response = method(projectId=self.project_id, **kwargs) \
            .execute(num_retries=self.num_retries)

        self.id = response['jobReference']['jobId']

        if 'errors' in response['status']:
            raise JobFailedException(response['status']['errors'])

        return response


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
        return FetchQueryResults(self.service).run(self.id)


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
            response = self._call('getQueryResults', **kwargs)
            has_token = 'pageToken' in response
            if has_token:
                kwargs['pageToken'] = response['pageToken']
            yield response


def _table(project_id, table):
    id1, id2 = table.split('.')
    return {'projectId': project_id, 'datasetId': id1, 'tableId': id2}


class DeleteTable(BigQueryJob):
    def run(self, table: str):
        table = _table(self.project_id, table)
        try:
            self.service.tables.delete(**table).execute()
        except HttpError:
            pass


class PersistQueryToTable(BigQueryJob):
    """Execute a query and save the results to a table."""
    def run(self, query: str, destination: str):
        DeleteTable(self.service).run(destination)
        body = {
            'configuration': {
                'query': {
                    'query': query,
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
    def __init__(self, responses):
        self._responses = responses
        self._first = next(self._responses)
        self.fields = [f['name'] for f in self._first['schema']['fields']]
        self.total_rows = int(self._first['totalRows'])

    def _as_tuple(self, row):
        return list(map(lambda x: x['v'], row['f']))

    def _as_dict(self, row):
        return dict(zip(self.fields, self._as_tuple(row)))

    def rows(self, as_dict: bool=False):
        for response in chain([self._first], self._responses):
            transform = self._as_dict if as_dict else self._as_tuple
            yield from (transform(row) for row in response['rows'])

    def __len__(self):
        return self.total_rows


class JobNotCompleteException(Exception):
    pass


class JobFailedException(Exception):
    pass
